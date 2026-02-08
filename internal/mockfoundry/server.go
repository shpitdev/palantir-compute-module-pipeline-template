package mockfoundry

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Call records a request made to the mock service.
type Call struct {
	Method string
	Path   string
}

// Upload records a file upload into a dataset transaction.
type Upload struct {
	DatasetRID string
	TxnID      string
	FilePath   string
	Bytes      []byte
}

// Server implements a minimal "Foundry-like" dataset API surface.
type Server struct {
	inputDir  string
	uploadDir string

	mu      sync.Mutex
	calls   []Call
	uploads []Upload

	expectedAuthorization string

	nextTxn int
	txns    map[string]txnState

	// heads stores the last committed dataset contents per dataset RID.
	// This allows read-after-write flows via the same readTable endpoint.
	heads map[string][]byte
}

type txnState struct {
	datasetRID string
	branch     string
	committed  bool

	txType    string
	createdAt time.Time
	closedAt  *time.Time

	// files are staged uploads for the transaction keyed by file path.
	files map[string][]byte
}

// New constructs a new mock server.
func New(inputDir, uploadDir string) *Server {
	return &Server{
		inputDir:  inputDir,
		uploadDir: uploadDir,
		nextTxn:   1,
		txns:      make(map[string]txnState),
		heads:     make(map[string][]byte),
	}
}

// RequireBearerToken enforces that requests include an Authorization header matching the token.
// If token is empty, authorization is not enforced.
func (s *Server) RequireBearerToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token = strings.TrimSpace(token)
	if token == "" {
		s.expectedAuthorization = ""
		return
	}
	s.expectedAuthorization = "Bearer " + token
}

// Handler returns an http.Handler that serves the mock API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/__debug/health", s.handleDebugHealth)
	mux.HandleFunc("/__debug/calls", s.handleDebugCalls)
	mux.HandleFunc("/__debug/uploads", s.handleDebugUploads)
	mux.HandleFunc("/api/v1/datasets/", s.handleV1Datasets)
	mux.HandleFunc("/api/v2/datasets/", s.handleV2Datasets)
	return mux
}

func (s *Server) handleDebugHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleDebugCalls(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Calls())
}

type debugUpload struct {
	DatasetRID string `json:"datasetRid"`
	TxnID      string `json:"txnId"`
	FilePath   string `json:"filePath"`
	SizeBytes  int    `json:"sizeBytes"`
	SHA256Hex  string `json:"sha256Hex"`
}

func (s *Server) handleDebugUploads(w http.ResponseWriter, _ *http.Request) {
	raw := s.Uploads()
	out := make([]debugUpload, 0, len(raw))
	for _, u := range raw {
		sum := sha256.Sum256(u.Bytes)
		out = append(out, debugUpload{
			DatasetRID: u.DatasetRID,
			TxnID:      u.TxnID,
			FilePath:   u.FilePath,
			SizeBytes:  len(u.Bytes),
			SHA256Hex:  hex.EncodeToString(sum[:]),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// Calls returns a snapshot of calls made to the server.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	copy(out, s.calls)
	return out
}

// Uploads returns a snapshot of uploads made to the server.
func (s *Server) Uploads() []Upload {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Upload, len(s.uploads))
	copy(out, s.uploads)
	return out
}

func (s *Server) recordCall(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, Call{Method: r.Method, Path: r.URL.Path})
}

type apiError struct {
	ErrorCode       string         `json:"errorCode"`
	ErrorName       string         `json:"errorName"`
	ErrorInstanceID string         `json:"errorInstanceId"`
	Parameters      map[string]any `json:"parameters,omitempty"`
}

func writeAPIError(w http.ResponseWriter, statusCode int, name string, code string, params map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(apiError{
		ErrorCode:       code,
		ErrorName:       name,
		ErrorInstanceID: newErrorInstanceID(),
		Parameters:      params,
	})
}

func newErrorInstanceID() string {
	// Foundry APIs follow Conjure error envelopes which include a stable instance id.
	// Use UUIDv4 format. Cryptographic strength isn't important here, but uniqueness is.
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3],
		b[4], b[5],
		b[6], b[7],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	)
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	expected := s.expectedAuthorization
	s.mu.Unlock()

	if expected == "" {
		return true
	}
	got := strings.TrimSpace(r.Header.Get("Authorization"))
	if got == "" {
		writeAPIError(w, http.StatusUnauthorized, "MissingCredentials", "UNAUTHORIZED", nil)
		return false
	}
	if got != expected {
		writeAPIError(w, http.StatusUnauthorized, "Default:Unauthorized", "UNAUTHORIZED", nil)
		return false
	}
	return true
}

func (s *Server) handleV1Datasets(w http.ResponseWriter, r *http.Request) {
	s.recordCall(r)
	if !s.authorize(w, r) {
		return
	}

	// /api/v1/datasets/{rid}/readTable
	// /api/v1/datasets/{rid}/transactions/{txn}/files/{path...}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/datasets/")
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	rid := parts[0]
	if !isSafeToken(rid) {
		writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
			"datasetRid": rid,
		})
		return
	}

	if len(parts) == 2 && parts[1] == "readTable" {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.serveReadTableCSV(w, r, rid)
		return
	}

	if len(parts) >= 5 && parts[1] == "transactions" && parts[3] == "files" {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		txnID := parts[2]
		if !isSafeToken(txnID) {
			writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
				"transactionRid": txnID,
			})
			return
		}
		filePath := strings.Join(parts[4:], "/")
		if !isSafeFilePath(filePath) {
			writeAPIError(w, http.StatusBadRequest, "InvalidFilePath", "INVALID_ARGUMENT", map[string]any{
				"filePath": filePath,
			})
			return
		}
		s.handleUpload(w, r, rid, txnID, filePath)
		return
	}

	http.NotFound(w, r)
}

func (s *Server) handleV2Datasets(w http.ResponseWriter, r *http.Request) {
	s.recordCall(r)
	if !s.authorize(w, r) {
		return
	}

	// /api/v2/datasets/{rid}/transactions
	// /api/v2/datasets/{rid}/transactions/{txn}/commit
	rest := strings.TrimPrefix(r.URL.Path, "/api/v2/datasets/")
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	rid := parts[0]
	if !isSafeToken(rid) {
		writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
			"datasetRid": rid,
		})
		return
	}

	if len(parts) == 2 && parts[1] == "transactions" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleCreateTransaction(w, r, rid)
		return
	}

	if len(parts) == 4 && parts[1] == "transactions" && parts[3] == "commit" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		txnID := parts[2]
		if !isSafeToken(txnID) {
			writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
				"transactionRid": txnID,
			})
			return
		}
		s.handleCommit(w, r, rid, txnID)
		return
	}

	http.NotFound(w, r)
}

func (s *Server) serveReadTableCSV(w http.ResponseWriter, r *http.Request, datasetRID string) {
	branchID := strings.TrimSpace(r.URL.Query().Get("branchId"))
	branchName := strings.TrimSpace(r.URL.Query().Get("branchName"))
	if branchID == "" && branchName != "" {
		branchID = branchName
	}

	// Prefer the last committed dataset head (API read-after-write), if present.
	s.mu.Lock()
	head, ok := s.heads[datasetRID]
	s.mu.Unlock()
	if ok && len(head) > 0 {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(head)
		return
	}

	// If the server restarted, allow the committed head to be reloaded from disk.
	committedPath := s.committedTablePath(datasetRID)
	if b, err := os.ReadFile(committedPath); err == nil && len(b) > 0 {
		s.mu.Lock()
		// Cache for future reads.
		s.heads[datasetRID] = b
		s.mu.Unlock()

		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(b)
		return
	}

	p := filepath.Join(s.inputDir, datasetRID+".csv")
	b, err := os.ReadFile(p)
	if err != nil {
		writeAPIError(w, http.StatusNotFound, "SchemaNotFound", "NOT_FOUND", map[string]any{
			"datasetRid":     datasetRID,
			"branchId":       branchID,
			"transactionRid": "",
		})
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	_, _ = w.Write(b)
}

type createTxnReq struct {
	Branch          string `json:"branch,omitempty"`
	TransactionType string `json:"transactionType,omitempty"`
}

type transactionResp struct {
	RID             string  `json:"rid"`
	TransactionType string  `json:"transactionType"`
	Status          string  `json:"status"`
	CreatedTime     string  `json:"createdTime"`
	ClosedTime      *string `json:"closedTime,omitempty"`
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request, datasetRID string) {
	var req createTxnReq
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		if len(b) > 0 {
			_ = json.Unmarshal(b, &req)
		}
	}

	s.mu.Lock()
	branch := strings.TrimSpace(r.URL.Query().Get("branchName"))
	if branch == "" {
		branch = strings.TrimSpace(r.URL.Query().Get("branchId"))
	}
	if branch == "" {
		branch = strings.TrimSpace(req.Branch)
	}
	if branch == "" {
		branch = "master"
	}

	for _, t := range s.txns {
		if t.datasetRID == datasetRID && !t.committed && strings.TrimSpace(t.branch) == branch {
			s.mu.Unlock()
			writeAPIError(w, http.StatusConflict, "OpenTransactionAlreadyExists", "CONFLICT", map[string]any{
				"datasetRid": datasetRID,
				"branchName": branch,
			})
			return
		}
	}

	txType := strings.TrimSpace(req.TransactionType)
	if txType == "" {
		txType = "SNAPSHOT"
	}

	createdAt := time.Now().UTC()
	txnID := fmt.Sprintf("ri.foundry.main.transaction.txn-%06d", s.nextTxn)
	s.nextTxn++
	s.txns[txnID] = txnState{
		datasetRID: datasetRID,
		branch:     branch,
		txType:     txType,
		createdAt:  createdAt,
		files:      make(map[string][]byte),
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(transactionResp{
		RID:             txnID,
		TransactionType: txType,
		Status:          "OPEN",
		CreatedTime:     createdAt.Format(time.RFC3339Nano),
	})
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request, datasetRID, txnID, filePath string) {
	s.mu.Lock()
	txn, ok := s.txns[txnID]
	s.mu.Unlock()
	if !ok || txn.datasetRID != datasetRID {
		writeAPIError(w, http.StatusNotFound, "TransactionNotFound", "NOT_FOUND", map[string]any{
			"datasetRid":     datasetRID,
			"transactionRid": txnID,
		})
		return
	}
	if txn.committed {
		writeAPIError(w, http.StatusBadRequest, "TransactionNotOpen", "INVALID_ARGUMENT", map[string]any{
			"datasetRid":        datasetRID,
			"transactionRid":    txnID,
			"transactionStatus": "COMMITTED",
		})
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
			"message": "read body",
		})
		return
	}

	dst := filepath.Join(s.uploadDir, datasetRID, txnID, filepath.FromSlash(filePath))
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "Default:Internal", "INTERNAL", map[string]any{
			"message": "mkdir upload dir",
		})
		return
	}
	if err := os.WriteFile(dst, b, 0644); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "Default:Internal", "INTERNAL", map[string]any{
			"message": "write upload",
		})
		return
	}

	s.mu.Lock()
	// Re-check state to avoid accepting uploads after commit in racy scenarios.
	txn, ok = s.txns[txnID]
	if !ok || txn.datasetRID != datasetRID {
		s.mu.Unlock()
		writeAPIError(w, http.StatusNotFound, "TransactionNotFound", "NOT_FOUND", map[string]any{
			"datasetRid":     datasetRID,
			"transactionRid": txnID,
		})
		return
	}
	if txn.committed {
		s.mu.Unlock()
		writeAPIError(w, http.StatusBadRequest, "TransactionNotOpen", "INVALID_ARGUMENT", map[string]any{
			"datasetRid":        datasetRID,
			"transactionRid":    txnID,
			"transactionStatus": "COMMITTED",
		})
		return
	}
	if txn.files == nil {
		txn.files = make(map[string][]byte)
	}
	txn.files[filePath] = b
	s.txns[txnID] = txn

	s.uploads = append(s.uploads, Upload{
		DatasetRID: datasetRID,
		TxnID:      txnID,
		FilePath:   filePath,
		Bytes:      b,
	})
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	updated := time.Now().UTC().Format(time.RFC3339Nano)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"path":           filePath,
		"transactionRid": txnID,
		"sizeBytes":      fmt.Sprintf("%d", len(b)),
		"updatedTime":    updated,
	})
}

func (s *Server) handleCommit(w http.ResponseWriter, _ *http.Request, datasetRID string, txnID string) {
	s.mu.Lock()
	txn, ok := s.txns[txnID]
	if !ok || txn.datasetRID != datasetRID {
		s.mu.Unlock()
		writeAPIError(w, http.StatusNotFound, "TransactionNotFound", "NOT_FOUND", map[string]any{
			"datasetRid":     datasetRID,
			"transactionRid": txnID,
		})
		return
	}
	if txn.committed {
		s.mu.Unlock()
		writeAPIError(w, http.StatusBadRequest, "TransactionNotOpen", "INVALID_ARGUMENT", map[string]any{
			"datasetRid":        datasetRID,
			"transactionRid":    txnID,
			"transactionStatus": "COMMITTED",
		})
		return
	}
	if len(txn.files) == 0 {
		s.mu.Unlock()
		writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
			"message":        "transaction has no uploaded files",
			"datasetRid":     datasetRID,
			"transactionRid": txnID,
		})
		return
	}
	if len(txn.files) != 1 {
		s.mu.Unlock()
		writeAPIError(w, http.StatusBadRequest, "Conjure:InvalidArgument", "INVALID_ARGUMENT", map[string]any{
			"message":        "transaction has multiple uploaded files",
			"datasetRid":     datasetRID,
			"transactionRid": txnID,
		})
		return
	}

	var head []byte
	for _, b := range txn.files {
		head = append([]byte(nil), b...)
		break
	}
	s.mu.Unlock()

	// Persist a "dataset head" so downstream consumers can read the committed state via readTable.
	committedPath := s.committedTablePath(datasetRID)
	if err := os.MkdirAll(filepath.Dir(committedPath), 0755); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "Default:Internal", "INTERNAL", map[string]any{
			"message": "mkdir committed dir",
		})
		return
	}
	if err := os.WriteFile(committedPath, head, 0644); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "Default:Internal", "INTERNAL", map[string]any{
			"message": "write committed head",
		})
		return
	}

	s.mu.Lock()
	// Re-check state after filesystem writes.
	txn, ok = s.txns[txnID]
	if !ok || txn.datasetRID != datasetRID {
		s.mu.Unlock()
		writeAPIError(w, http.StatusNotFound, "TransactionNotFound", "NOT_FOUND", map[string]any{
			"datasetRid":     datasetRID,
			"transactionRid": txnID,
		})
		return
	}
	if txn.committed {
		s.mu.Unlock()
		writeAPIError(w, http.StatusBadRequest, "TransactionNotOpen", "INVALID_ARGUMENT", map[string]any{
			"datasetRid":        datasetRID,
			"transactionRid":    txnID,
			"transactionStatus": "COMMITTED",
		})
		return
	}
	closedAt := time.Now().UTC()
	txn.committed = true
	txn.closedAt = &closedAt
	s.txns[txnID] = txn
	s.heads[datasetRID] = head
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	createdTime := txn.createdAt.UTC().Format(time.RFC3339Nano)
	var closedTime *string
	if txn.closedAt != nil {
		s := txn.closedAt.UTC().Format(time.RFC3339Nano)
		closedTime = &s
	}
	_ = json.NewEncoder(w).Encode(transactionResp{
		RID:             txnID,
		TransactionType: txn.txType,
		Status:          "COMMITTED",
		CreatedTime:     createdTime,
		ClosedTime:      closedTime,
	})
}

func (s *Server) committedTablePath(datasetRID string) string {
	// Keep this stable and human-inspectable for local harness use.
	return filepath.Join(s.uploadDir, datasetRID, "_committed", "readTable.csv")
}

func isSafeToken(s string) bool {
	if s == "" {
		return false
	}
	return !strings.ContainsAny(s, "/\\")
}

func isSafeFilePath(p string) bool {
	if p == "" {
		return false
	}
	if strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return false
	}
	parts := strings.Split(p, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
