package mockfoundry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	mux.HandleFunc("/api/v1/datasets/", s.handleV1Datasets)
	mux.HandleFunc("/api/v2/datasets/", s.handleV2Datasets)
	return mux
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

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	expected := s.expectedAuthorization
	s.mu.Unlock()

	if expected == "" {
		return true
	}
	if r.Header.Get("Authorization") != expected {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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
		http.Error(w, "invalid dataset rid", http.StatusBadRequest)
		return
	}

	if len(parts) == 2 && parts[1] == "readTable" {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.serveReadTableCSV(w, r, rid)
		return
	}

	if len(parts) >= 5 && parts[1] == "transactions" && parts[3] == "files" {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		txnID := parts[2]
		if !isSafeToken(txnID) {
			http.Error(w, "invalid transaction id", http.StatusBadRequest)
			return
		}
		filePath := strings.Join(parts[4:], "/")
		if !isSafeFilePath(filePath) {
			http.Error(w, "invalid file path", http.StatusBadRequest)
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
		http.Error(w, "invalid dataset rid", http.StatusBadRequest)
		return
	}

	if len(parts) == 2 && parts[1] == "transactions" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleCreateTransaction(w, r, rid)
		return
	}

	if len(parts) == 4 && parts[1] == "transactions" && parts[3] == "commit" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		txnID := parts[2]
		if !isSafeToken(txnID) {
			http.Error(w, "invalid transaction id", http.StatusBadRequest)
			return
		}
		s.handleCommit(w, r, rid, txnID)
		return
	}

	http.NotFound(w, r)
}

func (s *Server) serveReadTableCSV(w http.ResponseWriter, _ *http.Request, datasetRID string) {
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
		http.Error(w, fmt.Sprintf("read input csv: %v", err), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	_, _ = w.Write(b)
}

type createTxnReq struct {
	Branch string `json:"branch"`
}

type createTxnResp struct {
	TransactionID string `json:"transactionId"`
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
	txnID := fmt.Sprintf("txn-%06d", s.nextTxn)
	s.nextTxn++
	s.txns[txnID] = txnState{
		datasetRID: datasetRID,
		branch:     req.Branch,
		files:      make(map[string][]byte),
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(createTxnResp{TransactionID: txnID})
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request, datasetRID, txnID, filePath string) {
	s.mu.Lock()
	txn, ok := s.txns[txnID]
	s.mu.Unlock()
	if !ok || txn.datasetRID != datasetRID {
		http.Error(w, "unknown transaction", http.StatusNotFound)
		return
	}
	if txn.committed {
		http.Error(w, "transaction already committed", http.StatusConflict)
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	dst := filepath.Join(s.uploadDir, datasetRID, txnID, filepath.FromSlash(filePath))
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		http.Error(w, "mkdir upload dir", http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(dst, b, 0644); err != nil {
		http.Error(w, "write upload", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	// Re-check state to avoid accepting uploads after commit in racy scenarios.
	txn, ok = s.txns[txnID]
	if !ok || txn.datasetRID != datasetRID {
		s.mu.Unlock()
		http.Error(w, "unknown transaction", http.StatusNotFound)
		return
	}
	if txn.committed {
		s.mu.Unlock()
		http.Error(w, "transaction already committed", http.StatusConflict)
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

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCommit(w http.ResponseWriter, _ *http.Request, datasetRID string, txnID string) {
	s.mu.Lock()
	txn, ok := s.txns[txnID]
	if !ok || txn.datasetRID != datasetRID {
		s.mu.Unlock()
		http.Error(w, "unknown transaction", http.StatusNotFound)
		return
	}
	if txn.committed {
		s.mu.Unlock()
		http.Error(w, "transaction already committed", http.StatusConflict)
		return
	}
	if len(txn.files) == 0 {
		s.mu.Unlock()
		http.Error(w, "transaction has no uploaded files", http.StatusBadRequest)
		return
	}
	if len(txn.files) != 1 {
		s.mu.Unlock()
		http.Error(w, "transaction has multiple uploaded files", http.StatusBadRequest)
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
		http.Error(w, "mkdir committed dir", http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(committedPath, head, 0644); err != nil {
		http.Error(w, "write committed head", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	// Re-check state after filesystem writes.
	txn, ok = s.txns[txnID]
	if !ok || txn.datasetRID != datasetRID {
		s.mu.Unlock()
		http.Error(w, "unknown transaction", http.StatusNotFound)
		return
	}
	if txn.committed {
		s.mu.Unlock()
		http.Error(w, "transaction already committed", http.StatusConflict)
		return
	}
	txn.committed = true
	s.txns[txnID] = txn
	s.heads[datasetRID] = head
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"committed"}`))
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
