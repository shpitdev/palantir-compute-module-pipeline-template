package foundry

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// Client is a minimal HTTP client for the dataset endpoints used by this module.
//
// Note: This is intentionally minimal to support local harness + smoke tests.
type Client struct {
	apiBaseURL    *url.URL
	streamBaseURL *url.URL
	token         string
	http          *http.Client
}

type branchResponse struct {
	Name           string `json:"name"`
	BranchID       string `json:"branchId"`
	TransactionRID string `json:"transactionRid"`
}

// GetBranchTransactionRID returns the most recent OPEN or COMMITTED transaction on the branch.
// This value can be used to pin a readTable request to a deterministic snapshot.
func (c *Client) GetBranchTransactionRID(ctx context.Context, datasetRID, branch string) (string, error) {
	datasetRID = strings.TrimSpace(datasetRID)
	branch = strings.TrimSpace(branch)
	if datasetRID == "" {
		return "", fmt.Errorf("dataset rid is required")
	}
	if branch == "" {
		branch = "master"
	}

	u := c.resolveAPI(fmt.Sprintf(
		"v2/datasets/%s/branches/%s",
		url.PathEscape(datasetRID),
		url.PathEscape(branch),
	))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", newHTTPError("getBranch", resp, b)
	}

	var out branchResponse
	if err := json.Unmarshal(b, &out); err != nil {
		return "", fmt.Errorf("parse get branch response: %w", err)
	}
	return strings.TrimSpace(out.TransactionRID), nil
}

// NewClient constructs a client for Foundry service base URLs.
//
// apiGatewayURL should look like "https://<stack>.palantirfoundry.com/api".
// streamProxyURL should look like "https://<stack>.palantirfoundry.com/stream-proxy/api".
//
// defaultCAPath is optional and, when provided, will be used as the trust store for TLS.
func NewClient(apiGatewayURL, streamProxyURL, token, defaultCAPath string) (*Client, error) {
	apiBase, err := parseBaseURL(apiGatewayURL, "api gateway")
	if err != nil {
		return nil, err
	}
	streamBase, err := parseBaseURL(streamProxyURL, "stream-proxy")
	if err != nil {
		return nil, err
	}

	hc, err := newHTTPClient(defaultCAPath)
	if err != nil {
		return nil, err
	}

	return &Client{
		apiBaseURL:    apiBase,
		streamBaseURL: streamBase,
		token:         strings.TrimSpace(token),
		http:          hc,
	}, nil
}

func parseBaseURL(raw string, name string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("%s base URL is required", name)
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s base URL: %w", name, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("%s base URL must include a host (got %q)", name, raw)
	}
	// Ensure the base path ends with a slash so ResolveReference treats it as a directory.
	u.Path = strings.TrimRight(u.Path, "/") + "/"
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}

func newHTTPClient(defaultCAPath string) (*http.Client, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if strings.TrimSpace(defaultCAPath) != "" {
		b, err := os.ReadFile(strings.TrimSpace(defaultCAPath))
		if err != nil {
			return nil, fmt.Errorf("read DEFAULT_CA_PATH file: %w", err)
		}
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(b); !ok {
			return nil, fmt.Errorf("parse DEFAULT_CA_PATH PEM: no certs found")
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}, nil
}

// ReadTableCSV reads the dataset as CSV bytes from the (mock) readTable endpoint.
func (c *Client) ReadTableCSV(ctx context.Context, datasetRID, branch string) ([]byte, error) {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		branch = "master"
	}

	// Pin to the most recent transaction for deterministic reads. In practice, Foundry API examples
	// include start/end transaction RIDs; some stacks reject readTable without them.
	txnRID, err := c.GetBranchTransactionRID(ctx, datasetRID, branch)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	// Dataset API v2 uses branchName; branchId is accepted by some older APIs.
	q.Set("branchName", branch)
	if strings.TrimSpace(txnRID) != "" {
		q.Set("startTransactionRid", txnRID)
		q.Set("endTransactionRid", txnRID)
	}
	q.Set("format", "CSV")

	u := c.resolveAPI(fmt.Sprintf("v2/datasets/%s/readTable", url.PathEscape(datasetRID)))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "text/csv")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, newHTTPError("readTable", resp, b)
	}
	return b, nil
}

// ProbeStream checks whether the given RID is accessible as a stream via the stream-proxy API.
//
// Returns:
//   - (true, nil) if stream-proxy responds 2xx
//   - (false, nil) if stream-proxy responds 404 (not a stream / not found)
//   - (false, err) for other non-2xx responses or network errors
func (c *Client) ProbeStream(ctx context.Context, streamRID, branch string) (bool, error) {
	streamRID = strings.TrimSpace(streamRID)
	branch = strings.TrimSpace(branch)
	if streamRID == "" {
		return false, fmt.Errorf("stream rid is required")
	}
	if branch == "" {
		// Foundry examples typically default branches to master when omitted from alias map.
		branch = "master"
	}

	u := c.resolveStream(fmt.Sprintf(
		"streams/%s/branches/%s/records",
		url.PathEscape(streamRID),
		url.PathEscape(branch),
	))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode/100 != 2 {
		return false, newHTTPError("probeStream", resp, rb)
	}
	return true, nil
}

// ReadStreamRecords reads stream records for a stream branch via stream-proxy.
//
// Note: this endpoint returns the full record list in this minimal client.
// In real deployments, streams can be large; callers should treat this as best-effort.
func (c *Client) ReadStreamRecords(ctx context.Context, streamRID, branch string) ([]map[string]any, error) {
	streamRID = strings.TrimSpace(streamRID)
	branch = strings.TrimSpace(branch)
	if streamRID == "" {
		return nil, fmt.Errorf("stream rid is required")
	}
	if branch == "" {
		branch = "master"
	}

	u := c.resolveStream(fmt.Sprintf(
		"streams/%s/branches/%s/records",
		url.PathEscape(streamRID),
		url.PathEscape(branch),
	))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, newHTTPError("readStreamRecords", resp, rb)
	}

	recs, err := parseStreamRecordsResponse(rb)
	if err != nil {
		return nil, fmt.Errorf("parse stream records response: %w", err)
	}
	return recs, nil
}

func parseStreamRecordsResponse(body []byte) ([]map[string]any, error) {
	var top any
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, err
	}

	// Stream-proxy response shapes vary by stack/version.
	// Known patterns include:
	// - [ {..record..}, ... ]
	// - { "records": [ ... ] }
	// - { "values": [ ... ], "nextPageToken": "..." }
	// - { "values": [ {"record": {..}}, ... ] }
	//
	// We keep this permissive and best-effort.
	return extractRecordList(top)
}

func extractRecordList(v any) ([]map[string]any, error) {
	switch t := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(t))
		for _, item := range t {
			m, ok := item.(map[string]any)
			if !ok {
				// Ignore non-object items.
				continue
			}
			out = append(out, m)
		}
		return out, nil
	case map[string]any:
		// Prefer well-known paging keys.
		for _, key := range []string{"records", "values", "data", "items", "result"} {
			if inner, ok := t[key]; ok {
				if recs, err := extractRecordList(inner); err == nil {
					return recs, nil
				}
			}
		}

		// Fallback: pick the first array field that looks like a list of objects.
		for _, inner := range t {
			arr, ok := inner.([]any)
			if !ok {
				continue
			}
			// Heuristic: require at least one object element.
			for _, item := range arr {
				if _, ok := item.(map[string]any); ok {
					return extractRecordList(arr)
				}
			}
		}
		return nil, fmt.Errorf("unexpected json object shape")
	default:
		return nil, fmt.Errorf("unexpected json type %T", v)
	}
}

// PublishStreamJSONRecord publishes one JSON object to a stream branch via stream-proxy.
func (c *Client) PublishStreamJSONRecord(ctx context.Context, streamRID, branch string, record map[string]any) error {
	streamRID = strings.TrimSpace(streamRID)
	branch = strings.TrimSpace(branch)
	if streamRID == "" {
		return fmt.Errorf("stream rid is required")
	}
	if branch == "" {
		branch = "master"
	}
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}

	u := c.resolveStream(fmt.Sprintf(
		"streams/%s/branches/%s/jsonRecord",
		url.PathEscape(streamRID),
		url.PathEscape(branch),
	))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return newHTTPError("publishStreamJSONRecord", resp, rb)
	}
	return nil
}

type createTxnRequest struct {
	TransactionType string `json:"transactionType"`
}

type createTxnResponse struct {
	// Foundry returns a Transaction object with a transaction RID.
	RID string `json:"rid"`

	// Legacy: some mocks may return transactionId.
	TransactionID string `json:"transactionId"`
}

// CreateTransaction creates a dataset transaction and returns the transaction id.
func (c *Client) CreateTransaction(ctx context.Context, datasetRID, branch string) (string, error) {
	body := createTxnRequest{TransactionType: "SNAPSHOT"}
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	u := c.resolveAPI(fmt.Sprintf("v2/datasets/%s/transactions", url.PathEscape(datasetRID)))
	q := url.Values{}
	if strings.TrimSpace(branch) != "" {
		q.Set("branchName", branch)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", newHTTPError("createTransaction", resp, rb)
	}

	var out createTxnResponse
	if err := json.Unmarshal(rb, &out); err != nil {
		return "", fmt.Errorf("parse create transaction response: %w", err)
	}

	txnID := strings.TrimSpace(out.TransactionID)
	if txnID == "" {
		txnID = strings.TrimSpace(out.RID)
	}
	if txnID == "" {
		return "", fmt.Errorf("create transaction response missing rid")
	}
	return txnID, nil
}

type Transaction struct {
	TransactionType string  `json:"transactionType"`
	CreatedTime     string  `json:"createdTime"`
	RID             string  `json:"rid"`
	ClosedTime      *string `json:"closedTime,omitempty"`
	Status          string  `json:"status"`
}

type listTxnsResponse struct {
	Data          []Transaction `json:"data"`
	NextPageToken string        `json:"nextPageToken"`
}

// ListTransactions lists transactions for a dataset.
//
// Note: This endpoint is documented as preview and requires `preview=true`.
func (c *Client) ListTransactions(ctx context.Context, datasetRID string, pageSize int, pageToken string) ([]Transaction, string, error) {
	u := c.resolveAPI(fmt.Sprintf("v2/datasets/%s/transactions", url.PathEscape(datasetRID)))
	q := url.Values{}
	// Required by Foundry docs for this (preview) endpoint.
	q.Set("preview", "true")
	if pageSize > 0 {
		q.Set("pageSize", strconv.Itoa(pageSize))
	}
	if strings.TrimSpace(pageToken) != "" {
		q.Set("pageToken", strings.TrimSpace(pageToken))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode/100 != 2 {
		return nil, "", newHTTPError("listTransactions", resp, rb)
	}

	var out listTxnsResponse
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, "", fmt.Errorf("parse list transactions response: %w", err)
	}
	return out.Data, strings.TrimSpace(out.NextPageToken), nil
}

// FindLatestOpenTransaction returns the RID of the latest OPEN transaction for the dataset.
//
// Foundry documents that ListTransactions returns reverse chronological order, so the first OPEN is the most recent.
func (c *Client) FindLatestOpenTransaction(ctx context.Context, datasetRID string) (string, bool, error) {
	pageToken := ""
	for i := 0; i < 5; i++ {
		txns, next, err := c.ListTransactions(ctx, datasetRID, 100, pageToken)
		if err != nil {
			return "", false, err
		}
		for _, t := range txns {
			if strings.EqualFold(strings.TrimSpace(t.Status), "OPEN") && strings.TrimSpace(t.RID) != "" {
				return strings.TrimSpace(t.RID), true, nil
			}
		}
		if next == "" {
			break
		}
		pageToken = next
	}
	return "", false, nil
}

// UploadFile uploads file bytes to a transaction path.
func (c *Client) UploadFile(ctx context.Context, datasetRID, txnID, filePath string, contentType string, b []byte) error {
	escaped := escapeURLPath(filePath)
	u := c.resolveAPI(fmt.Sprintf(
		"v2/datasets/%s/files/%s/upload",
		url.PathEscape(datasetRID),
		escaped,
	))
	q := url.Values{}
	if strings.TrimSpace(txnID) != "" {
		q.Set("transactionRid", strings.TrimSpace(txnID))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return newHTTPError("uploadFile", resp, rb)
	}
	return nil
}

// CommitTransaction commits a transaction.
func (c *Client) CommitTransaction(ctx context.Context, datasetRID, txnID string) error {
	u := c.resolveAPI(fmt.Sprintf(
		"v2/datasets/%s/transactions/%s/commit",
		url.PathEscape(datasetRID),
		url.PathEscape(txnID),
	))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return newHTTPError("commitTransaction", resp, rb)
	}
	return nil
}

func (c *Client) resolveAPI(relPath string) *url.URL {
	relPath = strings.TrimPrefix(relPath, "/")
	rel := &url.URL{Path: relPath}
	return c.apiBaseURL.ResolveReference(rel)
}

func (c *Client) resolveStream(relPath string) *url.URL {
	relPath = strings.TrimPrefix(relPath, "/")
	rel := &url.URL{Path: relPath}
	return c.streamBaseURL.ResolveReference(rel)
}

func escapeURLPath(p string) string {
	// Preserve "/" separators while escaping each segment.
	cleaned := path.Clean("/" + p)
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." {
		return ""
	}
	parts := strings.Split(cleaned, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
