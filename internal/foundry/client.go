package foundry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// Client is a minimal HTTP client for the dataset endpoints used by this module.
//
// Note: This is intentionally minimal to support local harness + smoke tests.
type Client struct {
	baseURL *url.URL
	token   string
	http    *http.Client
}

// NewClient constructs a client for the Foundry base URL (for example, "https://<stack>.palantirfoundry.com").
func NewClient(foundryURL, token string) (*Client, error) {
	raw := strings.TrimSpace(foundryURL)
	if raw == "" {
		return nil, fmt.Errorf("Foundry URL is required")
	}
	// Foundry docs and examples often treat the stack value as a hostname.
	// Accept either a full URL or a hostname and default to https.
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse Foundry URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("Foundry URL must include a host (got %q)", foundryURL)
	}
	u.Path = strings.TrimRight(u.Path, "/")

	return &Client{
		baseURL: u,
		token:   strings.TrimSpace(token),
		http: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

// ReadTableCSV reads the dataset as CSV bytes from the (mock) readTable endpoint.
func (c *Client) ReadTableCSV(ctx context.Context, datasetRID, branch string) ([]byte, error) {
	q := url.Values{}
	if strings.TrimSpace(branch) != "" {
		// Dataset API v2 uses branchName; branchId is accepted by some older APIs.
		q.Set("branchName", branch)
	}
	q.Set("format", "CSV")

	u := c.resolve(fmt.Sprintf("/api/v2/datasets/%s/readTable", url.PathEscape(datasetRID)))
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

	u := c.resolve(fmt.Sprintf(
		"/stream-proxy/api/streams/%s/branches/%s/records",
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

	u := c.resolve(fmt.Sprintf(
		"/stream-proxy/api/streams/%s/branches/%s/jsonRecord",
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

	u := c.resolve(fmt.Sprintf("/api/v2/datasets/%s/transactions", url.PathEscape(datasetRID)))
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
	u := c.resolve(fmt.Sprintf("/api/v2/datasets/%s/transactions", url.PathEscape(datasetRID)))
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
	u := c.resolve(fmt.Sprintf(
		"/api/v2/datasets/%s/files/%s/upload",
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
	u := c.resolve(fmt.Sprintf(
		"/api/v2/datasets/%s/transactions/%s/commit",
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

func (c *Client) resolve(p string) *url.URL {
	// url.ResolveReference drops query/fragment and handles any base path.
	rel := &url.URL{Path: p}
	return c.baseURL.ResolveReference(rel)
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
