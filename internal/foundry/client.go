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
	u, err := url.Parse(strings.TrimSpace(foundryURL))
	if err != nil {
		return nil, fmt.Errorf("parse Foundry URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("Foundry URL must include scheme and host (got %q)", foundryURL)
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
		q.Set("branchId", branch)
	}
	q.Set("format", "CSV")

	u := c.resolve(fmt.Sprintf("/api/v1/datasets/%s/readTable", url.PathEscape(datasetRID)))
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

// UploadFile uploads file bytes to a transaction path.
func (c *Client) UploadFile(ctx context.Context, datasetRID, txnID, filePath string, contentType string, b []byte) error {
	escaped := escapeURLPath(filePath)
	u := c.resolve(fmt.Sprintf(
		"/api/v1/datasets/%s/transactions/%s/files/%s",
		url.PathEscape(datasetRID),
		url.PathEscape(txnID),
		escaped,
	))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader(b))
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
