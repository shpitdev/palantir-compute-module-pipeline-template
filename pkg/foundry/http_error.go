package foundry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/redact"
)

// conjureErrorEnvelope is the standard error envelope shape used by Foundry APIs.
// Real Foundry responses may include additional fields; we intentionally ignore them.
type conjureErrorEnvelope struct {
	ErrorCode       string `json:"errorCode"`
	ErrorName       string `json:"errorName"`
	ErrorInstanceID string `json:"errorInstanceId"`
}

// HTTPError is a sanitized summary of a non-2xx Foundry API response.
//
// Important: do not include raw response bodies here (can leak PII/tokens).
type HTTPError struct {
	Op              string
	StatusCode      int
	Status          string
	ErrorName       string
	ErrorCode       string
	ErrorInstanceID string

	// Snippet is a redacted, truncated hint for non-Conjure responses.
	Snippet string
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "foundry http error"
	}
	parts := []string{
		fmt.Sprintf("foundry api error: op=%s status=%s", strings.TrimSpace(e.Op), strings.TrimSpace(e.Status)),
	}
	if strings.TrimSpace(e.ErrorName) != "" {
		parts = append(parts, "errorName="+strings.TrimSpace(e.ErrorName))
	}
	if strings.TrimSpace(e.ErrorCode) != "" {
		parts = append(parts, "errorCode="+strings.TrimSpace(e.ErrorCode))
	}
	if strings.TrimSpace(e.ErrorInstanceID) != "" {
		parts = append(parts, "instance="+strings.TrimSpace(e.ErrorInstanceID))
	}
	if strings.TrimSpace(e.Snippet) != "" {
		parts = append(parts, "body="+strings.TrimSpace(e.Snippet))
	}
	return strings.Join(parts, " ")
}

func newHTTPError(op string, resp *http.Response, body []byte) error {
	h := &HTTPError{
		Op:         op,
		StatusCode: 0,
		Status:     "",
	}
	if resp != nil {
		h.StatusCode = resp.StatusCode
		h.Status = resp.Status
	}

	// Best effort: parse Conjure error envelope.
	var env conjureErrorEnvelope
	if len(body) > 0 && json.Unmarshal(body, &env) == nil {
		h.ErrorName = strings.TrimSpace(env.ErrorName)
		h.ErrorCode = strings.TrimSpace(env.ErrorCode)
		h.ErrorInstanceID = strings.TrimSpace(env.ErrorInstanceID)
		if h.ErrorName != "" || h.ErrorCode != "" || h.ErrorInstanceID != "" {
			return h
		}
	}

	// Fallback: include a small, redacted hint only.
	h.Snippet = redactAndTruncate(body)
	return h
}

func redactAndTruncate(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	// Keep this small: response bodies can contain sensitive data.
	const max = 256
	b := body
	if len(b) > max {
		b = b[:max]
	}
	s := redact.Secrets(string(b))
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(body) > max {
		return s + "..."
	}
	return s
}
