package util

import (
	"regexp"
	"strings"
)

var (
	// Matches "Bearer <token>" (JWTs and opaque tokens). Keep it broad: tokens show up
	// in logs via downstream libraries and HTTP error messages.
	bearerTokenRe = regexp.MustCompile(`(?i)\bBearer\s+[^\s"']+`)

	// Common key=value formats that sometimes leak in error strings.
	apiKeyKVRe = regexp.MustCompile(`(?i)\b(api[_-]?key|gemini[_-]?api[_-]?key)\b\s*[:=]\s*[^\s"']+`)
)

// RedactSecrets removes obvious secret-bearing substrings from error/log strings.
//
// This is intentionally conservative: it should be safe to call on any message,
// including user-provided inputs and upstream error strings.
func RedactSecrets(s string) string {
	if s == "" {
		return ""
	}
	out := s
	out = bearerTokenRe.ReplaceAllString(out, "Bearer <redacted>")
	out = apiKeyKVRe.ReplaceAllString(out, "<redacted_kv>")
	return strings.TrimSpace(out)
}
