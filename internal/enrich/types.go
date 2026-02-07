package enrich

import (
	"context"
)

// Result is the structured enrichment output for a single email.
//
// MVP: everything is a string to keep CSV output simple and stable.
type Result struct {
	LinkedInURL string
	Company     string
	Title       string
	Description string
	Confidence  string
}

// Enricher enriches a single email address.
type Enricher interface {
	Enrich(ctx context.Context, email string) (Result, error)
}

// TransientError marks an error as retryable.
//
// Worker pools should retry transient failures with backoff rather than immediately
// failing the full run.
type TransientError struct {
	Err error
}

func (e *TransientError) Error() string {
	if e == nil || e.Err == nil {
		return "transient error"
	}
	return e.Err.Error()
}

func (e *TransientError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
