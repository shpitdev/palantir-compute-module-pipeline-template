package enrich

import (
	"context"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/core"
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

	// Optional audit/debug fields.
	Model            string
	Sources          []string
	WebSearchQueries []string
}

// Enricher enriches a single email address.
type Enricher interface {
	Enrich(ctx context.Context, email string) (Result, error)
}

// TransientError marks an error as retryable by pipeline workers.
type TransientError = core.TransientError
