package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich/worker"
)

// Row is the stable output schema contract for the MVP.
type Row struct {
	Email       string
	LinkedInURL string
	Company     string
	Title       string
	Description string
	Confidence  string
	Status      string
	Error       string
}

type Options struct {
	Workers        int
	MaxRetries     int
	RequestTimeout time.Duration
	RateLimitRPS   float64
	FailFast       bool
}

// Header returns the stable CSV header for Row.
func Header() []string {
	return []string{
		"email",
		"linkedin_url",
		"company",
		"title",
		"description",
		"confidence",
		"status",
		"error",
	}
}

// EnrichEmails runs the enricher over all emails and returns stable output rows.
//
// Errors from enrichment are recorded per-row and do not fail the full run.
func EnrichEmails(ctx context.Context, emails []string, enricher enrich.Enricher, opts Options) ([]Row, error) {
	policy := worker.FailurePolicyPartialOutput
	if opts.FailFast {
		policy = worker.FailurePolicyFailFast
	}

	out, err := worker.EnrichAll(ctx, emails, enricher, worker.Options{
		Workers:           opts.Workers,
		MaxRetries:        opts.MaxRetries,
		RequestTimeout:    opts.RequestTimeout,
		RateLimitRPS:      opts.RateLimitRPS,
		FailurePolicy:     policy,
		BackoffInitial:    200 * time.Millisecond,
		BackoffMax:        2 * time.Second,
		BackoffJitterFrac: 0.2,
	})
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(out))
	for _, item := range out {
		if item.Err != nil {
			rows = append(rows, Row{
				Email:  strings.TrimSpace(item.Email),
				Status: "error",
				Error:  item.Err.Error(),
			})
			continue
		}

		rows = append(rows, Row{
			Email:       item.Email,
			LinkedInURL: item.Result.LinkedInURL,
			Company:     item.Result.Company,
			Title:       item.Result.Title,
			Description: item.Result.Description,
			Confidence:  item.Result.Confidence,
			Status:      "ok",
			Error:       "",
		})
	}
	return rows, nil
}
