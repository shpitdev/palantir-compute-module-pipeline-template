package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/redact"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/worker"
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

	Model            string
	Sources          string
	WebSearchQueries string
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
		"model",
		"sources",
		"web_search_queries",
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

	processor := func(reqCtx context.Context, raw string) (enrich.Result, error) {
		email := strings.TrimSpace(raw)
		if email == "" {
			return enrich.Result{}, errors.New("empty email")
		}
		return enricher.Enrich(reqCtx, email)
	}

	out, err := worker.ProcessAll(ctx, emails, processor, worker.Options{
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
		sources := jsonArrayOrEmpty(item.Output.Sources)
		queries := jsonArrayOrEmpty(item.Output.WebSearchQueries)

		if item.Err != nil {
			rows = append(rows, Row{
				Email:            strings.TrimSpace(item.Input),
				Status:           "error",
				Error:            redact.Secrets(item.Err.Error()),
				Model:            item.Output.Model,
				Sources:          sources,
				WebSearchQueries: queries,
			})
			continue
		}

		rows = append(rows, Row{
			Email:            strings.TrimSpace(item.Input),
			LinkedInURL:      item.Output.LinkedInURL,
			Company:          item.Output.Company,
			Title:            item.Output.Title,
			Description:      item.Output.Description,
			Confidence:       item.Output.Confidence,
			Status:           "ok",
			Error:            "",
			Model:            item.Output.Model,
			Sources:          sources,
			WebSearchQueries: queries,
		})
	}
	return rows, nil
}

func jsonArrayOrEmpty(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	b, err := json.Marshal(vals)
	if err != nil {
		// Should not happen for []string, but keep output stable.
		return ""
	}
	return string(b)
}
