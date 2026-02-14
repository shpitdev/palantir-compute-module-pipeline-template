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
	workerOpts := workerOptions(opts)
	processor := emailProcessor(enricher)

	out, err := worker.ProcessAll(ctx, emails, processor, workerOpts)
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(out))
	for _, item := range out {
		rows = append(rows, rowFromWorkerResult(item))
	}
	return rows, nil
}

// EnrichEmailsStream runs enrichment and calls onRow as each item completes.
//
// Rows are emitted in completion-order, which may differ from input order.
func EnrichEmailsStream(
	ctx context.Context,
	emails []string,
	enricher enrich.Enricher,
	opts Options,
	onRow func(Row) error,
) error {
	workerOpts := workerOptions(opts)
	processor := emailProcessor(enricher)

	_, err := worker.ProcessAllWithCallback(ctx, emails, processor, func(item worker.Result[string, enrich.Result]) error {
		if onRow == nil {
			return nil
		}
		return onRow(rowFromWorkerResult(item))
	}, workerOpts)
	if err != nil {
		return err
	}
	return nil
}

func workerOptions(opts Options) worker.Options {
	policy := worker.FailurePolicyPartialOutput
	if opts.FailFast {
		policy = worker.FailurePolicyFailFast
	}

	return worker.Options{
		Workers:           opts.Workers,
		MaxRetries:        opts.MaxRetries,
		RequestTimeout:    opts.RequestTimeout,
		RateLimitRPS:      opts.RateLimitRPS,
		FailurePolicy:     policy,
		BackoffInitial:    200 * time.Millisecond,
		BackoffMax:        2 * time.Second,
		BackoffJitterFrac: 0.2,
	}
}

func emailProcessor(enricher enrich.Enricher) func(context.Context, string) (enrich.Result, error) {
	return func(reqCtx context.Context, raw string) (enrich.Result, error) {
		email := strings.TrimSpace(raw)
		if email == "" {
			return enrich.Result{}, errors.New("empty email")
		}
		return enricher.Enrich(reqCtx, email)
	}
}

func rowFromWorkerResult(item worker.Result[string, enrich.Result]) Row {
	sources := jsonArrayOrEmpty(item.Output.Sources)
	queries := jsonArrayOrEmpty(item.Output.WebSearchQueries)

	if item.Err != nil {
		return Row{
			Email:            strings.TrimSpace(item.Input),
			Status:           "error",
			Error:            redact.Secrets(item.Err.Error()),
			Model:            item.Output.Model,
			Sources:          sources,
			WebSearchQueries: queries,
		}
	}

	return Row{
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
	}
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
