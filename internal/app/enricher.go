package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	foundryio "github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/foundry"
	localio "github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/local"
)

// RunLocal reads a local input CSV of emails and writes a local output CSV of enriched rows.
func RunLocal(ctx context.Context, inputPath, outputPath string, opts pipeline.Options, enricher enrich.Enricher) error {
	inF, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = inF.Close()
	}()

	emails, err := localio.ReadEmailsCSV(inF)
	if err != nil {
		return err
	}

	rows, err := pipeline.EnrichEmails(ctx, emails, enricher, opts)
	if err != nil {
		return err
	}

	outF, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = outF.Close()
	}()

	if err := pipeline.WriteCSV(outF, rows); err != nil {
		return err
	}
	return outF.Close()
}

// RunFoundry runs the pipeline-mode orchestration against the minimal dataset API surface.
func RunFoundry(
	ctx context.Context,
	env foundry.Env,
	inputAlias string,
	outputAlias string,
	outputFilename string,
	outputWriteMode string,
	opts pipeline.Options,
	enricher enrich.Enricher,
) error {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	runID := fmt.Sprintf("run-%d", time.Now().UnixNano())
	logf := func(format string, args ...any) {
		prefix := make([]any, 0, len(args)+1)
		prefix = append(prefix, runID)
		prefix = append(prefix, args...)
		logger.Printf("run=%s "+format, prefix...)
	}
	runStart := time.Now()

	inputRef, ok := env.Aliases[inputAlias]
	if !ok {
		return fmt.Errorf("missing alias %q in RESOURCE_ALIAS_MAP", inputAlias)
	}
	outputRef, ok := env.Aliases[outputAlias]
	if !ok {
		return fmt.Errorf("missing alias %q in RESOURCE_ALIAS_MAP", outputAlias)
	}
	inputBranch := strings.TrimSpace(inputRef.Branch)
	if inputBranch == "" {
		inputBranch = "master"
	}
	outputBranch := strings.TrimSpace(outputRef.Branch)
	if outputBranch == "" {
		outputBranch = "master"
	}
	logf(
		"foundry run start: input=%s@%s output=%s@%s writeMode=%s workers=%d maxRetries=%d timeout=%s rateLimitRPS=%g failFast=%t",
		inputRef.RID,
		inputBranch,
		outputRef.RID,
		outputBranch,
		outputWriteMode,
		opts.Workers,
		opts.MaxRetries,
		opts.RequestTimeout,
		opts.RateLimitRPS,
		opts.FailFast,
	)
	if outputFilename == "" {
		outputFilename = "enriched.csv"
	}

	client, err := foundry.NewClient(env.Services.APIGateway, env.Services.StreamProxy, env.Token, env.DefaultCAPath)
	if err != nil {
		return err
	}

	readStart := time.Now()
	emails, err := foundryio.ReadInputEmails(ctx, client, inputRef)
	if err != nil {
		return err
	}
	logf("loaded %d emails from input dataset in %s", len(emails), time.Since(readStart).Round(time.Millisecond))

	modeStart := time.Now()
	isStream, err := foundryio.ResolveOutputMode(ctx, client, outputRef, outputWriteMode)
	if err != nil {
		return err
	}
	mode := "dataset"
	if isStream {
		mode = "stream"
	}
	logf("resolved output mode=%s in %s", mode, time.Since(modeStart).Round(time.Millisecond))

	enrichStart := time.Now()
	if isStream {
		logf("incremental mode disabled for stream output; append-only streams require explicit checkpointing for dedupe")
		branch := strings.TrimSpace(outputRef.Branch)
		if branch == "" {
			branch = "master"
		}
		writeStart := time.Now()
		logf("publishing rows to stream-proxy (%s@%s)", outputRef.RID, branch)

		processedRows := 0
		publishedRows := 0
		okRows := 0
		errorRows := 0
		err = pipeline.EnrichEmailsStream(ctx, emails, newTracedEnricher(enricher, logger, runID, opts), opts, func(row pipeline.Row) error {
			processedRows++
			if strings.EqualFold(strings.TrimSpace(row.Status), "ok") {
				okRows++
			} else {
				errorRows++
			}

			logf(
				"stream row enriched: email=%q status=%q completed=%d/%d enrichElapsed=%s",
				row.Email,
				strings.TrimSpace(row.Status),
				processedRows,
				len(emails),
				time.Since(enrichStart).Round(time.Millisecond),
			)

			publishStart := time.Now()
			if err := foundryio.PublishJSONRecord(ctx, client, outputRef, rowToStreamRecord(row)); err != nil {
				return err
			}

			publishedRows++
			logf(
				"stream row published: email=%q status=%q publishDuration=%s published=%d/%d",
				row.Email,
				strings.TrimSpace(row.Status),
				time.Since(publishStart).Round(time.Millisecond),
				publishedRows,
				len(emails),
			)
			return nil
		})
		if err != nil {
			return err
		}
		logf(
			"enrichment complete: produced=%d ok=%d error=%d duration=%s",
			processedRows,
			okRows,
			errorRows,
			time.Since(enrichStart).Round(time.Millisecond),
		)
		logf(
			"foundry run complete: stream publish finished writeDuration=%s totalDuration=%s",
			time.Since(writeStart).Round(time.Millisecond),
			time.Since(runStart).Round(time.Millisecond),
		)
		return nil
	}

	existingByEmail, err := readExistingOutputRows(ctx, client, outputRef, logger, runID)
	if err != nil {
		return err
	}
	plan := buildIncrementalPlan(emails, existingByEmail)
	logf(
		"incremental plan: inputRows=%d cachedRows=%d rowsToEnrich=%d uniqueEmailsToEnrich=%d",
		len(emails),
		plan.cachedRows,
		plan.pendingRows,
		len(plan.pendingEmails),
	)
	if len(plan.pendingEmails) > 0 {
		freshRows, err := pipeline.EnrichEmails(ctx, plan.pendingEmails, newTracedEnricher(enricher, logger, runID, opts), opts)
		if err != nil {
			return err
		}
		if err := plan.applyEnrichedRows(freshRows); err != nil {
			return err
		}
	}
	rows := plan.rows
	okRows, errorRows := countStatuses(rows)
	logf(
		"enrichment complete: produced=%d ok=%d error=%d duration=%s",
		len(rows),
		okRows,
		errorRows,
		time.Since(enrichStart).Round(time.Millisecond),
	)

	writeStart := time.Now()
	var outBuf bytes.Buffer
	if err := pipeline.WriteCSV(&outBuf, rows); err != nil {
		return err
	}
	if err := foundryio.UploadDatasetCSV(ctx, client, outputRef, outputFilename, outBuf.Bytes()); err != nil {
		return err
	}
	logf(
		"foundry run complete: dataset output finished writeDuration=%s totalDuration=%s",
		time.Since(writeStart).Round(time.Millisecond),
		time.Since(runStart).Round(time.Millisecond),
	)
	return nil
}

func rowToStreamRecord(r pipeline.Row) map[string]any {
	// Use null for empty values so nullable string columns behave like "missing" rather than "".
	rec := map[string]any{
		"email": r.Email,
	}
	assignNullable(rec, "linkedin_url", r.LinkedInURL)
	assignNullable(rec, "company", r.Company)
	assignNullable(rec, "title", r.Title)
	assignNullable(rec, "description", r.Description)
	assignNullable(rec, "confidence", r.Confidence)
	assignNullable(rec, "status", r.Status)
	assignNullable(rec, "error", r.Error)
	assignNullable(rec, "model", r.Model)
	assignNullable(rec, "sources", r.Sources)
	assignNullable(rec, "web_search_queries", r.WebSearchQueries)
	return rec
}

func assignNullable(dst map[string]any, key string, value string) {
	if strings.TrimSpace(value) == "" {
		dst[key] = nil
		return
	}
	dst[key] = value
}

type tracedEnricher struct {
	next           enrich.Enricher
	logger         *log.Logger
	runID          string
	maxRetries     int
	requestTimeout time.Duration

	mu       sync.Mutex
	attempts map[string]int
}

func newTracedEnricher(next enrich.Enricher, logger *log.Logger, runID string, opts pipeline.Options) *tracedEnricher {
	return &tracedEnricher{
		next:           next,
		logger:         logger,
		runID:          runID,
		maxRetries:     opts.MaxRetries,
		requestTimeout: opts.RequestTimeout,
		attempts:       make(map[string]int),
	}
}

func (t *tracedEnricher) Enrich(ctx context.Context, email string) (enrich.Result, error) {
	email = strings.TrimSpace(email)
	attempt := t.nextAttempt(email)
	reqJSON, _ := json.Marshal(map[string]any{
		"email": email,
	})

	deadlineIn := "none"
	if d, ok := ctx.Deadline(); ok {
		deadlineIn = time.Until(d).Round(time.Millisecond).String()
	}
	t.logger.Printf(
		"run=%s enrich request: email=%q attempt=%d timeout=%s deadlineIn=%s request=%s",
		t.runID,
		email,
		attempt,
		t.requestTimeout,
		deadlineIn,
		string(reqJSON),
	)

	start := time.Now()
	out, err := t.next.Enrich(ctx, email)
	elapsed := time.Since(start).Round(time.Millisecond)

	respJSON, _ := json.Marshal(map[string]any{
		"linkedin_url":       out.LinkedInURL,
		"company":            out.Company,
		"title":              out.Title,
		"description":        out.Description,
		"confidence":         out.Confidence,
		"model":              out.Model,
		"sources":            out.Sources,
		"web_search_queries": out.WebSearchQueries,
	})

	if err != nil {
		maxRetries := maxRetryBudgetForErr(t.maxRetries, err)
		retryable := isRetryableError(err)
		willRetry := retryable && attempt <= maxRetries
		t.logger.Printf(
			"run=%s enrich response: email=%q attempt=%d duration=%s status=error retryable=%t willRetry=%t maxExtraRetries=%d error=%q partialResponse=%s",
			t.runID,
			email,
			attempt,
			elapsed,
			retryable,
			willRetry,
			maxRetries,
			err.Error(),
			string(respJSON),
		)
		return out, err
	}

	t.logger.Printf(
		"run=%s enrich response: email=%q attempt=%d duration=%s status=ok response=%s",
		t.runID,
		email,
		attempt,
		elapsed,
		string(respJSON),
	)
	return out, nil
}

func (t *tracedEnricher) nextAttempt(email string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts[email]++
	return t.attempts[email]
}

type retryCap interface {
	MaxExtraRetries() int
}

func maxRetryBudgetForErr(defaultMax int, err error) int {
	if defaultMax < 0 {
		defaultMax = 0
	}
	var capErr retryCap
	if errors.As(err, &capErr) {
		capMax := capErr.MaxExtraRetries()
		if capMax < 0 {
			capMax = 0
		}
		if capMax < defaultMax {
			return capMax
		}
	}
	return defaultMax
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	var transientErr *enrich.TransientError
	if errors.As(err, &transientErr) {
		return true
	}
	var limitedTransientErr *enrich.LimitedTransientError
	if errors.As(err, &limitedTransientErr) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	return false
}

type incrementalPlan struct {
	rows          []pipeline.Row
	pendingEmails []string
	pendingIdx    map[string][]int
	cachedRows    int
	pendingRows   int
}

func buildIncrementalPlan(inputEmails []string, existingByEmail map[string]pipeline.Row) incrementalPlan {
	plan := incrementalPlan{
		rows:       make([]pipeline.Row, len(inputEmails)),
		pendingIdx: make(map[string][]int),
	}
	for i, raw := range inputEmails {
		email := strings.TrimSpace(raw)
		key := emailKey(email)

		if prev, ok := existingByEmail[key]; ok && strings.EqualFold(strings.TrimSpace(prev.Status), "ok") {
			prev.Email = email
			plan.rows[i] = prev
			plan.cachedRows++
			continue
		}

		if _, seen := plan.pendingIdx[key]; !seen {
			plan.pendingEmails = append(plan.pendingEmails, email)
		}
		plan.pendingIdx[key] = append(plan.pendingIdx[key], i)
		plan.pendingRows++
	}
	return plan
}

func (p *incrementalPlan) applyEnrichedRows(rows []pipeline.Row) error {
	if len(rows) != len(p.pendingEmails) {
		return fmt.Errorf("incremental enrichment mismatch: got %d rows for %d pending emails", len(rows), len(p.pendingEmails))
	}
	for i, email := range p.pendingEmails {
		key := emailKey(email)
		idxs, ok := p.pendingIdx[key]
		if !ok || len(idxs) == 0 {
			return fmt.Errorf("incremental enrichment mismatch: missing pending indexes for %q", email)
		}
		row := rows[i]
		row.Email = strings.TrimSpace(email)
		for _, idx := range idxs {
			p.rows[idx] = row
		}
	}
	return nil
}

func readExistingOutputRows(
	ctx context.Context,
	client *foundry.Client,
	outputRef foundry.DatasetRef,
	logger *log.Logger,
	runID string,
) (map[string]pipeline.Row, error) {
	branch := strings.TrimSpace(outputRef.Branch)
	if branch == "" {
		branch = "master"
	}

	b, err := client.ReadTableCSV(ctx, outputRef.RID, branch)
	if err != nil {
		if isNotFoundError(err) {
			logger.Printf("run=%s incremental: no prior output snapshot found for %s@%s", runID, outputRef.RID, branch)
			return map[string]pipeline.Row{}, nil
		}
		return nil, fmt.Errorf("read prior output dataset snapshot: %w", err)
	}

	rows, err := pipeline.ReadCSV(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("parse prior output csv: %w", err)
	}

	out := make(map[string]pipeline.Row, len(rows))
	for _, row := range rows {
		key := emailKey(row.Email)
		if key == "" {
			continue
		}
		out[key] = row
	}
	logger.Printf("run=%s incremental: loaded %d prior output rows from %s@%s", runID, len(out), outputRef.RID, branch)
	return out, nil
}

func isNotFoundError(err error) bool {
	var he *foundry.HTTPError
	return errors.As(err, &he) && he.StatusCode == 404
}

func emailKey(email string) string {
	return strings.TrimSpace(email)
}

func countStatuses(rows []pipeline.Row) (okRows int, errorRows int) {
	for _, row := range rows {
		if strings.EqualFold(strings.TrimSpace(row.Status), "ok") {
			okRows++
			continue
		}
		errorRows++
	}
	return okRows, errorRows
}
