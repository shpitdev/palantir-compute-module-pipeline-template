package app

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

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
	logger.Printf(
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

	emails, err := foundryio.ReadInputEmails(ctx, client, inputRef)
	if err != nil {
		return err
	}
	logger.Printf("loaded %d emails from input dataset", len(emails))

	rows, err := pipeline.EnrichEmails(ctx, emails, enricher, opts)
	if err != nil {
		return err
	}
	logger.Printf("enrichment complete: produced %d rows", len(rows))

	isStream, err := foundryio.ResolveOutputMode(ctx, client, outputRef, outputWriteMode)
	if err != nil {
		return err
	}

	if isStream {
		branch := strings.TrimSpace(outputRef.Branch)
		if branch == "" {
			branch = "master"
		}
		logger.Printf("publishing %d rows to stream-proxy (%s@%s)", len(rows), outputRef.RID, branch)
		records := make([]map[string]any, 0, len(rows))
		for i, r := range rows {
			if i == 0 || (i+1)%100 == 0 || i == len(rows)-1 {
				logger.Printf("stream publish progress: %d/%d", i+1, len(rows))
			}
			records = append(records, rowToStreamRecord(r))
		}
		if err := foundryio.PublishJSONRecords(ctx, client, outputRef, records); err != nil {
			return err
		}
		logger.Printf("foundry run complete: stream publish finished")
		return nil
	}

	var outBuf bytes.Buffer
	if err := pipeline.WriteCSV(&outBuf, rows); err != nil {
		return err
	}
	if err := foundryio.UploadDatasetCSV(ctx, client, outputRef, outputFilename, outBuf.Bytes()); err != nil {
		return err
	}
	logger.Printf("foundry run complete: dataset output finished")
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
