package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/pipeline"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/util"
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

	emails, err := util.ReadEmailsCSV(inF)
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
	mode := strings.ToLower(strings.TrimSpace(outputWriteMode))
	if mode == "" {
		mode = "auto"
	}

	client, err := foundry.NewClient(env.Services.APIGateway, env.Services.StreamProxy, env.Token, env.DefaultCAPath)
	if err != nil {
		return err
	}

	var inputBytes []byte
	err = retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		var err error
		inputBytes, err = client.ReadTableCSV(ctx, inputRef.RID, inputRef.Branch)
		return err
	})
	if err != nil {
		return err
	}
	emails, err := util.ReadEmailsCSV(bytes.NewReader(inputBytes))
	if err != nil {
		return err
	}
	logger.Printf("loaded %d emails from input dataset", len(emails))

	rows, err := pipeline.EnrichEmails(ctx, emails, enricher, opts)
	if err != nil {
		return err
	}
	logger.Printf("enrichment complete: produced %d rows", len(rows))

	// Decide whether the output alias refers to a stream (stream-proxy) or a snapshot dataset (transactions + upload).
	isStream := false
	switch mode {
	case "auto":
		branch := strings.TrimSpace(outputRef.Branch)
		if branch == "" {
			branch = "master"
		}
		err = retryTransient(ctx, 8, 200*time.Millisecond, func() error {
			var err error
			isStream, err = client.ProbeStream(ctx, outputRef.RID, branch)
			return err
		})
		if err != nil {
			return err
		}
	case "stream":
		isStream = true
	case "dataset":
		isStream = false
	default:
		return fmt.Errorf("invalid output write mode %q (expected auto|dataset|stream)", outputWriteMode)
	}

	if isStream {
		branch := strings.TrimSpace(outputRef.Branch)
		if branch == "" {
			branch = "master"
		}
		logger.Printf("publishing %d rows to stream-proxy (%s@%s)", len(rows), outputRef.RID, branch)

		for i, r := range rows {
			if i == 0 || (i+1)%100 == 0 || i == len(rows)-1 {
				logger.Printf("stream publish progress: %d/%d", i+1, len(rows))
			}
			// Use null for empty values so nullable string columns behave like "missing" rather than "".
			rec := map[string]any{
				"email": r.Email,
			}
			if strings.TrimSpace(r.LinkedInURL) == "" {
				rec["linkedin_url"] = nil
			} else {
				rec["linkedin_url"] = r.LinkedInURL
			}
			if strings.TrimSpace(r.Company) == "" {
				rec["company"] = nil
			} else {
				rec["company"] = r.Company
			}
			if strings.TrimSpace(r.Title) == "" {
				rec["title"] = nil
			} else {
				rec["title"] = r.Title
			}
			if strings.TrimSpace(r.Description) == "" {
				rec["description"] = nil
			} else {
				rec["description"] = r.Description
			}
			if strings.TrimSpace(r.Confidence) == "" {
				rec["confidence"] = nil
			} else {
				rec["confidence"] = r.Confidence
			}
			if strings.TrimSpace(r.Status) == "" {
				rec["status"] = nil
			} else {
				rec["status"] = r.Status
			}
			if strings.TrimSpace(r.Error) == "" {
				rec["error"] = nil
			} else {
				rec["error"] = r.Error
			}
			if strings.TrimSpace(r.Model) == "" {
				rec["model"] = nil
			} else {
				rec["model"] = r.Model
			}
			if strings.TrimSpace(r.Sources) == "" {
				rec["sources"] = nil
			} else {
				rec["sources"] = r.Sources
			}
			if strings.TrimSpace(r.WebSearchQueries) == "" {
				rec["web_search_queries"] = nil
			} else {
				rec["web_search_queries"] = r.WebSearchQueries
			}

			if err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
				return client.PublishStreamJSONRecord(ctx, outputRef.RID, branch, rec)
			}); err != nil {
				return err
			}
		}
		logger.Printf("foundry run complete: stream publish finished")
		return nil
	}

	var outBuf bytes.Buffer
	if err := pipeline.WriteCSV(&outBuf, rows); err != nil {
		return err
	}

	var txnID string
	createdTxn := true
	err = retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		var err error
		txnID, err = client.CreateTransaction(ctx, outputRef.RID, outputRef.Branch)
		return err
	})
	if err != nil {
		// In Foundry pipeline mode, the build system may open the output dataset transaction
		// before starting the module. In that case, creating a new transaction will conflict.
		if !isOpenTransactionAlreadyExists(err) {
			return err
		}
		createdTxn = false

		var ok bool
		err = retryTransient(ctx, 8, 200*time.Millisecond, func() error {
			var err error
			txnID, ok, err = client.FindLatestOpenTransaction(ctx, outputRef.RID)
			return err
		})
		if err != nil {
			return err
		}
		if !ok || txnID == "" {
			return fmt.Errorf("output dataset has an open transaction but no OPEN transaction was returned by listTransactions (preview endpoint)")
		}
	}
	if err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		// Dataset upload endpoints expect raw bytes; the file extension drives CSV parsing on the Foundry side.
		return client.UploadFile(ctx, outputRef.RID, txnID, outputFilename, "application/octet-stream", outBuf.Bytes())
	}); err != nil {
		return err
	}

	// If Foundry pipeline opened the transaction, do not commit it: Foundry will handle commit as part of the build.
	if createdTxn {
		if err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
			return client.CommitTransaction(ctx, outputRef.RID, txnID)
		}); err != nil {
			return err
		}
	}
	logger.Printf("foundry run complete: dataset output finished")
	return nil
}

func isOpenTransactionAlreadyExists(err error) bool {
	var he *foundry.HTTPError
	if !errors.As(err, &he) {
		return false
	}
	if he.StatusCode != http.StatusConflict {
		return false
	}
	// Foundry: createTransaction returns OpenTransactionAlreadyExists when the output dataset already has an open txn.
	return he.ErrorName == "OpenTransactionAlreadyExists" || he.ErrorCode == "CONFLICT"
}

func retryTransient(ctx context.Context, attempts int, initialSleep time.Duration, f func() error) error {
	sleep := initialSleep
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := f(); err == nil {
			return nil
		} else if !isTransientNetErr(err) {
			return err
		} else {
			lastErr = err
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		if i < attempts-1 {
			time.Sleep(sleep)
			if sleep < 2*time.Second {
				sleep *= 2
			}
		}
	}
	return lastErr
}

func isTransientNetErr(err error) bool {
	// These cover the usual "service not ready yet" cases in docker-compose.
	return errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ETIMEDOUT)
}
