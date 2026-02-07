package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/foundry"
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
	opts pipeline.Options,
	enricher enrich.Enricher,
) error {
	inputRef, ok := env.Aliases[inputAlias]
	if !ok {
		return fmt.Errorf("missing alias %q in RESOURCE_ALIAS_MAP", inputAlias)
	}
	outputRef, ok := env.Aliases[outputAlias]
	if !ok {
		return fmt.Errorf("missing alias %q in RESOURCE_ALIAS_MAP", outputAlias)
	}
	if outputFilename == "" {
		outputFilename = "enriched.csv"
	}

	client, err := foundry.NewClient(env.FoundryURL, env.Token)
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

	rows, err := pipeline.EnrichEmails(ctx, emails, enricher, opts)
	if err != nil {
		return err
	}

	var outBuf bytes.Buffer
	if err := pipeline.WriteCSV(&outBuf, rows); err != nil {
		return err
	}

	var txnID string
	err = retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		var err error
		txnID, err = client.CreateTransaction(ctx, outputRef.RID, outputRef.Branch)
		return err
	})
	if err != nil {
		return err
	}
	if err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		return client.UploadFile(ctx, outputRef.RID, txnID, outputFilename, "text/csv", outBuf.Bytes())
	}); err != nil {
		return err
	}
	if err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		return client.CommitTransaction(ctx, outputRef.RID, txnID)
	}); err != nil {
		return err
	}
	return nil
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
