package foundryio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	localio "github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/local"
)

const (
	OutputModeAuto    = "auto"
	OutputModeDataset = "dataset"
	OutputModeStream  = "stream"
)

// ReadInputEmails reads input rows from a Foundry dataset and extracts the email column.
func ReadInputEmails(ctx context.Context, client *foundry.Client, inputRef foundry.DatasetRef) ([]string, error) {
	var inputBytes []byte
	err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		var err error
		inputBytes, err = client.ReadTableCSV(ctx, inputRef.RID, inputRef.Branch)
		return err
	})
	if err != nil {
		return nil, err
	}
	return localio.ReadEmailsCSV(bytes.NewReader(inputBytes))
}

// ResolveOutputMode resolves whether output should be written to stream-proxy.
func ResolveOutputMode(ctx context.Context, client *foundry.Client, outputRef foundry.DatasetRef, requestedMode string) (bool, error) {
	mode := strings.ToLower(strings.TrimSpace(requestedMode))
	if mode == "" {
		mode = OutputModeAuto
	}

	switch mode {
	case OutputModeAuto:
		branch := strings.TrimSpace(outputRef.Branch)
		if branch == "" {
			branch = "master"
		}
		isStream := false
		err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
			var err error
			isStream, err = client.ProbeStream(ctx, outputRef.RID, branch)
			return err
		})
		if err != nil {
			return false, err
		}
		return isStream, nil
	case OutputModeStream:
		return true, nil
	case OutputModeDataset:
		return false, nil
	default:
		return false, fmt.Errorf("invalid output write mode %q (expected auto|dataset|stream)", requestedMode)
	}
}

// PublishJSONRecords publishes one JSON object per row to stream-proxy.
func PublishJSONRecords(ctx context.Context, client *foundry.Client, outputRef foundry.DatasetRef, records []map[string]any) error {
	for _, rec := range records {
		if err := PublishJSONRecord(ctx, client, outputRef, rec); err != nil {
			return err
		}
	}
	return nil
}

// PublishJSONRecord publishes one JSON object to stream-proxy.
func PublishJSONRecord(ctx context.Context, client *foundry.Client, outputRef foundry.DatasetRef, record map[string]any) error {
	branch := strings.TrimSpace(outputRef.Branch)
	if branch == "" {
		branch = "master"
	}

	return retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		return client.PublishStreamJSONRecord(ctx, outputRef.RID, branch, record)
	})
}

// UploadDatasetCSV uploads CSV bytes to a dataset transaction and commits when appropriate.
func UploadDatasetCSV(ctx context.Context, client *foundry.Client, outputRef foundry.DatasetRef, outputFilename string, csv []byte) error {
	if strings.TrimSpace(outputFilename) == "" {
		outputFilename = "enriched.csv"
	}

	var txnID string
	createdTxn := true
	err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
		var err error
		txnID, err = client.CreateTransaction(ctx, outputRef.RID, outputRef.Branch)
		return err
	})
	if err != nil {
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
		return client.UploadFile(ctx, outputRef.RID, txnID, outputFilename, "application/octet-stream", csv)
	}); err != nil {
		return err
	}

	if createdTxn {
		if err := retryTransient(ctx, 8, 200*time.Millisecond, func() error {
			return client.CommitTransaction(ctx, outputRef.RID, txnID)
		}); err != nil {
			return err
		}
	}
	return nil
}

func isOpenTransactionAlreadyExists(err error) bool {
	var he *foundry.HTTPError
	if !errors.As(err, &he) {
		return false
	}
	if he.StatusCode != 409 {
		return false
	}
	return he.ErrorName == "OpenTransactionAlreadyExists" || he.ErrorCode == "CONFLICT"
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	var he *foundry.HTTPError
	if errors.As(err, &he) {
		return he.StatusCode == 429 || he.StatusCode/100 == 5
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout() || ne.Temporary()
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	return false
}

func retryTransient(ctx context.Context, attempts int, initialSleep time.Duration, f func() error) error {
	sleep := initialSleep
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := f(); err == nil {
			return nil
		} else {
			lastErr = err
			if !isTransient(err) || i == attempts-1 {
				return err
			}
		}

		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		sleep *= 2
		if sleep > 2*time.Second {
			sleep = 2 * time.Second
		}
	}
	return lastErr
}
