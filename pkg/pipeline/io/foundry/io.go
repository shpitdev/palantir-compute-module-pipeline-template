package foundryio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

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
	err := RetryTransient(ctx, DefaultRetryPolicy, func() error {
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
	return ResolveOutputModeWithBackend(ctx, NewLegacyStreamProxyBackend(client), outputRef, requestedMode)
}

// ResolveOutputModeWithBackend resolves whether output should be written through a stream backend.
func ResolveOutputModeWithBackend(ctx context.Context, backend StreamBackend, outputRef foundry.DatasetRef, requestedMode string) (bool, error) {
	mode := strings.ToLower(strings.TrimSpace(requestedMode))
	if mode == "" {
		mode = OutputModeAuto
	}

	switch mode {
	case OutputModeAuto:
		if backend == nil {
			return false, fmt.Errorf("stream backend is required for auto output mode")
		}
		isStream, err := backend.Probe(ctx, outputRef)
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
	backend := NewLegacyStreamProxyBackend(client)
	for _, rec := range records {
		if err := backend.PublishRecord(ctx, outputRef, rec); err != nil {
			return err
		}
	}
	return nil
}

// PublishJSONRecord publishes one JSON object to stream-proxy.
func PublishJSONRecord(ctx context.Context, client *foundry.Client, outputRef foundry.DatasetRef, record map[string]any) error {
	return NewLegacyStreamProxyBackend(client).PublishRecord(ctx, outputRef, record)
}

// UploadDatasetCSV uploads CSV bytes to a dataset transaction and commits when appropriate.
func UploadDatasetCSV(ctx context.Context, client *foundry.Client, outputRef foundry.DatasetRef, outputFilename string, csv []byte) error {
	if strings.TrimSpace(outputFilename) == "" {
		outputFilename = "enriched.csv"
	}

	var txnID string
	createdTxn := true
	err := RetryTransient(ctx, DefaultRetryPolicy, func() error {
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
		err = RetryTransient(ctx, DefaultRetryPolicy, func() error {
			var err error
			txnID, ok, err = client.FindLatestOpenTransactionForBranch(ctx, outputRef.RID, outputRef.Branch)
			return err
		})
		if err != nil {
			return err
		}
		if !ok || txnID == "" {
			return fmt.Errorf("output dataset has an open transaction but no OPEN transaction was returned by listTransactions (preview endpoint)")
		}
	}

	if err := RetryTransient(ctx, DefaultRetryPolicy, func() error {
		return client.UploadFile(ctx, outputRef.RID, txnID, outputFilename, "application/octet-stream", csv)
	}); err != nil {
		return err
	}

	if createdTxn {
		if err := RetryTransient(ctx, DefaultRetryPolicy, func() error {
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
