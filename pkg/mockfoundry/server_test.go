package mockfoundry_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/mockfoundry"
)

func TestMockFoundry_CommitUpdatesReadTable(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	srv := mockfoundry.New(inputDir, uploadDir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client, err := foundry.NewClient(ts.URL+"/api", ts.URL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}

	ctx := context.Background()
	datasetRID := "ri.foundry.main.dataset.99999999-9999-9999-9999-999999999999"

	txnID, err := client.CreateTransaction(ctx, datasetRID, "")
	if err != nil {
		t.Fatalf("create transaction: %v", err)
	}

	want := []byte("email\nalice@example.com\n")
	if err := client.UploadFile(ctx, datasetRID, txnID, "enriched.csv", "text/csv", want); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := client.CommitTransaction(ctx, datasetRID, txnID); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	got, err := client.ReadTableCSV(ctx, datasetRID, "")
	if err != nil {
		t.Fatalf("readTable: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("readTable output mismatch:\n--- got ---\n%s\n--- want ---\n%s\n", string(got), string(want))
	}
}

func TestMockFoundry_RejectUploadDatasetMismatch(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	srv := mockfoundry.New(inputDir, uploadDir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client, err := foundry.NewClient(ts.URL+"/api", ts.URL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}

	ctx := context.Background()
	ridA := "ri.foundry.main.dataset.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	ridB := "ri.foundry.main.dataset.bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	txnID, err := client.CreateTransaction(ctx, ridA, "")
	if err != nil {
		t.Fatalf("create transaction: %v", err)
	}

	err = client.UploadFile(ctx, ridB, txnID, "enriched.csv", "text/csv", []byte("email\nx@example.com\n"))
	if err == nil {
		t.Fatalf("expected upload to fail for dataset mismatch")
	}
	if !strings.Contains(err.Error(), "errorName=TransactionNotFound") {
		t.Fatalf("expected TransactionNotFound error, got: %v", err)
	}
}

func TestMockFoundry_RejectCommitWithoutUpload(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	srv := mockfoundry.New(inputDir, uploadDir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client, err := foundry.NewClient(ts.URL+"/api", ts.URL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}

	ctx := context.Background()
	rid := "ri.foundry.main.dataset.cccccccc-cccc-cccc-cccc-cccccccccccc"

	txnID, err := client.CreateTransaction(ctx, rid, "")
	if err != nil {
		t.Fatalf("create transaction: %v", err)
	}

	err = client.CommitTransaction(ctx, rid, txnID)
	if err == nil {
		t.Fatalf("expected commit to fail with no uploaded files")
	}
	if !strings.Contains(err.Error(), "errorName=Conjure:InvalidArgument") {
		t.Fatalf("expected InvalidArgument error, got: %v", err)
	}
}

func TestMockFoundry_RejectCommitMultipleFiles(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	srv := mockfoundry.New(inputDir, uploadDir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client, err := foundry.NewClient(ts.URL+"/api", ts.URL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}

	ctx := context.Background()
	rid := "ri.foundry.main.dataset.dddddddd-dddd-dddd-dddd-dddddddddddd"

	txnID, err := client.CreateTransaction(ctx, rid, "")
	if err != nil {
		t.Fatalf("create transaction: %v", err)
	}

	if err := client.UploadFile(ctx, rid, txnID, "enriched.csv", "text/csv", []byte("a")); err != nil {
		t.Fatalf("upload file 1: %v", err)
	}
	if err := client.UploadFile(ctx, rid, txnID, "other.csv", "text/csv", []byte("b")); err != nil {
		t.Fatalf("upload file 2: %v", err)
	}

	err = client.CommitTransaction(ctx, rid, txnID)
	if err == nil {
		t.Fatalf("expected commit to fail with multiple uploaded files")
	}
	if !strings.Contains(err.Error(), "errorName=Conjure:InvalidArgument") {
		t.Fatalf("expected InvalidArgument error, got: %v", err)
	}
}
