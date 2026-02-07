package app_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/mockfoundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/pipeline"
)

func TestRunFoundry_EndToEndAgainstMock(t *testing.T) {
	t.Parallel()

	inputRID := "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"
	outputRID := "ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222"

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(inputDir, inputRID+".csv"),
		[]byte("email\nalice@example.com\nbob@corp.test\n"),
		0644,
	); err != nil {
		t.Fatalf("write input csv: %v", err)
	}

	mock := mockfoundry.New(inputDir, uploadDir)
	mock.RequireBearerToken("dummy-token")
	ts := httptest.NewServer(mock.Handler())
	defer ts.Close()

	env := foundry.Env{
		FoundryURL: ts.URL,
		Token:      "dummy-token",
		Aliases: map[string]foundry.DatasetRef{
			"input":  {RID: inputRID, Branch: "master"},
			"output": {RID: outputRID, Branch: "master"},
		},
	}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", pipeline.Options{}, enrich.Stub{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	calls := mock.Calls()
	if len(calls) != 4 {
		t.Fatalf("expected 4 calls, got %d: %#v", len(calls), calls)
	}
	wantPaths := []string{
		"/api/v1/datasets/" + inputRID + "/readTable",
		"/api/v2/datasets/" + outputRID + "/transactions",
		"/api/v1/datasets/" + outputRID + "/transactions/txn-000001/files/enriched.csv",
		"/api/v2/datasets/" + outputRID + "/transactions/txn-000001/commit",
	}
	for i, wantPath := range wantPaths {
		if calls[i].Path != wantPath {
			t.Fatalf("call[%d] path: want %q, got %q (all calls=%#v)", i, wantPath, calls[i].Path, calls)
		}
	}

	uploads := mock.Uploads()
	if len(uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d: %#v", len(uploads), uploads)
	}
	if uploads[0].DatasetRID != outputRID || uploads[0].TxnID != "txn-000001" || uploads[0].FilePath != "enriched.csv" {
		t.Fatalf("unexpected upload metadata: %#v", uploads[0])
	}

	cr := csv.NewReader(bytes.NewReader(uploads[0].Bytes))
	records, err := cr.ReadAll()
	if err != nil {
		t.Fatalf("parse uploaded csv: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected header + 2 rows, got %d", len(records))
	}

	// Header matches contract.
	wantHeader := pipeline.Header()
	if len(records[0]) != len(wantHeader) {
		t.Fatalf("unexpected header len: got %d want %d", len(records[0]), len(wantHeader))
	}
	for i := range wantHeader {
		if records[0][i] != wantHeader[i] {
			t.Fatalf("header[%d]: want %q got %q", i, wantHeader[i], records[0][i])
		}
	}

	// Row contents are deterministic with the stub enricher.
	if records[1][0] != "alice@example.com" || records[1][2] != "example.com" || records[1][5] != "stub" || records[1][6] != "ok" {
		t.Fatalf("unexpected row[1]: %#v", records[1])
	}
	if records[2][0] != "bob@corp.test" || records[2][2] != "corp.test" || records[2][5] != "stub" || records[2][6] != "ok" {
		t.Fatalf("unexpected row[2]: %#v", records[2])
	}
}
