package app_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/mockfoundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/pipeline"
)

type testEnricher struct{}

func (testEnricher) Enrich(_ context.Context, email string) (enrich.Result, error) {
	domain := ""
	if at := strings.LastIndex(email, "@"); at >= 0 && at+1 < len(email) {
		domain = email[at+1:]
	}
	return enrich.Result{
		Company:          domain,
		Confidence:       "test",
		Model:            "test-model",
		Sources:          []string{"https://source.invalid/" + domain},
		WebSearchQueries: []string{"company " + domain},
	}, nil
}

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

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	calls := mock.Calls()
	if len(calls) != 6 {
		t.Fatalf("expected 6 calls, got %d: %#v", len(calls), calls)
	}
	if calls[0].Path != "/api/v2/datasets/"+inputRID+"/branches/master" {
		t.Fatalf("call[0] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+inputRID+"/branches/master", calls[0].Path, calls)
	}
	if calls[1].Path != "/api/v2/datasets/"+inputRID+"/readTable" {
		t.Fatalf("call[1] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+inputRID+"/readTable", calls[1].Path, calls)
	}
	wantProbePath := "/stream-proxy/api/streams/" + outputRID + "/branches/master/records"
	if calls[2].Path != wantProbePath {
		t.Fatalf("call[2] path: want %q, got %q (all calls=%#v)", wantProbePath, calls[2].Path, calls)
	}
	if calls[3].Path != "/api/v2/datasets/"+outputRID+"/transactions" {
		t.Fatalf("call[3] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/transactions", calls[3].Path, calls)
	}

	wantUploadPath := "/api/v2/datasets/" + outputRID + "/files/enriched.csv/upload"
	if calls[4].Path != wantUploadPath {
		t.Fatalf("call[4] path: want %q, got %q (all calls=%#v)", wantUploadPath, calls[4].Path, calls)
	}

	commitPrefix := "/api/v2/datasets/" + outputRID + "/transactions/"
	commitSuffix := "/commit"
	if !strings.HasPrefix(calls[5].Path, commitPrefix) || !strings.HasSuffix(calls[5].Path, commitSuffix) {
		t.Fatalf("call[5] path: expected prefix %q and suffix %q, got %q (all calls=%#v)", commitPrefix, commitSuffix, calls[5].Path, calls)
	}
	txnID := strings.TrimSuffix(strings.TrimPrefix(calls[5].Path, commitPrefix), commitSuffix)
	if strings.TrimSpace(txnID) == "" {
		t.Fatalf("call[5] path: failed to extract transaction id from %q", calls[5].Path)
	}

	uploads := mock.Uploads()
	if len(uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d: %#v", len(uploads), uploads)
	}
	if uploads[0].DatasetRID != outputRID || uploads[0].TxnID != txnID || uploads[0].FilePath != "enriched.csv" {
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

	// Row contents are deterministic with the test enricher.
	if records[1][0] != "alice@example.com" || records[1][2] != "example.com" || records[1][5] != "test" || records[1][6] != "ok" || records[1][8] != "test-model" {
		t.Fatalf("unexpected row[1]: %#v", records[1])
	}
	if records[2][0] != "bob@corp.test" || records[2][2] != "corp.test" || records[2][5] != "test" || records[2][6] != "ok" || records[2][8] != "test-model" {
		t.Fatalf("unexpected row[2]: %#v", records[2])
	}

	// The committed dataset can be retrieved via readTable (read-after-write semantics).
	client, err := foundry.NewClient(ts.URL, env.Token)
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}
	got, err := client.ReadTableCSV(context.Background(), outputRID, "master")
	if err != nil {
		t.Fatalf("read committed output via readTable: %v", err)
	}
	if !bytes.Equal(got, uploads[0].Bytes) {
		t.Fatalf("readTable output mismatch:\n--- got ---\n%s\n--- want ---\n%s\n", string(got), string(uploads[0].Bytes))
	}

	// Verify the extra readTable call was recorded.
	calls = mock.Calls()
	if len(calls) != 8 {
		t.Fatalf("expected 8 calls after readTable, got %d: %#v", len(calls), calls)
	}
	if calls[6].Path != "/api/v2/datasets/"+outputRID+"/branches/master" {
		t.Fatalf("call[6] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/branches/master", calls[6].Path, calls)
	}
	if calls[7].Path != "/api/v2/datasets/"+outputRID+"/readTable" {
		t.Fatalf("call[7] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/readTable", calls[7].Path, calls)
	}
}

func TestRunFoundry_UsesExistingOpenTransactionWhenCreateConflicts(t *testing.T) {
	t.Parallel()

	inputRID := "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"
	outputRID := "ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222"

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(inputDir, inputRID+".csv"),
		[]byte("email\nalice@example.com\n"),
		0644,
	); err != nil {
		t.Fatalf("write input csv: %v", err)
	}

	mock := mockfoundry.New(inputDir, uploadDir)
	mock.RequireBearerToken("dummy-token")
	ts := httptest.NewServer(mock.Handler())
	defer ts.Close()

	// Simulate pipeline mode: Foundry build pre-creates an OPEN output transaction.
	client, err := foundry.NewClient(ts.URL, "dummy-token")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}
	preTxnID, err := client.CreateTransaction(context.Background(), outputRID, "master")
	if err != nil {
		t.Fatalf("pre-create output transaction: %v", err)
	}
	beforeCalls := len(mock.Calls())

	env := foundry.Env{
		FoundryURL: ts.URL,
		Token:      "dummy-token",
		Aliases: map[string]foundry.DatasetRef{
			"input":  {RID: inputRID, Branch: "master"},
			"output": {RID: outputRID, Branch: "master"},
		},
	}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	calls := mock.Calls()[beforeCalls:]
	if len(calls) != 6 {
		t.Fatalf("expected 6 calls, got %d: %#v", len(calls), calls)
	}
	if calls[0].Method != "GET" || calls[0].Path != "/api/v2/datasets/"+inputRID+"/branches/master" {
		t.Fatalf("call[0] mismatch: %#v (all calls=%#v)", calls[0], calls)
	}
	if calls[1].Method != "GET" || calls[1].Path != "/api/v2/datasets/"+inputRID+"/readTable" {
		t.Fatalf("call[1] mismatch: %#v (all calls=%#v)", calls[1], calls)
	}
	wantProbePath := "/stream-proxy/api/streams/" + outputRID + "/branches/master/records"
	if calls[2].Method != "GET" || calls[2].Path != wantProbePath {
		t.Fatalf("call[2] mismatch: %#v (all calls=%#v)", calls[2], calls)
	}
	if calls[3].Method != "POST" || calls[3].Path != "/api/v2/datasets/"+outputRID+"/transactions" {
		t.Fatalf("call[3] mismatch: %#v (all calls=%#v)", calls[3], calls)
	}
	if calls[4].Method != "GET" || calls[4].Path != "/api/v2/datasets/"+outputRID+"/transactions" {
		t.Fatalf("call[4] mismatch: %#v (all calls=%#v)", calls[4], calls)
	}

	wantUploadPath := "/api/v2/datasets/" + outputRID + "/files/enriched.csv/upload"
	if calls[5].Method != "POST" || calls[5].Path != wantUploadPath {
		t.Fatalf("call[5] mismatch: %#v (all calls=%#v)", calls[5], calls)
	}

	uploads := mock.Uploads()
	if len(uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d: %#v", len(uploads), uploads)
	}
	if uploads[0].DatasetRID != outputRID || uploads[0].TxnID != preTxnID || uploads[0].FilePath != "enriched.csv" {
		t.Fatalf("unexpected upload metadata: %#v", uploads[0])
	}
}

func TestRunFoundry_WritesToStreamProxyWhenOutputIsStream(t *testing.T) {
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
	mock.CreateStream(outputRID)
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

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	calls := mock.Calls()
	if len(calls) != 5 {
		t.Fatalf("expected 5 calls, got %d: %#v", len(calls), calls)
	}
	if calls[0].Method != "GET" || calls[0].Path != "/api/v2/datasets/"+inputRID+"/branches/master" {
		t.Fatalf("call[0] mismatch: %#v (all calls=%#v)", calls[0], calls)
	}
	if calls[1].Method != "GET" || calls[1].Path != "/api/v2/datasets/"+inputRID+"/readTable" {
		t.Fatalf("call[1] mismatch: %#v (all calls=%#v)", calls[1], calls)
	}
	wantProbePath := "/stream-proxy/api/streams/" + outputRID + "/branches/master/records"
	if calls[2].Method != "GET" || calls[2].Path != wantProbePath {
		t.Fatalf("call[2] mismatch: %#v (all calls=%#v)", calls[2], calls)
	}
	wantPublishPath := "/stream-proxy/api/streams/" + outputRID + "/branches/master/jsonRecord"
	if calls[3].Method != "POST" || calls[3].Path != wantPublishPath {
		t.Fatalf("call[3] mismatch: %#v (all calls=%#v)", calls[3], calls)
	}
	if calls[4].Method != "POST" || calls[4].Path != wantPublishPath {
		t.Fatalf("call[4] mismatch: %#v (all calls=%#v)", calls[4], calls)
	}

	recs := mock.StreamRecords(outputRID, "master")
	if len(recs) != 2 {
		t.Fatalf("expected 2 stream records, got %d: %#v", len(recs), recs)
	}
	if recs[0]["email"] != "alice@example.com" || recs[0]["company"] != "example.com" || recs[0]["status"] != "ok" {
		t.Fatalf("unexpected record[0]: %#v", recs[0])
	}
	if recs[1]["email"] != "bob@corp.test" || recs[1]["company"] != "corp.test" || recs[1]["status"] != "ok" {
		t.Fatalf("unexpected record[1]: %#v", recs[1])
	}
}
