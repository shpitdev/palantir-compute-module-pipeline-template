package app_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/mockfoundry"
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
		Services: foundry.Services{
			APIGateway:  ts.URL + "/api",
			StreamProxy: ts.URL + "/stream-proxy/api",
		},
		Token: "dummy-token",
		Aliases: map[string]foundry.DatasetRef{
			"input":  {RID: inputRID, Branch: "master"},
			"output": {RID: outputRID, Branch: "master"},
		},
	}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	calls := mock.Calls()
	if len(calls) != 8 {
		t.Fatalf("expected 8 calls, got %d: %#v", len(calls), calls)
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
	if calls[3].Path != "/api/v2/datasets/"+outputRID+"/branches/master" {
		t.Fatalf("call[3] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/branches/master", calls[3].Path, calls)
	}
	if calls[4].Path != "/api/v2/datasets/"+outputRID+"/readTable" {
		t.Fatalf("call[4] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/readTable", calls[4].Path, calls)
	}
	if calls[5].Path != "/api/v2/datasets/"+outputRID+"/transactions" {
		t.Fatalf("call[5] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/transactions", calls[5].Path, calls)
	}

	wantUploadPath := "/api/v2/datasets/" + outputRID + "/files/enriched.csv/upload"
	if calls[6].Path != wantUploadPath {
		t.Fatalf("call[6] path: want %q, got %q (all calls=%#v)", wantUploadPath, calls[6].Path, calls)
	}

	commitPrefix := "/api/v2/datasets/" + outputRID + "/transactions/"
	commitSuffix := "/commit"
	if !strings.HasPrefix(calls[7].Path, commitPrefix) || !strings.HasSuffix(calls[7].Path, commitSuffix) {
		t.Fatalf("call[7] path: expected prefix %q and suffix %q, got %q (all calls=%#v)", commitPrefix, commitSuffix, calls[7].Path, calls)
	}
	txnID := strings.TrimSuffix(strings.TrimPrefix(calls[7].Path, commitPrefix), commitSuffix)
	if strings.TrimSpace(txnID) == "" {
		t.Fatalf("call[7] path: failed to extract transaction id from %q", calls[7].Path)
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
	client, err := foundry.NewClient(env.Services.APIGateway, env.Services.StreamProxy, env.Token, env.DefaultCAPath)
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
	if len(calls) != 10 {
		t.Fatalf("expected 10 calls after readTable, got %d: %#v", len(calls), calls)
	}
	if calls[8].Path != "/api/v2/datasets/"+outputRID+"/branches/master" {
		t.Fatalf("call[8] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/branches/master", calls[8].Path, calls)
	}
	if calls[9].Path != "/api/v2/datasets/"+outputRID+"/readTable" {
		t.Fatalf("call[9] path: want %q, got %q (all calls=%#v)", "/api/v2/datasets/"+outputRID+"/readTable", calls[9].Path, calls)
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
	client, err := foundry.NewClient(ts.URL+"/api", ts.URL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}
	preTxnID, err := client.CreateTransaction(context.Background(), outputRID, "master")
	if err != nil {
		t.Fatalf("pre-create output transaction: %v", err)
	}
	beforeCalls := len(mock.Calls())

	env := foundry.Env{
		Services: foundry.Services{
			APIGateway:  ts.URL + "/api",
			StreamProxy: ts.URL + "/stream-proxy/api",
		},
		Token: "dummy-token",
		Aliases: map[string]foundry.DatasetRef{
			"input":  {RID: inputRID, Branch: "master"},
			"output": {RID: outputRID, Branch: "master"},
		},
	}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	calls := mock.Calls()[beforeCalls:]
	if len(calls) != 8 {
		t.Fatalf("expected 8 calls, got %d: %#v", len(calls), calls)
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
	if calls[3].Method != "GET" || calls[3].Path != "/api/v2/datasets/"+outputRID+"/branches/master" {
		t.Fatalf("call[3] mismatch: %#v (all calls=%#v)", calls[3], calls)
	}
	if calls[4].Method != "GET" || calls[4].Path != "/api/v2/datasets/"+outputRID+"/readTable" {
		t.Fatalf("call[4] mismatch: %#v (all calls=%#v)", calls[4], calls)
	}
	if calls[5].Method != "POST" || calls[5].Path != "/api/v2/datasets/"+outputRID+"/transactions" {
		t.Fatalf("call[5] mismatch: %#v (all calls=%#v)", calls[5], calls)
	}
	if calls[6].Method != "GET" || calls[6].Path != "/api/v2/datasets/"+outputRID+"/transactions" {
		t.Fatalf("call[6] mismatch: %#v (all calls=%#v)", calls[6], calls)
	}

	wantUploadPath := "/api/v2/datasets/" + outputRID + "/files/enriched.csv/upload"
	if calls[7].Method != "POST" || calls[7].Path != wantUploadPath {
		t.Fatalf("call[7] mismatch: %#v (all calls=%#v)", calls[7], calls)
	}

	uploads := mock.Uploads()
	if len(uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d: %#v", len(uploads), uploads)
	}
	if uploads[0].DatasetRID != outputRID || uploads[0].TxnID != preTxnID || uploads[0].FilePath != "enriched.csv" {
		t.Fatalf("unexpected upload metadata: %#v", uploads[0])
	}
}

type countingEnricher struct {
	mu    sync.Mutex
	calls map[string]int
}

func (c *countingEnricher) Enrich(_ context.Context, email string) (enrich.Result, error) {
	c.mu.Lock()
	if c.calls == nil {
		c.calls = make(map[string]int)
	}
	c.calls[email]++
	c.mu.Unlock()

	domain := ""
	if at := strings.LastIndex(email, "@"); at >= 0 && at+1 < len(email) {
		domain = email[at+1:]
	}
	return enrich.Result{
		Company:    domain,
		Confidence: "test",
		Model:      "test-model",
	}, nil
}

func (c *countingEnricher) count(email string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[email]
}

func TestRunFoundry_IncrementalDatasetSkipsCachedRows(t *testing.T) {
	t.Parallel()

	inputRID := "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"
	outputRID := "ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222"

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	writeInput := func(content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(inputDir, inputRID+".csv"), []byte(content), 0644); err != nil {
			t.Fatalf("write input csv: %v", err)
		}
	}
	writeInput("email\nalice@example.com\nbob@corp.test\n")

	mock := mockfoundry.New(inputDir, uploadDir)
	mock.RequireBearerToken("dummy-token")
	ts := httptest.NewServer(mock.Handler())
	defer ts.Close()

	env := foundry.Env{
		Services: foundry.Services{
			APIGateway:  ts.URL + "/api",
			StreamProxy: ts.URL + "/stream-proxy/api",
		},
		Token: "dummy-token",
		Aliases: map[string]foundry.DatasetRef{
			"input":  {RID: inputRID, Branch: "master"},
			"output": {RID: outputRID, Branch: "master"},
		},
	}

	enricher := &countingEnricher{}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "dataset", pipeline.Options{}, enricher); err != nil {
		t.Fatalf("first RunFoundry failed: %v", err)
	}
	if enricher.count("alice@example.com") != 1 || enricher.count("bob@corp.test") != 1 {
		t.Fatalf("unexpected first-run call counts: alice=%d bob=%d", enricher.count("alice@example.com"), enricher.count("bob@corp.test"))
	}

	writeInput("email\nalice@example.com\nbob@corp.test\ncarol@new.test\n")

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "dataset", pipeline.Options{}, enricher); err != nil {
		t.Fatalf("second RunFoundry failed: %v", err)
	}

	if enricher.count("alice@example.com") != 1 {
		t.Fatalf("expected alice to be cached on second run, got %d calls", enricher.count("alice@example.com"))
	}
	if enricher.count("bob@corp.test") != 1 {
		t.Fatalf("expected bob to be cached on second run, got %d calls", enricher.count("bob@corp.test"))
	}
	if enricher.count("carol@new.test") != 1 {
		t.Fatalf("expected carol to be enriched once, got %d calls", enricher.count("carol@new.test"))
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
		Services: foundry.Services{
			APIGateway:  ts.URL + "/api",
			StreamProxy: ts.URL + "/stream-proxy/api",
		},
		Token: "dummy-token",
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
