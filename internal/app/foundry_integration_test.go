package app_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestRunFoundry_StreamMode_ContinuesWhenPriorOutputReadForbidden(t *testing.T) {
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
	mock.CreateStream(outputRID)

	base := mock.Handler()
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate an output stream where the publisher can write via stream-proxy
		// but cannot read the backing dataset via readTable.
		if r.Method == http.MethodGet && r.URL.Path == "/api/v2/datasets/"+outputRID+"/readTable" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"errorCode":       "PERMISSION_DENIED",
				"errorName":       "ReadTableDatasetPermissionDenied",
				"errorInstanceId": "00000000-0000-0000-0000-000000000000",
			})
			return
		}
		base.ServeHTTP(w, r)
	})

	ts := httptest.NewServer(wrapped)
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

	if err := app.RunFoundry(context.Background(), env, "input", "output", "", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	recs := mock.StreamRecords(outputRID, "master")
	if len(recs) != 2 {
		t.Fatalf("expected 2 published stream records, got %d: %#v", len(recs), recs)
	}

	byEmail := map[string]map[string]any{}
	for _, r := range recs {
		email, _ := r["email"].(string)
		byEmail[email] = r
	}
	if _, ok := byEmail["alice@example.com"]; !ok {
		t.Fatalf("missing alice record: %#v", byEmail)
	}
	if _, ok := byEmail["bob@corp.test"]; !ok {
		t.Fatalf("missing bob record: %#v", byEmail)
	}
	if byEmail["alice@example.com"]["company"] != "example.com" {
		t.Fatalf("alice record company: want %q got %#v", "example.com", byEmail["alice@example.com"]["company"])
	}
	if byEmail["bob@corp.test"]["company"] != "corp.test" {
		t.Fatalf("bob record company: want %q got %#v", "corp.test", byEmail["bob@corp.test"]["company"])
	}
	if _, ok := byEmail["alice@example.com"]["run_id"]; !ok {
		t.Fatalf("alice record missing run_id: %#v", byEmail["alice@example.com"])
	}
	if _, ok := byEmail["alice@example.com"]["written_at"]; !ok {
		t.Fatalf("alice record missing written_at: %#v", byEmail["alice@example.com"])
	}
}

func TestRunFoundry_StreamMode_UsesStreamCacheWhenDatasetReadForbidden(t *testing.T) {
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
	mock.CreateStream(outputRID)

	base := mock.Handler()
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Publishing and reading via stream-proxy is allowed; readTable on the backing dataset is forbidden.
		if r.Method == http.MethodGet && r.URL.Path == "/api/v2/datasets/"+outputRID+"/readTable" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"errorCode":       "PERMISSION_DENIED",
				"errorName":       "ReadTableDatasetPermissionDenied",
				"errorInstanceId": "00000000-0000-0000-0000-000000000000",
			})
			return
		}
		base.ServeHTTP(w, r)
	})

	ts := httptest.NewServer(wrapped)
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

	// Seed the stream with an OK row for alice so incremental can skip re-enrichment.
	client, err := foundry.NewClient(env.Services.APIGateway, env.Services.StreamProxy, env.Token, env.DefaultCAPath)
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}
	if err := client.PublishStreamJSONRecord(context.Background(), outputRID, "master", map[string]any{
		"email":      "alice@example.com",
		"company":    "example.com",
		"confidence": "seed",
		"status":     "ok",
	}); err != nil {
		t.Fatalf("seed stream record: %v", err)
	}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "", "auto", pipeline.Options{}, testEnricher{}); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}

	recs := mock.StreamRecords(outputRID, "master")
	if len(recs) != 2 {
		t.Fatalf("expected 2 total records (seed + bob), got %d: %#v", len(recs), recs)
	}

	countByEmail := map[string]int{}
	for _, r := range recs {
		email, _ := r["email"].(string)
		countByEmail[email]++
	}
	if countByEmail["alice@example.com"] != 1 {
		t.Fatalf("alice republished unexpectedly: %#v", countByEmail)
	}
	if countByEmail["bob@corp.test"] != 1 {
		t.Fatalf("bob not published exactly once: %#v", countByEmail)
	}
}

func TestRunFoundry_StreamMode_ParsesWrappedStreamRecordsResponse(t *testing.T) {
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
	mock.CreateStream(outputRID)

	base := mock.Handler()
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap stream-proxy records in an envelope and a per-record wrapper.
		if r.Method == http.MethodGet && r.URL.Path == "/stream-proxy/api/streams/"+outputRID+"/branches/master/records" {
			rr := httptest.NewRecorder()
			base.ServeHTTP(rr, r)
			if rr.Code/100 != 2 {
				w.WriteHeader(rr.Code)
				_, _ = w.Write(rr.Body.Bytes())
				return
			}

			var raw []map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("bad mock stream records"))
				return
			}
			values := make([]map[string]any, 0, len(raw))
			for _, rec := range raw {
				values = append(values, map[string]any{"record": rec})
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"values":        values,
				"nextPageToken": nil,
			})
			return
		}
		base.ServeHTTP(w, r)
	})

	ts := httptest.NewServer(wrapped)
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

	client, err := foundry.NewClient(env.Services.APIGateway, env.Services.StreamProxy, env.Token, env.DefaultCAPath)
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}
	if err := client.PublishStreamJSONRecord(context.Background(), outputRID, "master", map[string]any{
		"email":  "alice@example.com",
		"status": "ok",
	}); err != nil {
		t.Fatalf("seed stream record: %v", err)
	}

	enricher := &countingEnricher{}
	if err := app.RunFoundry(context.Background(), env, "input", "output", "", "auto", pipeline.Options{}, enricher); err != nil {
		t.Fatalf("RunFoundry failed: %v", err)
	}
	if enricher.count("alice@example.com") != 0 {
		t.Fatalf("expected alice to be cached from stream records, got %d calls", enricher.count("alice@example.com"))
	}
	if enricher.count("bob@corp.test") != 1 {
		t.Fatalf("expected bob to be enriched once, got %d calls", enricher.count("bob@corp.test"))
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

func TestRunFoundry_IncrementalStreamSkipsCachedRows(t *testing.T) {
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

	enricher := &countingEnricher{}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, enricher); err != nil {
		t.Fatalf("first RunFoundry failed: %v", err)
	}
	if enricher.count("alice@example.com") != 1 || enricher.count("bob@corp.test") != 1 {
		t.Fatalf("unexpected first-run call counts: alice=%d bob=%d", enricher.count("alice@example.com"), enricher.count("bob@corp.test"))
	}
	if recs := mock.StreamRecords(outputRID, "master"); len(recs) != 2 {
		t.Fatalf("expected 2 stream records after first run, got %d: %#v", len(recs), recs)
	}

	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, enricher); err != nil {
		t.Fatalf("second RunFoundry failed: %v", err)
	}
	if enricher.count("alice@example.com") != 1 {
		t.Fatalf("expected alice to be cached on second run, got %d calls", enricher.count("alice@example.com"))
	}
	if enricher.count("bob@corp.test") != 1 {
		t.Fatalf("expected bob to be cached on second run, got %d calls", enricher.count("bob@corp.test"))
	}
	if recs := mock.StreamRecords(outputRID, "master"); len(recs) != 2 {
		t.Fatalf("expected 2 stream records after second run, got %d: %#v", len(recs), recs)
	}

	writeInput("email\nalice@example.com\nbob@corp.test\ncarol@new.test\n")
	if err := app.RunFoundry(context.Background(), env, "input", "output", "enriched.csv", "auto", pipeline.Options{}, enricher); err != nil {
		t.Fatalf("third RunFoundry failed: %v", err)
	}
	if enricher.count("carol@new.test") != 1 {
		t.Fatalf("expected carol to be enriched once, got %d calls", enricher.count("carol@new.test"))
	}
	if recs := mock.StreamRecords(outputRID, "master"); len(recs) != 3 {
		t.Fatalf("expected 3 stream records after adding one email, got %d: %#v", len(recs), recs)
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
	// Stream mode reads incremental cache from stream-proxy records.
	if calls[3].Method != "GET" || calls[3].Path != wantProbePath {
		t.Fatalf("call[3] mismatch: %#v (all calls=%#v)", calls[3], calls)
	}
	wantPublishPath := "/stream-proxy/api/streams/" + outputRID + "/branches/master/jsonRecord"
	if calls[4].Method != "POST" || calls[4].Path != wantPublishPath {
		t.Fatalf("call[4] mismatch: %#v (all calls=%#v)", calls[4], calls)
	}
	if calls[5].Method != "POST" || calls[5].Path != wantPublishPath {
		t.Fatalf("call[5] mismatch: %#v (all calls=%#v)", calls[5], calls)
	}

	recs := mock.StreamRecords(outputRID, "master")
	if len(recs) != 2 {
		t.Fatalf("expected 2 stream records, got %d: %#v", len(recs), recs)
	}

	gotEmails := []string{
		recs[0]["email"].(string),
		recs[1]["email"].(string),
	}
	slices.Sort(gotEmails)
	if !slices.Equal(gotEmails, []string{"alice@example.com", "bob@corp.test"}) {
		t.Fatalf("unexpected stream emails: %v", gotEmails)
	}
	for _, rec := range recs {
		email, _ := rec["email"].(string)
		status, _ := rec["status"].(string)
		if status != "ok" {
			t.Fatalf("expected status ok for %q, got %#v", email, rec)
		}
		runID, _ := rec["run_id"].(string)
		if strings.TrimSpace(runID) == "" {
			t.Fatalf("expected run_id to be set for %q, got %#v", email, rec)
		}
		writtenAt, _ := rec["written_at"].(string)
		if strings.TrimSpace(writtenAt) == "" {
			t.Fatalf("expected written_at to be set for %q, got %#v", email, rec)
		}
		if _, err := time.Parse(time.RFC3339Nano, writtenAt); err != nil {
			t.Fatalf("expected written_at to be RFC3339Nano for %q, got %q (%v)", email, writtenAt, err)
		}
		if email == "alice@example.com" && rec["company"] != "example.com" {
			t.Fatalf("unexpected alice record: %#v", rec)
		}
		if email == "bob@corp.test" && rec["company"] != "corp.test" {
			t.Fatalf("unexpected bob record: %#v", rec)
		}
	}
}

type blockingStreamEnricher struct {
	releaseSlow chan struct{}
	startedSlow chan struct{}
	startedOnce sync.Once
}

func (e *blockingStreamEnricher) Enrich(_ context.Context, email string) (enrich.Result, error) {
	if strings.EqualFold(strings.TrimSpace(email), "slow@example.com") {
		e.startedOnce.Do(func() {
			close(e.startedSlow)
		})
		<-e.releaseSlow
	}
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

func TestRunFoundry_StreamPublishesBeforeAllRowsFinish(t *testing.T) {
	t.Parallel()

	inputRID := "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"
	outputRID := "ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222"

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(inputDir, inputRID+".csv"),
		[]byte("email\nslow@example.com\nfast@example.com\n"),
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

	releaseSlow := make(chan struct{})
	startedSlow := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- app.RunFoundry(
			context.Background(),
			env,
			"input",
			"output",
			"enriched.csv",
			"stream",
			pipeline.Options{Workers: 2},
			&blockingStreamEnricher{releaseSlow: releaseSlow, startedSlow: startedSlow},
		)
	}()

	select {
	case <-startedSlow:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow enrichment to start")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		recs := mock.StreamRecords(outputRID, "master")
		if len(recs) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	recs := mock.StreamRecords(outputRID, "master")
	if len(recs) != 1 {
		t.Fatalf("expected 1 published record before releasing slow email, got %d (%#v)", len(recs), recs)
	}
	if recs[0]["email"] != "fast@example.com" {
		t.Fatalf("expected fast email to publish first, got %#v", recs[0])
	}

	close(releaseSlow)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunFoundry failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream run to finish")
	}

	recs = mock.StreamRecords(outputRID, "master")
	if len(recs) != 2 {
		t.Fatalf("expected 2 stream records after completion, got %d (%#v)", len(recs), recs)
	}
}
