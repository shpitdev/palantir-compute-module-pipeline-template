package mockfoundry_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
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

func TestMockFoundry_ReadTableUsesBranchScopedCommittedViews(t *testing.T) {
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
	datasetRID := "ri.foundry.main.dataset.12121212-1212-1212-1212-121212121212"

	masterCSV := []byte("email\nmaster@example.com\n")
	masterTxn := createUploadCommit(t, ctx, client, datasetRID, "master", "enriched.csv", masterCSV)

	devCSV := []byte("email\ndev@example.com\n")
	devTxn := createUploadCommit(t, ctx, client, datasetRID, "dev", "enriched.csv", devCSV)
	if masterTxn == devTxn {
		t.Fatalf("expected unique transaction rids, got %q", masterTxn)
	}

	gotMaster, err := client.ReadTableCSV(ctx, datasetRID, "master")
	if err != nil {
		t.Fatalf("read master branch: %v", err)
	}
	if !bytes.Equal(gotMaster, masterCSV) {
		t.Fatalf("master readTable mismatch:\n--- got ---\n%s\n--- want ---\n%s\n", gotMaster, masterCSV)
	}

	gotDev, err := client.ReadTableCSV(ctx, datasetRID, "dev")
	if err != nil {
		t.Fatalf("read dev branch: %v", err)
	}
	if !bytes.Equal(gotDev, devCSV) {
		t.Fatalf("dev readTable mismatch:\n--- got ---\n%s\n--- want ---\n%s\n", gotDev, devCSV)
	}
}

func TestMockFoundry_ReadTableCanPinExactCommittedTransaction(t *testing.T) {
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
	datasetRID := "ri.foundry.main.dataset.34343434-3434-3434-3434-343434343434"

	firstCSV := []byte("email\nfirst@example.com\n")
	firstTxn := createUploadCommit(t, ctx, client, datasetRID, "master", "enriched.csv", firstCSV)

	secondCSV := []byte("email\nsecond@example.com\n")
	_ = createUploadCommit(t, ctx, client, datasetRID, "master", "enriched.csv", secondCSV)

	resp, err := http.Get(ts.URL + "/api/v2/datasets/" + datasetRID + "/readTable?branchName=master&startTransactionRid=" + firstTxn + "&endTransactionRid=" + firstTxn + "&format=CSV")
	if err != nil {
		t.Fatalf("read exact transaction: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), firstCSV) {
		t.Fatalf("exact transaction read mismatch:\n--- got ---\n%s\n--- want ---\n%s\n", buf.Bytes(), firstCSV)
	}
}

func TestMockFoundry_OpenTransactionsDoNotAdvanceBranchView(t *testing.T) {
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
	datasetRID := "ri.foundry.main.dataset.56565656-5656-5656-5656-565656565656"

	committed := []byte("email\ncommitted@example.com\n")
	committedTxn := createUploadCommit(t, ctx, client, datasetRID, "master", "enriched.csv", committed)

	openTxn, err := client.CreateTransaction(ctx, datasetRID, "master")
	if err != nil {
		t.Fatalf("create open transaction: %v", err)
	}
	if err := client.UploadFile(ctx, datasetRID, openTxn, "enriched.csv", "text/csv", []byte("email\nopen@example.com\n")); err != nil {
		t.Fatalf("upload to open transaction: %v", err)
	}

	branchTxn, err := client.GetBranchTransactionRID(ctx, datasetRID, "master")
	if err != nil {
		t.Fatalf("get branch transaction: %v", err)
	}
	if branchTxn != committedTxn {
		t.Fatalf("branch head transaction = %q, want committed transaction %q", branchTxn, committedTxn)
	}

	got, err := client.ReadTableCSV(ctx, datasetRID, "master")
	if err != nil {
		t.Fatalf("read committed branch view: %v", err)
	}
	if !bytes.Equal(got, committed) {
		t.Fatalf("open transaction leaked into branch view:\n--- got ---\n%s\n--- want ---\n%s\n", got, committed)
	}
}

func TestMockFoundry_ListTransactionsPreservesBranchForOpenTransactionReuse(t *testing.T) {
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
	datasetRID := "ri.foundry.main.dataset.78787878-7878-7878-7878-787878787878"

	masterTxn, err := client.CreateTransaction(ctx, datasetRID, "master")
	if err != nil {
		t.Fatalf("create master transaction: %v", err)
	}
	featureTxn, err := client.CreateTransaction(ctx, datasetRID, "feature")
	if err != nil {
		t.Fatalf("create feature transaction: %v", err)
	}
	if masterTxn == featureTxn {
		t.Fatalf("expected distinct transactions, got %q", masterTxn)
	}

	got, ok, err := client.FindLatestOpenTransactionForBranch(ctx, datasetRID, "master")
	if err != nil {
		t.Fatalf("find latest open transaction for master: %v", err)
	}
	if !ok {
		t.Fatalf("expected an open transaction for master")
	}
	if got != masterTxn {
		t.Fatalf("latest master open transaction = %q, want %q", got, masterTxn)
	}
}

func TestMockFoundry_MissingDatasetViewIsDistinctFromAuthFailure(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	srv := mockfoundry.New(inputDir, uploadDir)
	srv.RequireBearerToken("dummy-token")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	datasetRID := "ri.foundry.main.dataset.90909090-9090-9090-9090-909090909090"

	resp, err := http.Get(ts.URL + "/api/v2/datasets/" + datasetRID + "/readTable?branchName=master")
	if err != nil {
		t.Fatalf("unauthenticated read: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d want 401", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v2/datasets/"+datasetRID+"/readTable?branchName=master", nil)
	if err != nil {
		t.Fatalf("new read request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dummy-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("authorized missing view read: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing view status=%d want 404", resp.StatusCode)
	}
	var body struct {
		ErrorName string `json:"errorName"`
		ErrorCode string `json:"errorCode"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.ErrorName != "DatasetViewNotFound" || body.ErrorCode != "NOT_FOUND" {
		t.Fatalf("unexpected missing view error: %#v", body)
	}
}

func createUploadCommit(t *testing.T, ctx context.Context, client *foundry.Client, datasetRID, branch, filePath string, csvBytes []byte) string {
	t.Helper()
	txnID, err := client.CreateTransaction(ctx, datasetRID, branch)
	if err != nil {
		t.Fatalf("create transaction branch=%q: %v", branch, err)
	}
	if err := client.UploadFile(ctx, datasetRID, txnID, filePath, "text/csv", csvBytes); err != nil {
		t.Fatalf("upload file branch=%q txn=%q: %v", branch, txnID, err)
	}
	if err := client.CommitTransaction(ctx, datasetRID, txnID); err != nil {
		t.Fatalf("commit transaction branch=%q txn=%q: %v", branch, txnID, err)
	}
	return txnID
}

func TestMockFoundry_StreamReadTableUsesConfiguredHeader(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	uploadDir := t.TempDir()

	srv := mockfoundry.New(inputDir, uploadDir)
	srv.SetStreamReadTableHeader(pipeline.StreamTableHeader())

	rid := "ri.foundry.main.dataset.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	srv.CreateStream(rid)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client, err := foundry.NewClient(ts.URL+"/api", ts.URL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		t.Fatalf("new foundry client: %v", err)
	}

	if err := client.PublishStreamJSONRecord(context.Background(), rid, "master", map[string]any{
		"email":      "alice@example.com",
		"company":    "Example",
		"status":     "ok",
		"run_id":     "run-123",
		"written_at": "2026-04-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("publish stream record: %v", err)
	}

	resp, err := http.Get(ts.URL + "/api/v2/datasets/" + rid + "/readTable?branchName=master")
	if err != nil {
		t.Fatalf("read stream readTable: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}

	records, err := csv.NewReader(resp.Body).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected header + row, got %d records", len(records))
	}
	if !slices.Equal(records[0], pipeline.StreamTableHeader()) {
		t.Fatalf("header mismatch: got %#v want %#v", records[0], pipeline.StreamTableHeader())
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
