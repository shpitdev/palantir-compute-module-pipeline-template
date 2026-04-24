package devx

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/mockfoundry"
)

func TestSeedDatasetCopiesCSVToAliasRID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	aliasMap := writeAliasMap(t, dir, "input", "ri.foundry.main.dataset.input", "")
	csvPath := filepath.Join(dir, "input.csv")
	if err := os.WriteFile(csvPath, []byte("email,name\na@example.com,Alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := SeedDataset(SeedDatasetOptions{CSVPath: csvPath, AliasMap: aliasMap, Alias: "input", Root: filepath.Join(dir, ".local/mock-foundry")})
	if err != nil {
		t.Fatalf("SeedDataset failed: %v", err)
	}
	if res.Rows != 1 {
		t.Fatalf("Rows = %d, want 1", res.Rows)
	}
	got, err := os.ReadFile(filepath.Join(dir, ".local/mock-foundry/inputs/ri.foundry.main.dataset.input.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "email,name\na@example.com,Alice\n" {
		t.Fatalf("unexpected copied csv: %q", got)
	}
}

func TestSeedStreamPublishesCSVRowsToMockFoundry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	streamRID := "ri.foundry.main.dataset.output"
	aliasMap := writeAliasMap(t, dir, "output", streamRID, "dev")
	csvPath := filepath.Join(dir, "records.csv")
	if err := os.WriteFile(csvPath, []byte("email,status,empty\na@example.com,ok,\nb@example.com,error,\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := mockfoundry.New(filepath.Join(dir, "inputs"), filepath.Join(dir, "uploads"))
	srv.CreateStream(streamRID)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	res, err := SeedStream(context.Background(), SeedStreamOptions{CSVPath: csvPath, AliasMap: aliasMap, Alias: "output", URL: ts.URL, Token: "dummy-token"})
	if err != nil {
		t.Fatalf("SeedStream failed: %v", err)
	}
	if res.Records != 2 || res.Branch != "dev" {
		t.Fatalf("unexpected result: %#v", res)
	}
	recs := srv.StreamRecords(streamRID, "dev")
	if len(recs) != 2 {
		t.Fatalf("stream records = %d, want 2", len(recs))
	}
	if recs[0]["email"] != "a@example.com" || recs[0]["status"] != "ok" || recs[0]["empty"] != nil {
		t.Fatalf("unexpected first record: %#v", recs[0])
	}
}

func writeAliasMap(t *testing.T, dir, alias, rid, branch string) string {
	t.Helper()
	path := filepath.Join(dir, "alias-map.json")
	var branchPtr *string
	if branch != "" {
		branchPtr = &branch
	}
	raw, err := json.Marshal(map[string]any{
		alias: map[string]any{"rid": rid, "branch": branchPtr},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
