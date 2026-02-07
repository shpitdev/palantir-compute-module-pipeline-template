package pipeline_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/pipeline"
)

func TestEnrichEmails(t *testing.T) {
	rows, err := pipeline.EnrichEmails(context.Background(), []string{" alice@example.com ", "bob@error.test", ""}, enrich.Stub{}, pipeline.Options{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	if rows[0].Email != "alice@example.com" || rows[0].Status != "ok" || rows[0].Company != "example.com" {
		t.Fatalf("unexpected row[0]: %#v", rows[0])
	}
	if rows[1].Email != "bob@error.test" || rows[1].Status != "error" || !strings.Contains(rows[1].Error, "forced error") {
		t.Fatalf("unexpected row[1]: %#v", rows[1])
	}
	if rows[2].Status != "error" || rows[2].Error != "empty email" {
		t.Fatalf("unexpected row[2]: %#v", rows[2])
	}
}

func TestWriteCSV(t *testing.T) {
	var buf bytes.Buffer
	err := pipeline.WriteCSV(&buf, []pipeline.Row{{
		Email:  "alice@example.com",
		Status: "ok",
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "email,linkedin_url,company,title,description,confidence,status,error\n") {
		t.Fatalf("unexpected header: %q", out)
	}
	if !strings.Contains(out, "\nalice@example.com,,,,,,"+"ok,\n") {
		t.Fatalf("unexpected body: %q", out)
	}
}
