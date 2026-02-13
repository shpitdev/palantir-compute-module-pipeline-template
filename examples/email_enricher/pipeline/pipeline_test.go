package pipeline_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"strings"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
)

type testEnricher struct{}

func (testEnricher) Enrich(_ context.Context, email string) (enrich.Result, error) {
	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(email)), "@error.test") {
		return enrich.Result{}, errors.New("test enricher: forced error")
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

func TestEnrichEmails(t *testing.T) {
	rows, err := pipeline.EnrichEmails(context.Background(), []string{" alice@example.com ", "bob@error.test", ""}, testEnricher{}, pipeline.Options{})
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

	cr := csv.NewReader(bytes.NewReader(buf.Bytes()))
	records, err := cr.ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected header + 1 row, got %d records", len(records))
	}

	wantHeader := pipeline.Header()
	if len(records[0]) != len(wantHeader) {
		t.Fatalf("unexpected header len: got %d want %d", len(records[0]), len(wantHeader))
	}
	for i := range wantHeader {
		if records[0][i] != wantHeader[i] {
			t.Fatalf("header[%d]: want %q got %q", i, wantHeader[i], records[0][i])
		}
	}

	if records[1][0] != "alice@example.com" || records[1][6] != "ok" {
		t.Fatalf("unexpected row: %#v", records[1])
	}
}

func TestReadCSV(t *testing.T) {
	in := strings.Join([]string{
		strings.Join(pipeline.Header(), ","),
		"alice@example.com,https://www.linkedin.com/in/alice,Example,Alice,desc,high,ok,,gemini,s1,q1",
		"",
	}, "\n")

	rows, err := pipeline.ReadCSV(strings.NewReader(in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Email != "alice@example.com" || rows[0].Status != "ok" || rows[0].Company != "Example" {
		t.Fatalf("unexpected row: %#v", rows[0])
	}
}
