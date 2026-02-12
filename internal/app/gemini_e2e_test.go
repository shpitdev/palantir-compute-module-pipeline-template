//go:build gemini_e2e

package app_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich/gemini"
	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
)

func TestRunLocal_RealGemini_EndToEnd(t *testing.T) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		t.Fatalf("GEMINI_API_KEY is required for gemini_e2e tests")
	}
	model := os.Getenv("GEMINI_MODEL")
	if model == "" {
		t.Fatalf("GEMINI_MODEL is required for gemini_e2e tests")
	}
	baseURL := os.Getenv("GEMINI_BASE_URL")

	ctx := context.Background()

	baseDir := t.TempDir()
	if artifactDir := os.Getenv("GEMINI_E2E_ARTIFACT_DIR"); artifactDir != "" {
		if err := os.MkdirAll(artifactDir, 0755); err != nil {
			t.Fatalf("create GEMINI_E2E_ARTIFACT_DIR: %v", err)
		}
		baseDir = artifactDir
	}

	// Use synthetic emails only (public repo); we just validate API/tooling assumptions.
	in := "email\nalice@example.com\nbob@example.com\n"

	run := func(t *testing.T, captureAudit bool) {
		t.Helper()

		enricher, err := gemini.New(ctx, gemini.Config{
			APIKey:       apiKey,
			Model:        model,
			BaseURL:      baseURL,
			CaptureAudit: captureAudit,
		})
		if err != nil {
			t.Fatalf("create gemini enricher: %v", err)
		}

		suffix := "audit_off"
		if captureAudit {
			suffix = "audit_on"
		}
		inputPath := filepath.Join(baseDir, "emails_"+suffix+".csv")
		outputPath := filepath.Join(baseDir, "enriched_"+suffix+".csv")

		if err := os.WriteFile(inputPath, []byte(in), 0644); err != nil {
			t.Fatalf("write input: %v", err)
		}

		if err := app.RunLocal(ctx, inputPath, outputPath, pipeline.Options{
			Workers:        1,
			MaxRetries:     2,
			RequestTimeout: 30 * time.Second,
			FailFast:       true,
		}, enricher); err != nil {
			t.Fatalf("RunLocal failed: %v", err)
		}

		b, err := os.ReadFile(outputPath)
		if err != nil {
			t.Fatalf("read output: %v", err)
		}

		cr := csv.NewReader(bytes.NewReader(b))
		records, err := cr.ReadAll()
		if err != nil {
			t.Fatalf("parse output csv: %v", err)
		}
		if len(records) != 1+2 {
			t.Fatalf("expected header + 2 rows, got %d records", len(records))
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

		for i := 1; i < len(records); i++ {
			row := records[i]
			if row[0] == "" {
				t.Fatalf("row[%d] missing email: %#v", i, row)
			}
			if row[6] != "ok" {
				t.Fatalf("row[%d] expected status ok, got %#v", i, row)
			}
			if row[7] != "" {
				t.Fatalf("row[%d] expected empty error, got %#v", i, row)
			}
			if row[8] != model {
				t.Fatalf("row[%d] expected model %q, got %#v", i, model, row)
			}

			if !captureAudit {
				if row[9] != "" || row[10] != "" {
					t.Fatalf("row[%d] expected empty audit fields, got sources=%q web_search_queries=%q", i, row[9], row[10])
				}
				continue
			}

			// Audit fields are JSON-encoded arrays when non-empty.
			if row[9] != "" {
				var urls []string
				if err := json.Unmarshal([]byte(row[9]), &urls); err != nil {
					t.Fatalf("row[%d] invalid sources json: %v (val=%q)", i, err, row[9])
				}
			}
			if row[10] != "" {
				var qs []string
				if err := json.Unmarshal([]byte(row[10]), &qs); err != nil {
					t.Fatalf("row[%d] invalid web_search_queries json: %v (val=%q)", i, err, row[10])
				}
			}
		}
	}

	t.Run("AuditOn", func(t *testing.T) { run(t, true) })
	t.Run("AuditOff", func(t *testing.T) { run(t, false) })
}
