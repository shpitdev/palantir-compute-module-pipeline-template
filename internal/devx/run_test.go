package devx

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPreviewAndBuildGeneratedDatasetProject(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "starter")
	res, err := GenerateProject(GenerateOptions{
		Name:         "starter",
		Module:       "example.com/acme/starter",
		Dir:          projectDir,
		Example:      "dataset",
		LocalReplace: repoRoot(t),
	})
	if err != nil {
		t.Fatalf("GenerateProject failed: %v", err)
	}

	ctx := context.Background()
	preview, err := Preview(ctx, RunOptions{Rows: 1, Timeout: 2 * time.Minute, WorkDir: res.Dir})
	if err != nil {
		t.Fatalf("Preview failed: %v", err)
	}
	if preview.Kind != "preview" || preview.SampledRows != 1 || preview.OutputRows != 1 {
		t.Fatalf("unexpected preview summary: %+v", preview)
	}
	if preview.OutputMode != "dataset" || preview.OutputPath == "" {
		t.Fatalf("expected dataset output path, got %+v", preview)
	}

	build, err := Build(ctx, RunOptions{Timeout: 2 * time.Minute, WorkDir: res.Dir})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if build.Kind != "build" || build.SampledRows != 2 || build.OutputRows != 2 {
		t.Fatalf("unexpected build summary: %+v", build)
	}
	if _, err := os.Stat(build.OutputPath); err != nil {
		t.Fatalf("expected committed dataset output %s: %v", build.OutputPath, err)
	}
	last, err := InspectLastIn(res.Dir)
	if err != nil {
		t.Fatalf("InspectLast failed: %v", err)
	}
	if last.Result.RunID != build.RunID {
		t.Fatalf("last run = %s, want %s", last.Result.RunID, build.RunID)
	}
}

func TestPreviewGeneratedStreamProject(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "starter")
	res, err := GenerateProject(GenerateOptions{
		Name:         "starter",
		Module:       "example.com/acme/starter",
		Dir:          projectDir,
		Example:      "stream",
		LocalReplace: repoRoot(t),
	})
	if err != nil {
		t.Fatalf("GenerateProject failed: %v", err)
	}

	preview, err := Preview(context.Background(), RunOptions{Rows: 2, Timeout: 2 * time.Minute, WorkDir: res.Dir})
	if err != nil {
		t.Fatalf("Preview failed: %v", err)
	}
	if preview.OutputMode != "stream" || preview.OutputRows != 2 || len(preview.StreamRecords) != 2 || preview.OutputPath == "" {
		t.Fatalf("unexpected stream preview: %+v", preview)
	}
	if _, err := os.Stat(preview.OutputPath); err != nil {
		t.Fatalf("expected stream records output %s: %v", preview.OutputPath, err)
	}
}
