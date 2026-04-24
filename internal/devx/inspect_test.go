package devx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectLastNoRunAndMalformed(t *testing.T) {
	root := t.TempDir()
	_, err := InspectLastIn(root)
	if err == nil || !strings.Contains(err.Error(), "run `foundry-cmgo preview`") {
		t.Fatalf("expected actionable no-last-run error, got %v", err)
	}

	manifestPath := filepath.Join(root, ".local", "foundry-cmgo", "last-run.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		t.Fatalf("mkdir manifest dir: %v", err)
	}
	if err := os.WriteFile(manifestPath, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write malformed manifest: %v", err)
	}
	_, err = InspectLastIn(root)
	if err == nil || !strings.Contains(err.Error(), "parse last run") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

func TestInspectConfigResolvesGeneratedProject(t *testing.T) {
	root := t.TempDir()
	res, err := GenerateProject(GenerateOptions{
		Name:         "starter",
		Module:       "example.com/acme/starter",
		Dir:          filepath.Join(root, "starter"),
		Example:      "stream",
		LocalReplace: repoRoot(t),
	})
	if err != nil {
		t.Fatalf("GenerateProject failed: %v", err)
	}
	inspection, err := InspectConfig(InspectOptions{WorkDir: res.Dir})
	if err != nil {
		t.Fatalf("InspectConfig failed: %v", err)
	}
	if inspection.Inferred || inspection.ConfigPath == "" {
		t.Fatalf("expected file-backed config, got %+v", inspection)
	}
	if len(inspection.Inputs) != 1 || !filepath.IsAbs(inspection.Inputs[0].ResolvedPath) {
		t.Fatalf("expected resolved input path, got %+v", inspection.Inputs)
	}
	if len(inspection.Outputs) != 1 || inspection.Outputs[0].Mode != "stream" {
		t.Fatalf("expected stream output, got %+v", inspection.Outputs)
	}
}

func TestInspectOutputsNoLastRunIsHumanFriendly(t *testing.T) {
	root := t.TempDir()
	inspection, err := InspectOutputs(InspectOptions{WorkDir: root})
	if err != nil {
		t.Fatalf("InspectOutputs failed: %v", err)
	}
	if inspection.HasLastRun || !strings.HasSuffix(inspection.LastRunManifest, filepath.Join(".local", "foundry-cmgo", "last-run.json")) {
		t.Fatalf("unexpected no-last-run inspection: %+v", inspection)
	}
}
