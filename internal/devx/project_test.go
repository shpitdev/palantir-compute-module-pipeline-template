package devx

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateProjectCreatesCompilableStarter(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	projectDir := filepath.Join(root, "starter")
	repoRoot := repoRoot(t)

	res, err := GenerateProject(GenerateOptions{
		Name:         "starter",
		Module:       "example.com/acme/starter",
		Dir:          projectDir,
		LocalReplace: repoRoot,
	})
	if err != nil {
		t.Fatalf("GenerateProject failed: %v", err)
	}
	if res.Dir != projectDir {
		t.Fatalf("Dir = %q, want %q", res.Dir, projectDir)
	}
	for _, rel := range []string{"go.mod", "README.md", "Dockerfile", "cmd/compute-module/main.go", "processor/processor.go", "processor/processor_test.go", ".gitignore"} {
		if _, err := os.Stat(filepath.Join(projectDir, rel)); err != nil {
			t.Fatalf("expected generated file %s: %v", rel, err)
		}
	}

	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = projectDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated project go test failed: %v\n%s", err, out)
	}
}

func TestGenerateProjectRejectsNonEmptyTargetUnlessForced(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "existing.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := GenerateProject(GenerateOptions{Name: "starter", Module: "example.com/acme/starter", Dir: dir})
	if err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("expected non-empty target error, got %v", err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatal("could not find repo root")
		}
		wd = parent
	}
}
