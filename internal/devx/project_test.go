package devx

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateProjectCreatesCompilableStarters(t *testing.T) {
	t.Parallel()

	for _, example := range []string{"minimal", "dataset", "stream"} {
		example := example
		t.Run(example, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			projectDir := filepath.Join(root, "starter")
			repoRoot := repoRoot(t)

			res, err := GenerateProject(GenerateOptions{
				Name:         "starter",
				Module:       "example.com/acme/starter",
				Dir:          projectDir,
				Example:      example,
				LocalReplace: repoRoot,
			})
			if err != nil {
				t.Fatalf("GenerateProject failed: %v", err)
			}
			if res.Dir != projectDir {
				t.Fatalf("Dir = %q, want %q", res.Dir, projectDir)
			}
			assertGeneratedFiles(t, projectDir, example)

			run(t, projectDir, "go", "test", "./...")
			if example != "minimal" {
				run(t, projectDir, "go", "run", "./cmd/compute-module", "local", "--input", "data/input.csv", "--output", "out/output.csv")
				if _, err := os.Stat(filepath.Join(projectDir, "out/output.csv")); err != nil {
					t.Fatalf("expected local output csv: %v", err)
				}
			}
		})
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

func assertGeneratedFiles(t *testing.T, projectDir, example string) {
	t.Helper()
	files := []string{"go.mod", "README.md", "Dockerfile", "cmd/compute-module/main.go", "processor/processor.go", "processor/processor_test.go", ".gitignore"}
	if example != "minimal" {
		files = append(files,
			"docker-compose.local.yml",
			"foundry-cmgo.yaml",
			"pipeline/csv.go",
			"pipeline/pipeline_test.go",
			"test/fixtures/alias-map.json",
			"test/fixtures/token.txt",
			"data/input.csv",
		)
	}
	for _, rel := range files {
		if _, err := os.Stat(filepath.Join(projectDir, rel)); err != nil {
			t.Fatalf("expected generated file %s: %v", rel, err)
		}
	}
	if example != "minimal" {
		dockerfile, err := os.ReadFile(filepath.Join(projectDir, "Dockerfile"))
		if err != nil {
			t.Fatalf("read Dockerfile: %v", err)
		}
		text := string(dockerfile)
		for _, want := range []string{"--platform=linux/amd64", "debian:bookworm-slim", "ca-certificates", "USER 5000:5000"} {
			if !strings.Contains(text, want) {
				t.Fatalf("Dockerfile missing %q:\n%s", want, text)
			}
		}
		if strings.Contains(text, "alpine") {
			t.Fatalf("Dockerfile should not use Alpine:\n%s", text)
		}
	}
}

func run(t *testing.T, dir string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, out)
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
