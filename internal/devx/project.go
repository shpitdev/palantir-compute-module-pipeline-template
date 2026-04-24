package devx

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	internalversion "github.com/palantir/palantir-compute-module-pipeline-search/internal/version"
)

//go:embed templates/*
var starterTemplates embed.FS

var modulePathPattern = regexp.MustCompile(`^[A-Za-z0-9._~\-]+(/[A-Za-z0-9._~\-]+)*$`)

type GenerateOptions struct {
	Name            string
	Module          string
	Dir             string
	Example         string
	PipelineVersion string
	LocalReplace    string
	Force           bool
}

type GenerateResult struct {
	Dir   string
	Files []string
}

type templateData struct {
	Name            string
	Module          string
	Example         string
	OutputMode      string
	IsStream        bool
	PipelineVersion string
	LocalReplace    string
}

func GenerateProject(opts GenerateOptions) (GenerateResult, error) {
	opts.Name = strings.TrimSpace(opts.Name)
	opts.Module = strings.TrimSpace(opts.Module)
	opts.Dir = strings.TrimSpace(opts.Dir)
	opts.Example = strings.TrimSpace(opts.Example)
	opts.PipelineVersion = strings.TrimPrefix(strings.TrimSpace(opts.PipelineVersion), "v")
	opts.LocalReplace = strings.TrimSpace(opts.LocalReplace)

	if opts.Example == "" {
		opts.Example = "minimal"
	}
	root, outputMode, err := templateRoot(opts.Example)
	if err != nil {
		return GenerateResult{}, err
	}
	if opts.Name == "" {
		return GenerateResult{}, fmt.Errorf("project name is required")
	}
	if opts.Module == "" {
		return GenerateResult{}, fmt.Errorf("module path is required")
	}
	if !modulePathPattern.MatchString(opts.Module) || !strings.Contains(opts.Module, "/") {
		return GenerateResult{}, fmt.Errorf("module path %q must look like a Go module path, for example github.com/acme/my-module", opts.Module)
	}
	if opts.Dir == "" {
		opts.Dir = opts.Name
	}
	if opts.PipelineVersion == "" {
		opts.PipelineVersion = internalversion.Current
	}

	absDir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return GenerateResult{}, err
	}
	if err := ensureWritableTarget(absDir, opts.Force); err != nil {
		return GenerateResult{}, err
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return GenerateResult{}, fmt.Errorf("create project dir: %w", err)
	}

	data := templateData{
		Name:            opts.Name,
		Module:          opts.Module,
		Example:         opts.Example,
		OutputMode:      outputMode,
		IsStream:        outputMode == "stream",
		PipelineVersion: opts.PipelineVersion,
		LocalReplace:    filepath.ToSlash(opts.LocalReplace),
	}
	var written []string
	err = fs.WalkDir(starterTemplates, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if strings.HasSuffix(rel, ".tmpl") {
			rel = strings.TrimSuffix(rel, ".tmpl")
		}
		if filepath.Base(rel) == "gitignore" {
			rel = filepath.Join(filepath.Dir(rel), ".gitignore")
		}

		raw, err := starterTemplates.ReadFile(path)
		if err != nil {
			return err
		}
		content, err := renderTemplate(path, raw, data)
		if err != nil {
			return err
		}

		dst := filepath.Join(absDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if !opts.Force {
			if _, err := os.Stat(dst); err == nil {
				return fmt.Errorf("refusing to overwrite %s (use --force)", dst)
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		if err := os.WriteFile(dst, content, 0o644); err != nil {
			return err
		}
		written = append(written, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return GenerateResult{}, err
	}
	return GenerateResult{Dir: absDir, Files: written}, nil
}

func templateRoot(example string) (root string, outputMode string, err error) {
	switch example {
	case "minimal":
		return "templates/minimal", "dataset", nil
	case "dataset":
		return "templates/foundry", "dataset", nil
	case "stream":
		return "templates/foundry", "stream", nil
	default:
		return "", "", fmt.Errorf("unsupported example %q (expected minimal|dataset|stream)", example)
	}
}

func ensureWritableTarget(dir string, force bool) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect project dir: %w", err)
	}
	if len(entries) > 0 && !force {
		return fmt.Errorf("target directory %s is not empty (use --force)", dir)
	}
	return nil
}

func renderTemplate(name string, raw []byte, data templateData) ([]byte, error) {
	t, err := template.New(filepath.Base(name)).Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("parse template %s: %w", name, err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("render template %s: %w", name, err)
	}
	return buf.Bytes(), nil
}
