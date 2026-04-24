package devx

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type runPreflightOptions struct {
	Kind          string
	WorkDir       string
	ConfigPath    string
	Config        ProjectConfig
	Input         ResourceConfig
	InputFSPath   string
	Output        ResourceConfig
	OutputMode    string
	Container     bool
	LocalFallback string
}

func preflightRun(opts runPreflightOptions) error {
	if len(opts.Config.Inputs) != 1 {
		return fmt.Errorf("foundry-cmgo currently supports exactly one input; found %d entries in inputs", len(opts.Config.Inputs))
	}
	if len(opts.Config.Outputs) != 1 {
		return fmt.Errorf("foundry-cmgo currently supports exactly one output; found %d entries in outputs", len(opts.Config.Outputs))
	}
	if strings.TrimSpace(opts.Input.Alias) == "" {
		return fmt.Errorf("inputs[0].alias is required")
	}
	if strings.TrimSpace(opts.Input.Path) == "" {
		return fmt.Errorf("inputs[0].path is required")
	}
	if err := readableFile(opts.InputFSPath); err != nil {
		return fmt.Errorf("input alias %q points to missing/unreadable file %s (resolved: %s): update foundry-cmgo.yaml inputs[0].path or pass --input: %w", opts.Input.Alias, opts.Input.Path, opts.InputFSPath, err)
	}
	if opts.OutputMode != "dataset" && opts.OutputMode != "stream" {
		return fmt.Errorf("outputs[0].mode must be dataset or stream (got %q)", opts.OutputMode)
	}
	if len(opts.Config.Module.Command) == 0 || strings.TrimSpace(opts.Config.Module.Command[0]) == "" {
		return fmt.Errorf("module.command is required")
	}
	if err := executableAvailable(opts.WorkDir, opts.Config.Module.Command[0]); err != nil {
		return fmt.Errorf("module.command[0] %q is not executable: %w", opts.Config.Module.Command[0], err)
	}
	if opts.Kind == "build" && opts.Container {
		if err := preflightDocker(opts.WorkDir, opts.LocalFallback); err != nil {
			return err
		}
	}
	return nil
}

func preflightDocker(workDir, localFallback string) error {
	dockerfile := filepath.Join(workDir, "Dockerfile")
	if err := readableFile(dockerfile); err != nil {
		return fmt.Errorf("Dockerfile is required for the default container build but %s is not readable: %w; run %s for a faster non-container check", dockerfile, err, localFallback)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("Docker is required for the default container build, but docker CLI was not found: %w. Install/start Docker, or run `%s` for a faster non-container check", err, localFallback)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "info")
	cmd.Dir = workDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			msg = ": " + tailString(msg, 800)
		}
		return fmt.Errorf("Docker is required for the default container build, but `docker info` failed%s. Start Docker, or run `%s` for a faster non-container check", msg, localFallback)
	}
	return nil
}

func preflightContainerStateFiles(paths ...string) error {
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("container state file %s is not readable: %w", path, err)
		}
		if info.IsDir() {
			return fmt.Errorf("container state path %s is a directory, expected a file", path)
		}
		if info.Mode().Perm()&0o004 == 0 {
			return fmt.Errorf("container state file %s must be world-readable because the generated container runs as numeric user 5000:5000", path)
		}
	}
	return nil
}

func executableAvailable(workDir, cmdName string) error {
	cmdName = strings.TrimSpace(cmdName)
	if cmdName == "" {
		return fmt.Errorf("empty command")
	}
	if strings.ContainsRune(cmdName, filepath.Separator) || strings.Contains(cmdName, "/") {
		path := cmdName
		if !filepath.IsAbs(path) {
			path = filepath.Join(workDir, path)
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return fmt.Errorf("%s is a directory", path)
		}
		if info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("%s is not executable", path)
		}
		return nil
	}
	_, err := exec.LookPath(cmdName)
	return err
}

func readableFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("is a directory")
	}
	return nil
}

func resolvePath(workDir, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(workDir, path)
}
