package devx

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type dockerNetworkStrategy struct {
	Name       string
	RunArgs    []string
	FoundryURL string
}

func selectDockerNetworkStrategy(foundryURL string) dockerNetworkStrategy {
	return dockerNetworkStrategy{
		Name:       "host",
		RunArgs:    []string{"--network", "host"},
		FoundryURL: foundryURL,
	}
}

func runContainerModule(ctx context.Context, workDir, stateRoot, tokenPath, aliasMapPath, foundryURL, inputAlias, outputAlias, outputMode string, output io.Writer) ([]string, string, error) {
	imageTag := "foundry-cmgo-local:" + sanitizeDockerTag(filepath.Base(stateRoot))
	contextDir, err := prepareDockerBuildContext(workDir, stateRoot)
	if err != nil {
		return nil, "", err
	}
	buildArgs := []string{"build", "--platform", "linux/amd64", "-t", imageTag, "."}
	if err := runDockerCommand(ctx, contextDir, output, buildArgs...); err != nil {
		return append([]string{"docker"}, buildArgs...), "", err
	}

	strategy := selectDockerNetworkStrategy(foundryURL)
	_, _ = fmt.Fprintf(output, "foundry-cmgo: docker network strategy=%s foundry_url=%s\n", strategy.Name, strategy.FoundryURL)
	containerStateDir := "/foundry-cmgo-run"
	runArgs := []string{"run", "--rm"}
	runArgs = append(runArgs, strategy.RunArgs...)
	runArgs = append(runArgs,
		"-e", "FOUNDRY_URL="+strategy.FoundryURL,
		"-e", "BUILD2_TOKEN="+filepath.ToSlash(filepath.Join(containerStateDir, filepath.Base(tokenPath))),
		"-e", "RESOURCE_ALIAS_MAP="+filepath.ToSlash(filepath.Join(containerStateDir, filepath.Base(aliasMapPath))),
		"-v", stateRoot+":"+containerStateDir+":ro",
		imageTag,
		"foundry",
		"--input-alias", inputAlias,
		"--output-alias", outputAlias,
		"--output-mode", outputMode,
	)
	if err := runDockerCommand(ctx, workDir, output, runArgs...); err != nil {
		return append([]string{"docker"}, runArgs...), strategy.Name, err
	}
	return append([]string{"docker"}, runArgs...), strategy.Name, nil
}

func prepareDockerBuildContext(workDir, stateRoot string) (string, error) {
	goModPath := filepath.Join(workDir, "go.mod")
	raw, err := os.ReadFile(goModPath)
	if err != nil {
		return workDir, nil
	}
	lines := strings.Split(string(raw), "\n")
	needsCopy := false
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[0] == "replace" && fields[2] == "=>" && filepath.IsAbs(fields[3]) {
			needsCopy = true
			break
		}
	}
	if !needsCopy {
		return workDir, nil
	}

	contextDir := filepath.Join(stateRoot, "docker-context")
	if err := copyTree(workDir, contextDir, map[string]struct{}{
		".git":   {},
		".local": {},
		"out":    {},
	}); err != nil {
		return "", fmt.Errorf("prepare docker context: %w", err)
	}

	rewritten := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[0] == "replace" && fields[2] == "=>" && filepath.IsAbs(fields[3]) {
			modulePath := fields[1]
			localDstRel := filepath.ToSlash(filepath.Join("_local_replaces", sanitizePathName(modulePath)))
			localDst := filepath.Join(contextDir, filepath.FromSlash(localDstRel))
			if err := copyTree(fields[3], localDst, map[string]struct{}{
				".git":   {},
				".local": {},
				"out":    {},
			}); err != nil {
				return "", fmt.Errorf("copy local replace %s: %w", modulePath, err)
			}
			rewritten = append(rewritten, fmt.Sprintf("replace %s => ./%s", modulePath, localDstRel))
			continue
		}
		rewritten = append(rewritten, line)
	}
	if err := os.WriteFile(filepath.Join(contextDir, "go.mod"), []byte(strings.Join(rewritten, "\n")), 0o644); err != nil {
		return "", fmt.Errorf("rewrite docker context go.mod: %w", err)
	}
	return contextDir, nil
}

func copyTree(src, dst string, skipNames map[string]struct{}) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if _, skip := skipNames[name]; skip && path != src {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return os.MkdirAll(dst, 0o755)
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

func runDockerCommand(ctx context.Context, workDir string, output io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = workDir
	cmd.Stdout = output
	cmd.Stderr = output
	return cmd.Run()
}

func looksLikeDockerNetworkFailure(output string) bool {
	lower := strings.ToLower(output)
	needles := []string{"connection refused", "connection reset", "no route to host", "network is unreachable", "i/o timeout", "context deadline exceeded"}
	for _, needle := range needles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func sanitizeDockerTag(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "latest"
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" {
		return "run"
	}
	return out
}

func sanitizePathName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "module"
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := strings.Trim(b.String(), "._-")
	if out == "" {
		return "module"
	}
	return out
}
