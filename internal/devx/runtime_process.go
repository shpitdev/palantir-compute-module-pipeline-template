package devx

import (
	"context"
	"io"
	"os"
	"os/exec"
)

func runLocalModule(ctx context.Context, workDir, tokenPath, aliasMapPath, foundryURL string, cmdArgs []string, output io.Writer) error {
	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"FOUNDRY_URL="+foundryURL,
		"BUILD2_TOKEN="+tokenPath,
		"RESOURCE_ALIAS_MAP="+aliasMapPath,
	)
	cmd.Stdout = output
	cmd.Stderr = output
	return cmd.Run()
}
