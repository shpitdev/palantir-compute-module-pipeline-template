package devx

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func InspectLast() (LastRunManifest, error) {
	return InspectLastIn(".")
}

func InspectLastIn(workDir string) (LastRunManifest, error) {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		workDir = "."
	}
	path := lastRunPath(workDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return LastRunManifest{}, fmt.Errorf("no previous preview/build run found at %s; run `foundry-cmgo preview` or `foundry-cmgo build` first", path)
		}
		return LastRunManifest{}, fmt.Errorf("read last run %s: %w", path, err)
	}
	var out LastRunManifest
	if err := json.Unmarshal(raw, &out); err != nil {
		return LastRunManifest{}, fmt.Errorf("parse last run %s: %w", path, err)
	}
	return out, nil
}

func writeLastRun(workDir string, result LocalRunResult) error {
	path := lastRunPath(workDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(LastRunManifest{Result: result, UpdatedAt: time.Now().UTC()}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func lastRunPath(workDir string) string {
	return filepath.Join(workDir, ".local", "foundry-cmgo", "last-run.json")
}

func writeRunAliasMap(path, inputAlias, inputRID, outputAlias, outputRID, branch string) error {
	type entry struct {
		RID    string  `json:"rid"`
		Branch *string `json:"branch"`
	}
	b := branch
	doc := map[string]entry{
		inputAlias:  {RID: inputRID},
		outputAlias: {RID: outputRID, Branch: &b},
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return fmt.Errorf("write alias map: %w", err)
	}
	return nil
}

func tailString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return "..." + s[len(s)-max:]
}
