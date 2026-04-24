package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/devx"
	internalversion "github.com/palantir/palantir-compute-module-pipeline-search/internal/version"
)

var (
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("63"))
	successStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("42"))
	mutedStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr *os.File) int {
	if len(args) == 0 {
		usage(stdout)
		return 2
	}

	switch args[0] {
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	case "version", "--version":
		_, _ = fmt.Fprintln(stdout, internalversion.Current)
		return 0
	case "new":
		return runNew(args[1:], stdout, stderr)
	case "seed":
		return runSeed(ctx, args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command: %s\n\n", args[0])
		usage(stderr)
		return 2
	}
}

func usage(w *os.File) {
	_, _ = fmt.Fprintf(w, `%s

Usage:
  foundry-devx new [--name NAME --module MODULE --dir DIR]
  foundry-devx seed dataset --csv INPUT.csv --alias-map alias-map.json [--alias input]
  foundry-devx seed stream --csv RECORDS.csv --alias-map alias-map.json [--alias output] --url http://localhost:8080
  foundry-devx version

Commands:
  new           Generate a minimal Go compute-module starter repo
  seed dataset  Copy a CSV into the mock Foundry dataset input layout
  seed stream   Publish CSV rows to a running mock Foundry stream endpoint

`, titleStyle.Render("foundry-devx: local Foundry Compute Module development"))
}

func runNew(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("new", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "Project name")
	module := fs.String("module", "", "Go module path for the generated project")
	dir := fs.String("dir", "", "Output directory (defaults to --name)")
	example := fs.String("example", "minimal", "Starter example to generate")
	pipelineVersion := fs.String("pipeline-version", internalversion.Current, "palantir-compute-module-pipeline-search version to require")
	localReplace := fs.String("local-replace", "", "Optional local replace path for this pipeline module")
	force := fs.Bool("force", false, "Allow writing into a non-empty directory and overwriting generated files")
	interactive := fs.Bool("interactive", false, "Prompt for missing values with huh")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected arg: %s\n", fs.Arg(0))
		return 2
	}

	if (*interactive || shouldPrompt(stdout, stderr)) && (*name == "" || *module == "") {
		if err := promptNew(name, module, dir, example); err != nil {
			_, _ = fmt.Fprintf(stderr, "prompt failed: %v\n", err)
			return 2
		}
	}

	res, err := devx.GenerateProject(devx.GenerateOptions{
		Name:            *name,
		Module:          *module,
		Dir:             *dir,
		Example:         *example,
		PipelineVersion: *pipelineVersion,
		LocalReplace:    *localReplace,
		Force:           *force,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "new failed: %v\n", err)
		return 1
	}

	_, _ = fmt.Fprintf(stdout, "%s %s\n", successStyle.Render("created"), res.Dir)
	_, _ = fmt.Fprintf(stdout, "%s %d files\n", mutedStyle.Render("wrote"), len(res.Files))
	_, _ = fmt.Fprintln(stdout, mutedStyle.Render("next: cd "+shellPath(res.Dir)+" && go test ./..."))
	return 0
}

func runSeed(ctx context.Context, args []string, stdout, stderr *os.File) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "seed requires a target: dataset or stream")
		return 2
	}
	switch args[0] {
	case "dataset":
		return runSeedDataset(args[1:], stdout, stderr)
	case "stream":
		return runSeedStream(ctx, args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown seed target: %s\n", args[0])
		return 2
	}
}

func runSeedDataset(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("seed dataset", flag.ContinueOnError)
	fs.SetOutput(stderr)
	csvPath := fs.String("csv", "", "Input CSV to expose through mock Foundry readTable")
	aliasMap := fs.String("alias-map", "test/fixtures/alias-map.json", "RESOURCE_ALIAS_MAP-compatible JSON file")
	alias := fs.String("alias", "input", "Alias to seed")
	root := fs.String("root", ".local/mock-foundry", "Mock Foundry state root")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	res, err := devx.SeedDataset(devx.SeedDatasetOptions{CSVPath: *csvPath, AliasMap: *aliasMap, Alias: *alias, Root: *root})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "seed dataset failed: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "%s %d rows for %s\n", successStyle.Render("seeded dataset"), res.Rows, res.RID)
	_, _ = fmt.Fprintln(stdout, mutedStyle.Render(res.Path))
	return 0
}

func runSeedStream(ctx context.Context, args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("seed stream", flag.ContinueOnError)
	fs.SetOutput(stderr)
	csvPath := fs.String("csv", "", "CSV records to publish to stream-proxy")
	aliasMap := fs.String("alias-map", "test/fixtures/alias-map.json", "RESOURCE_ALIAS_MAP-compatible JSON file")
	alias := fs.String("alias", "output", "Alias to publish into")
	mockURL := fs.String("url", "", "Running mock Foundry base URL, for example http://localhost:8080")
	token := fs.String("token", "dummy-token", "Bearer token expected by mock Foundry")
	branch := fs.String("branch", "", "Stream branch (defaults to alias branch or master)")
	timeout := fs.Duration("timeout", 30*time.Second, "Publish timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	res, err := devx.SeedStream(ctx, devx.SeedStreamOptions{CSVPath: *csvPath, AliasMap: *aliasMap, Alias: *alias, URL: *mockURL, Token: *token, Branch: *branch})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "seed stream failed: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "%s %d records to %s@%s\n", successStyle.Render("seeded stream"), res.Records, res.RID, res.Branch)
	return 0
}

func promptNew(name, module, dir, example *string) error {
	if *dir == "" && *name != "" {
		*dir = *name
	}
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().Title("Project name").Value(name).Validate(nonEmpty("project name")),
			huh.NewInput().Title("Go module path").Description("Example: github.com/acme/customer-enricher").Value(module).Validate(nonEmpty("module path")),
			huh.NewInput().Title("Output directory").Description("Defaults to project name").Value(dir),
			huh.NewSelect[string]().Title("Starter example").Options(huh.NewOption("Minimal transform", "minimal")).Value(example),
		),
	)
	return form.Run()
}

func nonEmpty(name string) func(string) error {
	return func(v string) error {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s is required", name)
		}
		return nil
	}
}

func shouldPrompt(stdout, stderr *os.File) bool {
	return isTerminal(os.Stdin) && isTerminal(stdout) && isTerminal(stderr)
}

func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && (info.Mode()&os.ModeCharDevice) != 0
}

func shellPath(path string) string {
	if rel, err := filepath.Rel(".", path); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return path
}
