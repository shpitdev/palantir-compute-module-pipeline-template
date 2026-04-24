package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
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
	case "preview":
		return runPreview(ctx, args[1:], stdout, stderr)
	case "build":
		return runBuild(ctx, args[1:], stdout, stderr)
	case "inspect":
		return runInspect(args[1:], stdout, stderr)
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
  foundry-cmgo new [--name NAME --module MODULE --dir DIR --example minimal|dataset|stream]
  foundry-cmgo preview [--rows 1000 --input data/input.csv --output-mode dataset|stream]
  foundry-cmgo build [--input data/input.csv --output-mode dataset|stream] [--container=false|--local-process]
  foundry-cmgo inspect last
  foundry-cmgo seed dataset --csv INPUT.csv --alias-map alias-map.json [--alias input]
  foundry-cmgo seed stream --csv RECORDS.csv --alias-map alias-map.json [--alias output] --url http://localhost:8080
  foundry-cmgo version

Commands:
  new           Generate a Go compute-module starter repo
  preview       Run the module against sampled local input using in-process mock Foundry
  build         Run the container against full local input and commit local mock Foundry output
  inspect last  Show the last preview/build output location and summary
  seed dataset  Copy a CSV into the mock Foundry dataset input layout
  seed stream   Publish CSV rows to a running mock Foundry stream endpoint

`, titleStyle.Render("foundry-cmgo: local Foundry Compute Module development"))
}

func runNew(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("new", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "Project name")
	module := fs.String("module", "", "Go module path for the generated project")
	dir := fs.String("dir", "", "Output directory (defaults to --name)")
	example := fs.String("example", "minimal", "Starter example to generate: minimal|dataset|stream")
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

func runPreview(ctx context.Context, args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("preview", flag.ContinueOnError)
	fs.SetOutput(stderr)
	config := fs.String("config", "", "Project config path (defaults to foundry-cmgo.yaml with inference fallback)")
	rows := fs.Int("rows", 0, "Preview input row limit (defaults to config, usually 1000)")
	input := fs.String("input", "", "Override configured input CSV")
	outputMode := fs.String("output-mode", "", "Override output mode: dataset|stream")
	full := fs.Bool("full", false, "Use full input instead of sampled preview input")
	jsonOut := fs.Bool("json", false, "Print machine-readable JSON")
	timeout := fs.Duration("timeout", 2*time.Minute, "Preview timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected arg: %s\n", fs.Arg(0))
		return 2
	}

	res, err := devx.Preview(ctx, devx.RunOptions{
		ConfigPath: *config,
		InputPath:  *input,
		OutputMode: *outputMode,
		Rows:       *rows,
		Full:       *full,
		JSON:       *jsonOut,
		Timeout:    *timeout,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "preview failed: %v\n", err)
		return 1
	}
	return renderRunResult(stdout, stderr, res, *jsonOut)
}

func runBuild(ctx context.Context, args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	fs.SetOutput(stderr)
	config := fs.String("config", "", "Project config path (defaults to foundry-cmgo.yaml with inference fallback)")
	input := fs.String("input", "", "Override configured input CSV")
	outputMode := fs.String("output-mode", "", "Override output mode: dataset|stream")
	container := fs.Bool("container", true, "Run build through Docker by default to catch container/Foundry parity issues (set false for local process)")
	localProcess := fs.Bool("local-process", false, "Disable the default Docker build/run path and execute module.command directly")
	jsonOut := fs.Bool("json", false, "Print machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Minute, "Build timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected arg: %s\n", fs.Arg(0))
		return 2
	}

	res, err := devx.Build(ctx, devx.RunOptions{
		ConfigPath: *config,
		InputPath:  *input,
		OutputMode: *outputMode,
		Container:  *container && !*localProcess,
		JSON:       *jsonOut,
		Timeout:    *timeout,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "build failed: %v\n", err)
		return 1
	}
	return renderRunResult(stdout, stderr, res, *jsonOut)
}

func runInspect(args []string, stdout, stderr *os.File) int {
	if len(args) == 0 || args[0] != "last" {
		_, _ = fmt.Fprintln(stderr, "inspect requires target: last")
		return 2
	}
	manifest, err := devx.InspectLast()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "inspect last failed: %v\n", err)
		return 1
	}
	res := manifest.Result
	_, _ = fmt.Fprintf(stdout, "%s %s\n\n", titleStyle.Render("Foundry CMGO Last Run"), successStyle.Render("✓"))
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "Kind\t%s\n", res.Kind)
	_, _ = fmt.Fprintf(tw, "Updated\t%s\n", manifest.UpdatedAt.Local().Format(time.RFC3339))
	_, _ = fmt.Fprintf(tw, "Input\t%s\t%s\t%d rows\n", res.InputAlias, res.InputPath, res.SampledRows)
	_, _ = fmt.Fprintf(tw, "Output\t%s\t%s @ %s\t%d rows\n", res.OutputAlias, res.OutputMode, res.OutputBranch, res.OutputRows)
	if res.OutputPath != "" {
		_, _ = fmt.Fprintf(tw, "Output path\t%s\n", res.OutputPath)
	}
	_, _ = fmt.Fprintf(tw, "State\t%s\n", res.StateDir)
	_, _ = fmt.Fprintf(tw, "Log\t%s\n", res.LogPath)
	_ = tw.Flush()
	return 0
}

func renderRunResult(stdout, _ *os.File, res devx.LocalRunResult, jsonOut bool) int {
	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(res); err != nil {
			return 1
		}
		return 0
	}

	name := "Foundry CMGO Preview"
	next := "foundry-cmgo build"
	if res.Kind == "build" {
		name = "Foundry CMGO Build"
		next = "foundry-cmgo inspect last"
	}
	_, _ = fmt.Fprintf(stdout, "%s %s\n\n", titleStyle.Render(name), successStyle.Render("✓"))
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "Transform\t%s\n", strings.Join(res.Command, " "))
	inputShape := fmt.Sprintf("%d rows", res.SampledRows)
	if res.Kind == "preview" && res.SampledRows < res.InputRows {
		inputShape = fmt.Sprintf("sampled %d/%d rows", res.SampledRows, res.InputRows)
	}
	_, _ = fmt.Fprintf(tw, "Input\t%s\t%s\t%s\n", res.InputAlias, res.InputPath, inputShape)
	_, _ = fmt.Fprintf(tw, "Output\t%s\t%s @ %s\t%d rows\n", res.OutputAlias, res.OutputMode, res.OutputBranch, res.OutputRows)
	runner := res.Runner
	if runner == "" {
		runner = "local process"
	}
	if res.Kind == "build" && res.Container {
		runner += " (default)"
	}
	_, _ = fmt.Fprintf(tw, "Runtime\t%s\t%s\n", runner, res.Duration.Round(time.Millisecond))
	if res.OutputPath != "" {
		label := "Output records"
		if res.OutputMode == "dataset" {
			label = "Committed dataset"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\n", label, res.OutputPath)
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintln(stdout)
	if res.Kind == "build" && res.Container {
		_, _ = fmt.Fprintln(stdout, mutedStyle.Render("note: build runs through Docker by default to catch Foundry container issues; use --container=false or --local-process for the faster local-process path."))
		_, _ = fmt.Fprintln(stdout)
	}
	renderTable(stdout, res.PreviewHeader, res.PreviewRows)
	_, _ = fmt.Fprintln(stdout)
	_, _ = fmt.Fprintf(stdout, "%d rows output · state: %s · log: %s\n", res.OutputRows, res.StateDir, res.LogPath)
	_, _ = fmt.Fprintln(stdout, mutedStyle.Render("next: "+next))
	return 0
}

func renderTable(w *os.File, header []string, rows [][]string) {
	if len(header) == 0 {
		_, _ = fmt.Fprintln(w, mutedStyle.Render("(no output rows)"))
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for i, h := range header {
		if i > 0 {
			_, _ = fmt.Fprint(tw, "\t")
		}
		_, _ = fmt.Fprint(tw, h)
	}
	_, _ = fmt.Fprintln(tw)
	for _, row := range rows {
		for i := range header {
			if i > 0 {
				_, _ = fmt.Fprint(tw, "\t")
			}
			if i < len(row) {
				_, _ = fmt.Fprint(tw, row[i])
			}
		}
		_, _ = fmt.Fprintln(tw)
	}
	_ = tw.Flush()
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
			huh.NewSelect[string]().Title("Starter example").Options(
				huh.NewOption("Minimal transform", "minimal"),
				huh.NewOption("Dataset pipeline", "dataset"),
				huh.NewOption("Stream pipeline", "stream"),
			).Value(example),
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
