package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/pipeline"
)

func main() {
	ctx := context.Background()

	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}

	switch os.Args[1] {
	case "help", "-h", "--help":
		usage(os.Stdout)
		return
	case "local":
		os.Exit(runLocal(ctx, os.Args[2:]))
	case "foundry":
		os.Exit(runFoundry(ctx, os.Args[2:]))
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		usage(os.Stderr)
		os.Exit(2)
	}
}

func runLocal(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("local", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var inputPath string
	var outputPath string
	fs.StringVar(&inputPath, "input", "", "Input CSV file path (must include an 'email' column)")
	fs.StringVar(&outputPath, "output", "", "Output CSV file path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if inputPath == "" || outputPath == "" {
		_, _ = fmt.Fprintln(os.Stderr, "local requires --input and --output")
		return 2
	}

	// MVP: always use the stub enricher to stay hermetic (Gemini implementation comes later).
	if err := app.RunLocal(ctx, inputPath, outputPath, pipeline.Options{}, enrich.Stub{}); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "local run failed: %v\n", err)
		return 1
	}
	return 0
}

func runFoundry(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("foundry", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	inputAlias := fs.String("input-alias", "input", "Alias name for the input dataset in RESOURCE_ALIAS_MAP")
	outputAlias := fs.String("output-alias", "output", "Alias name for the output dataset in RESOURCE_ALIAS_MAP")
	outputFilename := fs.String("output-filename", "enriched.csv", "Filename to upload into the output dataset transaction")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	env, err := foundry.LoadEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "foundry env error: %v\n", err)
		return 2
	}

	// MVP: always use the stub enricher to keep default runs hermetic.
	if err := app.RunFoundry(ctx, env, *inputAlias, *outputAlias, *outputFilename, pipeline.Options{}, enrich.Stub{}); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "foundry run failed: %v\n", err)
		return 1
	}
	return 0
}

func usage(w *os.File) {
	_, _ = fmt.Fprintf(w, `enricher: pipeline-mode Foundry Compute Module (local + Foundry modes)

Usage:
  enricher <command> [flags]

Commands:
  local    Run against a local input CSV (no Foundry required)
  foundry  Run in Foundry/pipeline mode (uses BUILD2_TOKEN + RESOURCE_ALIAS_MAP)

Examples:
  enricher local --input emails.csv --output enriched.csv

Environment (foundry):
  FOUNDRY_URL         Foundry base URL (e.g. https://<stack>.palantirfoundry.com)
  BUILD2_TOKEN        File path containing a bearer token
  RESOURCE_ALIAS_MAP  File path containing alias -> {rid, branch} JSON

`)
}
