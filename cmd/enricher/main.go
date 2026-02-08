package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich/gemini"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/pipeline"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/util"
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
	pipeEnv, err := loadPipelineOptionsFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}
	gemEnv, err := loadGeminiConfigFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}

	fs := flag.NewFlagSet("local", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var inputPath string
	var outputPath string
	var workers int
	var maxRetries int
	var requestTimeout time.Duration
	var rateLimitRPS float64
	var failFast bool
	var geminiModel string
	var geminiBaseURL string
	var captureAudit bool

	fs.StringVar(&inputPath, "input", "", "Input CSV file path (must include an 'email' column)")
	fs.StringVar(&outputPath, "output", "", "Output CSV file path")
	fs.IntVar(&workers, "workers", pipeEnv.Workers, "Number of concurrent enrichment workers (env: WORKERS)")
	fs.IntVar(&maxRetries, "max-retries", pipeEnv.MaxRetries, "Max retries per email for transient failures (env: MAX_RETRIES)")
	fs.DurationVar(&requestTimeout, "request-timeout", pipeEnv.RequestTimeout, "Per-email request timeout (env: REQUEST_TIMEOUT)")
	fs.Float64Var(&rateLimitRPS, "rate-limit-rps", pipeEnv.RateLimitRPS, "Global request rate limit (RPS), 0 disables (env: RATE_LIMIT_RPS)")
	fs.BoolVar(&failFast, "fail-fast", pipeEnv.FailFast, "Fail fast on first enrichment error (env: FAIL_FAST)")
	fs.StringVar(&geminiModel, "gemini-model", gemEnv.Model, "Gemini model name (env: GEMINI_MODEL)")
	fs.StringVar(&geminiBaseURL, "gemini-base-url", gemEnv.BaseURL, "Gemini API base URL override (env: GEMINI_BASE_URL)")
	fs.BoolVar(&captureAudit, "capture-audit", gemEnv.CaptureAudit, "Capture sources/queries into output (env: GEMINI_CAPTURE_AUDIT)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if inputPath == "" || outputPath == "" {
		_, _ = fmt.Fprintln(os.Stderr, "local requires --input and --output")
		return 2
	}

	enricher, err := gemini.New(ctx, gemini.Config{
		APIKey:       gemEnv.APIKey,
		Model:        geminiModel,
		BaseURL:      geminiBaseURL,
		CaptureAudit: captureAudit,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gemini config error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}

	if err := app.RunLocal(ctx, inputPath, outputPath, pipeline.Options{
		Workers:        workers,
		MaxRetries:     maxRetries,
		RequestTimeout: requestTimeout,
		RateLimitRPS:   rateLimitRPS,
		FailFast:       failFast,
	}, enricher); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "local run failed: %s\n", util.RedactSecrets(err.Error()))
		return 1
	}
	return 0
}

func runFoundry(ctx context.Context, args []string) int {
	pipeEnv, err := loadPipelineOptionsFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}
	gemEnv, err := loadGeminiConfigFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}

	fs := flag.NewFlagSet("foundry", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	inputAlias := fs.String("input-alias", "input", "Alias name for the input dataset in RESOURCE_ALIAS_MAP")
	outputAlias := fs.String("output-alias", "output", "Alias name for the output dataset in RESOURCE_ALIAS_MAP")
	outputFilename := fs.String("output-filename", "enriched.csv", "Filename to upload into the output dataset transaction")
	workers := fs.Int("workers", pipeEnv.Workers, "Number of concurrent enrichment workers (env: WORKERS)")
	maxRetries := fs.Int("max-retries", pipeEnv.MaxRetries, "Max retries per email for transient failures (env: MAX_RETRIES)")
	requestTimeout := fs.Duration("request-timeout", pipeEnv.RequestTimeout, "Per-email request timeout (env: REQUEST_TIMEOUT)")
	rateLimitRPS := fs.Float64("rate-limit-rps", pipeEnv.RateLimitRPS, "Global request rate limit (RPS), 0 disables (env: RATE_LIMIT_RPS)")
	failFast := fs.Bool("fail-fast", pipeEnv.FailFast, "Fail fast on first enrichment error (env: FAIL_FAST)")
	geminiModel := fs.String("gemini-model", gemEnv.Model, "Gemini model name (env: GEMINI_MODEL)")
	geminiBaseURL := fs.String("gemini-base-url", gemEnv.BaseURL, "Gemini API base URL override (env: GEMINI_BASE_URL)")
	captureAudit := fs.Bool("capture-audit", gemEnv.CaptureAudit, "Capture sources/queries into output (env: GEMINI_CAPTURE_AUDIT)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	env, err := foundry.LoadEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "foundry env error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}

	enricher, err := gemini.New(ctx, gemini.Config{
		APIKey:       gemEnv.APIKey,
		Model:        *geminiModel,
		BaseURL:      *geminiBaseURL,
		CaptureAudit: *captureAudit,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gemini config error: %s\n", util.RedactSecrets(err.Error()))
		return 2
	}

	if err := app.RunFoundry(ctx, env, *inputAlias, *outputAlias, *outputFilename, pipeline.Options{
		Workers:        *workers,
		MaxRetries:     *maxRetries,
		RequestTimeout: *requestTimeout,
		RateLimitRPS:   *rateLimitRPS,
		FailFast:       *failFast,
	}, enricher); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "foundry run failed: %s\n", util.RedactSecrets(err.Error()))
		return 1
	}
	return 0
}

func usage(w *os.File) {
	_, _ = fmt.Fprintf(w, `enricher: pipeline-mode Foundry Compute Module (local + Foundry modes)

Usage:
  enricher <command> [flags]

Commands:
  local    Run against a local input CSV (Gemini required)
  foundry  Run in Foundry/pipeline mode (uses BUILD2_TOKEN + RESOURCE_ALIAS_MAP)

Examples:
  enricher local --input emails.csv --output enriched.csv

Environment (foundry):
  FOUNDRY_URL         Foundry base URL (e.g. https://<stack>.palantirfoundry.com)
  BUILD2_TOKEN        File path containing a bearer token
  RESOURCE_ALIAS_MAP  File path containing alias -> {rid, branch} JSON

Environment (Gemini):
  GEMINI_API_KEY        Gemini API key (required)
  GEMINI_MODEL          Gemini model name (required)
  GEMINI_BASE_URL       Optional base URL override (proxies/testing)
  GEMINI_CAPTURE_AUDIT  If set to true/1, include sources/queries in output

`)
}

func loadGeminiConfigFromEnv() (gemini.Config, error) {
	apiKey := strings.TrimSpace(os.Getenv("GEMINI_API_KEY"))
	if apiKey == "" {
		return gemini.Config{}, fmt.Errorf("GEMINI_API_KEY is required")
	}

	captureAudit, err := envBool("GEMINI_CAPTURE_AUDIT")
	if err != nil {
		return gemini.Config{}, err
	}

	return gemini.Config{
		APIKey:       apiKey,
		Model:        strings.TrimSpace(os.Getenv("GEMINI_MODEL")),
		BaseURL:      strings.TrimSpace(os.Getenv("GEMINI_BASE_URL")),
		CaptureAudit: captureAudit,
	}, nil
}

func loadPipelineOptionsFromEnv() (pipeline.Options, error) {
	workers, err := envInt("WORKERS", 10)
	if err != nil {
		return pipeline.Options{}, err
	}
	maxRetries, err := envInt("MAX_RETRIES", 3)
	if err != nil {
		return pipeline.Options{}, err
	}
	requestTimeout, err := envDuration("REQUEST_TIMEOUT", 30*time.Second)
	if err != nil {
		return pipeline.Options{}, err
	}
	failFast, err := envBool("FAIL_FAST")
	if err != nil {
		return pipeline.Options{}, err
	}
	rateLimitRPS, err := envFloat("RATE_LIMIT_RPS", 0)
	if err != nil {
		return pipeline.Options{}, err
	}

	return pipeline.Options{
		Workers:        workers,
		MaxRetries:     maxRetries,
		RequestTimeout: requestTimeout,
		RateLimitRPS:   rateLimitRPS,
		FailFast:       failFast,
	}, nil
}

func envInt(varName string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(varName))
	if v == "" {
		return fallback, nil
	}
	out, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", varName, v, err)
	}
	return out, nil
}

func envFloat(varName string, fallback float64) (float64, error) {
	v := strings.TrimSpace(os.Getenv(varName))
	if v == "" {
		return fallback, nil
	}
	out, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", varName, v, err)
	}
	return out, nil
}

func envDuration(varName string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(varName))
	if v == "" {
		return fallback, nil
	}
	out, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", varName, v, err)
	}
	return out, nil
}

func envBool(varName string) (bool, error) {
	v := strings.TrimSpace(os.Getenv(varName))
	if v == "" {
		return false, nil
	}
	out, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s=%q: %w", varName, v, err)
	}
	return out, nil
}
