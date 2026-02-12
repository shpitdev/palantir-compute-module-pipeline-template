package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich/gemini"
	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/app"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry/keepalive"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/redact"
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
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", redact.Secrets(err.Error()))
		return 2
	}
	gemEnv, err := loadGeminiConfigFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", redact.Secrets(err.Error()))
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
		_, _ = fmt.Fprintf(os.Stderr, "gemini config error: %s\n", redact.Secrets(err.Error()))
		return 2
	}

	if err := app.RunLocal(ctx, inputPath, outputPath, pipeline.Options{
		Workers:        workers,
		MaxRetries:     maxRetries,
		RequestTimeout: requestTimeout,
		RateLimitRPS:   rateLimitRPS,
		FailFast:       failFast,
	}, enricher); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "local run failed: %s\n", redact.Secrets(err.Error()))
		return 1
	}
	return 0
}

func runFoundry(ctx context.Context, args []string) int {
	pipeEnv, err := loadPipelineOptionsFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", redact.Secrets(err.Error()))
		return 2
	}
	gemEnv, err := loadGeminiConfigFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config error: %s\n", redact.Secrets(err.Error()))
		return 2
	}

	fs := flag.NewFlagSet("foundry", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	inputAlias := fs.String("input-alias", "input", "Alias name for the input dataset in RESOURCE_ALIAS_MAP")
	outputAlias := fs.String("output-alias", "output", "Alias name for the output dataset in RESOURCE_ALIAS_MAP")
	outputFilename := fs.String("output-filename", "enriched.csv", "Filename to upload into the output dataset transaction (dataset mode only)")
	outputWriteMode := fs.String("output-write-mode", "auto", "Output write mode: auto|dataset|stream (auto probes stream-proxy first)")
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
		_, _ = fmt.Fprintf(os.Stderr, "foundry env error: %s\n", redact.Secrets(err.Error()))
		return 2
	}

	// Some Foundry stacks expect the compute module to poll the internal runtime (GET_JOB_URI) in order
	// to be considered responsive. The TypeScript SDK does this automatically in the background.
	//
	// In pipeline mode we still run our pipeline logic autonomously; this background loop is only to
	// satisfy the runtime health expectations and to avoid leaving internal jobs un-acked.
	cmCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	keepAlive := false
	if ccfg, ok, err := keepalive.LoadConfigFromEnv(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "compute module client config error: %s\n", redact.Secrets(err.Error()))
		return 2
	} else if ok {
		keepAlive = true
		go func() {
			_ = keepalive.RunLoop(cmCtx, ccfg, func(context.Context, keepalive.Job) ([]byte, error) {
				// We don't expose any interactive functions; acknowledge any internal jobs so they don't block routing.
				return []byte("ok"), nil
			})
		}()
	}

	enricher, err := gemini.New(ctx, gemini.Config{
		APIKey:       gemEnv.APIKey,
		Model:        *geminiModel,
		BaseURL:      *geminiBaseURL,
		CaptureAudit: *captureAudit,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gemini config error: %s\n", redact.Secrets(err.Error()))
		return 2
	}

	// Pipeline execution: run once on container start.
	if err := app.RunFoundry(ctx, env, *inputAlias, *outputAlias, *outputFilename, *outputWriteMode, pipeline.Options{
		Workers:        *workers,
		MaxRetries:     *maxRetries,
		RequestTimeout: *requestTimeout,
		RateLimitRPS:   *rateLimitRPS,
		FailFast:       *failFast,
	}, enricher); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "foundry run failed: %s\n", redact.Secrets(err.Error()))
		return 1
	}

	// In Foundry Compute Modules, the container is expected to be long-running. If we exit after
	// producing output, the module will be restarted and the pipeline may re-run, duplicating stream
	// records. Keep the process alive when Foundry has injected the internal module endpoints.
	if keepAlive {
		_, _ = fmt.Fprintln(os.Stdout, "foundry run complete; keeping module alive")
		select {}
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
  GEMINI_API_KEY        Gemini API key (required). Can be the literal key or a file path containing the key.
  GEMINI_MODEL          Gemini model name (required)
  GEMINI_BASE_URL       Optional base URL override (proxies/testing)
  GEMINI_CAPTURE_AUDIT  If set to true/1, include sources/queries in output

Environment (Foundry Sources, optional):
  SOURCE_CREDENTIALS         File path containing a JSON dictionary of Source credentials (injected by Foundry)
  GEMINI_SOURCE_API_NAME     Source API name to read GEMINI key from SOURCE_CREDENTIALS
  GEMINI_SOURCE_SECRET_NAME  Secret name within that Source (if omitted, this binary will try to infer)

`)
}

func loadGeminiConfigFromEnv() (gemini.Config, error) {
	apiKey, err := loadGeminiAPIKey()
	if err != nil {
		return gemini.Config{}, err
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

func loadGeminiAPIKey() (string, error) {
	// 1) Prefer explicit env var injection.
	if v := strings.TrimSpace(os.Getenv("GEMINI_API_KEY")); v != "" {
		key, err := readValueOrFile(v, "GEMINI_API_KEY")
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(key) == "" {
			return "", fmt.Errorf("GEMINI_API_KEY is required")
		}
		return key, nil
	}

	// 2) Fall back to Foundry Sources credentials (recommended by Foundry docs).
	creds, err := foundry.LoadSourceCredentialsFromEnv()
	if err != nil {
		return "", fmt.Errorf("GEMINI_API_KEY is required (or configure Sources and provide SOURCE_CREDENTIALS): %w", err)
	}

	sourceAPIName := strings.TrimSpace(os.Getenv("GEMINI_SOURCE_API_NAME"))
	secretName := strings.TrimSpace(os.Getenv("GEMINI_SOURCE_SECRET_NAME"))

	if sourceAPIName != "" {
		// Fully specified: source + optional secret name.
		key, ok, err := pickSecretFromSource(creds, sourceAPIName, secretName)
		if err != nil {
			return "", err
		}
		if ok {
			return key, nil
		}
		return "", fmt.Errorf(
			"could not find Gemini API key in SOURCE_CREDENTIALS for source %q (available secrets: %v); set GEMINI_SOURCE_SECRET_NAME or GEMINI_API_KEY",
			sourceAPIName,
			creds.SecretNames(sourceAPIName),
		)
	}

	// Not specified: attempt inference.
	if len(creds) == 1 {
		var onlySource string
		for k := range creds {
			onlySource = k
		}
		key, ok, err := pickSecretFromSource(creds, onlySource, secretName)
		if err != nil {
			return "", err
		}
		if ok {
			return key, nil
		}
		return "", fmt.Errorf(
			"could not infer Gemini API key from SOURCE_CREDENTIALS (source %q has secrets %v); set GEMINI_SOURCE_SECRET_NAME or GEMINI_API_KEY",
			onlySource,
			creds.SecretNames(onlySource),
		)
	}

	// Multiple sources: try to find a single unambiguous match.
	type match struct {
		source string
		secret string
		value  string
	}
	var matches []match
	for _, srcName := range creds.SourceNames() {
		key, ok, _ := pickSecretFromSource(creds, srcName, secretName)
		if ok {
			// pickSecretFromSource uses a deterministic preference order; record which key it picked for debugging.
			picked := secretName
			if picked == "" {
				picked = "<inferred>"
			}
			matches = append(matches, match{source: srcName, secret: picked, value: key})
		}
	}
	if len(matches) == 1 {
		return matches[0].value, nil
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("multiple Sources in SOURCE_CREDENTIALS could provide the Gemini API key; set GEMINI_SOURCE_API_NAME (available sources: %v)", creds.SourceNames())
	}
	return "", fmt.Errorf("could not infer Gemini API key from SOURCE_CREDENTIALS; set GEMINI_SOURCE_API_NAME and GEMINI_SOURCE_SECRET_NAME (available sources: %v)", creds.SourceNames())
}

func pickSecretFromSource(creds foundry.SourceCredentials, sourceAPIName, preferredSecretName string) (string, bool, error) {
	sourceAPIName = strings.TrimSpace(sourceAPIName)
	if sourceAPIName == "" {
		return "", false, fmt.Errorf("GEMINI_SOURCE_API_NAME is empty")
	}
	if _, ok := creds[sourceAPIName]; !ok {
		return "", false, fmt.Errorf("SOURCE_CREDENTIALS missing source %q (available sources: %v)", sourceAPIName, creds.SourceNames())
	}

	// If the user specifies the secret name, respect it.
	if strings.TrimSpace(preferredSecretName) != "" {
		if v, ok := creds.GetSecret(sourceAPIName, preferredSecretName); ok {
			return v, true, nil
		}
		return "", false, nil
	}

	// Otherwise try common API key-ish names.
	for _, candidate := range []string{"GEMINI_API_KEY", "GeminiAPIKey", "apiKey", "api_key", "apikey"} {
		if v, ok := creds.GetSecret(sourceAPIName, candidate); ok {
			return v, true, nil
		}
	}

	// If there's exactly one secret, assume it's the API key.
	names := creds.SecretNames(sourceAPIName)
	if len(names) == 1 {
		if v, ok := creds.GetSecret(sourceAPIName, names[0]); ok {
			return v, true, nil
		}
	}
	return "", false, nil
}

func readValueOrFile(v string, varName string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", nil
	}
	// Some platforms (including Foundry) may inject secrets as file paths.
	// Treat values that look like paths as file paths and read the contents.
	if looksLikePath(v) {
		b, err := os.ReadFile(v)
		if err != nil {
			return "", fmt.Errorf("read %s file: %w", varName, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return v, nil
}

func looksLikePath(v string) bool {
	// Prefer conservative heuristics to avoid accidentally treating a literal key as a file name.
	return strings.HasPrefix(v, "/") || strings.HasPrefix(v, "./") || strings.HasPrefix(v, "../") || strings.Contains(v, "/")
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
