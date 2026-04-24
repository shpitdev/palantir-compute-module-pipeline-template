package devx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/mockfoundry"
)

type RunOptions struct {
	ConfigPath string
	InputPath  string
	OutputMode string
	Rows       int
	Full       bool
	JSON       bool
	Timeout    time.Duration
	WorkDir    string
	Container  bool
}

type LocalRunResult struct {
	Kind                  string           `json:"kind"`
	RunID                 string           `json:"runId"`
	StateDir              string           `json:"stateDir"`
	LogPath               string           `json:"logPath"`
	InputAlias            string           `json:"inputAlias"`
	InputPath             string           `json:"inputPath"`
	InputRID              string           `json:"inputRid"`
	InputRows             int              `json:"inputRows"`
	SampledRows           int              `json:"sampledRows"`
	OutputAlias           string           `json:"outputAlias"`
	OutputRID             string           `json:"outputRid"`
	OutputMode            string           `json:"outputMode"`
	OutputBranch          string           `json:"outputBranch"`
	OutputPath            string           `json:"outputPath,omitempty"`
	OutputRows            int              `json:"outputRows"`
	PreviewRows           [][]string       `json:"previewRows,omitempty"`
	PreviewHeader         []string         `json:"previewHeader,omitempty"`
	StreamRecords         []map[string]any `json:"streamRecords,omitempty"`
	Duration              time.Duration    `json:"duration"`
	Command               []string         `json:"command"`
	Runner                string           `json:"runner"`
	Container             bool             `json:"container"`
	DockerNetworkStrategy string           `json:"dockerNetworkStrategy,omitempty"`
	ConfigPath            string           `json:"configPath,omitempty"`
}

type LastRunManifest struct {
	Result    LocalRunResult `json:"result"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

func Preview(ctx context.Context, opts RunOptions) (LocalRunResult, error) {
	return runLocalFoundry(ctx, "preview", opts)
}

func Build(ctx context.Context, opts RunOptions) (LocalRunResult, error) {
	opts.Full = true
	return runLocalFoundry(ctx, "build", opts)
}

func runLocalFoundry(ctx context.Context, kind string, opts RunOptions) (LocalRunResult, error) {
	workDir := strings.TrimSpace(opts.WorkDir)
	if workDir == "" {
		workDir = "."
	}
	if abs, err := filepath.Abs(workDir); err == nil {
		workDir = abs
	}
	configPath := opts.ConfigPath
	if strings.TrimSpace(configPath) == "" {
		configPath = defaultConfigPath
	}
	if strings.TrimSpace(configPath) != "" && !filepath.IsAbs(configPath) {
		configPath = filepath.Join(workDir, configPath)
	}
	cfg, configPath, err := LoadProjectConfig(configPath)
	if err != nil {
		return LocalRunResult{}, err
	}
	input := cfg.Inputs[0]
	output := cfg.Outputs[0]
	if strings.TrimSpace(opts.InputPath) != "" {
		input.Path = strings.TrimSpace(opts.InputPath)
	}
	inputFSPath := resolvePath(workDir, input.Path)
	mode := strings.ToLower(strings.TrimSpace(output.Mode))
	if strings.TrimSpace(opts.OutputMode) != "" {
		mode = strings.ToLower(strings.TrimSpace(opts.OutputMode))
	}
	if mode != "dataset" && mode != "stream" {
		return LocalRunResult{}, fmt.Errorf("outputs[0].mode must be dataset or stream (got %q)", mode)
	}
	branch := strings.TrimSpace(output.Branch)
	if branch == "" {
		branch = strings.TrimSpace(cfg.MockFoundry.Branch)
	}
	if branch == "" {
		branch = "master"
	}
	inputRowsLimit := opts.Rows
	if inputRowsLimit <= 0 {
		inputRowsLimit = cfg.Preview.Rows
	}
	if kind == "build" || opts.Full {
		inputRowsLimit = 0
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Minute
	}

	preflight := runPreflightOptions{
		Kind:          kind,
		WorkDir:       workDir,
		ConfigPath:    configPath,
		Config:        cfg,
		Input:         input,
		InputFSPath:   inputFSPath,
		Output:        output,
		OutputMode:    mode,
		Container:     opts.Container,
		LocalFallback: "foundry-cmgo build --local-process",
	}
	if err := preflightRun(preflight); err != nil {
		return LocalRunResult{}, err
	}

	runID := time.Now().UTC().Format("20060102T150405.000000000Z")
	stateRoot := filepath.Join(workDir, ".local", "foundry-cmgo", kind+"s", runID)
	inputDir := filepath.Join(stateRoot, "inputs")
	uploadDir := filepath.Join(stateRoot, "uploads")
	if kind == "build" {
		mockRoot := resolvePath(workDir, cfg.MockFoundry.Root)
		inputDir = filepath.Join(mockRoot, "inputs")
		uploadDir = filepath.Join(mockRoot, "uploads")
	}
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return LocalRunResult{}, fmt.Errorf("create input dir: %w", err)
	}
	if err := os.MkdirAll(uploadDir, 0o755); err != nil {
		return LocalRunResult{}, fmt.Errorf("create upload dir: %w", err)
	}

	inputRID := defaultString(strings.TrimSpace(input.Alias), "input")
	outputRID := defaultString(strings.TrimSpace(output.Alias), "output")
	// Stable, Foundry-like local RIDs keep output paths readable and match generated fixtures.
	if input.Alias == "input" {
		inputRID = "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"
	}
	if output.Alias == "output" {
		outputRID = "ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222"
	}

	sampledRaw, totalRows, sampledRows, err := materializeInputCSV(inputFSPath, filepath.Join(inputDir, inputRID+".csv"), inputRowsLimit)
	if err != nil {
		return LocalRunResult{}, err
	}
	_ = sampledRaw

	aliasMapPath := filepath.Join(stateRoot, "alias-map.json")
	tokenPath := filepath.Join(stateRoot, "token.txt")
	if err := os.MkdirAll(stateRoot, 0o755); err != nil {
		return LocalRunResult{}, fmt.Errorf("create state dir: %w", err)
	}
	if err := writeRunAliasMap(aliasMapPath, input.Alias, inputRID, output.Alias, outputRID, branch); err != nil {
		return LocalRunResult{}, err
	}
	if err := os.WriteFile(tokenPath, []byte("dummy-token\n"), 0o644); err != nil {
		return LocalRunResult{}, fmt.Errorf("write token: %w", err)
	}
	if opts.Container {
		if err := preflightContainerStateFiles(tokenPath, aliasMapPath); err != nil {
			return LocalRunResult{}, err
		}
	}

	srv := mockfoundry.New(inputDir, uploadDir)
	srv.RequireBearerToken("dummy-token")
	if mode == "stream" {
		srv.CreateStream(outputRID)
		srv.SetStreamReadTableHeader([]string{"email", "value", "status"})
	}
	mockServer, err := startMockFoundryServer(srv.Handler(), opts.Container)
	if err != nil {
		return LocalRunResult{}, err
	}
	defer mockServer.Close()

	logPath := filepath.Join(stateRoot, "run.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return LocalRunResult{}, fmt.Errorf("create run log: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	cmdArgs := append([]string{}, cfg.Module.Command...)
	cmdArgs = append(cmdArgs, "--input-alias", input.Alias, "--output-alias", output.Alias, "--output-mode", mode)
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	var procOut bytes.Buffer
	mw := io.MultiWriter(logFile, &procOut)

	start := time.Now()
	runner := "local process"
	dockerNetworkStrategy := ""
	if opts.Container {
		runner = "container"
		cmdArgs, dockerNetworkStrategy, err = runContainerModule(ctx, workDir, stateRoot, tokenPath, aliasMapPath, mockServer.ContainerURL, input.Alias, output.Alias, mode, mw)
	} else {
		err = runLocalModule(ctx, workDir, tokenPath, aliasMapPath, mockServer.HostURL, cmdArgs, mw)
	}
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return LocalRunResult{}, fmt.Errorf("%s timed out after %s (log: %s)", kind, opts.Timeout, logPath)
		}
		if opts.Container && looksLikeDockerNetworkFailure(procOut.String()) {
			return LocalRunResult{}, fmt.Errorf("Docker network cannot reach mock Foundry at %s using strategy %q; rerun with --local-process to isolate module logic (log: %s): %w\n%s", mockServer.ContainerURL, dockerNetworkStrategy, logPath, err, tailString(procOut.String(), 1200))
		}
		return LocalRunResult{}, fmt.Errorf("%s command failed: %w (log: %s)\n%s", kind, err, logPath, tailString(procOut.String(), 1200))
	}
	duration := time.Since(start)

	client, err := foundry.NewClient(mockServer.HostURL+"/api", mockServer.HostURL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		return LocalRunResult{}, err
	}
	result := LocalRunResult{
		Kind:                  kind,
		RunID:                 runID,
		StateDir:              stateRoot,
		LogPath:               logPath,
		InputAlias:            input.Alias,
		InputPath:             input.Path,
		InputRID:              inputRID,
		InputRows:             totalRows,
		SampledRows:           sampledRows,
		OutputAlias:           output.Alias,
		OutputRID:             outputRID,
		OutputMode:            mode,
		OutputBranch:          branch,
		Duration:              duration,
		Command:               cmdArgs,
		Runner:                runner,
		Container:             opts.Container,
		DockerNetworkStrategy: dockerNetworkStrategy,
		ConfigPath:            configPath,
	}

	if mode == "stream" {
		recs, err := client.ReadStreamRecords(ctx, outputRID, branch)
		if err != nil {
			return LocalRunResult{}, fmt.Errorf("read stream output: %w", err)
		}
		streamPath, err := writeStreamRecords(stateRoot, outputRID, branch, recs)
		if err != nil {
			return LocalRunResult{}, err
		}
		result.StreamRecords = recs
		result.OutputRows = len(recs)
		result.OutputPath = streamPath
		result.PreviewHeader, result.PreviewRows = streamRecordsTable(recs, 20)
	} else {
		csvBytes, err := client.ReadTableCSV(ctx, outputRID, branch)
		if err != nil {
			return LocalRunResult{}, fmt.Errorf("read dataset output: %w", err)
		}
		header, rows, err := parseCSVPreview(csvBytes, 20)
		if err != nil {
			return LocalRunResult{}, fmt.Errorf("parse dataset output: %w", err)
		}
		result.PreviewHeader = header
		result.PreviewRows = rows
		result.OutputRows = csvRowCount(csvBytes)
		result.OutputPath = filepath.Join(uploadDir, outputRID, "_branches", filesystemName(branch), "_committed", "readTable.csv")
	}

	if err := writeLastRun(workDir, result); err != nil {
		return LocalRunResult{}, err
	}
	return result, nil
}
