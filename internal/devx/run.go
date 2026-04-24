package devx

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	Kind          string           `json:"kind"`
	RunID         string           `json:"runId"`
	StateDir      string           `json:"stateDir"`
	LogPath       string           `json:"logPath"`
	InputAlias    string           `json:"inputAlias"`
	InputPath     string           `json:"inputPath"`
	InputRID      string           `json:"inputRid"`
	InputRows     int              `json:"inputRows"`
	SampledRows   int              `json:"sampledRows"`
	OutputAlias   string           `json:"outputAlias"`
	OutputRID     string           `json:"outputRid"`
	OutputMode    string           `json:"outputMode"`
	OutputBranch  string           `json:"outputBranch"`
	OutputPath    string           `json:"outputPath,omitempty"`
	OutputRows    int              `json:"outputRows"`
	PreviewRows   [][]string       `json:"previewRows,omitempty"`
	PreviewHeader []string         `json:"previewHeader,omitempty"`
	StreamRecords []map[string]any `json:"streamRecords,omitempty"`
	Duration      time.Duration    `json:"duration"`
	Command       []string         `json:"command"`
	Runner        string           `json:"runner"`
	Container     bool             `json:"container"`
	ConfigPath    string           `json:"configPath,omitempty"`
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

func InspectLast() (LastRunManifest, error) {
	return InspectLastIn(".")
}

func InspectLastIn(workDir string) (LastRunManifest, error) {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		workDir = "."
	}
	path := filepath.Join(workDir, ".local", "foundry-cmgo", "last-run.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return LastRunManifest{}, fmt.Errorf("read last run: %w", err)
	}
	var out LastRunManifest
	if err := json.Unmarshal(raw, &out); err != nil {
		return LastRunManifest{}, fmt.Errorf("parse last run: %w", err)
	}
	return out, nil
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
	inputFSPath := input.Path
	if !filepath.IsAbs(inputFSPath) {
		inputFSPath = filepath.Join(workDir, inputFSPath)
	}
	mode := strings.ToLower(strings.TrimSpace(output.Mode))
	if strings.TrimSpace(opts.OutputMode) != "" {
		mode = strings.ToLower(strings.TrimSpace(opts.OutputMode))
	}
	if mode != "dataset" && mode != "stream" {
		return LocalRunResult{}, fmt.Errorf("output mode must be dataset or stream")
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

	runID := time.Now().UTC().Format("20060102T150405.000000000Z")
	stateRoot := filepath.Join(workDir, ".local", "foundry-cmgo", kind+"s", runID)
	inputDir := filepath.Join(stateRoot, "inputs")
	uploadDir := filepath.Join(stateRoot, "uploads")
	if kind == "build" {
		mockRoot := cfg.MockFoundry.Root
		if !filepath.IsAbs(mockRoot) {
			mockRoot = filepath.Join(workDir, mockRoot)
		}
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
	if opts.Container {
		runner = "container"
		cmdArgs, err = runContainerModule(ctx, workDir, stateRoot, tokenPath, aliasMapPath, mockServer.ContainerURL, input.Alias, output.Alias, mode, mw)
	} else {
		err = runLocalModule(ctx, workDir, tokenPath, aliasMapPath, mockServer.HostURL, cmdArgs, mw)
	}
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return LocalRunResult{}, fmt.Errorf("%s timed out after %s (log: %s)", kind, opts.Timeout, logPath)
		}
		return LocalRunResult{}, fmt.Errorf("%s command failed: %w (log: %s)\n%s", kind, err, logPath, tailString(procOut.String(), 1200))
	}
	duration := time.Since(start)

	client, err := foundry.NewClient(mockServer.HostURL+"/api", mockServer.HostURL+"/stream-proxy/api", "dummy-token", "")
	if err != nil {
		return LocalRunResult{}, err
	}
	result := LocalRunResult{
		Kind:         kind,
		RunID:        runID,
		StateDir:     stateRoot,
		LogPath:      logPath,
		InputAlias:   input.Alias,
		InputPath:    input.Path,
		InputRID:     inputRID,
		InputRows:    totalRows,
		SampledRows:  sampledRows,
		OutputAlias:  output.Alias,
		OutputRID:    outputRID,
		OutputMode:   mode,
		OutputBranch: branch,
		Duration:     duration,
		Command:      cmdArgs,
		Runner:       runner,
		Container:    opts.Container,
		ConfigPath:   configPath,
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

func writeStreamRecords(stateRoot, streamRID, branch string, recs []map[string]any) (string, error) {
	path := filepath.Join(stateRoot, "streams", streamRID, filesystemName(branch), "records.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create stream output dir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create stream output: %w", err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	for _, rec := range recs {
		if err := enc.Encode(rec); err != nil {
			return "", fmt.Errorf("write stream output: %w", err)
		}
	}
	return path, nil
}

type mockFoundryRuntime struct {
	HostURL      string
	ContainerURL string
	close        func()
}

func (m mockFoundryRuntime) Close() {
	if m.close != nil {
		m.close()
	}
}

func startMockFoundryServer(handler http.Handler, forContainer bool) (mockFoundryRuntime, error) {
	if !forContainer {
		s := httptest.NewServer(handler)
		return mockFoundryRuntime{
			HostURL:      s.URL,
			ContainerURL: s.URL,
			close:        s.Close,
		}, nil
	}

	ln, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		return mockFoundryRuntime{}, fmt.Errorf("start mock Foundry listener: %w", err)
	}
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		return mockFoundryRuntime{}, fmt.Errorf("mock Foundry listener returned unexpected address %T", ln.Addr())
	}
	srv := &http.Server{Handler: handler}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			// The request path will surface connection failures; avoid racing logs into stdout.
		}
	}()
	port := tcpAddr.Port
	return mockFoundryRuntime{
		HostURL:      fmt.Sprintf("http://127.0.0.1:%d", port),
		ContainerURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		close: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
			<-done
		},
	}, nil
}

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

func runContainerModule(ctx context.Context, workDir, stateRoot, tokenPath, aliasMapPath, foundryURL, inputAlias, outputAlias, outputMode string, output io.Writer) ([]string, error) {
	imageTag := "foundry-cmgo-local:" + sanitizeDockerTag(filepath.Base(stateRoot))
	contextDir, err := prepareDockerBuildContext(workDir, stateRoot)
	if err != nil {
		return nil, err
	}
	buildArgs := []string{"build", "--platform", "linux/amd64", "-t", imageTag, "."}
	if err := runDockerCommand(ctx, contextDir, output, buildArgs...); err != nil {
		return append([]string{"docker"}, buildArgs...), err
	}

	containerStateDir := "/foundry-cmgo-run"
	runArgs := []string{
		"run", "--rm",
		"--network", "host",
		"-e", "FOUNDRY_URL=" + foundryURL,
		"-e", "BUILD2_TOKEN=" + filepath.ToSlash(filepath.Join(containerStateDir, filepath.Base(tokenPath))),
		"-e", "RESOURCE_ALIAS_MAP=" + filepath.ToSlash(filepath.Join(containerStateDir, filepath.Base(aliasMapPath))),
		"-v", stateRoot + ":" + containerStateDir + ":ro",
		imageTag,
		"foundry",
		"--input-alias", inputAlias,
		"--output-alias", outputAlias,
		"--output-mode", outputMode,
	}
	if err := runDockerCommand(ctx, workDir, output, runArgs...); err != nil {
		return append([]string{"docker"}, runArgs...), err
	}
	return append([]string{"docker"}, runArgs...), nil
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

func materializeInputCSV(src, dst string, maxRows int) ([]byte, int, int, error) {
	raw, err := os.ReadFile(src)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read input %s: %w", src, err)
	}
	cr := csv.NewReader(bytes.NewReader(raw))
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read input header: %w", err)
	}
	var out bytes.Buffer
	cw := csv.NewWriter(&out)
	if err := cw.Write(header); err != nil {
		return nil, 0, 0, err
	}
	total := 0
	sampled := 0
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, 0, fmt.Errorf("read input row: %w", err)
		}
		total++
		if maxRows <= 0 || sampled < maxRows {
			if err := cw.Write(rec); err != nil {
				return nil, 0, 0, err
			}
			sampled++
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, 0, 0, err
	}
	if err := os.WriteFile(dst, out.Bytes(), 0o644); err != nil {
		return nil, 0, 0, fmt.Errorf("write sampled input: %w", err)
	}
	return out.Bytes(), total, sampled, nil
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

func parseCSVPreview(raw []byte, maxRows int) ([]string, [][]string, error) {
	cr := csv.NewReader(bytes.NewReader(raw))
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, nil, err
	}
	var rows [][]string
	for len(rows) < maxRows {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, rec)
	}
	return header, rows, nil
}

func csvRowCount(raw []byte) int {
	cr := csv.NewReader(bytes.NewReader(raw))
	cr.FieldsPerRecord = -1
	if _, err := cr.Read(); err != nil {
		return 0
	}
	rows := 0
	for {
		_, err := cr.Read()
		if err == io.EOF {
			return rows
		}
		if err != nil {
			return rows
		}
		rows++
	}
}

func streamRecordsTable(recs []map[string]any, maxRows int) ([]string, [][]string) {
	if len(recs) == 0 {
		return nil, nil
	}
	preferred := []string{"email", "value", "status"}
	keys := map[string]struct{}{}
	for _, rec := range recs {
		for k := range rec {
			keys[k] = struct{}{}
		}
	}
	var header []string
	for _, k := range preferred {
		if _, ok := keys[k]; ok {
			header = append(header, k)
			delete(keys, k)
		}
	}
	var rest []string
	for k := range keys {
		rest = append(rest, k)
	}
	sort.Strings(rest)
	header = append(header, rest...)
	limit := len(recs)
	if maxRows > 0 && limit > maxRows {
		limit = maxRows
	}
	rows := make([][]string, 0, limit)
	for _, rec := range recs[:limit] {
		row := make([]string, 0, len(header))
		for _, col := range header {
			if v, ok := rec[col]; ok && v != nil {
				row = append(row, fmt.Sprint(v))
			} else {
				row = append(row, "")
			}
		}
		rows = append(rows, row)
	}
	return header, rows
}

func writeLastRun(workDir string, result LocalRunResult) error {
	path := filepath.Join(workDir, ".local", "foundry-cmgo", "last-run.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(LastRunManifest{Result: result, UpdatedAt: time.Now().UTC()}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func filesystemName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "master"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(s)
}

func tailString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return "..." + s[len(s)-max:]
}
