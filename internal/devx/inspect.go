package devx

import (
	"os"
	"path/filepath"
	"strings"
)

type InspectOptions struct {
	WorkDir    string
	ConfigPath string
}

type ResolvedResourceConfig struct {
	Alias        string `json:"alias"`
	Path         string `json:"path,omitempty"`
	ResolvedPath string `json:"resolvedPath,omitempty"`
	Mode         string `json:"mode,omitempty"`
	Branch       string `json:"branch,omitempty"`
}

type ProjectConfigInspection struct {
	WorkDir          string                   `json:"workDir"`
	ConfigPath       string                   `json:"configPath,omitempty"`
	Inferred         bool                     `json:"inferred"`
	ModuleCommand    []string                 `json:"moduleCommand"`
	Inputs           []ResolvedResourceConfig `json:"inputs"`
	Outputs          []ResolvedResourceConfig `json:"outputs"`
	MockRoot         string                   `json:"mockRoot"`
	ResolvedMockRoot string                   `json:"resolvedMockRoot"`
	MockBranch       string                   `json:"mockBranch"`
	PreviewRows      int                      `json:"previewRows"`
	PreviewStrategy  string                   `json:"previewStrategy"`
}

type OutputsInspection struct {
	HasLastRun      bool       `json:"hasLastRun"`
	UpdatedAt       string     `json:"updatedAt,omitempty"`
	Kind            string     `json:"kind,omitempty"`
	RunID           string     `json:"runId,omitempty"`
	OutputAlias     string     `json:"outputAlias,omitempty"`
	OutputRID       string     `json:"outputRid,omitempty"`
	OutputMode      string     `json:"outputMode,omitempty"`
	OutputBranch    string     `json:"outputBranch,omitempty"`
	OutputPath      string     `json:"outputPath,omitempty"`
	RowsOrRecords   int        `json:"rowsOrRecords,omitempty"`
	StateDir        string     `json:"stateDir,omitempty"`
	RunLogPath      string     `json:"runLogPath,omitempty"`
	Container       bool       `json:"container"`
	DockerNetwork   string     `json:"dockerNetworkStrategy,omitempty"`
	LastRunManifest string     `json:"lastRunManifest,omitempty"`
	PreviewHeader   []string   `json:"previewHeader,omitempty"`
	PreviewRows     [][]string `json:"previewRows,omitempty"`
}

func InspectConfig(opts InspectOptions) (ProjectConfigInspection, error) {
	workDir := strings.TrimSpace(opts.WorkDir)
	if workDir == "" {
		workDir = "."
	}
	if abs, err := filepath.Abs(workDir); err == nil {
		workDir = abs
	}
	configPath := strings.TrimSpace(opts.ConfigPath)
	if configPath == "" {
		configPath = defaultConfigPath
	}
	if configPath != "" && !filepath.IsAbs(configPath) {
		configPath = filepath.Join(workDir, configPath)
	}
	cfg, loadedPath, err := LoadProjectConfig(configPath)
	if err != nil {
		return ProjectConfigInspection{}, err
	}
	out := ProjectConfigInspection{
		WorkDir:          workDir,
		ConfigPath:       loadedPath,
		Inferred:         loadedPath == "",
		ModuleCommand:    append([]string{}, cfg.Module.Command...),
		MockRoot:         cfg.MockFoundry.Root,
		ResolvedMockRoot: resolvePath(workDir, cfg.MockFoundry.Root),
		MockBranch:       cfg.MockFoundry.Branch,
		PreviewRows:      cfg.Preview.Rows,
		PreviewStrategy:  cfg.Preview.Strategy,
	}
	for _, input := range cfg.Inputs {
		out.Inputs = append(out.Inputs, ResolvedResourceConfig{
			Alias:        input.Alias,
			Path:         input.Path,
			ResolvedPath: resolvePath(workDir, input.Path),
		})
	}
	for _, output := range cfg.Outputs {
		out.Outputs = append(out.Outputs, ResolvedResourceConfig{
			Alias:  output.Alias,
			Mode:   output.Mode,
			Branch: defaultString(output.Branch, cfg.MockFoundry.Branch),
		})
	}
	return out, nil
}

func InspectOutputs(opts InspectOptions) (OutputsInspection, error) {
	workDir := strings.TrimSpace(opts.WorkDir)
	if workDir == "" {
		workDir = "."
	}
	manifest, err := InspectLastIn(workDir)
	if err != nil {
		if _, statErr := os.Stat(lastRunPath(workDir)); os.IsNotExist(statErr) {
			return OutputsInspection{HasLastRun: false, LastRunManifest: lastRunPath(workDir)}, nil
		}
		return OutputsInspection{}, err
	}
	res := manifest.Result
	return OutputsInspection{
		HasLastRun:      true,
		UpdatedAt:       manifest.UpdatedAt.Local().Format("2006-01-02T15:04:05-07:00"),
		Kind:            res.Kind,
		RunID:           res.RunID,
		OutputAlias:     res.OutputAlias,
		OutputRID:       res.OutputRID,
		OutputMode:      res.OutputMode,
		OutputBranch:    res.OutputBranch,
		OutputPath:      res.OutputPath,
		RowsOrRecords:   res.OutputRows,
		StateDir:        res.StateDir,
		RunLogPath:      res.LogPath,
		Container:       res.Container,
		DockerNetwork:   res.DockerNetworkStrategy,
		LastRunManifest: lastRunPath(workDir),
		PreviewHeader:   res.PreviewHeader,
		PreviewRows:     res.PreviewRows,
	}, nil
}
