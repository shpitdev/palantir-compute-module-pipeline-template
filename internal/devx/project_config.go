package devx

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultConfigPath = "foundry-cmgo.yaml"

type ProjectConfig struct {
	Version     int               `yaml:"version" json:"version"`
	Module      ModuleConfig      `yaml:"module" json:"module"`
	Inputs      []ResourceConfig  `yaml:"inputs" json:"inputs"`
	Outputs     []ResourceConfig  `yaml:"outputs" json:"outputs"`
	MockFoundry MockFoundryConfig `yaml:"mockFoundry" json:"mockFoundry"`
	Preview     PreviewConfig     `yaml:"preview" json:"preview"`
}

type ModuleConfig struct {
	Command []string `yaml:"command" json:"command"`
}

type ResourceConfig struct {
	Alias  string `yaml:"alias" json:"alias"`
	Path   string `yaml:"path,omitempty" json:"path,omitempty"`
	Mode   string `yaml:"mode,omitempty" json:"mode,omitempty"`
	Branch string `yaml:"branch,omitempty" json:"branch,omitempty"`
}

type MockFoundryConfig struct {
	Root   string `yaml:"root" json:"root"`
	Branch string `yaml:"branch" json:"branch"`
}

type PreviewConfig struct {
	Rows     int    `yaml:"rows" json:"rows"`
	Strategy string `yaml:"strategy" json:"strategy"`
}

func LoadProjectConfig(path string) (ProjectConfig, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = defaultConfigPath
	}
	if !filepath.IsAbs(path) {
		path = filepath.Clean(path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && filepath.Base(path) == defaultConfigPath {
			cfg := inferProjectConfig()
			return cfg, "", nil
		}
		return ProjectConfig{}, "", fmt.Errorf("read config: %w", err)
	}
	var cfg ProjectConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return ProjectConfig{}, "", fmt.Errorf("parse config: %w", err)
	}
	applyProjectConfigDefaults(&cfg)
	if err := validateProjectConfig(cfg); err != nil {
		return ProjectConfig{}, "", err
	}
	return cfg, path, nil
}

func inferProjectConfig() ProjectConfig {
	cfg := ProjectConfig{
		Version:     1,
		Module:      ModuleConfig{Command: []string{"go", "run", "./cmd/compute-module", "foundry"}},
		Inputs:      []ResourceConfig{{Alias: "input", Path: "data/input.csv"}},
		Outputs:     []ResourceConfig{{Alias: "output", Mode: "dataset"}},
		MockFoundry: MockFoundryConfig{Root: ".local/mock-foundry", Branch: "master"},
		Preview:     PreviewConfig{Rows: 1000, Strategy: "sampled"},
	}
	return cfg
}

func applyProjectConfigDefaults(cfg *ProjectConfig) {
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	if len(cfg.Module.Command) == 0 {
		cfg.Module.Command = []string{"go", "run", "./cmd/compute-module", "foundry"}
	}
	if len(cfg.Inputs) == 0 {
		cfg.Inputs = []ResourceConfig{{Alias: "input", Path: "data/input.csv"}}
	}
	if len(cfg.Outputs) == 0 {
		cfg.Outputs = []ResourceConfig{{Alias: "output", Mode: "dataset"}}
	}
	if strings.TrimSpace(cfg.Inputs[0].Alias) == "" {
		cfg.Inputs[0].Alias = "input"
	}
	if strings.TrimSpace(cfg.Inputs[0].Path) == "" {
		cfg.Inputs[0].Path = "data/input.csv"
	}
	if strings.TrimSpace(cfg.Outputs[0].Alias) == "" {
		cfg.Outputs[0].Alias = "output"
	}
	if strings.TrimSpace(cfg.Outputs[0].Mode) == "" {
		cfg.Outputs[0].Mode = "dataset"
	}
	if strings.TrimSpace(cfg.MockFoundry.Root) == "" {
		cfg.MockFoundry.Root = ".local/mock-foundry"
	}
	if strings.TrimSpace(cfg.MockFoundry.Branch) == "" {
		cfg.MockFoundry.Branch = "master"
	}
	if cfg.Preview.Rows <= 0 {
		cfg.Preview.Rows = 1000
	}
	if strings.TrimSpace(cfg.Preview.Strategy) == "" {
		cfg.Preview.Strategy = "sampled"
	}
}

func validateProjectConfig(cfg ProjectConfig) error {
	if cfg.Version != 1 {
		return fmt.Errorf("unsupported foundry-cmgo.yaml version %d", cfg.Version)
	}
	if len(cfg.Module.Command) == 0 || strings.TrimSpace(cfg.Module.Command[0]) == "" {
		return fmt.Errorf("module.command is required")
	}
	if len(cfg.Inputs) == 0 {
		return fmt.Errorf("at least one input is required")
	}
	if len(cfg.Outputs) == 0 {
		return fmt.Errorf("at least one output is required")
	}
	if strings.TrimSpace(cfg.Inputs[0].Alias) == "" {
		return fmt.Errorf("inputs[0].alias is required")
	}
	if strings.TrimSpace(cfg.Inputs[0].Path) == "" {
		return fmt.Errorf("inputs[0].path is required")
	}
	if strings.TrimSpace(cfg.Outputs[0].Alias) == "" {
		return fmt.Errorf("outputs[0].alias is required")
	}
	mode := strings.ToLower(strings.TrimSpace(cfg.Outputs[0].Mode))
	if mode != "dataset" && mode != "stream" {
		return fmt.Errorf("outputs[0].mode must be dataset or stream")
	}
	return nil
}
