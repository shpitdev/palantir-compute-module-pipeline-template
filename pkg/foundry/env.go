package foundry

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// DatasetRef identifies a dataset RID and branch.
type DatasetRef struct {
	RID    string
	Branch string
}

// Env is the runtime configuration needed to run in Foundry pipeline mode.
type Env struct {
	// Services contains the discovered Foundry service base URLs to call.
	Services Services
	// DefaultCAPath is the path to a PEM bundle that should be trusted for TLS.
	// In Foundry compute modules, this is provided via DEFAULT_CA_PATH.
	DefaultCAPath string
	Token         string
	Aliases       map[string]DatasetRef
}

// LoadEnv reads required pipeline-mode env vars.
//
// Required:
//   - BUILD2_TOKEN (file path)
//   - RESOURCE_ALIAS_MAP (file path)
func LoadEnv() (Env, error) {
	services, err := loadServicesFromEnv()
	if err != nil {
		return Env{}, err
	}
	defaultCAPath := strings.TrimSpace(os.Getenv("DEFAULT_CA_PATH"))

	token, err := readFileEnv("BUILD2_TOKEN")
	if err != nil {
		return Env{}, err
	}

	aliases, err := readAliasMapEnv("RESOURCE_ALIAS_MAP")
	if err != nil {
		return Env{}, err
	}

	return Env{
		Services:      services,
		DefaultCAPath: defaultCAPath,
		Token:         token,
		Aliases:       aliases,
	}, nil
}

func loadServicesFromEnv() (Services, error) {
	if p := strings.TrimSpace(os.Getenv("FOUNDRY_SERVICE_DISCOVERY_V2")); p != "" {
		return loadServicesFromDiscoveryFile(p)
	}

	// Back-compat: allow explicit FOUNDRY_URL when service discovery is not present.
	foundryURL := strings.TrimSpace(os.Getenv("FOUNDRY_URL"))
	if foundryURL == "" {
		return Services{}, fmt.Errorf("FOUNDRY_SERVICE_DISCOVERY_V2 or FOUNDRY_URL is required")
	}
	if !strings.Contains(foundryURL, "://") {
		foundryURL = "https://" + foundryURL
	}
	foundryURL = strings.TrimRight(foundryURL, "/")

	return Services{
		APIGateway:  foundryURL + "/api",
		StreamProxy: foundryURL + "/stream-proxy/api",
	}, nil
}

func readFileEnv(varName string) (string, error) {
	path := strings.TrimSpace(os.Getenv(varName))
	if path == "" {
		return "", fmt.Errorf("%s is required", varName)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s file: %w", varName, err)
	}
	return strings.TrimSpace(string(b)), nil
}

type aliasEntry struct {
	RID    string  `json:"rid"`
	Branch *string `json:"branch"`
}

func readAliasMapEnv(varName string) (map[string]DatasetRef, error) {
	path := strings.TrimSpace(os.Getenv(varName))
	if path == "" {
		return nil, fmt.Errorf("%s is required", varName)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s file: %w", varName, err)
	}

	var raw map[string]aliasEntry
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("parse %s JSON: %w", varName, err)
	}
	out := make(map[string]DatasetRef, len(raw))
	for k, v := range raw {
		if strings.TrimSpace(v.RID) == "" {
			return nil, fmt.Errorf("alias %q: rid is required", k)
		}
		branch := ""
		if v.Branch != nil && strings.TrimSpace(*v.Branch) != "" {
			branch = strings.TrimSpace(*v.Branch)
		}
		out[k] = DatasetRef{
			RID:    v.RID,
			Branch: branch,
		}
	}
	return out, nil
}
