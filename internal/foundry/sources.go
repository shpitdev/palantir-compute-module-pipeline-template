package foundry

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// SourceCredentials is the parsed contents of the Foundry compute modules SOURCE_CREDENTIALS file.
//
// The top-level keys are the "API name" of each configured Source. Each source maps secret name -> secret value.
type SourceCredentials map[string]map[string]string

// LoadSourceCredentialsFromEnv reads and parses the SOURCE_CREDENTIALS file.
func LoadSourceCredentialsFromEnv() (SourceCredentials, error) {
	path := strings.TrimSpace(os.Getenv("SOURCE_CREDENTIALS"))
	if path == "" {
		return nil, fmt.Errorf("SOURCE_CREDENTIALS is not set")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read SOURCE_CREDENTIALS file: %w", err)
	}
	var out SourceCredentials
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("parse SOURCE_CREDENTIALS JSON: %w", err)
	}
	if out == nil {
		out = make(SourceCredentials)
	}
	return out, nil
}

// SourceNames returns stable-sorted Source API names present in the credentials file.
func (sc SourceCredentials) SourceNames() []string {
	if sc == nil {
		return nil
	}
	out := make([]string, 0, len(sc))
	for k := range sc {
		if strings.TrimSpace(k) != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// SecretNames returns stable-sorted secret names for the given Source API name.
func (sc SourceCredentials) SecretNames(sourceAPIName string) []string {
	if sc == nil {
		return nil
	}
	sourceAPIName = strings.TrimSpace(sourceAPIName)
	if sourceAPIName == "" {
		return nil
	}
	src := sc[sourceAPIName]
	if src == nil {
		return nil
	}
	out := make([]string, 0, len(src))
	for k := range src {
		if strings.TrimSpace(k) != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// GetSecret gets a secret value for a given Source API name and secret name.
//
// Some source types (for example REST sources) expose "additionalSecret<SecretName>" keys in SOURCE_CREDENTIALS.
// This helper tries both the raw secret name and that prefixed form.
func (sc SourceCredentials) GetSecret(sourceAPIName, secretName string) (string, bool) {
	if sc == nil {
		return "", false
	}
	sourceAPIName = strings.TrimSpace(sourceAPIName)
	secretName = strings.TrimSpace(secretName)
	if sourceAPIName == "" || secretName == "" {
		return "", false
	}
	src := sc[sourceAPIName]
	if src == nil {
		return "", false
	}
	if v := strings.TrimSpace(src[secretName]); v != "" {
		return v, true
	}
	if v := strings.TrimSpace(src["additionalSecret"+secretName]); v != "" {
		return v, true
	}
	return "", false
}
