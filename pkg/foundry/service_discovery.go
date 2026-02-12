package foundry

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// serviceDiscoveryV2 mirrors the format used by Foundry compute modules, where each service id maps
// to a single-element list containing the base URL.
//
// Example (YAML):
//
//	api_gateway:
//	  - https://<stack>.palantirfoundry.com/api
//	stream_proxy:
//	  - https://<stack>.palantirfoundry.com/stream-proxy/api
type serviceDiscoveryV2 map[string][]string

type Services struct {
	APIGateway  string
	StreamProxy string
}

func loadServicesFromDiscoveryFile(path string) (Services, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Services{}, fmt.Errorf("FOUNDRY_SERVICE_DISCOVERY_V2 is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Services{}, fmt.Errorf("read FOUNDRY_SERVICE_DISCOVERY_V2 file: %w", err)
	}

	var raw serviceDiscoveryV2
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return Services{}, fmt.Errorf("parse FOUNDRY_SERVICE_DISCOVERY_V2 YAML: %w", err)
	}

	getOne := func(key string) (string, bool) {
		vals, ok := raw[key]
		if !ok || len(vals) == 0 {
			return "", false
		}
		v := strings.TrimSpace(vals[0])
		if v == "" {
			return "", false
		}
		return v, true
	}

	apiGateway, ok := getOne("api_gateway")
	if !ok {
		return Services{}, fmt.Errorf("FOUNDRY_SERVICE_DISCOVERY_V2 missing api_gateway")
	}
	streamProxy, ok := getOne("stream_proxy")
	if !ok {
		return Services{}, fmt.Errorf("FOUNDRY_SERVICE_DISCOVERY_V2 missing stream_proxy")
	}

	return Services{
		APIGateway:  apiGateway,
		StreamProxy: streamProxy,
	}, nil
}
