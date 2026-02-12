package foundryio

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/schema"
)

// ContractFromMetadataJSON translates Foundry dataset metadata into the minimal logical contract.
func ContractFromMetadataJSON(raw []byte) (schema.DatasetContract, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return schema.DatasetContract{}, fmt.Errorf("parse metadata json: %w", err)
	}

	fields := extractFields(doc)
	if len(fields) == 0 {
		return schema.DatasetContract{}, fmt.Errorf("metadata missing schema fields")
	}

	mode := schema.NormalizeMode(extractMode(doc))
	return schema.DatasetContract{Mode: mode, Fields: fields}, nil
}

func extractFields(doc map[string]any) []schema.Field {
	paths := [][]string{
		{"schema", "fieldSchemaList"},
		{"schema", "fields"},
		{"schema", "columns"},
		{"fields"},
	}
	for _, path := range paths {
		nodes := getPath(doc, path)
		if len(nodes) == 0 {
			continue
		}
		fields := make([]schema.Field, 0, len(nodes))
		for _, n := range nodes {
			m, ok := n.(map[string]any)
			if !ok {
				continue
			}
			name := firstString(m, "name", "fieldName")
			typeName := firstString(m, "type", "baseType")
			nullable, _ := m["nullable"].(bool)
			name = strings.TrimSpace(name)
			typeName = strings.TrimSpace(typeName)
			if name == "" || typeName == "" {
				continue
			}
			fields = append(fields, schema.Field{Name: name, Type: typeName, Nullable: nullable})
		}
		if len(fields) > 0 {
			return fields
		}
	}
	return nil
}

func extractMode(doc map[string]any) string {
	if v := strings.TrimSpace(firstString(doc, "datasetMode", "mode", "datasetType")); v != "" {
		if strings.Contains(strings.ToLower(v), "stream") {
			return "stream"
		}
		return "batch"
	}
	if b, ok := doc["streamingDataset"].(bool); ok && b {
		return "stream"
	}
	if m, ok := doc["dataset"].(map[string]any); ok {
		if v := strings.TrimSpace(firstString(m, "mode", "datasetMode", "type")); v != "" {
			if strings.Contains(strings.ToLower(v), "stream") {
				return "stream"
			}
			return "batch"
		}
	}
	return "batch"
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			s, _ := v.(string)
			if strings.TrimSpace(s) != "" {
				return s
			}
		}
	}
	return ""
}

func getPath(doc map[string]any, path []string) []any {
	var cur any = doc
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		next, ok := m[p]
		if !ok {
			return nil
		}
		cur = next
	}
	out, _ := cur.([]any)
	return out
}
