package schema

import (
	"strings"
)

// DatasetMode captures behavior-relevant output semantics.
type DatasetMode string

const (
	DatasetModeBatch  DatasetMode = "batch"
	DatasetModeStream DatasetMode = "stream"
)

// Field captures the minimal behavior-relevant schema fields.
type Field struct {
	Name     string
	Type     string
	Nullable bool
}

// DatasetContract is the logical schema contract used by pipeline execution.
type DatasetContract struct {
	Mode   DatasetMode
	Fields []Field
}

func NormalizeMode(raw string) DatasetMode {
	s := strings.TrimSpace(strings.ToLower(raw))
	switch s {
	case "stream", "streaming":
		return DatasetModeStream
	default:
		return DatasetModeBatch
	}
}
