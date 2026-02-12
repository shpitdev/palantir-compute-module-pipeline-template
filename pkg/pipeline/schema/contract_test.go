package schema_test

import (
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/schema"
)

func TestNormalizeMode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want schema.DatasetMode
	}{
		{name: "batch default", in: "", want: schema.DatasetModeBatch},
		{name: "batch explicit", in: "batch", want: schema.DatasetModeBatch},
		{name: "stream", in: "stream", want: schema.DatasetModeStream},
		{name: "streaming", in: "Streaming", want: schema.DatasetModeStream},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := schema.NormalizeMode(tt.in); got != tt.want {
				t.Fatalf("NormalizeMode(%q)=%q want=%q", tt.in, got, tt.want)
			}
		})
	}
}
