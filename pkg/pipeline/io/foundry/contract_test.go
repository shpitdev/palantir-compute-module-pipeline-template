package foundryio_test

import (
	"os"
	"path/filepath"
	"testing"

	foundryio "github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/schema"
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "test", "fixtures", "foundry-metadata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

func TestContractFromMetadataJSON_BatchDataset(t *testing.T) {
	contract, err := foundryio.ContractFromMetadataJSON(loadFixture(t, "input_dataset_metadata.json"))
	if err != nil {
		t.Fatalf("ContractFromMetadataJSON failed: %v", err)
	}
	if contract.Mode != schema.DatasetModeBatch {
		t.Fatalf("mode=%q want=batch", contract.Mode)
	}
	if len(contract.Fields) != 2 {
		t.Fatalf("fields len=%d want=2", len(contract.Fields))
	}
	if contract.Fields[0].Name != "email" || contract.Fields[0].Type != "STRING" || contract.Fields[0].Nullable {
		t.Fatalf("unexpected field[0]: %#v", contract.Fields[0])
	}
}

func TestContractFromMetadataJSON_StreamDataset(t *testing.T) {
	contract, err := foundryio.ContractFromMetadataJSON(loadFixture(t, "stream_output_metadata.json"))
	if err != nil {
		t.Fatalf("ContractFromMetadataJSON failed: %v", err)
	}
	if contract.Mode != schema.DatasetModeStream {
		t.Fatalf("mode=%q want=stream", contract.Mode)
	}
	if len(contract.Fields) != 2 {
		t.Fatalf("fields len=%d want=2", len(contract.Fields))
	}
}

func TestContractFromMetadataJSON_IgnoresTransportMetadata(t *testing.T) {
	a, err := foundryio.ContractFromMetadataJSON(loadFixture(t, "input_dataset_metadata.json"))
	if err != nil {
		t.Fatalf("parse fixture A: %v", err)
	}
	b, err := foundryio.ContractFromMetadataJSON(loadFixture(t, "input_dataset_metadata_variant.json"))
	if err != nil {
		t.Fatalf("parse fixture B: %v", err)
	}
	if a.Mode != b.Mode {
		t.Fatalf("mode mismatch: %q vs %q", a.Mode, b.Mode)
	}
	if len(a.Fields) != len(b.Fields) {
		t.Fatalf("field length mismatch: %d vs %d", len(a.Fields), len(b.Fields))
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			t.Fatalf("field[%d] mismatch: %#v vs %#v", i, a.Fields[i], b.Fields[i])
		}
	}
}
