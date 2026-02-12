package consumer

import (
	"context"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/mockfoundry"
	foundryio "github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/foundry"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/schema"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/worker"
)

func TestPublicPackagesCompile(t *testing.T) {
	t.Parallel()

	_ = foundry.Env{}
	_ = schema.DatasetContract{}

	srv := mockfoundry.New(t.TempDir(), t.TempDir())
	if srv.Handler() == nil {
		t.Fatalf("handler must not be nil")
	}

	_, err := worker.ProcessAll(context.Background(), []string{"x"}, func(_ context.Context, in string) (string, error) {
		return in, nil
	}, worker.Options{Workers: 1})
	if err != nil {
		t.Fatalf("ProcessAll failed: %v", err)
	}

	_, err = foundryio.ContractFromMetadataJSON([]byte(`{"datasetType":"DATASET","schema":{"fieldSchemaList":[{"name":"email","type":"STRING","nullable":false}]}}`))
	if err != nil {
		t.Fatalf("ContractFromMetadataJSON failed: %v", err)
	}
}
