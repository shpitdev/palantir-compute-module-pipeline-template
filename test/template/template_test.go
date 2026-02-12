package template

import (
	"context"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/core"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/worker"
	"github.com/palantir/palantir-compute-module-pipeline-search/test/template/processor"
)

func TestTemplateCompilesWithPipelineKit(t *testing.T) {
	t.Parallel()

	p := processor.Processor{}
	runner := core.ProcessFunc[string, processor.Result](p.Process)

	out, err := worker.ProcessAll(context.Background(), []string{"bob@corp.test"}, runner.Process, worker.Options{Workers: 1})
	if err != nil {
		t.Fatalf("ProcessAll failed: %v", err)
	}
	if len(out) != 1 || out[0].Output.Output != "BOB@CORP.TEST" {
		t.Fatalf("unexpected output: %#v", out)
	}
}
