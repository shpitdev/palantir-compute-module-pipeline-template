package main

import (
	"context"
	"fmt"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/core"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/worker"
	"github.com/palantir/palantir-compute-module-pipeline-search/test/template/processor"
)

func main() {
	p := processor.Processor{}
	runner := core.ProcessFunc[string, processor.Result](p.Process)

	out, err := worker.ProcessAll(context.Background(), []string{"alice@example.com"}, runner.Process, worker.Options{Workers: 1})
	if err != nil {
		panic(err)
	}
	fmt.Println(out[0].Output.Output)
}
