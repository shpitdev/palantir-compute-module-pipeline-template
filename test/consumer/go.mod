module github.com/palantir/palantir-compute-module-pipeline-search/test/consumer

go 1.25

require github.com/palantir/palantir-compute-module-pipeline-search v0.0.0

require (
	golang.org/x/time v0.14.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/palantir/palantir-compute-module-pipeline-search => ../..
