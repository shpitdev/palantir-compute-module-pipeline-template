package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/mockfoundry"
)

func main() {
	addr := defaultString("MOCK_FOUNDRY_ADDR", ":8080")
	inputDir := defaultString("MOCK_FOUNDRY_INPUT_DIR", "/data/inputs")
	uploadDir := defaultString("MOCK_FOUNDRY_UPLOAD_DIR", "/data/uploads")

	fs := flag.NewFlagSet("mock-foundry", flag.ExitOnError)
	fs.StringVar(&addr, "addr", addr, "Listen address")
	fs.StringVar(&inputDir, "input-dir", inputDir, "Directory containing input CSVs named <rid>.csv")
	fs.StringVar(&uploadDir, "upload-dir", uploadDir, "Directory to persist uploaded files")
	_ = fs.Parse(os.Args[1:])

	srv := mockfoundry.New(inputDir, uploadDir)

	_, _ = fmt.Fprintf(os.Stdout, "mock-foundry listening on %s (input=%s upload=%s)\n", addr, inputDir, uploadDir)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func defaultString(envVar string, fallback string) string {
	v := strings.TrimSpace(os.Getenv(envVar))
	if v == "" {
		return fallback
	}
	return v
}
