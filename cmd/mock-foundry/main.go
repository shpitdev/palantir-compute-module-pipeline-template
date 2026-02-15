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
	streamRIDs := defaultString("MOCK_FOUNDRY_STREAM_RIDS", "")

	fs := flag.NewFlagSet("mock-foundry", flag.ExitOnError)
	fs.StringVar(&addr, "addr", addr, "Listen address")
	fs.StringVar(&inputDir, "input-dir", inputDir, "Directory containing input CSVs named <rid>.csv")
	fs.StringVar(&uploadDir, "upload-dir", uploadDir, "Directory to persist uploaded files")
	fs.StringVar(&streamRIDs, "stream-rids", streamRIDs, "Comma-separated dataset RIDs to treat as streams (also supports env: MOCK_FOUNDRY_STREAM_RIDS)")
	_ = fs.Parse(os.Args[1:])

	srv := mockfoundry.New(inputDir, uploadDir)
	for _, rid := range splitCSV(streamRIDs) {
		srv.CreateStream(rid)
	}

	_, _ = fmt.Fprintf(os.Stdout, "mock-foundry listening on %s (input=%s upload=%s)\n", addr, inputDir, uploadDir)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

func defaultString(envVar string, fallback string) string {
	v := strings.TrimSpace(os.Getenv(envVar))
	if v == "" {
		return fallback
	}
	return v
}
