package devx

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func writeStreamRecords(stateRoot, streamRID, branch string, recs []map[string]any) (string, error) {
	path := filepath.Join(stateRoot, "streams", streamRID, filesystemName(branch), "records.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create stream output dir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create stream output: %w", err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	for _, rec := range recs {
		if err := enc.Encode(rec); err != nil {
			return "", fmt.Errorf("write stream output: %w", err)
		}
	}
	return path, nil
}

func parseCSVPreview(raw []byte, maxRows int) ([]string, [][]string, error) {
	cr := csv.NewReader(bytes.NewReader(raw))
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, nil, err
	}
	var rows [][]string
	for len(rows) < maxRows {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, rec)
	}
	return header, rows, nil
}

func csvRowCount(raw []byte) int {
	cr := csv.NewReader(bytes.NewReader(raw))
	cr.FieldsPerRecord = -1
	if _, err := cr.Read(); err != nil {
		return 0
	}
	rows := 0
	for {
		_, err := cr.Read()
		if err == io.EOF {
			return rows
		}
		if err != nil {
			return rows
		}
		rows++
	}
}

func streamRecordsTable(recs []map[string]any, maxRows int) ([]string, [][]string) {
	if len(recs) == 0 {
		return nil, nil
	}
	preferred := []string{"email", "value", "status"}
	keys := map[string]struct{}{}
	for _, rec := range recs {
		for k := range rec {
			keys[k] = struct{}{}
		}
	}
	var header []string
	for _, k := range preferred {
		if _, ok := keys[k]; ok {
			header = append(header, k)
			delete(keys, k)
		}
	}
	var rest []string
	for k := range keys {
		rest = append(rest, k)
	}
	sort.Strings(rest)
	header = append(header, rest...)
	limit := len(recs)
	if maxRows > 0 && limit > maxRows {
		limit = maxRows
	}
	rows := make([][]string, 0, limit)
	for _, rec := range recs[:limit] {
		row := make([]string, 0, len(header))
		for _, col := range header {
			if v, ok := rec[col]; ok && v != nil {
				row = append(row, fmt.Sprint(v))
			} else {
				row = append(row, "")
			}
		}
		rows = append(rows, row)
	}
	return header, rows
}

func filesystemName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "master"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(s)
}
