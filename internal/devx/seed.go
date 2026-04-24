package devx

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type AliasRef struct {
	RID    string `json:"rid"`
	Branch string `json:"branch"`
}

type SeedDatasetOptions struct {
	CSVPath  string
	AliasMap string
	Alias    string
	Root     string
}

type SeedDatasetResult struct {
	RID  string
	Path string
	Rows int
}

type SeedStreamOptions struct {
	CSVPath  string
	AliasMap string
	Alias    string
	URL      string
	Token    string
	Branch   string
}

type SeedStreamResult struct {
	RID     string
	Branch  string
	Records int
}

func LoadAliasMap(path string) (map[string]AliasRef, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("alias map path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read alias map: %w", err)
	}
	var doc map[string]struct {
		RID    string  `json:"rid"`
		Branch *string `json:"branch"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse alias map: %w", err)
	}
	out := make(map[string]AliasRef, len(doc))
	for alias, ref := range doc {
		branch := ""
		if ref.Branch != nil {
			branch = strings.TrimSpace(*ref.Branch)
		}
		out[alias] = AliasRef{RID: strings.TrimSpace(ref.RID), Branch: branch}
	}
	return out, nil
}

func SeedDataset(opts SeedDatasetOptions) (SeedDatasetResult, error) {
	opts.CSVPath = strings.TrimSpace(opts.CSVPath)
	opts.AliasMap = strings.TrimSpace(opts.AliasMap)
	opts.Alias = defaultString(strings.TrimSpace(opts.Alias), "input")
	opts.Root = defaultString(strings.TrimSpace(opts.Root), ".local/mock-foundry")
	if opts.CSVPath == "" {
		return SeedDatasetResult{}, fmt.Errorf("csv path is required")
	}

	ref, err := aliasRef(opts.AliasMap, opts.Alias)
	if err != nil {
		return SeedDatasetResult{}, err
	}
	if err := validateRID(ref.RID, opts.Alias); err != nil {
		return SeedDatasetResult{}, err
	}

	raw, rows, err := readCSVFile(opts.CSVPath)
	if err != nil {
		return SeedDatasetResult{}, err
	}
	dst := filepath.Join(opts.Root, "inputs", ref.RID+".csv")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return SeedDatasetResult{}, fmt.Errorf("create input dir: %w", err)
	}
	if err := os.WriteFile(dst, raw, 0o644); err != nil {
		return SeedDatasetResult{}, fmt.Errorf("write dataset seed: %w", err)
	}
	return SeedDatasetResult{RID: ref.RID, Path: dst, Rows: rows}, nil
}

func SeedStream(ctx context.Context, opts SeedStreamOptions) (SeedStreamResult, error) {
	opts.CSVPath = strings.TrimSpace(opts.CSVPath)
	opts.AliasMap = strings.TrimSpace(opts.AliasMap)
	opts.Alias = defaultString(strings.TrimSpace(opts.Alias), "output")
	opts.URL = strings.TrimRight(strings.TrimSpace(opts.URL), "/")
	opts.Token = defaultString(strings.TrimSpace(opts.Token), "dummy-token")
	if opts.CSVPath == "" {
		return SeedStreamResult{}, fmt.Errorf("csv path is required")
	}
	if opts.URL == "" {
		return SeedStreamResult{}, fmt.Errorf("mock Foundry URL is required for stream seeding")
	}

	ref, err := aliasRef(opts.AliasMap, opts.Alias)
	if err != nil {
		return SeedStreamResult{}, err
	}
	if err := validateRID(ref.RID, opts.Alias); err != nil {
		return SeedStreamResult{}, err
	}
	branch := strings.TrimSpace(opts.Branch)
	if branch == "" {
		branch = strings.TrimSpace(ref.Branch)
	}
	if branch == "" {
		branch = "master"
	}

	records, err := readCSVRecords(opts.CSVPath)
	if err != nil {
		return SeedStreamResult{}, err
	}
	for i, rec := range records {
		if err := postStreamRecord(ctx, opts.URL, opts.Token, ref.RID, branch, rec); err != nil {
			return SeedStreamResult{}, fmt.Errorf("publish record %d: %w", i+1, err)
		}
	}
	return SeedStreamResult{RID: ref.RID, Branch: branch, Records: len(records)}, nil
}

func aliasRef(aliasMap, alias string) (AliasRef, error) {
	aliases, err := LoadAliasMap(aliasMap)
	if err != nil {
		return AliasRef{}, err
	}
	ref, ok := aliases[alias]
	if !ok {
		return AliasRef{}, fmt.Errorf("alias %q not found in %s", alias, aliasMap)
	}
	return ref, nil
}

func readCSVFile(path string) ([]byte, int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("read csv: %w", err)
	}
	rows, err := countCSVRows(bytes.NewReader(raw))
	if err != nil {
		return nil, 0, err
	}
	return raw, rows, nil
}

func countCSVRows(r io.Reader) (int, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	if _, err := cr.Read(); err != nil {
		return 0, fmt.Errorf("read csv header: %w", err)
	}
	rows := 0
	for {
		_, err := cr.Read()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return 0, fmt.Errorf("read csv row: %w", err)
		}
		rows++
	}
}

func validateRID(rid, alias string) error {
	rid = strings.TrimSpace(rid)
	if rid == "" {
		return fmt.Errorf("alias %q has empty rid", alias)
	}
	if strings.ContainsAny(rid, `/\`) {
		return fmt.Errorf("alias %q has invalid rid %q: path separators are not allowed", alias, rid)
	}
	return nil
}

func readCSVRecords(path string) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	cr := csv.NewReader(f)
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header: %w", err)
	}
	seen := make(map[string]struct{}, len(header))
	for i := range header {
		header[i] = strings.TrimSpace(strings.TrimPrefix(header[i], "\uFEFF"))
		if header[i] == "" {
			return nil, fmt.Errorf("csv header column %d is empty", i+1)
		}
		if _, ok := seen[header[i]]; ok {
			return nil, fmt.Errorf("csv header column %q is duplicated", header[i])
		}
		seen[header[i]] = struct{}{}
	}

	var records []map[string]any
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read csv row: %w", err)
		}
		m := make(map[string]any, len(header))
		for i, col := range header {
			var v any
			if i < len(rec) && strings.TrimSpace(rec[i]) != "" {
				v = rec[i]
			}
			m[col] = v
		}
		records = append(records, m)
	}
}

func postStreamRecord(ctx context.Context, baseURL, token, rid, branch string, rec map[string]any) error {
	u, err := url.Parse(baseURL + "/stream-proxy/api/streams/" + url.PathEscape(rid) + "/branches/" + url.PathEscape(branch) + "/jsonRecord")
	if err != nil {
		return err
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("stream endpoint returned %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
