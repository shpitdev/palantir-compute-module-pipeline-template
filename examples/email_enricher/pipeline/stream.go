package pipeline

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// StreamMetadataHeader returns stream-only metadata columns that may be present
// in the stream-backed dataset view. These are intentionally extra columns:
// ReadCSV ignores them, but local emulation preserves them for inspection.
func StreamMetadataHeader() []string {
	return []string{"run_id", "written_at"}
}

// StreamTableHeader returns the CSV table projection used when stream records
// are exposed through a dataset-style readTable view.
func StreamTableHeader() []string {
	header := append([]string{}, Header()...)
	header = append(header, StreamMetadataHeader()...)
	return header
}

// NormalizeStreamRecord unwraps stream-proxy record envelopes into the logical
// record payload used by the email enricher schema.
func NormalizeStreamRecord(rec map[string]any) map[string]any {
	if rec == nil {
		return nil
	}
	for _, key := range []string{"record", "value", "data"} {
		if inner, ok := rec[key].(map[string]any); ok {
			return inner
		}
	}
	return rec
}

// RowFromStreamRecord converts a legacy stream-proxy JSON record into Row.
func RowFromStreamRecord(rec map[string]any) Row {
	rec = NormalizeStreamRecord(rec)
	get := func(key string) string {
		v, ok := rec[key]
		if !ok || v == nil {
			return ""
		}
		s, ok := v.(string)
		if !ok {
			return ""
		}
		return s
	}

	return Row{
		Email:            strings.TrimSpace(get("email")),
		LinkedInURL:      get("linkedin_url"),
		Company:          get("company"),
		Title:            get("title"),
		Description:      get("description"),
		Confidence:       get("confidence"),
		Status:           get("status"),
		Error:            get("error"),
		Model:            get("model"),
		Sources:          get("sources"),
		WebSearchQueries: get("web_search_queries"),
	}
}

// RowToStreamRecord converts Row into the legacy stream-proxy JSON record
// shape. Empty optional values are emitted as nil so nullable string columns
// behave like missing values rather than empty strings.
func RowToStreamRecord(r Row) map[string]any {
	rec := map[string]any{
		"email": r.Email,
	}
	assignNullable(rec, "linkedin_url", r.LinkedInURL)
	assignNullable(rec, "company", r.Company)
	assignNullable(rec, "title", r.Title)
	assignNullable(rec, "description", r.Description)
	assignNullable(rec, "confidence", r.Confidence)
	assignNullable(rec, "status", r.Status)
	assignNullable(rec, "error", r.Error)
	assignNullable(rec, "model", r.Model)
	assignNullable(rec, "sources", r.Sources)
	assignNullable(rec, "web_search_queries", r.WebSearchQueries)
	return rec
}

// WriteStreamRecordsCSV projects stream records as a CSV table using the same
// logical output contract as WriteCSV, plus stream metadata columns.
func WriteStreamRecordsCSV(w io.Writer, records []map[string]any) error {
	cw := csv.NewWriter(w)
	header := StreamTableHeader()
	if err := cw.Write(header); err != nil {
		return err
	}
	for _, rec := range records {
		rec = NormalizeStreamRecord(rec)
		row := make([]string, 0, len(header))
		for _, col := range header {
			v, ok := rec[col]
			if !ok || v == nil {
				row = append(row, "")
				continue
			}
			s, ok := v.(string)
			if ok {
				row = append(row, s)
				continue
			}
			row = append(row, fmt.Sprint(v))
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func assignNullable(dst map[string]any, key string, value string) {
	if strings.TrimSpace(value) == "" {
		dst[key] = nil
		return
	}
	dst[key] = value
}
