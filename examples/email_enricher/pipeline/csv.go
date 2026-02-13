package pipeline

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// WriteCSV writes rows as a CSV with the stable Header() ordering.
func WriteCSV(w io.Writer, rows []Row) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(Header()); err != nil {
		return err
	}
	for _, r := range rows {
		if err := cw.Write([]string{
			r.Email,
			r.LinkedInURL,
			r.Company,
			r.Title,
			r.Description,
			r.Confidence,
			r.Status,
			r.Error,
			r.Model,
			r.Sources,
			r.WebSearchQueries,
		}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ReadCSV reads rows from a CSV using the stable Header() contract.
//
// Extra columns are ignored. Required columns from Header() must exist.
func ReadCSV(r io.Reader) ([]Row, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1

	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[strings.TrimSpace(name)] = i
	}
	for _, name := range Header() {
		if _, ok := index[name]; !ok {
			return nil, fmt.Errorf("missing required column %q", name)
		}
	}

	var rows []Row
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}

		get := func(col string) string {
			i := index[col]
			if i < 0 || i >= len(rec) {
				return ""
			}
			return rec[i]
		}

		rows = append(rows, Row{
			Email:            get("email"),
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
		})
	}
}
