package pipeline

import (
	"encoding/csv"
	"io"
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
