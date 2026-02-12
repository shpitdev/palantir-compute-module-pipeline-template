package local

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// ReadEmailsCSV reads a CSV file and returns the values from the "email" column.
func ReadEmailsCSV(r io.Reader) ([]string, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	emailIdx := -1
	for i, col := range header {
		if strings.EqualFold(strings.TrimSpace(col), "email") {
			emailIdx = i
			break
		}
	}
	if emailIdx < 0 {
		return nil, fmt.Errorf("missing required column %q", "email")
	}

	var emails []string
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		if emailIdx >= len(rec) {
			return nil, fmt.Errorf("row has %d columns, want at least %d", len(rec), emailIdx+1)
		}
		emails = append(emails, rec[emailIdx])
	}
	return emails, nil
}
