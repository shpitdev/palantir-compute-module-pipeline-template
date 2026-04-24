package devx

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

func materializeInputCSV(src, dst string, maxRows int) ([]byte, int, int, error) {
	raw, err := os.ReadFile(src)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read input %s: %w", src, err)
	}
	cr := csv.NewReader(bytes.NewReader(raw))
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read input header: %w", err)
	}
	var out bytes.Buffer
	cw := csv.NewWriter(&out)
	if err := cw.Write(header); err != nil {
		return nil, 0, 0, err
	}
	total := 0
	sampled := 0
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, 0, fmt.Errorf("read input row: %w", err)
		}
		total++
		if maxRows <= 0 || sampled < maxRows {
			if err := cw.Write(rec); err != nil {
				return nil, 0, 0, err
			}
			sampled++
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, 0, 0, err
	}
	if err := os.WriteFile(dst, out.Bytes(), 0o644); err != nil {
		return nil, 0, 0, fmt.Errorf("write sampled input: %w", err)
	}
	return out.Bytes(), total, sampled, nil
}
