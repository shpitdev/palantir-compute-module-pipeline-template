package app

import (
	"fmt"
	"strings"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/pipeline"
)

type incrementalPlan struct {
	rows          []pipeline.Row
	pendingEmails []string
	pendingIdx    map[string][]int
	cachedRows    int
	pendingRows   int
}

func buildIncrementalPlan(inputEmails []string, existingByEmail map[string]pipeline.Row) incrementalPlan {
	plan := incrementalPlan{
		rows:       make([]pipeline.Row, len(inputEmails)),
		pendingIdx: make(map[string][]int),
	}
	for i, raw := range inputEmails {
		email := strings.TrimSpace(raw)
		key := emailKey(email)

		if prev, ok := existingByEmail[key]; ok && strings.EqualFold(strings.TrimSpace(prev.Status), "ok") {
			prev.Email = email
			plan.rows[i] = prev
			plan.cachedRows++
			continue
		}

		if _, seen := plan.pendingIdx[key]; !seen {
			plan.pendingEmails = append(plan.pendingEmails, email)
		}
		plan.pendingIdx[key] = append(plan.pendingIdx[key], i)
		plan.pendingRows++
	}
	return plan
}

func (p *incrementalPlan) applyEnrichedRows(rows []pipeline.Row) error {
	if len(rows) != len(p.pendingEmails) {
		return fmt.Errorf("incremental enrichment mismatch: got %d rows for %d pending emails", len(rows), len(p.pendingEmails))
	}
	for i, email := range p.pendingEmails {
		key := emailKey(email)
		idxs, ok := p.pendingIdx[key]
		if !ok || len(idxs) == 0 {
			return fmt.Errorf("incremental enrichment mismatch: missing pending indexes for %q", email)
		}
		row := rows[i]
		row.Email = strings.TrimSpace(email)
		for _, idx := range idxs {
			p.rows[idx] = row
		}
	}
	return nil
}

func chooseBestIncrementalRow(a, b pipeline.Row) pipeline.Row {
	aOk := strings.EqualFold(strings.TrimSpace(a.Status), "ok")
	bOk := strings.EqualFold(strings.TrimSpace(b.Status), "ok")
	if aOk && !bOk {
		return a
	}
	if bOk && !aOk {
		return b
	}

	// If neither is ok (or both are ok), prefer the latter. This is a best-effort heuristic:
	// readTable ordering is not always stable, but we mainly want "any ok" to win.
	return b
}

func emailKey(email string) string {
	return strings.TrimSpace(email)
}

func countStatuses(rows []pipeline.Row) (okRows int, errorRows int) {
	for _, row := range rows {
		if strings.EqualFold(strings.TrimSpace(row.Status), "ok") {
			okRows++
			continue
		}
		errorRows++
	}
	return okRows, errorRows
}
