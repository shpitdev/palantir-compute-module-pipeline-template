package gemini

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich"
	"google.golang.org/genai"
)

type Config struct {
	APIKey string
	Model  string

	// BaseURL overrides the Gemini API base URL. Useful for proxies/testing.
	BaseURL string

	// CaptureAudit controls whether sources/queries are extracted into the output.
	CaptureAudit bool
}

type Enricher struct {
	client       *genai.Client
	model        string
	captureAudit bool
}

func New(ctx context.Context, cfg Config) (*Enricher, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("GEMINI_API_KEY is required")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, fmt.Errorf("GEMINI_MODEL is required")
	}

	cc := &genai.ClientConfig{
		APIKey:  strings.TrimSpace(cfg.APIKey),
		Backend: genai.BackendGeminiAPI,
	}
	if strings.TrimSpace(cfg.BaseURL) != "" {
		cc.HTTPOptions.BaseURL = strings.TrimSpace(cfg.BaseURL)
	}

	client, err := genai.NewClient(ctx, cc)
	if err != nil {
		return nil, err
	}
	return &Enricher{
		client:       client,
		model:        strings.TrimSpace(cfg.Model),
		captureAudit: cfg.CaptureAudit,
	}, nil
}

type responseSchema struct {
	LinkedInURL string `json:"linkedin_url"`
	Company     string `json:"company"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Confidence  string `json:"confidence"`
}

var outputSchema = &genai.Schema{
	Type: genai.TypeObject,
	Properties: map[string]*genai.Schema{
		"linkedin_url": {Type: genai.TypeString},
		"company":      {Type: genai.TypeString},
		"title":        {Type: genai.TypeString},
		"description":  {Type: genai.TypeString},
		"confidence":   {Type: genai.TypeString},
	},
	Required: []string{
		"linkedin_url",
		"company",
		"title",
		"description",
		"confidence",
	},
}

func (e *Enricher) Enrich(ctx context.Context, email string) (enrich.Result, error) {
	email = strings.TrimSpace(email)
	base := enrich.Result{Model: e.model}
	if email == "" {
		return base, errors.New("empty email")
	}

	prompt := buildPrompt(email)
	resp, err := e.client.Models.GenerateContent(
		ctx,
		e.model,
		genai.Text(prompt),
		&genai.GenerateContentConfig{
			Tools: []*genai.Tool{
				{GoogleSearch: &genai.GoogleSearch{}},
				{URLContext: &genai.URLContext{}},
			},
			CandidateCount:   1,
			ResponseMIMEType: "application/json",
			ResponseSchema:   outputSchema,
		},
	)
	if err != nil {
		return base, classifyErr(err)
	}

	var parsed responseSchema
	if err := json.Unmarshal([]byte(resp.Text()), &parsed); err != nil {
		return base, fmt.Errorf("gemini: parse structured json: %w", err)
	}

	out := enrich.Result{
		LinkedInURL: strings.TrimSpace(parsed.LinkedInURL),
		Company:     strings.TrimSpace(parsed.Company),
		Title:       strings.TrimSpace(parsed.Title),
		Description: strings.TrimSpace(parsed.Description),
		Confidence:  strings.TrimSpace(parsed.Confidence),
		Model:       e.model,
	}

	if e.captureAudit {
		out.Sources = extractSources(resp)
		out.WebSearchQueries = extractWebSearchQueries(resp)
	}

	return out, nil
}

func buildPrompt(email string) string {
	// Keep this prompt public-safe: do not include any secrets, and avoid embedding
	// unnecessary PII beyond the email itself (required input to enrichment).
	return strings.TrimSpace(`
You are a data enrichment tool. Given an email address, use web search and URL context to find likely public profile/company information.

Return ONLY a single JSON object with these keys:
- linkedin_url (string)
- company (string)
- title (string)
- description (string)
- confidence (string; one of: low, medium, high)

Rules:
- If you cannot find a field, set it to an empty string.
- Do not include extra keys.

Email: ` + email + `
`)
}

func classifyErr(err error) error {
	// Wrap transient failures so the worker pool will retry with backoff.
	var apiErr genai.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Code == 429 || apiErr.Code/100 == 5 {
			return &enrich.TransientError{Err: err}
		}
		return err
	}
	var ne net.Error
	if errors.As(err, &ne) && (ne.Timeout() || ne.Temporary()) {
		return &enrich.TransientError{Err: err}
	}
	return err
}

func extractSources(resp *genai.GenerateContentResponse) []string {
	if resp == nil || len(resp.Candidates) == 0 || resp.Candidates[0] == nil {
		return nil
	}
	c := resp.Candidates[0]

	var out []string
	if c.GroundingMetadata != nil {
		for _, chunk := range c.GroundingMetadata.GroundingChunks {
			if chunk == nil || chunk.Web == nil {
				continue
			}
			if strings.TrimSpace(chunk.Web.URI) != "" {
				out = append(out, strings.TrimSpace(chunk.Web.URI))
			}
		}
	}
	if c.URLContextMetadata != nil {
		for _, m := range c.URLContextMetadata.URLMetadata {
			if m == nil {
				continue
			}
			if strings.TrimSpace(m.RetrievedURL) != "" {
				out = append(out, strings.TrimSpace(m.RetrievedURL))
			}
		}
	}

	return dedupePreserveOrder(out)
}

func extractWebSearchQueries(resp *genai.GenerateContentResponse) []string {
	if resp == nil || len(resp.Candidates) == 0 || resp.Candidates[0] == nil {
		return nil
	}
	c := resp.Candidates[0]
	if c.GroundingMetadata == nil {
		return nil
	}
	return dedupePreserveOrder(c.GroundingMetadata.WebSearchQueries)
}

func dedupePreserveOrder(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
