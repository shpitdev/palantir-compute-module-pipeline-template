package gemini

import (
	"errors"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/examples/email_enricher/enrich"
	"google.golang.org/genai"
)

type tempNetErr struct{}

func (tempNetErr) Error() string   { return "temp net err" }
func (tempNetErr) Timeout() bool   { return false }
func (tempNetErr) Temporary() bool { return true }

func TestClassifyErr(t *testing.T) {
	tests := []struct {
		name          string
		in            error
		wantTransient bool
	}{
		{name: "nil", in: nil, wantTransient: false},
		{name: "api_429", in: genai.APIError{Code: 429}, wantTransient: true},
		{name: "api_500", in: genai.APIError{Code: 500}, wantTransient: true},
		{name: "api_401", in: genai.APIError{Code: 401}, wantTransient: false},
		{name: "net_temporary", in: tempNetErr{}, wantTransient: true},
		{name: "wrapped_api_429", in: errors.New(genai.APIError{Code: 429}.Error()), wantTransient: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyErr(tt.in)
			var te *enrich.TransientError
			isTransient := errors.As(got, &te)
			if isTransient != tt.wantTransient {
				t.Fatalf("transient=%v want=%v (err=%T %v)", isTransient, tt.wantTransient, got, got)
			}
		})
	}
}
