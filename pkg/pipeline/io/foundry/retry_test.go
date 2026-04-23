package foundryio_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
	foundryio "github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/foundry"
)

func TestIsTransient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "rate limited", err: &foundry.HTTPError{StatusCode: 429}, want: true},
		{name: "server error", err: &foundry.HTTPError{StatusCode: 503}, want: true},
		{name: "permission denied", err: &foundry.HTTPError{StatusCode: 403}, want: false},
		{name: "not found", err: &foundry.HTTPError{StatusCode: 404}, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "plain error", err: errors.New("boom"), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := foundryio.IsTransient(tt.err); got != tt.want {
				t.Fatalf("IsTransient(%v)=%t want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestRetryTransient(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := foundryio.RetryTransient(context.Background(), foundryio.RetryPolicy{
		Attempts:     3,
		InitialSleep: time.Nanosecond,
		MaxSleep:     time.Nanosecond,
	}, func() error {
		attempts++
		if attempts < 3 {
			return &foundry.HTTPError{StatusCode: 500}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetryTransient returned error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}
