package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich/worker"
)

type fnEnricher struct {
	f func(ctx context.Context, email string) (enrich.Result, error)
}

func (e fnEnricher) Enrich(ctx context.Context, email string) (enrich.Result, error) {
	return e.f(ctx, email)
}

func TestEnrichAll_RetriesTransient(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0
	failUntil := 2

	e := fnEnricher{f: func(_ context.Context, _ string) (enrich.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= failUntil {
			return enrich.Result{}, &enrich.TransientError{Err: errors.New("try again")}
		}
		return enrich.Result{Company: "ok"}, nil
	}}

	out, err := worker.EnrichAll(context.Background(), []string{"alice@example.com"}, e, worker.Options{
		Workers:           1,
		MaxRetries:        3,
		FailurePolicy:     worker.FailurePolicyPartialOutput,
		RequestTimeout:    1 * time.Second,
		BackoffInitial:    1 * time.Millisecond,
		BackoffMax:        2 * time.Millisecond,
		BackoffJitterFrac: 0, // deterministic
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Err != nil || out[0].Result.Company != "ok" {
		t.Fatalf("unexpected output: %#v", out[0])
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestEnrichAll_DoesNotRetryPermanent(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0

	e := fnEnricher{f: func(_ context.Context, _ string) (enrich.Result, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return enrich.Result{}, errors.New("permanent")
	}}

	out, err := worker.EnrichAll(context.Background(), []string{"alice@example.com"}, e, worker.Options{
		Workers:           1,
		MaxRetries:        10,
		FailurePolicy:     worker.FailurePolicyPartialOutput,
		BackoffInitial:    1 * time.Millisecond,
		BackoffMax:        1 * time.Millisecond,
		BackoffJitterFrac: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Err == nil || out[0].Err.Error() != "permanent" {
		t.Fatalf("unexpected output: %#v", out[0])
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestEnrichAll_FailFastStops(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0

	e := fnEnricher{f: func(_ context.Context, email string) (enrich.Result, error) {
		mu.Lock()
		calls++
		mu.Unlock()

		if email == "bad@example.com" {
			return enrich.Result{}, errors.New("boom")
		}
		t.Fatalf("unexpected call for %q", email)
		return enrich.Result{}, nil
	}}

	out, err := worker.EnrichAll(context.Background(), []string{"bad@example.com", "good@example.com"}, e, worker.Options{
		Workers:       1,
		MaxRetries:    0,
		FailurePolicy: worker.FailurePolicyFailFast,
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom error, got %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil output on fail-fast, got %#v", out)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestEnrichAll_PartialOutputContinues(t *testing.T) {
	t.Parallel()

	e := fnEnricher{f: func(_ context.Context, email string) (enrich.Result, error) {
		if email == "bad@example.com" {
			return enrich.Result{}, errors.New("boom")
		}
		return enrich.Result{Company: "ok"}, nil
	}}

	out, err := worker.EnrichAll(context.Background(), []string{"bad@example.com", "good@example.com"}, e, worker.Options{
		Workers:       1,
		MaxRetries:    0,
		FailurePolicy: worker.FailurePolicyPartialOutput,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(out))
	}
	if out[0].Err == nil || out[0].Err.Error() != "boom" {
		t.Fatalf("unexpected out[0]: %#v", out[0])
	}
	if out[1].Err != nil || out[1].Result.Company != "ok" {
		t.Fatalf("unexpected out[1]: %#v", out[1])
	}
}

func TestEnrichAll_EmptyEmailDoesNotCallEnricher(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0

	e := fnEnricher{f: func(_ context.Context, _ string) (enrich.Result, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return enrich.Result{}, nil
	}}

	out, err := worker.EnrichAll(context.Background(), []string{"", "alice@example.com"}, e, worker.Options{
		Workers:       1,
		FailurePolicy: worker.FailurePolicyPartialOutput,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(out))
	}
	if out[0].Err == nil || out[0].Err.Error() != "empty email" {
		t.Fatalf("unexpected out[0]: %#v", out[0])
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}
