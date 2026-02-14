package worker_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/core"
	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/worker"
)

func TestProcessAll_RetriesTransient(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0
	failUntil := 2

	fn := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= failUntil {
			return "", &core.TransientError{Err: errors.New("try again")}
		}
		return "ok", nil
	}

	out, err := worker.ProcessAll(context.Background(), []string{"alice@example.com"}, fn, worker.Options{
		Workers:           1,
		MaxRetries:        3,
		FailurePolicy:     worker.FailurePolicyPartialOutput,
		RequestTimeout:    1 * time.Second,
		BackoffInitial:    1 * time.Millisecond,
		BackoffMax:        2 * time.Millisecond,
		BackoffJitterFrac: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Err != nil || out[0].Output != "ok" {
		t.Fatalf("unexpected output: %#v", out[0])
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestProcessAll_DoesNotRetryPermanent(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0

	fn := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return "", errors.New("permanent")
	}

	out, err := worker.ProcessAll(context.Background(), []string{"alice@example.com"}, fn, worker.Options{
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

func TestProcessAll_RespectsPerErrorRetryCap(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0

	fn := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return "", &core.LimitedTransientError{
			Err:          errors.New("cancelled"),
			ExtraRetries: 1, // one extra retry max
		}
	}

	out, err := worker.ProcessAll(context.Background(), []string{"alice@example.com"}, fn, worker.Options{
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
	if out[0].Err == nil {
		t.Fatalf("expected error output, got %#v", out[0])
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 calls (1 initial + 1 retry), got %d", calls)
	}
}

func TestProcessAll_FailFastStops(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0

	fn := func(_ context.Context, email string) (string, error) {
		mu.Lock()
		calls++
		mu.Unlock()

		if email == "bad@example.com" {
			return "", errors.New("boom")
		}
		t.Fatalf("unexpected call for %q", email)
		return "", nil
	}

	out, err := worker.ProcessAll(context.Background(), []string{"bad@example.com", "good@example.com"}, fn, worker.Options{
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

func TestProcessAll_PartialOutputContinues(t *testing.T) {
	t.Parallel()

	fn := func(_ context.Context, email string) (string, error) {
		if email == "bad@example.com" {
			return "", errors.New("boom")
		}
		return "ok", nil
	}

	out, err := worker.ProcessAll(context.Background(), []string{"bad@example.com", "good@example.com"}, fn, worker.Options{
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
	if out[1].Err != nil || out[1].Output != "ok" {
		t.Fatalf("unexpected out[1]: %#v", out[1])
	}
}

func TestProcessAllWithCallback_CompletesInCompletionOrder(t *testing.T) {
	t.Parallel()

	releaseSlow := make(chan struct{})
	startedSlow := make(chan struct{})
	var firstCallbackInput atomic.Value
	firstCallbackInput.Store("")

	fn := func(_ context.Context, email string) (string, error) {
		if email == "slow@example.com" {
			close(startedSlow)
			<-releaseSlow
		}
		return email, nil
	}

	var mu sync.Mutex
	var seen []string
	doneErr := make(chan error, 1)
	go func() {
		_, err := worker.ProcessAllWithCallback(
			context.Background(),
			[]string{"slow@example.com", "fast@example.com"},
			fn,
			func(res worker.Result[string, string]) error {
				mu.Lock()
				defer mu.Unlock()
				seen = append(seen, res.Input)
				if len(seen) == 1 {
					firstCallbackInput.Store(res.Input)
				}
				return nil
			},
			worker.Options{Workers: 2},
		)
		doneErr <- err
	}()

	select {
	case <-startedSlow:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for slow task to start")
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if firstCallbackInput.Load().(string) == "fast@example.com" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := firstCallbackInput.Load().(string); got != "fast@example.com" {
		t.Fatalf("expected fast callback first, got %q", got)
	}

	close(releaseSlow)
	select {
	case err := <-doneErr:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for completion")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("expected 2 callbacks, got %d (%v)", len(seen), seen)
	}
	if !slices.Equal(seen, []string{"fast@example.com", "slow@example.com"}) {
		t.Fatalf("unexpected callback order: %v", seen)
	}
}

func TestProcessAllWithCallback_CallbackErrorStopsRun(t *testing.T) {
	t.Parallel()

	callbackErr := errors.New("callback failed")
	_, err := worker.ProcessAllWithCallback(
		context.Background(),
		[]string{"alice@example.com"},
		func(_ context.Context, email string) (string, error) {
			return email, nil
		},
		func(worker.Result[string, string]) error {
			return callbackErr
		},
		worker.Options{Workers: 1},
	)
	if !errors.Is(err, callbackErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
}
