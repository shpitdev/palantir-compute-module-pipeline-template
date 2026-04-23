package foundryio

import (
	"context"
	"errors"
	"net"
	"syscall"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
)

// RetryPolicy captures retry behavior for Foundry I/O calls.
type RetryPolicy struct {
	Attempts     int
	InitialSleep time.Duration
	MaxSleep     time.Duration
}

// DefaultRetryPolicy is intentionally shared by dataset and legacy stream-proxy
// calls so retryability stays consistent across Foundry I/O surfaces.
var DefaultRetryPolicy = RetryPolicy{
	Attempts:     8,
	InitialSleep: 200 * time.Millisecond,
	MaxSleep:     2 * time.Second,
}

// RetryTransient retries f when it returns an error classified as transient.
func RetryTransient(ctx context.Context, policy RetryPolicy, f func() error) error {
	policy = normalizeRetryPolicy(policy)
	sleep := policy.InitialSleep
	var lastErr error
	for i := 0; i < policy.Attempts; i++ {
		if err := f(); err == nil {
			return nil
		} else {
			lastErr = err
			if !IsTransient(err) || i == policy.Attempts-1 {
				return err
			}
		}

		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		sleep *= 2
		if sleep > policy.MaxSleep {
			sleep = policy.MaxSleep
		}
	}
	return lastErr
}

// IsTransient classifies retryable Foundry I/O failures.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var he *foundry.HTTPError
	if errors.As(err, &he) {
		return he.StatusCode == 429 || he.StatusCode/100 == 5
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout() || ne.Temporary()
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	return false
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.Attempts <= 0 {
		policy.Attempts = DefaultRetryPolicy.Attempts
	}
	if policy.InitialSleep <= 0 {
		policy.InitialSleep = DefaultRetryPolicy.InitialSleep
	}
	if policy.MaxSleep <= 0 {
		policy.MaxSleep = DefaultRetryPolicy.MaxSleep
	}
	if policy.MaxSleep < policy.InitialSleep {
		policy.MaxSleep = policy.InitialSleep
	}
	return policy
}
