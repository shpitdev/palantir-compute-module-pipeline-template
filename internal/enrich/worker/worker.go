package worker

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/enrich"
	"golang.org/x/time/rate"
)

type FailurePolicy int

const (
	FailurePolicyPartialOutput FailurePolicy = iota
	FailurePolicyFailFast
)

type Options struct {
	Workers        int
	MaxRetries     int
	RequestTimeout time.Duration

	// RateLimitRPS is a global limit across all workers. Set to <=0 to disable.
	RateLimitRPS float64

	FailurePolicy FailurePolicy

	// BackoffInitial is the initial sleep before retrying a transient failure.
	BackoffInitial time.Duration
	// BackoffMax caps exponential backoff.
	BackoffMax time.Duration
	// BackoffJitterFrac applies +/- jitter to backoff sleeps (0.2 = +/-20%).
	BackoffJitterFrac float64
}

type Output struct {
	Email  string
	Result enrich.Result
	Err    error
}

func (o Options) withDefaults() Options {
	if o.Workers <= 0 {
		o.Workers = 10
	}
	if o.MaxRetries < 0 {
		o.MaxRetries = 0
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 30 * time.Second
	}
	if o.BackoffInitial <= 0 {
		o.BackoffInitial = 200 * time.Millisecond
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 2 * time.Second
	}
	if o.BackoffJitterFrac <= 0 {
		o.BackoffJitterFrac = 0.2
	}
	return o
}

func EnrichAll(ctx context.Context, emails []string, enricher enrich.Enricher, opts Options) ([]Output, error) {
	opts = opts.withDefaults()

	runCtx := ctx
	var cancel context.CancelFunc
	if opts.FailurePolicy == FailurePolicyFailFast {
		runCtx, cancel = context.WithCancel(ctx)
	}
	if cancel != nil {
		defer cancel()
	}

	var limiter *rate.Limiter
	if opts.RateLimitRPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(opts.RateLimitRPS), 1)
	}

	out := make([]Output, len(emails))

	type job struct {
		idx int
		raw string
	}
	jobs := make(chan job)

	var wg sync.WaitGroup

	var mu sync.Mutex
	var firstErr error
	fail := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			if cancel != nil {
				cancel()
			}
		}
		mu.Unlock()
	}

	worker := func() {
		defer wg.Done()
		for j := range jobs {
			if runCtx.Err() != nil {
				return
			}
			res := enrichOne(runCtx, j.raw, enricher, limiter, opts)
			out[j.idx] = res
			if res.Err != nil && opts.FailurePolicy == FailurePolicyFailFast {
				fail(res.Err)
				return
			}
		}
	}

	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go worker()
	}

	for i, raw := range emails {
		select {
		case jobs <- job{idx: i, raw: raw}:
		case <-runCtx.Done():
			break
		}
	}
	close(jobs)
	wg.Wait()

	if opts.FailurePolicy == FailurePolicyFailFast {
		mu.Lock()
		err := firstErr
		mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func enrichOne(ctx context.Context, raw string, enricher enrich.Enricher, limiter *rate.Limiter, opts Options) Output {
	email := strings.TrimSpace(raw)
	if email == "" {
		return Output{Email: "", Err: errors.New("empty email")}
	}

	res, err := enrichWithRetry(ctx, enricher, email, limiter, opts)
	return Output{
		Email:  email,
		Result: res,
		Err:    err,
	}
}

func enrichWithRetry(ctx context.Context, enricher enrich.Enricher, email string, limiter *rate.Limiter, opts Options) (enrich.Result, error) {
	var lastErr error
	var lastRes enrich.Result
	attempts := 1 + opts.MaxRetries
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return lastRes, err
		}

		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return lastRes, err
			}
		}

		reqCtx := ctx
		var cancel context.CancelFunc
		if opts.RequestTimeout > 0 {
			reqCtx, cancel = context.WithTimeout(ctx, opts.RequestTimeout)
		}
		res, err := enricher.Enrich(reqCtx, email)
		lastRes = res
		if cancel != nil {
			cancel()
		}
		if err == nil {
			return res, nil
		}
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return lastRes, ctx.Err()
		}
		lastErr = err
		if !isTransient(err) || attempt == attempts-1 {
			return lastRes, err
		}

		sleep := backoffSleep(opts.BackoffInitial, opts.BackoffMax, opts.BackoffJitterFrac, attempt)
		t := time.NewTimer(sleep)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return lastRes, ctx.Err()
		}
	}
	return lastRes, lastErr
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	var te *enrich.TransientError
	if errors.As(err, &te) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout() || ne.Temporary()
	}
	return false
}

func backoffSleep(initial, max time.Duration, jitterFrac float64, attempt int) time.Duration {
	sleep := initial
	for i := 0; i < attempt && sleep < max; i++ {
		sleep *= 2
		if sleep > max {
			sleep = max
			break
		}
	}
	if jitterFrac <= 0 {
		return sleep
	}
	// Apply +/- jitterFrac.
	j := 1 + (rand.Float64()*2-1)*jitterFrac
	return time.Duration(float64(sleep) * j)
}
