package worker

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/core"
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

// Result holds the output for one input item.
type Result[In any, Out any] struct {
	Input  In
	Output Out
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

// ProcessAll runs the processor over all input items.
func ProcessAll[In any, Out any](
	ctx context.Context,
	items []In,
	processor func(context.Context, In) (Out, error),
	opts Options,
) ([]Result[In, Out], error) {
	return ProcessAllWithCallback(ctx, items, processor, nil, opts)
}

// ProcessAllWithCallback runs the processor over all input items and invokes onResult
// as each item completes. The callback receives completion-order results.
func ProcessAllWithCallback[In any, Out any](
	ctx context.Context,
	items []In,
	processor func(context.Context, In) (Out, error),
	onResult func(Result[In, Out]) error,
	opts Options,
) ([]Result[In, Out], error) {
	opts = opts.withDefaults()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var limiter *rate.Limiter
	if opts.RateLimitRPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(opts.RateLimitRPS), 1)
	}

	out := make([]Result[In, Out], len(items))

	type job struct {
		idx int
		in  In
	}
	type completion struct {
		idx int
		res Result[In, Out]
	}

	jobs := make(chan job)
	done := make(chan completion, opts.Workers)

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

	workerFn := func() {
		defer wg.Done()
		for j := range jobs {
			if runCtx.Err() != nil {
				return
			}
			res := processOne(runCtx, j.in, processor, limiter, opts)
			select {
			case done <- completion{idx: j.idx, res: res}:
			case <-runCtx.Done():
				return
			}
			if res.Err != nil && opts.FailurePolicy == FailurePolicyFailFast {
				fail(res.Err)
				return
			}
		}
	}

	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go workerFn()
	}

	go func() {
		defer close(jobs)
		for i, item := range items {
			select {
			case jobs <- job{idx: i, in: item}:
			case <-runCtx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	for item := range done {
		out[item.idx] = item.res
		if onResult != nil {
			if err := onResult(item.res); err != nil {
				fail(err)
			}
		}
	}

	mu.Lock()
	err := firstErr
	mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func processOne[In any, Out any](
	ctx context.Context,
	item In,
	processor func(context.Context, In) (Out, error),
	limiter *rate.Limiter,
	opts Options,
) Result[In, Out] {
	res, err := processWithRetry(ctx, item, processor, limiter, opts)
	return Result[In, Out]{
		Input:  item,
		Output: res,
		Err:    err,
	}
}

func processWithRetry[In any, Out any](
	ctx context.Context,
	item In,
	processor func(context.Context, In) (Out, error),
	limiter *rate.Limiter,
	opts Options,
) (Out, error) {
	var lastOut Out
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return lastOut, err
		}

		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return lastOut, err
			}
		}

		reqCtx := ctx
		var cancel context.CancelFunc
		if opts.RequestTimeout > 0 {
			reqCtx, cancel = context.WithTimeout(ctx, opts.RequestTimeout)
		}
		result, err := processor(reqCtx, item)
		lastOut = result
		if cancel != nil {
			cancel()
		}
		if err == nil {
			return result, nil
		}
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return lastOut, ctx.Err()
		}
		maxRetries := maxExtraRetries(opts.MaxRetries, err)
		if !isTransient(err) || attempt >= maxRetries {
			return lastOut, err
		}

		sleep := backoffSleep(opts.BackoffInitial, opts.BackoffMax, opts.BackoffJitterFrac, attempt)
		t := time.NewTimer(sleep)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return lastOut, ctx.Err()
		}
	}
}

type retryCap interface {
	MaxExtraRetries() int
}

func maxExtraRetries(defaultRetries int, err error) int {
	if defaultRetries < 0 {
		defaultRetries = 0
	}
	var capErr retryCap
	if errors.As(err, &capErr) {
		limited := capErr.MaxExtraRetries()
		if limited < 0 {
			limited = 0
		}
		if limited < defaultRetries {
			return limited
		}
	}
	return defaultRetries
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	var te *core.TransientError
	if errors.As(err, &te) {
		return true
	}
	var lte *core.LimitedTransientError
	if errors.As(err, &lte) {
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
