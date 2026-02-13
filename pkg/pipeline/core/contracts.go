package core

import (
	"context"
)

// InputAdapter loads input records for pipeline processing.
type InputAdapter[In any] interface {
	Load(ctx context.Context) ([]In, error)
}

// OutputAdapter persists output records produced by pipeline processing.
type OutputAdapter[Out any] interface {
	Store(ctx context.Context, rows []Out) error
}

// Processor transforms one input item into one output item.
type Processor[In any, Out any] interface {
	Process(ctx context.Context, in In) (Out, error)
}

// ProcessFunc adapts a function to the Processor interface.
type ProcessFunc[In any, Out any] func(ctx context.Context, in In) (Out, error)

func (f ProcessFunc[In, Out]) Process(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

// TransientError marks an error as retryable by worker implementations.
type TransientError struct {
	Err error
}

func (e *TransientError) Error() string {
	if e == nil || e.Err == nil {
		return "transient error"
	}
	return e.Err.Error()
}

func (e *TransientError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// LimitedTransientError marks an error as retryable, but caps extra retries.
//
// ExtraRetries is the number of retries after the first failed attempt.
// For example, ExtraRetries=1 means "at most one retry".
type LimitedTransientError struct {
	Err error

	// ExtraRetries caps retries for this specific error instance.
	ExtraRetries int
}

func (e *LimitedTransientError) Error() string {
	if e == nil || e.Err == nil {
		return "limited transient error"
	}
	return e.Err.Error()
}

func (e *LimitedTransientError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// MaxExtraRetries returns the per-error retry cap.
func (e *LimitedTransientError) MaxExtraRetries() int {
	if e == nil {
		return 0
	}
	return e.ExtraRetries
}
