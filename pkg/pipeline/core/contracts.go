package core

import "context"

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
