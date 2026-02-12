package processor

import (
	"context"
	"strings"
)

type Result struct {
	Input  string
	Output string
}

type Processor struct{}

func (Processor) Process(_ context.Context, in string) (Result, error) {
	trimmed := strings.TrimSpace(in)
	return Result{Input: in, Output: strings.ToUpper(trimmed)}, nil
}
