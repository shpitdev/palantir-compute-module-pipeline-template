package foundryio

import (
	"context"
	"fmt"
	"strings"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/foundry"
)

// StreamBackend isolates the stream API family used for stream outputs.
//
// The current implementation targets the legacy compute-module stream-proxy
// surface. A future high-scale streams implementation can satisfy this same
// boundary without changing app orchestration.
type StreamBackend interface {
	Probe(ctx context.Context, ref foundry.DatasetRef) (bool, error)
	ReadRecords(ctx context.Context, ref foundry.DatasetRef) ([]map[string]any, error)
	PublishRecord(ctx context.Context, ref foundry.DatasetRef, record map[string]any) error
}

// LegacyStreamProxyBackend implements StreamBackend using the legacy
// /streams/{rid}/branches/{branch}/records and /jsonRecord endpoints.
type LegacyStreamProxyBackend struct {
	client *foundry.Client
	retry  RetryPolicy
}

// NewLegacyStreamProxyBackend constructs a stream backend for the current
// compute-module-compatible stream-proxy surface.
func NewLegacyStreamProxyBackend(client *foundry.Client) *LegacyStreamProxyBackend {
	return &LegacyStreamProxyBackend{
		client: client,
		retry:  DefaultRetryPolicy,
	}
}

// WithRetryPolicy returns a copy of the backend with a custom retry policy.
func (b *LegacyStreamProxyBackend) WithRetryPolicy(policy RetryPolicy) *LegacyStreamProxyBackend {
	cp := *b
	cp.retry = normalizeRetryPolicy(policy)
	return &cp
}

func (b *LegacyStreamProxyBackend) Probe(ctx context.Context, ref foundry.DatasetRef) (bool, error) {
	if b == nil || b.client == nil {
		return false, fmt.Errorf("legacy stream-proxy backend requires a foundry client")
	}
	branch := defaultBranch(ref.Branch)
	isStream := false
	err := RetryTransient(ctx, b.retry, func() error {
		var err error
		isStream, err = b.client.ProbeStream(ctx, ref.RID, branch)
		return err
	})
	if err != nil {
		return false, err
	}
	return isStream, nil
}

func (b *LegacyStreamProxyBackend) ReadRecords(ctx context.Context, ref foundry.DatasetRef) ([]map[string]any, error) {
	if b == nil || b.client == nil {
		return nil, fmt.Errorf("legacy stream-proxy backend requires a foundry client")
	}
	branch := defaultBranch(ref.Branch)
	var records []map[string]any
	err := RetryTransient(ctx, b.retry, func() error {
		var err error
		records, err = b.client.ReadStreamRecords(ctx, ref.RID, branch)
		return err
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (b *LegacyStreamProxyBackend) PublishRecord(ctx context.Context, ref foundry.DatasetRef, record map[string]any) error {
	if b == nil || b.client == nil {
		return fmt.Errorf("legacy stream-proxy backend requires a foundry client")
	}
	branch := defaultBranch(ref.Branch)
	return RetryTransient(ctx, b.retry, func() error {
		return b.client.PublishStreamJSONRecord(ctx, ref.RID, branch, record)
	})
}

func defaultBranch(branch string) string {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return "master"
	}
	return branch
}
