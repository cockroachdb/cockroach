// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perftrace

import (
	"context"
	"time"
)

// SpanType categorizes the type of work span.
type SpanType string

const (
	// SpanTypeGateway represents the entire statement execution from the gateway's perspective.
	SpanTypeGateway SpanType = "gateway"
	// SpanTypeProcessor represents a relational operator (TableReader, HashJoiner, etc.).
	SpanTypeProcessor SpanType = "processor"
	// SpanTypeKVBatch represents a batch-level KV operation (dist sender send).
	SpanTypeKVBatch SpanType = "kv_batch"
)

// WorkSpan represents a captured execution span.
type WorkSpan struct {
	// ID is a unique identifier for this span, generated via unique_rowid().
	ID int64
	// ParentID is the ID of the parent span (0 if no parent).
	ParentID int64
	// NodeID is the SQL instance ID of the node where this span was captured.
	NodeID int32
	// StatementFingerprintID is the fingerprint ID of the statement.
	StatementFingerprintID uint64
	// Timestamp is when the span started.
	Timestamp time.Time
	// Duration is the wall-clock time of the span in nanoseconds.
	Duration time.Duration
	// CPUTime is the CPU time consumed by this span in nanoseconds.
	CPUTime time.Duration
	// ContentionTime is the time spent waiting on locks and latches in nanoseconds.
	ContentionTime time.Duration
	// SpanType categorizes the span (gateway, processor, kv_batch).
	SpanType SpanType
	// SpanName is the name of the span (e.g., "TableReader", "dist sender send").
	SpanName string
}

// contextKey is used for storing work span context values.
type contextKey int

const (
	// parentSpanIDKey is the context key for the parent work span ID.
	parentSpanIDKey contextKey = iota
	// fingerprintIDKey is the context key for the statement fingerprint ID.
	fingerprintIDKey
	// currentSpanHandleKey is the context key for the current work span handle.
	currentSpanHandleKey
)

// WithParentSpanID returns a new context with the parent work span ID set.
func WithParentSpanID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, parentSpanIDKey, id)
}

// GetParentSpanIDFromContext returns the parent work span ID from the context.
// Returns 0 if not set.
func GetParentSpanIDFromContext(ctx context.Context) int64 {
	if v := ctx.Value(parentSpanIDKey); v != nil {
		return v.(int64)
	}
	return 0
}

// WithFingerprintID returns a new context with the statement fingerprint ID set.
func WithFingerprintID(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, fingerprintIDKey, id)
}

// GetFingerprintFromContext returns the statement fingerprint ID from the context.
// Returns 0 if not set.
func GetFingerprintFromContext(ctx context.Context) uint64 {
	if v := ctx.Value(fingerprintIDKey); v != nil {
		return v.(uint64)
	}
	return 0
}

// collectorKey is the context key for the work span collector.
type collectorKeyType struct{}

var collectorKey = collectorKeyType{}

// WithCollector returns a new context with the work span collector set.
// This is used to make the collector available to the KV layer.
func WithCollector(ctx context.Context, collector *Collector) context.Context {
	return context.WithValue(ctx, collectorKey, collector)
}

// GetCollectorFromContext returns the work span collector from the context.
// Returns nil if not set.
func GetCollectorFromContext(ctx context.Context) *Collector {
	if v := ctx.Value(collectorKey); v != nil {
		return v.(*Collector)
	}
	return nil
}

// WithCurrentSpanHandle returns a new context with the current work span handle set.
// This is used to make the span handle available to low-level waiting code
// (latch manager, lock table waiter) so they can add contention time directly.
func WithCurrentSpanHandle(ctx context.Context, handle *SpanHandle) context.Context {
	return context.WithValue(ctx, currentSpanHandleKey, handle)
}

// GetCurrentSpanHandleFromContext returns the current work span handle from the context.
// Returns nil if not set.
func GetCurrentSpanHandleFromContext(ctx context.Context) *SpanHandle {
	if v := ctx.Value(currentSpanHandleKey); v != nil {
		return v.(*SpanHandle)
	}
	return nil
}

// AddContentionTimeFromContext adds the given contention duration to the current
// work span handle in the context. This is a convenience function for use by
// low-level waiting code that doesn't need to directly manipulate the handle.
// If there is no current span handle in the context, this is a no-op.
func AddContentionTimeFromContext(ctx context.Context, d time.Duration) {
	if h := GetCurrentSpanHandleFromContext(ctx); h != nil {
		h.AddContentionTime(d)
	}
}
