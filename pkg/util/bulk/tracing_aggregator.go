// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// TracingAggregatorEvent describes an event that can be aggregated and stored by the
// TracingAggregator. A TracingAggregatorEvent also implements the tracing.LazyTag interface
// to render its information on the associated tracing span.
type TracingAggregatorEvent interface {
	tracing.LazyTag

	// Identity returns a TracingAggregatorEvent that when combined with another
	// event returns the other TracingAggregatorEvent unchanged.
	Identity() TracingAggregatorEvent
	// Combine combines two TracingAggregatorEvents together.
	Combine(other TracingAggregatorEvent)
	// Tag returns a string used to identify the TracingAggregatorEvent.
	Tag() string
}

// A TracingAggregator can be used to aggregate and render AggregatorEvents that
// are emitted as part of its tracing spans' recording.
type TracingAggregator struct {
	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the tag identifying the
		// TracingAggregatorEvent to the running aggregate of the TracingAggregatorEvent.
		aggregatedEvents map[string]TracingAggregatorEvent
		// sp is the tracing span managed by the TracingAggregator.
		sp *tracing.Span
		// closed is set to true if the TracingAggregator has already been closed.
		closed bool
	}
}

// Notify implements the tracing.EventListener interface.
func (b *TracingAggregator) Notify(event tracing.Structured) {
	bulkEvent, ok := event.(TracingAggregatorEvent)
	if !ok {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If this is the first AggregatorEvent with this tag, set it as a LazyTag on
	// the associated tracing span.
	eventTag := bulkEvent.Tag()
	if _, ok := b.mu.aggregatedEvents[bulkEvent.Tag()]; !ok {
		b.mu.aggregatedEvents[eventTag] = bulkEvent.Identity()
		b.mu.sp.SetLazyTag(eventTag, b.mu.aggregatedEvents[eventTag])
	}
	b.mu.aggregatedEvents[eventTag].Combine(bulkEvent)
}

// Close is responsible for finishing the Aggregators' tracing span.
func (b *TracingAggregator) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.mu.closed {
		b.mu.sp.Finish()
		b.mu.closed = true
	}
}

var _ tracing.EventListener = &TracingAggregator{}

// MakeTracingAggregatorWithSpan returns an instance of an TracingAggregator along with a
// newly created child context. The TracingAggregator is registered as a
// tracing.EventListener on the span associated with newly created context.
//
// The TracingAggregator instance is responsible for finishing the returned span, and
// so the user must call Close().
func MakeTracingAggregatorWithSpan(
	ctx context.Context, aggregatorName string, tracer *tracing.Tracer,
) (context.Context, *TracingAggregator) {
	agg := &TracingAggregator{}
	aggCtx, aggSpan := tracing.EnsureChildSpan(ctx, tracer, aggregatorName,
		tracing.WithEventListeners(agg))

	agg.mu.Lock()
	defer agg.mu.Unlock()
	agg.mu.aggregatedEvents = make(map[string]TracingAggregatorEvent)
	agg.mu.sp = aggSpan

	return aggCtx, agg
}
