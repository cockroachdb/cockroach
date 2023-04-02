// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// AggregatorEvent describes an event that can be aggregated and stored by the
// Aggregator. A AggregatorEvent also implements the tracing.LazyTag interface
// to render its information on the associated tracing span.
type AggregatorEvent interface {
	LazyTag

	// Identity returns a AggregatorEvent that when combined with another
	// event returns the other AggregatorEvent unchanged.
	Identity() AggregatorEvent
	// Combine combines two TracingAggregatorEvents together.
	Combine(other AggregatorEvent)
	// Tag returns a string used to identify the AggregatorEvent.
	Tag() string
}

// An Aggregator can be used to aggregate and render AggregatorEvents that are
// emitted as part of its tracing spans' recording.
type Aggregator struct {
	// sp is the tracing span managed by the TracingAggregator.
	sp *Span

	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the tag identifying the
		// AggregatorEvent to the running aggregate of the AggregatorEvent.
		aggregatedEvents map[string]AggregatorEvent
	}
}

// Notify implements the tracing.EventListener interface.
func (b *Aggregator) Notify(event Structured) EventConsumptionStatus {
	bulkEvent, ok := event.(AggregatorEvent)
	if !ok {
		return EventNotConsumed
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If this is the first AggregatorEvent with this tag, set it as a LazyTag on
	// the associated tracing span.
	eventTag := bulkEvent.Tag()
	if _, ok := b.mu.aggregatedEvents[bulkEvent.Tag()]; !ok {
		b.mu.aggregatedEvents[eventTag] = bulkEvent.Identity()
		b.mu.sp.SetLazyTagLocked(eventTag, b.mu.aggregatedEvents[eventTag])
	}
	b.mu.aggregatedEvents[eventTag].Combine(bulkEvent)
	return EventNotConsumed
}

// Close is responsible for finishing the Aggregator's tracing span.
//
// NOTE: it must be called exactly once.
func (b *Aggregator) Close() {
	b.sp.Finish()
}

var _ EventListener = &Aggregator{}

// MakeAggregatorWithSpan returns an instance of an Aggregator along with a
// newly created child context. The Aggregator is registered as a
// tracing.EventListener on the span associated with newly created context.
//
// The Aggregator instance is responsible for finishing the returned span, and
// so the user must call Close().
func MakeAggregatorWithSpan(
	ctx context.Context, aggregatorName string, tracer *Tracer,
) (context.Context, *Aggregator) {
	agg := &Aggregator{}
	aggCtx, aggSpan := EnsureChildSpan(ctx, tracer, aggregatorName,
		WithEventListeners(agg))

	agg.mu.Lock()
	defer agg.mu.Unlock()
	agg.mu.aggregatedEvents = make(map[string]AggregatorEvent)
	agg.sp = aggSpan

	return aggCtx, agg
}
