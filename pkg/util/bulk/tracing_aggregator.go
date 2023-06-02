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
	// sp is the tracing span managed by the TracingAggregator.
	sp *tracing.Span
	// aggregatedEvents is a mapping from the tag identifying the
	// TracingAggregatorEvent to the running aggregate of the
	// TracingAggregatorEvent.
	aggregatedEvents map[string]TracingAggregatorEvent
}

// Notify implements the tracing.EventListener interface.
func (b *TracingAggregator) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	bulkEvent, ok := event.(TracingAggregatorEvent)
	if !ok {
		return tracing.EventNotConsumed
	}

	// If this is the first TracingAggregatorEvent with this tag, set it as a
	// LazyTag on the associated tracing span.
	eventTag := bulkEvent.Tag()
	if _, ok := b.aggregatedEvents[bulkEvent.Tag()]; !ok {
		b.aggregatedEvents[eventTag] = bulkEvent.Identity()
		b.sp.SetLazyTagLocked(eventTag, b.aggregatedEvents[eventTag])
	}
	b.aggregatedEvents[eventTag].Combine(bulkEvent)
	return tracing.EventNotConsumed
}

// Close is responsible for finishing the TracingAggregator's tracing span.
//
// NOTE: it must be called exactly once.
func (b *TracingAggregator) Close() {
	b.sp.Finish()
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

	agg.aggregatedEvents = make(map[string]TracingAggregatorEvent)
	agg.sp = aggSpan

	return aggCtx, agg
}
