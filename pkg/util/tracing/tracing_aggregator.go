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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// WrappedAggregatorEvent wraps an AggregatorEvent with the timestamp at which
// the Aggregator was notified about this event.
type WrappedAggregatorEvent struct {
	AggregatorEvent

	// ts is the time at which the Aggregator is notified about the
	// AggregatorEvent.
	ts time.Time
}

// An Aggregator can be used to aggregate and render AggregatorEvents that are
// emitted as part of its tracing spans' recording.
type Aggregator struct {
	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the tag identifying the
		// AggregatorEvent to the running aggregate of the AggregatorEvent.
		aggregatedEvents map[string]*WrappedAggregatorEvent
		// sp is the tracing span managed by the Aggregator.
		sp *Span
		// closed is set to true if the Aggregator has already been closed.
		closed bool
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
		b.mu.aggregatedEvents[eventTag] = &WrappedAggregatorEvent{
			AggregatorEvent: bulkEvent.Identity(),
		}
		b.mu.sp.SetLazyTagLocked(eventTag, b.mu.aggregatedEvents[eventTag].AggregatorEvent)
	}
	b.mu.aggregatedEvents[eventTag].Combine(bulkEvent)
	b.mu.aggregatedEvents[eventTag].ts = timeutil.Now()
	return EventNotConsumed
}

// Close is responsible for finishing the Aggregators' tracing span.
func (b *Aggregator) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.mu.closed {
		b.mu.sp.Finish()
		b.mu.closed = true
	}
}

// Reset resets the Aggregator state.
func (b *Aggregator) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.aggregatedEvents = make(map[string]*WrappedAggregatorEvent)
}

// ForEachAggregatedEvent loops over the Aggregator's current aggregated events.
// The events are looped over in chronological order of when the Aggregator was
// notified about them.
func (b *Aggregator) ForEachAggregatedEvent(
	_ context.Context, f func(tag string, aggregatorEvent AggregatorEvent),
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, len(b.mu.aggregatedEvents))
	i := 0
	for tag := range b.mu.aggregatedEvents {
		keys[i] = tag
		i++
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return b.mu.aggregatedEvents[keys[i]].ts.Before(
			b.mu.aggregatedEvents[keys[j]].ts)
	})
	for _, k := range keys {
		f(k, b.mu.aggregatedEvents[k].AggregatorEvent)
	}
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
	agg.mu.aggregatedEvents = make(map[string]*WrappedAggregatorEvent)
	agg.mu.sp = aggSpan

	return aggCtx, agg
}
