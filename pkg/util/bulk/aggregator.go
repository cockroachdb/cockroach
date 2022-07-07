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

// AggregatorEvent describes an event that can be aggregated and stored by the
// Aggregator. An AggregatorEvent also implements the tracing.LazyTag interface
// to render its information on the associated tracing span.
type AggregatorEvent interface {
	tracing.LazyTag

	// Combine combines two AggregatorEvents together.
	Combine(other AggregatorEvent)
	// Tag returns a string used to identify the AggregatorEvent.
	Tag() string
}

// An Aggregator can be used to aggregate and render AggregatorEvents that are
// emitted as part of its tracing spans' recording.
type Aggregator struct {
	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the tag identifying the
		// AggregatorEvent to the running aggregate of the AggregatorEvent.
		aggregatedEvents map[string]AggregatorEvent
		// sp is the tracing span managed by the Aggregator.
		sp *tracing.Span
	}
}

// Notify implements the tracing.EventListener interface.
func (b *Aggregator) Notify(event tracing.Structured) {
	bulkEvent, ok := event.(AggregatorEvent)
	if !ok {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If this is the first AggregatorEvent with this tag, set it as a LazyTag on
	// the associated tracing span. This way the AggregatorEvent will be
	// dynamically Render()ed everytime we pull the tracing for the associated
	// span.
	eventTag := bulkEvent.Tag()
	if _, ok := b.mu.aggregatedEvents[bulkEvent.Tag()]; !ok {
		b.mu.aggregatedEvents[eventTag] = bulkEvent
		b.mu.sp.SetLazyTag(eventTag, b.mu.aggregatedEvents[eventTag])
	} else {
		b.mu.aggregatedEvents[eventTag].Combine(bulkEvent)
	}
}

// Close is responsible for finishing the Aggregators' tracing span.
func (b *Aggregator) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.sp.Finish()
}

var _ tracing.EventListener = &Aggregator{}

// MakeAggregatorWithSpan returns an instance of an Aggregator along with a
// newly created child context. The Aggregator is registered as a
// tracing.EventListener on the span associated with newly created context.
//
// The Aggregator instance is responsible for finishing the returned span, and
// so the user must call Close().
func MakeAggregatorWithSpan(
	ctx context.Context, aggregatorName string,
) (context.Context, *Aggregator) {
	agg := &Aggregator{}
	sp := tracing.SpanFromContext(ctx)

	aggCtx, aggSpan := sp.Tracer().StartSpanCtx(ctx, aggregatorName,
		tracing.WithEventListeners([]tracing.EventListener{agg}), tracing.WithParent(sp))

	agg.mu.Lock()
	defer agg.mu.Unlock()
	agg.mu.aggregatedEvents = make(map[string]AggregatorEvent)
	agg.mu.sp = aggSpan

	return aggCtx, agg
}
