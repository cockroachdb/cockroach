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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// A TracingAggregator can be used to aggregate and render AggregatorEvents that
// are emitted as part of its tracing spans' recording.
type TracingAggregator struct {
	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the name identifying the
		// TracingAggregatorEvent to the running aggregate of the
		// TracingAggregatorEvent.
		aggregatedEvents map[string]tracing.TracingAggregatorEvent
	}
}

// ForEachAggregatedEvent executes f on each event in the TracingAggregator's
// in-memory map.
func (b *TracingAggregator) ForEachAggregatedEvent(
	f func(name string, event tracing.TracingAggregatorEvent),
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for name, event := range b.mu.aggregatedEvents {
		f(name, event)
	}
}

// Notify implements the tracing.EventListener interface.
func (b *TracingAggregator) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	bulkEvent, ok := event.(tracing.TracingAggregatorEvent)
	if !ok {
		return tracing.EventNotConsumed
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If this is the first AggregatorEvent with this name, set it as a LazyTag on
	// the associated tracing span.
	eventName := bulkEvent.ProtoName()
	if _, ok := b.mu.aggregatedEvents[bulkEvent.ProtoName()]; !ok {
		b.mu.aggregatedEvents[eventName] = bulkEvent.Identity()
	}
	b.mu.aggregatedEvents[eventName].Combine(bulkEvent)
	return tracing.EventNotConsumed
}

// TracingAggregatorEventToBytes marshals an event into a byte slice.
func TracingAggregatorEventToBytes(
	_ context.Context, event tracing.TracingAggregatorEvent,
) ([]byte, error) {
	msg, ok := event.(protoutil.Message)
	if !ok {
		// This should never happen but if it does skip the aggregated event.
		return nil, errors.Newf("event is not a protoutil.Message: %T", event)
	}
	data := make([]byte, msg.Size())
	if _, err := msg.MarshalTo(data); err != nil {
		// This should never happen but if it does skip the aggregated event.
		return nil, errors.Newf("event is not a protoutil.Message: %T", event)
	}

	return data, nil
}

var _ tracing.EventListener = &TracingAggregator{}

// TracingAggregatorForContext creates a TracingAggregator if the provided
// context has a tracing span.
func TracingAggregatorForContext(ctx context.Context) *TracingAggregator {
	if tracing.SpanFromContext(ctx) == nil {
		log.Warning(ctx, "tracing aggregator cannot be created without a tracing span")
		return nil
	}
	agg := &TracingAggregator{}
	agg.mu.aggregatedEvents = make(map[string]tracing.TracingAggregatorEvent)
	return agg
}
