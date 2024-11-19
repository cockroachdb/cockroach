// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// TracingAggregatorEvent describes an event that can be aggregated and stored by the
// TracingAggregator. A TracingAggregatorEvent also implements the tracing.LazyTag interface
// to render its information on the associated tracing span.
type TracingAggregatorEvent interface {
	// Identity returns a TracingAggregatorEvent that when combined with another
	// event returns the other TracingAggregatorEvent unchanged.
	Identity() TracingAggregatorEvent
	// Combine combines two TracingAggregatorEvents together.
	Combine(other TracingAggregatorEvent)
	// ProtoName returns the fully qualified name of the underlying proto that is
	// a TracingAggregatorEvent.
	ProtoName() string
	// String returns the string representation of the TracingAggregatorEvent.
	String() string
}

// A TracingAggregator can be used to aggregate and render AggregatorEvents that
// are emitted as part of its tracing spans' recording.
type TracingAggregator struct {
	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the name identifying the
		// TracingAggregatorEvent to the running aggregate of the
		// TracingAggregatorEvent.
		aggregatedEvents map[string]TracingAggregatorEvent
	}
}

// ForEachAggregatedEvent executes f on each event in the TracingAggregator's
// in-memory map.
func (b *TracingAggregator) ForEachAggregatedEvent(
	f func(name string, event TracingAggregatorEvent),
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for name, event := range b.mu.aggregatedEvents {
		f(name, event)
	}
}

// Notify implements the tracing.EventListener interface.
func (b *TracingAggregator) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	bulkEvent, ok := event.(TracingAggregatorEvent)
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
	_ context.Context, event TracingAggregatorEvent,
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
	agg.mu.aggregatedEvents = make(map[string]TracingAggregatorEvent)
	return agg
}
