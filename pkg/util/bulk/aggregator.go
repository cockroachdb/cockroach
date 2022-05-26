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

// Aggregator implements the tracing.EventListener interface.
//
// An Aggregator when registered as an EventListener with a tracing span, will
// aggregate all AggregatorEvents that are emitted in the span's recording.
//
// Each AggregatorEvent will set itself as a LazyTag on the associated span and
// thereby Render() its aggregated information whenever the span's recording is
// pulled.
type Aggregator struct {
	mu struct {
		syncutil.Mutex
		// aggregatedEvents is a mapping from the tag identifying the
		// AggregatorEvent to the running aggregate of the AggregatorEvent.
		aggregatedEvents map[string]AggregatorEvent
		// sp is the tracing span associated with the Aggregator.
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

	if b.mu.sp == nil {
		panic("Init not called on Aggregator before use")
	}

	// If this is the first AggregatorEvent with this tag, set it as a LazyTag on
	// the associated tracing span. This way the AggregatorEvent will be
	// dynamically Render()ed everytime we pull the tracing for the associated
	// span.
	if _, ok := b.mu.aggregatedEvents[bulkEvent.Tag()]; !ok {
		b.mu.aggregatedEvents[bulkEvent.Tag()] = bulkEvent
		b.mu.sp.SetLazyTag(bulkEvent.Tag(), b.mu.aggregatedEvents[bulkEvent.Tag()])
	} else {
		b.mu.aggregatedEvents[bulkEvent.Tag()].Combine(bulkEvent)
	}
}

// Init initializes the Aggregator. The Aggregator is only ready to use after
// Init has been called.
func (b *Aggregator) Init(sp *tracing.Span) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.sp = sp
}

var _ tracing.EventListener = &Aggregator{}

// MakeAggregator returns an instance of an Aggregator.
func MakeAggregator() *Aggregator {
	agg := &Aggregator{}
	agg.mu.aggregatedEvents = make(map[string]AggregatorEvent)
	return agg
}
