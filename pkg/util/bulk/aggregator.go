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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"go.opentelemetry.io/otel/attribute"
)

// AggregatorEvent describes an event that can be aggregated and stored by the
// Aggregator.
type AggregatorEvent interface {
	// Combine combines two AggregatorEvents together.
	Combine(other AggregatorEvent)
	// Tag returns a string used to identify the AggregatorEvent.
	Tag() string
	// Render produces the list of key-value tags that will be Render()ed on the
	// tracing span's LazyTag.
	Render() []attribute.KeyValue
}

// Aggregator implements the tracing.EventListener and tracing.LazyTag
// interfaces.
//
// An Aggregator when registered as an EventListener with a tracing span, will
// aggregate all AggregatorEvents that are emitted in the span's recording. This
// aggregated information can be Render()ed on the associated tracing span by
// setting the Aggregator as a LazyTag.
type Aggregator struct {
	aggregatedEvents map[string]AggregatorEvent
}

// Render implements the tracing.LazyTag interface.
func (b *Aggregator) Render() []attribute.KeyValue {
	tags := make([]attribute.KeyValue, 0)
	for _, bulkEvent := range b.aggregatedEvents {
		eventTags := bulkEvent.Render()
		if len(eventTags) > 0 {
			tags = append(tags, eventTags...)
		}
	}

	return tags
}

var _ tracing.LazyTag = &Aggregator{}

// Notify implements the tracing.EventListener interface.
func (b *Aggregator) Notify(event tracing.Structured) {
	bulkEvent, ok := event.(AggregatorEvent)
	if !ok {
		return
	}

	if _, ok := b.aggregatedEvents[bulkEvent.Tag()]; !ok {
		b.aggregatedEvents[bulkEvent.Tag()] = bulkEvent
	} else {
		b.aggregatedEvents[bulkEvent.Tag()].Combine(bulkEvent)
	}
}

var _ tracing.EventListener = &Aggregator{}

// MakeAggregator returns an instance of an Aggregator.
func MakeAggregator() *Aggregator {
	return &Aggregator{aggregatedEvents: make(map[string]AggregatorEvent)}
}
