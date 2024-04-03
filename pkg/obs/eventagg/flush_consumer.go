// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventagg

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// flushConsumer is the interface type used to define a post-processing consumer of
// the aggregations flushed by a MapReduceAggregator.
type flushConsumer[K comparable, V any] interface {
	onFlush(ctx context.Context, aggInfo AggInfo, m map[K]V)
}

// LogWriteConsumer is an example flushConsumer, which provides an easy plug-and-play
// method of logging all the values V flushed by a MapReduceAggregator[V].
type LogWriteConsumer[K comparable, V any] struct {
	EventType log.EventType
}

var _ flushConsumer[int, any] = &LogWriteConsumer[int, any]{}

// KeyValueLog is the type logged to log.Structured via the LogWriteConsumer.
// It wraps the flush metadata, key, and value flushed from the associated
// aggregator.
type KeyValueLog[K any, V any] struct {
	AggInfo AggInfo `json:"agg_info"`
	Key     K       `json:"key"`
	Value   V       `json:"value"`
}

// NewLogWriteConsumer returns a new *LogWriteConsumer[K, V] instance.
func NewLogWriteConsumer[K comparable, V any](eventType log.EventType) *LogWriteConsumer[K, V] {
	return &LogWriteConsumer[K, V]{
		EventType: eventType,
	}
}

// onFlush emits all events in the provided map as logs via log.Structured
func (l *LogWriteConsumer[K, V]) onFlush(ctx context.Context, aggInfo AggInfo, m map[K]V) {
	metadata := log.StructuredMeta{
		EventType: l.EventType,
	}
	for k, v := range m {
		KVLog := KeyValueLog[K, V]{
			AggInfo: aggInfo,
			Key:     k,
			Value:   v,
		}
		log.Structured(ctx, metadata, KVLog)
	}
}
