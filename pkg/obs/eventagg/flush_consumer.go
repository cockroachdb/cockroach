// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	EventMeta log.StructuredLogMeta
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

// NewLogWriteConsumer returns a new *LogWriteConsumer[K, V] instance. All consumed events are
// expected to be of the type represented by the provided log.StructuredLogMeta.
func NewLogWriteConsumer[K comparable, V any](
	eventMeta log.StructuredLogMeta,
) *LogWriteConsumer[K, V] {
	return &LogWriteConsumer[K, V]{
		EventMeta: eventMeta,
	}
}

// onFlush emits all events in the provided map as logs via log.Structured
func (l *LogWriteConsumer[K, V]) onFlush(ctx context.Context, aggInfo AggInfo, m map[K]V) {
	for k, v := range m {
		KVLog := KeyValueLog[K, V]{
			AggInfo: aggInfo,
			Key:     k,
			Value:   v,
		}
		log.Structured(ctx, l.EventMeta, KVLog)
	}
}
