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

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
)

// LogWriteConsumer is an example mapReduceFlushConsumer, which provides an easy plug-and-play
// method of logging all the values T flushed by a MapReduceAggregator[T].
type LogWriteConsumer[T any] struct{}

// NewLogWriteConsumer returns a new *LogWriteConsumer[T] instance.
func NewLogWriteConsumer[T any]() *LogWriteConsumer[T] {
	return &LogWriteConsumer[T]{}
}

// TODO(abarganier): The values flushed out of a MapReduceAggregator ideally should be able
// to be provided to log.StructuredEvent(), or a similar facility, without transformation.
//
// For now, we punt this problem until we have the lower level interfaces fleshed out. Currently,
// log.StructuredEvent() requires a protobuf format, which may be cumbersome & something we'd like
// to avoid if possible within this system.
//
// One option would be to expand the API of pkg/util/log to enable direct JSON logging, e.g.:
//
//	log.Structured(ctx, eventMetadata, eventJSON)
//
// This would allow us to construct a single metadata object that can be applied to every flushed
// value T.
func (l LogWriteConsumer[T]) onFlush(ctx context.Context, m map[string]T) {
	//metadata := &logmeta.EventMetadata{
	//  EventType: ...,
	//  EventTimestamp: timeutil.Now().UnixNano(),
	//  NodeDetails: logmeta.GetNodeDetails(ctx),
	//	...,
	//}
	//for _, v := range m {
	//	marshalled, err := json.Marshal(v)
	//	if err != nil {
	//		log.Errorf(ctx, "failed to marshal JSON for event: %v", v)
	//  }
	//	log.Structured(ctx, metadata, marshalled)
	//}
}

// TopKFlushConsumer is an example mapReduceFlushConsumer that performs a TopK aggregation
// on T elements based off the flush results from a MapReduceAggregator[T].
//
// A min-heap is the intended use, where all N elements of type T are fed in, but we don't
// allow the min-heap size to grow past K. In the end, the remaining K elements represent the
// top K elements, based on the provided priorityFn.
//
// Afterward, the results are fed to the provided consume function.
type TopKFlushConsumer[T any] struct {
	name string
	k    int
	// TODO(abarganier): For now, we use the bare interface type while imagining it functions as a min-heap.
	pq         heap.Interface[T]
	consume    func(pq heap.Interface[T])
	priorityFn func(e T) int
}

// NewTopKFlushConsumer returnes a new *TopKFlushConsumer[T] instance.
func NewTopKFlushConsumer[T any](
	name string, k int, priorityFn func(e T) int, consumeFn func(pq heap.Interface[T]),
) *TopKFlushConsumer[T] {
	return &TopKFlushConsumer[T]{
		name:       name,
		k:          k,
		pq:         nil, // Pretend it's a min-heap implementation.
		consume:    consumeFn,
		priorityFn: priorityFn,
	}
}

func (t *TopKFlushConsumer[T]) onFlush(_ context.Context, m map[string]T) {
	for _, v := range m {
		// TODO(abarganier): For now, we pretend our slice is a priority queue representing a min-heap.
		// We add elements until K is reached, and then we pop the min elements off the min-heap.
		t.pq.Push(v)
		if t.pq.Len() >= t.k {
			heap.Pop(t.pq)
		}
	}
	toConsume := t.pq
	t.pq = nil // imagine: reinitialize the priority queue
	t.consume(toConsume)
}
