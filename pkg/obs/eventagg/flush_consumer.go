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

import "context"

// flushConsumer is the interface type used to define a post-processing consumer of
// the aggregations flushed by a MapReduceAggregator.
type flushConsumer[K comparable, V any] interface {
	// TODO(abarganier): It'd be better if we didn't pass down the reduceContainer and instead
	// provided the unwrapped values, but for now I'd rather copying over to a new map.
	onFlush(ctx context.Context, m map[K]V)
}

// LogWriteConsumer is an example flushConsumer, which provides an easy plug-and-play
// method of logging all the values Agg flushed by a MapReduceAggregator[Agg].
type LogWriteConsumer[K comparable, V any] struct{}

var _ flushConsumer[int, any] = &LogWriteConsumer[int, any]{}

// TODO(abarganier): remove once used in future patch.
var _ = LogWriteConsumer[int, any].onFlush

// NewLogWriteConsumer returns a new *LogWriteConsumer[Agg] instance.
func NewLogWriteConsumer[K comparable, V any]() *LogWriteConsumer[K, V] {
	return &LogWriteConsumer[K, V]{}
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
// value Agg.
func (l LogWriteConsumer[K, V]) onFlush(ctx context.Context, m map[K]V) {
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
