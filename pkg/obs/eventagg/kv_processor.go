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

	"github.com/cockroachdb/cockroach/pkg/obs/logstream"
	"github.com/cockroachdb/errors"
)

// EmittedKVProcessor is a logstream.Processor implementation that unpacks KeyValueLog
// events logged via log.Structured (generally, logged by something like a LogWriteConsumer
// during a MapReduceAggregator flush) before delegating them onto a processor function.
//
// This saves users of pkg/util/eventagg the hassle of doing type assertions of their own,
// when trying to define a logstream.Processor to process emitted K/V logs from a
// MapReduceAggregator & LogWriteConsumer. Instead, it uses generics to perform the type
// assertions for us.
//
//   - K is the type of the key.
//   - V is the type of the value.
type EmittedKVProcessor[K any, V any] struct {
	processor func(ctx context.Context, aggInfo AggInfo, k K, v V) error
}

var _ logstream.Processor = (*EmittedKVProcessor[any, any])(nil)

// NewEmittedKVProcessor returns a new EmittedKVProcessor, which invokes the provided
// KVProcessor for each event processed.
func NewEmittedKVProcessor[K any, V any](
	processor func(ctx context.Context, aggInfo AggInfo, k K, v V) error,
) *EmittedKVProcessor[K, V] {
	return &EmittedKVProcessor[K, V]{
		processor: processor,
	}
}

// Process implements the logstream.Processor interface.
func (e *EmittedKVProcessor[K, V]) Process(ctx context.Context, event any) error {
	kvLog, ok := event.(KeyValueLog)
	if !ok {
		return errors.Newf("unexpected type for event: %v", event)
	}
	key, ok := kvLog.Key.(K)
	if !ok {
		return errors.Newf("unexpected key type for event: %v", event)
	}
	value, ok := kvLog.Value.(V)
	if !ok {
		return errors.Newf("unexpected value type for event: %v", event)
	}
	return e.processor(ctx, kvLog.AggInfo, key, value)
}
