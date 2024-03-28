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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// MapReduceAggregator performs map/reduce aggregations on type T, leveraging
// the Mergeable interface to define the aggregation key and the reduce logic.
//
// MapReduceAggregator can be configured with one or more mapReduceFlushConsumer's,
// which will be given a reference to the flushed data for further processing on
// Flush.
type MapReduceAggregator[T Mergeable[T]] struct {
	name string
	mu   struct {
		syncutil.Mutex
		cache map[string]T
	}
	consumers []mapReduceFlushConsumer[T]
}

type mapReduceFlushConsumer[T Mergeable[T]] interface {
	onFlush(ctx context.Context, m map[string]T)
}

// NewMapReduceAggregator returns a new MapReduceAggregator[T].
func NewMapReduceAggregator[T Mergeable[T]](
	name string, flushConsumers ...mapReduceFlushConsumer[T],
) *MapReduceAggregator[T] {
	m := &MapReduceAggregator[T]{
		name:      name,
		consumers: flushConsumers,
	}
	m.mu.cache = make(map[string]T)
	return m
}

// PushEvent implements the aggregator interface.
func (m *MapReduceAggregator[T]) PushEvent(_ context.Context, e T) {
	k := e.Key()
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.mu.cache[k]
	if !ok {
		value = e
	} else {
		value.Merge(e)
	}
	m.mu.cache[k] = value
}

// Flush triggers a flush of the in-memory aggregate data, which resets the
// underlying cache for a new aggregation window.
//
// The flushed data will be passed to each of the configured mapReduceFlushConsumer's
// provided at construction for further processing.
func (m *MapReduceAggregator[T]) Flush(ctx context.Context) {
	flushed := func() map[string]T {
		m.mu.Lock()
		defer m.mu.Unlock()
		flushed := m.mu.cache
		m.mu.cache = make(map[string]T)
		return flushed
	}()
	for _, c := range m.consumers {
		c.onFlush(ctx, flushed)
	}
}
