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

// MapReduceAggregator performs map/reduce aggregations on type V, keyed by type K,
// which are reduced into type V.
//
// It leverages the Mergeable interface to define the aggregation key and the reduce
// logic.
//
// MapReduceAggregator can be configured with one or more flushConsumers,
// which will be given a reference to the flushed data for further processing on
// Flush.
type MapReduceAggregator[E Mergeable[K, V], K comparable, V any] struct {
	newFn func() V
	mu    struct {
		syncutil.Mutex
		cache map[K]V
	}
	consumers []flushConsumer[K, V]
}

// NewMapReduceAggregator returns a new MapReduceAggregator[V].
func NewMapReduceAggregator[E Mergeable[K, V], K comparable, V any](
	newFn func() V, flushConsumers ...flushConsumer[K, V],
) *MapReduceAggregator[E, K, V] {
	m := &MapReduceAggregator[E, K, V]{
		newFn:     newFn,
		consumers: flushConsumers,
	}
	m.mu.cache = make(map[K]V)
	return m
}

// Add implements the aggregator interface.
func (m *MapReduceAggregator[E, K, V]) Add(_ context.Context, e E) {
	if !envEnableStructuredEvents {
		return
	}
	k := e.GroupingKey()
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.mu.cache[k]
	if !ok {
		v = m.newFn()
		m.mu.cache[k] = v
	}
	e.MergeInto(v)
}

// Flush triggers a flush of the in-memory aggregate data, which resets the
// underlying cache for a new aggregation window.
//
// The flushed data will be passed to each of the configured flushConsumer's
// provided at construction for further processing.
// TODO(abarganier): implement more robust flush mechanism, with configurable triggers.
func (m *MapReduceAggregator[E, K, V]) Flush(ctx context.Context) {
	flushed := func() map[K]V {
		m.mu.Lock()
		defer m.mu.Unlock()
		flushed := m.mu.cache
		m.mu.cache = make(map[K]V)
		return flushed
	}()
	// TODO(abarganier): We should probably use a stopper async task here.
	// TODO(abarganier): Should we make whether this is done async configurable?
	go func() {
		for _, c := range m.consumers {
			c.onFlush(ctx, flushed)
		}
	}()
}
