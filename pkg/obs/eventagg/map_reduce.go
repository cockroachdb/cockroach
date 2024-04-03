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

// MapReduceAggregator performs map/reduce aggregations on type Agg, keyed by type K,
// which are reduced into type Agg.
//
// It leverages the Mergeable interface to define the aggregation key and the reduce
// logic.
//
// MapReduceAggregator can be configured with one or more flushConsumers,
// which will be given a reference to the flushed data for further processing on
// Flush.
type MapReduceAggregator[E Mergeable[K, Agg], K comparable, Agg any] struct {
	newFn func() Agg
	mu    struct {
		syncutil.Mutex
		cache        map[K]Agg
		flushTrigger FlushTrigger
	}
	consumers []flushConsumer[K, Agg]
}

// NewMapReduceAggregator returns a new MapReduceAggregator[Agg].
func NewMapReduceAggregator[E Mergeable[K, V], K comparable, V any](
	newFn func() V, flushTrigger FlushTrigger, flushConsumers ...flushConsumer[K, V],
) *MapReduceAggregator[E, K, V] {
	m := &MapReduceAggregator[E, K, V]{
		newFn:     newFn,
		consumers: flushConsumers,
	}
	m.mu.cache = make(map[K]V)
	m.mu.flushTrigger = flushTrigger
	return m
}

// Add implements the aggregator interface.
func (m *MapReduceAggregator[E, K, Agg]) Add(ctx context.Context, e E) {
	if !envEnableStructuredEvents {
		return
	}
	k := e.GroupingKey()
	m.mu.Lock()
	defer m.mu.Unlock()
	// If it's time to flush, do so async before processing the event.
	// This will reset the cache, meaning our event will be added to
	// a fresh aggregation window.
	if shouldFlush, aggInfo := m.mu.flushTrigger.shouldFlush(); shouldFlush {
		m.flushAsync(ctx, aggInfo, m.getAndResetCacheLocked())
	}
	v, ok := m.mu.cache[k]
	if !ok {
		v = m.newFn()
		m.mu.cache[k] = v
	}
	e.MergeInto(v)
}

// flushAsync spawns a new goroutine to asynchronously invoke each flushConsumer associated
// with this MapReduceAggregator.
//
// The flushed data will be passed to each of the configured flushConsumer's
// provided at construction for further processing.
func (m *MapReduceAggregator[E, K, Agg]) flushAsync(
	ctx context.Context, aggInfo AggInfo, flushed map[K]Agg,
) {
	// TODO(abarganier): We should probably use a stopper async task here.
	// TODO(abarganier): Should we make whether this is done async configurable?
	go func() {
		for _, c := range m.consumers {
			c.onFlush(ctx, aggInfo, flushed)
		}
	}()
}

func (m *MapReduceAggregator[E, K, V]) getAndResetCacheLocked() map[K]V {
	flushed := m.mu.cache
	m.mu.cache = make(map[K]V)
	return flushed
}
