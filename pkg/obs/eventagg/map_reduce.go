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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
	stopper *stop.Stopper
	newFn   func() Agg
	mu      struct {
		syncutil.Mutex
		cache map[K]Agg
	}
	consumers []flushConsumer[K, Agg]
}

// NewMapReduceAggregator returns a new MapReduceAggregator[Agg].
func NewMapReduceAggregator[E Mergeable[K, V], K comparable, V any](
	stopper *stop.Stopper, newFn func() V, flushConsumers ...flushConsumer[K, V],
) *MapReduceAggregator[E, K, V] {
	m := &MapReduceAggregator[E, K, V]{
		stopper:   stopper,
		newFn:     newFn,
		consumers: flushConsumers,
	}
	m.mu.cache = make(map[K]V)
	return m
}

// Add implements the aggregator interface.
func (m *MapReduceAggregator[E, K, Agg]) Add(_ context.Context, e E) {
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
func (m *MapReduceAggregator[E, K, Agg]) Flush(ctx context.Context) {
	flushed := func() map[K]Agg {
		m.mu.Lock()
		defer m.mu.Unlock()
		flushed := m.mu.cache
		m.mu.cache = make(map[K]Agg)
		return flushed
	}()
	if err := m.stopper.RunAsyncTask(ctx, "map-reduce-flush", func(ctx context.Context) {
		for _, c := range m.consumers {
			c.onFlush(ctx, flushed)
		}
	}); err != nil {
		log.Errorf(ctx, "a problem occurred attempting to flush an aggregation: %v", err)
	}
}
