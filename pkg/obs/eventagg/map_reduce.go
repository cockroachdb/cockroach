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

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// envEnableStructuredEvents determines whether the eventagg package is enabled. The features within
// this package are currently experimental, and must be explicitly enabled via this envvar.
var envEnableStructuredEvents = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_STRUCTURED_EVENTS", false)

// MapReduceAggregator performs aggregations on events of type E, keyed by type K,
// which are reduced into type Agg.
//
// Users define and provide the map and reduce functions at construction. MapReduceAggregator can
// be configured with one or more flushConsumers, which will be given a reference to the flushed
// data for further processing upon flush. flushConsumers are invoked asynchronously, putting the
// post-processing outside the critical path of calls to Add.
//
//   - newFn defines how to instantiate a new Agg type, since Golang generics doesn't have
//     great support for this based on the generic type alone.
//   - keyFn defines how to derive a key from incoming events of type E. This key will be used
//     to determine which bucket the event should be aggregated into, represented by type Agg.
//   - mergeFn defines how to merge an incoming event of type E into a pre-existing aggregation
//     for the same key (derived via keyFn).
type MapReduceAggregator[E any, K comparable, Agg any] struct {
	stopper *stop.Stopper
	newFn   func() Agg
	keyFn   func(e E) K
	mergeFn func(e E, agg Agg)
	mu      struct {
		syncutil.Mutex
		cache map[K]Agg
	}
	consumers []flushConsumer[K, Agg]
}

// NewMapReduceAggregator returns a new MapReduceAggregator[Agg].
func NewMapReduceAggregator[E any, K comparable, Agg any](
	stopper *stop.Stopper,
	newFn func() Agg,
	keyFn func(e E) K,
	mergeFn func(e E, agg Agg),
	flushConsumers ...flushConsumer[K, Agg],
) *MapReduceAggregator[E, K, Agg] {
	m := &MapReduceAggregator[E, K, Agg]{
		stopper:   stopper,
		newFn:     newFn,
		keyFn:     keyFn,
		mergeFn:   mergeFn,
		consumers: flushConsumers,
	}
	m.mu.cache = make(map[K]Agg)
	return m
}

// Add processes the provided event e, aggregating it based on the provided keyFn and mergeFn.
func (m *MapReduceAggregator[E, K, Agg]) Add(_ context.Context, e E) {
	if !envEnableStructuredEvents {
		return
	}
	k := m.keyFn(e)
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.mu.cache[k]
	if !ok {
		v = m.newFn()
		m.mu.cache[k] = v
	}
	m.mergeFn(e, v)
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
