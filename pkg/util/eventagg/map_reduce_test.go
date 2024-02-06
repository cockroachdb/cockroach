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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestMapReduceAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	envEnableStructuredEvents = true
	defer func() {
		envEnableStructuredEvents = false
	}()

	// verifyFlush verifies all elements in expected match those in actual.
	verifyFlush := func(expected map[uuid.UUID]*aggTestEvent, actual map[uuid.UUID]*aggTestEvent) {
		require.Equal(t, len(expected), len(actual))
		for k, expectedV := range expected {
			actualV, ok := actual[k]
			if !ok {
				t.Errorf("unable to find expected key %v in consumed flush", k)
			}
			require.Equal(t, expectedV, actualV)
		}
	}

	t.Run("aggregates events properly based on GroupingKey", func(t *testing.T) {
		ctx := context.Background()
		consumer := newTestFlushConsumer[uuid.UUID, *aggTestEvent]()
		mapReduce := NewMapReduceAggregator[*testEvent, uuid.UUID, *aggTestEvent](
			func() *aggTestEvent { return &aggTestEvent{} },
			consumer)
		id1 := uuid.Must(uuid.NewV4())
		id2 := uuid.Must(uuid.NewV4())
		id3 := uuid.Must(uuid.NewV4())
		events := []*testEvent{
			{id: id1, count: 5},
			{id: id1, count: 3},
			{id: id2, count: 13},
			{id: id2, count: 12},
			{id: id3, count: 9},
		}
		expectedFlush := map[uuid.UUID]*aggTestEvent{
			id1: aggWithCount(8),
			id2: aggWithCount(25),
			id3: aggWithCount(9),
		}
		for _, e := range events {
			mapReduce.Add(ctx, e)
		}
		mapReduce.Flush(ctx)
		consumer.awaitConsumption()
		verifyFlush(expectedFlush, consumer.flushed)
	})

	t.Run("properly clears aggregation cache after flush", func(t *testing.T) {
		ctx := context.Background()
		consumer := newTestFlushConsumer[uuid.UUID, *aggTestEvent]()
		mapReduce := NewMapReduceAggregator[*testEvent, uuid.UUID, *aggTestEvent](
			func() *aggTestEvent { return &aggTestEvent{} },
			consumer)
		id1 := uuid.Must(uuid.NewV4())
		id2 := uuid.Must(uuid.NewV4())
		id3 := uuid.Must(uuid.NewV4())
		postFlushEvents := []*testEvent{
			{id: id1, count: 5},
			{id: id1, count: 3},
			{id: id2, count: 13},
			{id: id2, count: 12},
			{id: id3, count: 9},
		}
		expectedFlush := map[uuid.UUID]*aggTestEvent{
			id1: aggWithCount(8),
			id2: aggWithCount(25),
			id3: aggWithCount(9),
		}
		// Create initial state within the aggregation cache, which
		// we expect to clear after Flush().
		mapReduce.Add(ctx, &testEvent{id: id1, count: 5})
		mapReduce.Add(ctx, &testEvent{id: id2, count: 5})
		mapReduce.Add(ctx, &testEvent{id: id3, count: 5})
		mapReduce.Flush(ctx)
		consumer.awaitConsumption()
		// Now, send a new batch of events and verify that the previously
		// flushed data has no impact on the new aggregation window.
		for _, e := range postFlushEvents {
			mapReduce.Add(ctx, e)
		}
		mapReduce.Flush(ctx)
		consumer.awaitConsumption()
		verifyFlush(expectedFlush, consumer.flushed)
	})

	// If a consumer is hanging, we don't want the entire MapReduceAggregator to
	// break. Only the goroutine handling that Flush should be impacted. Test to
	// ensure that other goroutines are able to push events into a new aggregation
	// window despite a hanging flush.
	t.Run("hanging consumption does not block new aggregations", func(t *testing.T) {
		ctx := context.Background()
		consumer := newTestFlushConsumer[uuid.UUID, *aggTestEvent]()
		mapReduce := NewMapReduceAggregator[*testEvent, uuid.UUID, *aggTestEvent](
			func() *aggTestEvent { return &aggTestEvent{} },
			consumer)
		id1 := uuid.Must(uuid.NewV4())
		id2 := uuid.Must(uuid.NewV4())
		id3 := uuid.Must(uuid.NewV4())
		// Create initial state within the aggregation cache, which
		// we expect to clear after Flush(). Then, trigger a Flush but
		// delay listening on the consumer's channel via awaitConsumption()
		// to simulate a hanging Flush(). We can then push new events to the
		// MapReduceAggregator and verify they're included in a new window.
		mapReduce.Add(ctx, &testEvent{id: id1, count: 5})
		expectedFlush1 := map[uuid.UUID]*aggTestEvent{
			id1: aggWithCount(5),
		}
		// Initialize some data to feed to the aggregator after the initial flush
		// is signaled
		postFlushEvents := []*testEvent{
			{id: id1, count: 4},
			{id: id1, count: 3},
			{id: id2, count: 13},
			{id: id2, count: 14},
			{id: id3, count: 9},
		}
		expectedFlush2 := map[uuid.UUID]*aggTestEvent{
			id1: aggWithCount(7),
			id2: aggWithCount(27),
			id3: aggWithCount(9),
		}
		done := make(chan struct{})
		go func() {
			// Execute this after we call mapReduce.Flush() below.
			time.AfterFunc(100*time.Millisecond, func() {
				// Now, send a new batch of events and verify that the previously
				// flushed data has no impact on the new aggregation window.
				for _, e := range postFlushEvents {
					mapReduce.Add(ctx, e)
				}
				done <- struct{}{}
			})
			mapReduce.Flush(ctx) // Should hang
		}()
		<-done
		// This will unblock the first Flush() call
		consumer.awaitConsumption()
		verifyFlush(expectedFlush1, consumer.flushed)
		// Now, perform the final flush and assert it to be correct.
		mapReduce.Flush(ctx)
		consumer.awaitConsumption()
		verifyFlush(expectedFlush2, consumer.flushed)
	})
}

// testFlushConsumer simply grabs hold of a reference to the data flushed by its
// MapReduceConsumer. The data can then be used to make assertions in tests.
type testFlushConsumer[K comparable, V any] struct {
	flushed map[K]V
	// consumed is a channel that's signaled when onFlush is called.
	// Callers can
	consumed chan struct{}
}

var _ flushConsumer[any, any] = (*testFlushConsumer[any, any])(nil)

func newTestFlushConsumer[K comparable, V any]() *testFlushConsumer[K, V] {
	return &testFlushConsumer[K, V]{
		consumed: make(chan struct{}),
	}
}

// awaitConsumption listens on this testFlushConsumer's consumed channel. The channel
// is signaled each time onFlush is called on this testFlushConsumer.
func (t *testFlushConsumer[K, V]) awaitConsumption() {
	<-t.consumed
}

// onFlush implements the flushConsumer interface.
func (t *testFlushConsumer[K, V]) onFlush(_ context.Context, flushed map[K]V) {
	t.flushed = flushed
	t.consumed <- struct{}{}
}

// testEvent is a Mergeable[uuid.UUID, *aggTestEvent] usable for testing purposes.
type testEvent struct {
	id    uuid.UUID
	count int
}

var _ Mergeable[uuid.UUID, *aggTestEvent] = (*testEvent)(nil)

// MergeInto implements the Mergeable interface.
func (t *testEvent) MergeInto(aggregate *aggTestEvent) {
	aggregate.count += t.count
}

// GroupingKey implements the Mergeable interface.
func (t *testEvent) GroupingKey() uuid.UUID {
	return t.id
}

// aggTestEvent is the aggregate type derived from testEvent, for usage in tests.
type aggTestEvent struct {
	count int
}

func aggWithCount(count int) *aggTestEvent {
	return &aggTestEvent{count: count}
}
