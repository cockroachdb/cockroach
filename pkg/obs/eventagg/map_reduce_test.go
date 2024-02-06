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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestMapReduceAggregator_Add(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	envEnableStructuredEvents = true
	defer func() {
		envEnableStructuredEvents = false
	}()

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "mapreduce"), func(t *testing.T, td *datadriven.TestData) string {
		testArgs := make([]*testEvent, 0)
		switch td.Cmd {
		case "args":
			args := strings.Split(td.Input, "\n")
			for _, arg := range args {
				e := &testEvent{}
				require.NoError(t, json.Unmarshal([]byte(arg), &e))
				testArgs = append(testArgs, e)
			}
		}
		ctx := context.Background()
		consumer := newTestFlushConsumer[string, *aggTestEvent]()
		mapReduce := NewMapReduceAggregator[*testEvent, string, *aggTestEvent](
			func() *aggTestEvent { return &aggTestEvent{} },
			consumer)
		for _, e := range testArgs {
			mapReduce.Add(ctx, e)
		}
		mapReduce.Flush(ctx)
		select {
		case <-time.After(10 * time.Second):
			t.Fatalf("flush timed out")
		case <-consumer.consumedCh():
		}

		type sortable struct {
			k string
			v *aggTestEvent
		}
		sorted := make([]sortable, 0, len(consumer.flushed))
		out := bytes.Buffer{}
		for k, v := range consumer.flushed {
			sorted = append(sorted, sortable{
				k: k,
				v: v,
			})
		}
		sort.SliceStable(sorted, func(i, j int) bool {
			return sorted[i].k < sorted[j].k
		})
		for _, s := range sorted {
			out.WriteString(fmt.Sprintf("{\"k\": %q, \"v\": %d}\n", s.k, s.v.count))
		}
		return out.String()
	})
}

func TestMapReduceAggregator_Flush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	envEnableStructuredEvents = true
	defer func() {
		envEnableStructuredEvents = false
	}()

	// verifyFlush verifies all elements in expected match those in actual.
	verifyFlush := func(expected map[string]*aggTestEvent, actual map[string]*aggTestEvent) {
		require.Equal(t, len(expected), len(actual))
		for k, expectedV := range expected {
			actualV, ok := actual[k]
			if !ok {
				t.Errorf("unable to find expected key %v in consumed flush", k)
			}
			require.Equalf(t, expectedV, actualV, "key=%v, expected=%v, actual=%v", k, expectedV, actualV)
		}
	}

	t.Run("properly clears aggregation cache after flush", func(t *testing.T) {
		ctx := context.Background()
		consumer := newTestFlushConsumer[string, *aggTestEvent]()
		mapReduce := NewMapReduceAggregator[*testEvent, string, *aggTestEvent](
			func() *aggTestEvent { return &aggTestEvent{} },
			consumer)
		keyA := "A"
		keyB := "B"
		keyC := "C"
		postFlushEvents := []*testEvent{
			{Key: keyA, Count: 5},
			{Key: keyA, Count: 3},
			{Key: keyB, Count: 13},
			{Key: keyB, Count: 12},
			{Key: keyC, Count: 9},
		}
		expectedFlush := map[string]*aggTestEvent{
			keyA: aggWithCount(8),
			keyB: aggWithCount(25),
			keyC: aggWithCount(9),
		}
		// Create initial state within the aggregation cache, which
		// we expect to clear after Flush().
		mapReduce.Add(ctx, &testEvent{Key: keyA, Count: 5})
		mapReduce.Add(ctx, &testEvent{Key: keyB, Count: 5})
		mapReduce.Add(ctx, &testEvent{Key: keyC, Count: 5})
		mapReduce.Flush(ctx)
		<-consumer.consumedCh()
		// Now, send a new batch of events and verify that the previously
		// flushed data has no impact on the new aggregation window.
		for _, e := range postFlushEvents {
			mapReduce.Add(ctx, e)
		}
		mapReduce.Flush(ctx)
		<-consumer.consumedCh()
		verifyFlush(expectedFlush, consumer.flushed)
	})

	// If a consumer is hanging, we don't want the entire MapReduceAggregator to
	// break. Only the goroutine handling that Flush should be impacted. Test to
	// ensure that other goroutines are able to push events into a new aggregation
	// window despite a hanging flush.
	t.Run("hanging consumption does not block new aggregations", func(t *testing.T) {
		ctx := context.Background()
		consumer := newTestFlushConsumer[string, *aggTestEvent]()
		mapReduce := NewMapReduceAggregator[*testEvent, string, *aggTestEvent](
			func() *aggTestEvent { return &aggTestEvent{} },
			consumer)
		keyA := "A"
		keyB := "B"
		keyC := "C"
		// Create initial state within the aggregation cache, which
		// we expect to clear after Flush(). Then, trigger a Flush but
		// delay listening on the consumer's channel via consumedCh()
		// to simulate a hanging Flush(). We can then push new events to the
		// MapReduceAggregator and verify they're included in a new window.
		mapReduce.Add(ctx, &testEvent{Key: keyA, Count: 5})
		expectedFlush1 := map[string]*aggTestEvent{
			keyA: aggWithCount(5),
		}
		// Initialize some data to feed to the aggregator after the initial flush
		// is signaled
		postFlushEvents := []*testEvent{
			{Key: keyA, Count: 4},
			{Key: keyA, Count: 3},
			{Key: keyB, Count: 13},
			{Key: keyB, Count: 14},
			{Key: keyC, Count: 9},
		}
		expectedFlush2 := map[string]*aggTestEvent{
			keyA: aggWithCount(7),
			keyB: aggWithCount(27),
			keyC: aggWithCount(9),
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
		<-consumer.consumedCh()
		verifyFlush(expectedFlush1, consumer.flushed)
		// Now, perform the final flush and assert it to be correct.
		mapReduce.Flush(ctx)
		<-consumer.consumedCh()
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

// The linter is making bogus claims that onFlush is unused, so this is a workaround.
var _ = (*testFlushConsumer[any, any]).onFlush

func newTestFlushConsumer[K comparable, V any]() *testFlushConsumer[K, V] {
	return &testFlushConsumer[K, V]{
		consumed: make(chan struct{}),
	}
}

// consumedCh returns this testFlushConsumer's consumed channel. The channel
// is signaled each time onFlush is called on this testFlushConsumer.
func (t *testFlushConsumer[K, V]) consumedCh() <-chan struct{} {
	return t.consumed
}

// onFlush implements the flushConsumer interface.
func (t *testFlushConsumer[K, V]) onFlush(_ context.Context, flushed map[K]V) {
	t.flushed = flushed
	t.consumed <- struct{}{}
}

// testEvent is a Mergeable[uuid.UUID, *aggTestEvent] usable for testing purposes.
type testEvent struct {
	Key   string `json:"k"`
	Count int    `json:"v"`
}

var _ Mergeable[string, *aggTestEvent] = (*testEvent)(nil)

// MergeInto implements the Mergeable interface.
func (t *testEvent) MergeInto(aggregate *aggTestEvent) {
	aggregate.count += t.Count
}

// GroupingKey implements the Mergeable interface.
func (t *testEvent) GroupingKey() string {
	return t.Key
}

// aggTestEvent is the aggregate type derived from testEvent, for usage in tests.
type aggTestEvent struct {
	count int
}

func aggWithCount(count int) *aggTestEvent {
	return &aggTestEvent{count: count}
}
