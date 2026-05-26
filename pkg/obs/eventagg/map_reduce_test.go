// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

var dummyTestEvent = &testEvent{Key: "XYZ", Count: 1}

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
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		consumer := newTestFlushConsumer[string, *aggTestEvent]()
		trigger := &testFlushTrigger{}
		mapReduce := NewMapReduceAggregator[*testEvent, string, *aggTestEvent](
			stopper,
			func() *aggTestEvent { return &aggTestEvent{} },
			mapTestEvent,
			mergeTestEvent,
			trigger,
			consumer)
		for _, e := range testArgs {
			mapReduce.Add(ctx, e)
		}
		triggerTestFlush(ctx, mapReduce, trigger, dummyTestEvent)
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
	keyA := "A"
	keyB := "B"
	keyC := "C"

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
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		consumer := newTestFlushConsumer[string, *aggTestEvent]()
		trigger := &testFlushTrigger{}
		mapReduce := NewMapReduceAggregator[*testEvent, string, *aggTestEvent](
			stopper,
			func() *aggTestEvent { return &aggTestEvent{} },
			mapTestEvent,
			mergeTestEvent,
			trigger,
			consumer)
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
		// we expect to clear after the first flush.
		mapReduce.Add(ctx, &testEvent{Key: keyA, Count: 5})
		mapReduce.Add(ctx, &testEvent{Key: keyB, Count: 5})
		mapReduce.Add(ctx, &testEvent{Key: keyC, Count: 5})
		// We start by triggering the flush of the previous state with the 1st
		// of the new events.
		triggerTestFlush(ctx, mapReduce, trigger, postFlushEvents[0])
		<-consumer.consumedCh()
		// Now, send a new batch of events and verify that the previously
		// flushed data has no impact on the new aggregation window.
		for _, e := range postFlushEvents[1:] {
			mapReduce.Add(ctx, e)
		}
		// Trigger a final flush with a dummy event, so we can validate what's flushed.
		triggerTestFlush(ctx, mapReduce, trigger, dummyTestEvent)
		<-consumer.consumedCh()
		verifyFlush(expectedFlush, consumer.flushed)
	})

	// If a consumer is hanging, we don't want the entire MapReduceAggregator to
	// break. Only the goroutine handling that Flush should be impacted. Test to
	// ensure that other goroutines are able to push events into a new aggregation
	// window despite a hanging flush.
	t.Run("hanging consumption does not block new aggregations", func(t *testing.T) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		consumer := newTestFlushConsumer[string, *aggTestEvent]()
		trigger := &testFlushTrigger{}
		mapReduce := NewMapReduceAggregator[*testEvent, string, *aggTestEvent](
			stopper,
			func() *aggTestEvent { return &aggTestEvent{} },
			mapTestEvent,
			mergeTestEvent,
			trigger,
			consumer)
		// Create initial state within the aggregation cache, which
		// we expect to clear after the first flush. Then, trigger a flush but
		// delay listening on the consumer's channel via consumedCh()
		// to simulate a hanging flushConsumer. We can then push new events to the
		// MapReduceAggregator and verify they're included in a new window, and that
		// they don't block on the hanging flush.
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
				for _, e := range postFlushEvents[1:] {
					mapReduce.Add(ctx, e)
				}
				done <- struct{}{}
			})
			// Trigger a flush of the previous window with the first event of the new window.
			triggerTestFlush(ctx, mapReduce, trigger, postFlushEvents[0])
		}()
		<-done
		// This will unblock the first flush.
		<-consumer.consumedCh()
		verifyFlush(expectedFlush1, consumer.flushed)
		// Now, trigger the final flush and assert it to be correct.
		triggerTestFlush(ctx, mapReduce, trigger, dummyTestEvent)
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
func (t *testFlushConsumer[K, V]) onFlush(_ context.Context, _ AggInfo, flushed map[K]V) {
	t.flushed = flushed
	t.consumed <- struct{}{}
}

// testEvent is a Mergeable[uuid.UUID, *aggTestEvent] usable for testing purposes.
type testEvent struct {
	Key   string `json:"k"`
	Count int    `json:"v"`
}

// MergeInto implements the Mergeable interface.
func mergeTestEvent(t *testEvent, aggregate *aggTestEvent) {
	aggregate.count += t.Count
}

// GroupingKey implements the Mergeable interface.
func mapTestEvent(t *testEvent) string {
	return t.Key
}

// aggTestEvent is the aggregate type derived from testEvent, for usage in tests.
type aggTestEvent struct {
	count int
}

func aggWithCount(count int) *aggTestEvent {
	return &aggTestEvent{count: count}
}

// testFlushTrigger is a FlushTrigger for use in tests that stores a bool indicating whether
// it should flush. Use setShouldFlush to configure.
type testFlushTrigger struct {
	mu struct {
		syncutil.Mutex
		flush bool
	}
}

var _ FlushTrigger = (*testFlushTrigger)(nil)

// shouldFlush implements the FlushTrigger interface.
func (t *testFlushTrigger) shouldFlush() (bool, AggInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.flush, AggInfo{}
}

func (t *testFlushTrigger) setShouldFlush(to bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.flush = to
}

// When an event is passed to the MapReduceAggregator via `.Add()`, it checks if we should flush *first*, before
// aggregating the event. So, to trigger a flush, we indicate via the trigger that a flush should occur, call `.Add()`
// with the event, and then reset the trigger.
//
// The events aggregated prior to our call to `.Add()` will be flushed, and the event we pass to `.Add()` will be
// aggregated into the new window.
func triggerTestFlush(
	ctx context.Context,
	mr *MapReduceAggregator[*testEvent, string, *aggTestEvent],
	trigger *testFlushTrigger,
	e *testEvent,
) {
	trigger.setShouldFlush(true)
	mr.Add(ctx, e)
	trigger.setShouldFlush(false)
}
