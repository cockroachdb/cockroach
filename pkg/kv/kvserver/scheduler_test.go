// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRangeIDChunk(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var c rangeIDChunk
	if c.Len() != 0 {
		t.Fatalf("expected empty chunk, but found %d", c.Len())
	}
	if c.WriteCap() != rangeIDChunkSize {
		t.Fatalf("expected %d, but found %d", rangeIDChunkSize, c.WriteCap())
	}
	if _, ok := c.PopFront(); ok {
		t.Fatalf("successfully popped from empty chunk")
	}

	for i := 1; i <= rangeIDChunkSize; i++ {
		if !c.PushBack(roachpb.RangeID(i)) {
			t.Fatalf("%d: failed to push", i)
		}
		if e := i; e != c.Len() {
			t.Fatalf("expected %d, but found %d", e, c.Len())
		}
		if e := rangeIDChunkSize - i; e != c.WriteCap() {
			t.Fatalf("expected %d, but found %d", e, c.WriteCap())
		}
	}
	if c.PushBack(0) {
		t.Fatalf("successfully pushed to full chunk")
	}

	for i := 1; i <= rangeIDChunkSize; i++ {
		id, ok := c.PopFront()
		if !ok {
			t.Fatalf("%d: failed to pop", i)
		}
		if roachpb.RangeID(i) != id {
			t.Fatalf("expected %d, but found %d", i, id)
		}
		if e := rangeIDChunkSize - i; e != c.Len() {
			t.Fatalf("expected %d, but found %d", e, c.Len())
		}
		if c.WriteCap() != 0 {
			t.Fatalf("expected full chunk, but found %d", c.WriteCap())
		}
	}
	if c.Len() != 0 {
		t.Fatalf("expected empty chunk, but found %d", c.Len())
	}
	if c.WriteCap() != 0 {
		t.Fatalf("expected full chunk, but found %d", c.WriteCap())
	}
	if _, ok := c.PopFront(); ok {
		t.Fatalf("successfully popped from empty chunk")
	}
}

func TestRangeIDQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var q rangeIDQueue
	if q.Len() != 0 {
		t.Fatalf("expected empty queue, but found %d", q.Len())
	}
	if _, ok := q.PopFront(); ok {
		t.Fatalf("successfully popped from empty queue")
	}

	const count = 3 * rangeIDChunkSize
	for i := 1; i <= count; i++ {
		q.Push(roachpb.RangeID(i))
		if e := i; e != q.Len() {
			t.Fatalf("expected %d, but found %d", e, q.Len())
		}
	}

	for i := 1; i <= count; i++ {
		id, ok := q.PopFront()
		if !ok {
			t.Fatalf("%d: failed to pop", i)
		}
		if roachpb.RangeID(i) != id {
			t.Fatalf("expected %d, but found %d", i, id)
		}
		if e := count - i; e != q.Len() {
			t.Fatalf("expected %d, but found %d", e, q.Len())
		}
	}
	if q.Len() != 0 {
		t.Fatalf("expected empty queue, but found %d", q.Len())
	}
	if _, ok := q.PopFront(); ok {
		t.Fatalf("successfully popped from empty queue")
	}
}

func TestRangeIDQueuePrioritization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var q rangeIDQueue
	for _, withPriority := range []bool{false, true} {
		if withPriority {
			q.SetPriorityID(3)
		}

		// Push 5 ranges in order, then pop them off.
		for i := 1; i <= 5; i++ {
			q.Push(roachpb.RangeID(i))
			require.Equal(t, i, q.Len())
		}
		var popped []int
		for i := 5; ; i-- {
			require.Equal(t, i, q.Len())
			id, ok := q.PopFront()
			if !ok {
				require.Equal(t, i, 0)
				break
			}
			popped = append(popped, int(id))
		}

		// Assert pop order.
		if withPriority {
			require.Equal(t, []int{3, 1, 2, 4, 5}, popped)
		} else {
			require.Equal(t, []int{1, 2, 3, 4, 5}, popped)
		}
	}
}

type testProcessor struct {
	mu struct {
		syncutil.Mutex
		raftReady   map[roachpb.RangeID]int
		raftRequest map[roachpb.RangeID]int
		raftTick    map[roachpb.RangeID]int
	}
}

func newTestProcessor() *testProcessor {
	p := &testProcessor{}
	p.mu.raftReady = make(map[roachpb.RangeID]int)
	p.mu.raftRequest = make(map[roachpb.RangeID]int)
	p.mu.raftTick = make(map[roachpb.RangeID]int)
	return p
}

func (p *testProcessor) processReady(_ context.Context, rangeID roachpb.RangeID) {
	p.mu.Lock()
	p.mu.raftReady[rangeID]++
	p.mu.Unlock()
}

func (p *testProcessor) processRequestQueue(_ context.Context, rangeID roachpb.RangeID) bool {
	p.mu.Lock()
	p.mu.raftRequest[rangeID]++
	p.mu.Unlock()
	return false
}

func (p *testProcessor) processTick(_ context.Context, rangeID roachpb.RangeID) bool {
	p.mu.Lock()
	p.mu.raftTick[rangeID]++
	p.mu.Unlock()
	return false
}

func (p *testProcessor) countsLocked(m map[roachpb.RangeID]int) string {
	var ids roachpb.RangeIDSlice
	for id := range m {
		ids = append(ids, id)
	}
	sort.Sort(ids)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[")
	for i, id := range ids {
		if i > 0 {
			fmt.Fprintf(&buf, ",")
		}
		fmt.Fprintf(&buf, "%d:%d", id, m[id])
	}
	fmt.Fprintf(&buf, "]")
	return buf.String()
}

func (p *testProcessor) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("ready=%s request=%s tick=%s",
		p.countsLocked(p.mu.raftReady),
		p.countsLocked(p.mu.raftRequest),
		p.countsLocked(p.mu.raftTick))
}

// Verify that enqueuing more ranges than the number of workers correctly
// processes all of the ranges. This exercises a code path that was buggy
// during development.
func TestSchedulerLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(m, p, 1)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	s.Start(ctx, stopper)
	s.EnqueueRaftTicks(1, 2, 3)

	testutils.SucceedsSoon(t, func() error {
		const expected = "ready=[] request=[] tick=[1:1,2:1,3:1]"
		if s := p.String(); expected != s {
			return errors.Errorf("expected %s, but got %s", expected, s)
		}
		return nil
	})

	require.Equal(t, int64(3), m.RaftSchedulerLatency.TotalCount())
}

// Verify that when we enqueue the same range multiple times for the same
// reason, it is only processed once.
func TestSchedulerBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(m, p, 1)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	s.Start(ctx, stopper)

	testCases := []struct {
		flag     raftScheduleFlags
		expected string
	}{
		{stateRaftReady, "ready=[1:1] request=[] tick=[]"},
		{stateRaftRequest, "ready=[1:1] request=[1:1] tick=[]"},
		{stateRaftTick, "ready=[1:1] request=[1:1] tick=[1:1]"},
		{stateRaftReady | stateRaftRequest | stateRaftTick, "ready=[1:2] request=[1:2] tick=[1:2]"},
	}

	for _, c := range testCases {
		s.signal(s.enqueueN(c.flag, 1, 1, 1, 1, 1))

		testutils.SucceedsSoon(t, func() error {
			if s := p.String(); c.expected != s {
				return errors.Errorf("expected %s, but got %s", c.expected, s)
			}
			return nil
		})
	}
}
