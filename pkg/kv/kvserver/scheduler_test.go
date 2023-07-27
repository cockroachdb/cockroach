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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
		ready       func()
	}
}

func newTestProcessor() *testProcessor {
	p := &testProcessor{}
	p.mu.raftReady = make(map[roachpb.RangeID]int)
	p.mu.raftRequest = make(map[roachpb.RangeID]int)
	p.mu.raftTick = make(map[roachpb.RangeID]int)
	return p
}

func (p *testProcessor) onReady(f func()) {
	p.mu.Lock()
	p.mu.ready = f
	p.mu.Unlock()
}

func (p *testProcessor) processReady(rangeID roachpb.RangeID) {
	p.mu.Lock()
	p.mu.raftReady[rangeID]++
	if p.mu.ready != nil {
		p.mu.ready()
		p.mu.ready = nil
	}
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

	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(log.MakeTestingAmbientContext(stopper.Tracer()), m, p, 1, 1, 1)
	s.Start(stopper)

	batch := s.NewEnqueueBatch()
	defer batch.Close()
	batch.Add(1)
	batch.Add(2)
	batch.Add(3)
	s.EnqueueRaftTicks(batch)

	testutils.SucceedsSoon(t, func() error {
		const expected = "ready=[] request=[] tick=[1:1,2:1,3:1]"
		if s := p.String(); expected != s {
			return errors.Errorf("expected %s, but got %s", expected, s)
		}
		return nil
	})

	count, _ := m.RaftSchedulerLatency.Total()
	require.Equal(t, int64(3), count)
}

// Verify that when we enqueue the same range multiple times for the same
// reason, it is only processed once, except the ticking. Ticking is special
// because it adapts to scheduling delays in order to keep heartbeating delays
// minimal.
func TestSchedulerBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(log.MakeTestingAmbientContext(stopper.Tracer()), m, p, 1, 1, 5)
	s.Start(stopper)

	testCases := []struct {
		flag  raftScheduleFlags
		slow  bool // true if Ready processing is slow
		ticks int
		want  string
	}{
		{flag: stateRaftReady, want: "ready=[1:1] request=[] tick=[]"},
		{flag: stateRaftRequest, want: "ready=[1:1] request=[1:1] tick=[]"},
		{flag: stateRaftTick, want: "ready=[1:1] request=[1:1] tick=[1:5]"},
		{flag: stateRaftReady | stateRaftRequest | stateRaftTick,
			want: "ready=[1:2] request=[1:2] tick=[1:10]"},
		{flag: stateRaftTick, want: "ready=[1:2] request=[1:2] tick=[1:15]"},
		// All 4 ticks are processed.
		{flag: 0, ticks: 4, want: "ready=[1:2] request=[1:2] tick=[1:19]"},
		// Only 5/10 ticks are buffered while Raft processing is slow.
		{flag: stateRaftReady, slow: true, ticks: 10, want: "ready=[1:3] request=[1:2] tick=[1:24]"},
		// All 3 ticks are processed even if processing is slow.
		{flag: stateRaftReady, slow: true, ticks: 3, want: "ready=[1:4] request=[1:2] tick=[1:27]"},
	}

	for _, c := range testCases {
		var started, done chan struct{}
		if c.slow {
			started, done = make(chan struct{}), make(chan struct{})
			p.onReady(func() {
				close(started)
				<-done
			})
		}

		const id = roachpb.RangeID(1)
		if c.flag != 0 {
			batch := s.NewEnqueueBatch()
			for i := 0; i < 5; i++ {
				batch.Add(id)
			}
			s.enqueueBatch(c.flag, batch)
			batch.Close()
			if started != nil {
				<-started // wait until slow Ready processing has started
				// NB: this is necessary to work around the race between the subsequent
				// event enqueue calls and the scheduler loop which may process events
				// before we've added them. This makes the test deterministic.
			}
		}

		for t := c.ticks; t > 0; t-- {
			s.enqueue1(stateRaftTick, id)
		}
		if done != nil {
			close(done) // finish slow Ready processing to unblock progress
		}

		testutils.SucceedsSoon(t, func() error {
			if s := p.String(); c.want != s {
				return errors.Errorf("expected %s, but got %s", c.want, s)
			}
			return nil
		})
	}
}

func TestNewSchedulerShards(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := []struct {
		workers      int
		shardSize    int
		expectShards []int
	}{
		// NB: We balance workers across shards instead of filling up shards. We
		// assume ranges are evenly distributed across shards, and want ranges to
		// have about the same number of workers available on average.
		{-1, -1, []int{1}},
		{0, 0, []int{1}},
		{1, -1, []int{1}},
		{1, 0, []int{1}},
		{1, 1, []int{1}},
		{1, 2, []int{1}},
		{2, 2, []int{2}},
		{3, 2, []int{2, 1}},
		{1, 3, []int{1}},
		{2, 3, []int{2}},
		{3, 3, []int{3}},
		{4, 3, []int{2, 2}},
		{5, 3, []int{3, 2}},
		{6, 3, []int{3, 3}},
		{7, 3, []int{3, 2, 2}},
		{8, 3, []int{3, 3, 2}},
		{9, 3, []int{3, 3, 3}},
		{10, 3, []int{3, 3, 2, 2}},
		{11, 3, []int{3, 3, 3, 2}},
		{12, 3, []int{3, 3, 3, 3}},

		// Typical examples, using 8 workers per CPU core. Note that we cap workers
		// at 96 by default.
		{1 * 8, 16, []int{8}},
		{2 * 8, 16, []int{16}},
		{3 * 8, 16, []int{12, 12}},
		{4 * 8, 16, []int{16, 16}},
		{6 * 8, 16, []int{16, 16, 16}},
		{8 * 8, 16, []int{16, 16, 16, 16}},
		{12 * 8, 16, []int{16, 16, 16, 16, 16, 16}}, // 96 workers
		{16 * 8, 16, []int{16, 16, 16, 16, 16, 16, 16, 16}},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("workers=%d/shardSize=%d", tc.workers, tc.shardSize), func(t *testing.T) {
			m := newStoreMetrics(metric.TestSampleInterval)
			p := newTestProcessor()
			s := newRaftScheduler(log.MakeTestingAmbientContext(nil), m, p, tc.workers, tc.shardSize, 5)

			var shardWorkers []int
			for _, shard := range s.shards {
				shardWorkers = append(shardWorkers, shard.numWorkers)
			}
			require.Equal(t, tc.expectShards, shardWorkers)
		})
	}
}

// BenchmarkSchedulerEnqueueRaftTicks benchmarks the performance of enqueueing
// Raft ticks in the scheduler. This does *not* take contention into account,
// and does not run any workers that pull work off of the queue: it enqueues the
// messages in a single thread and then resets the queue.
func BenchmarkSchedulerEnqueueRaftTicks(b *testing.B) {
	defer log.Scope(b).Close(b)
	skip.UnderShort(b)

	ctx := context.Background()

	for _, collect := range []bool{false, true} {
		for _, numRanges := range []int{1, 10, 100, 1000, 10000, 100000} {
			for _, numWorkers := range []int{8, 16, 32, 64, 128} {
				b.Run(fmt.Sprintf("collect=%t/ranges=%d/workers=%d", collect, numRanges, numWorkers),
					func(b *testing.B) {
						runSchedulerEnqueueRaftTicks(ctx, b, collect, numRanges, numWorkers)
					},
				)
			}
		}
	}
}

func runSchedulerEnqueueRaftTicks(
	ctx context.Context, b *testing.B, collect bool, numRanges, numWorkers int,
) {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	a := log.MakeTestingAmbientContext(stopper.Tracer())
	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(a, m, p, numWorkers, defaultRaftSchedulerShardSize, 5)

	// raftTickLoop keeps unquiesced ranges in a map, so we do the same.
	ranges := make(map[roachpb.RangeID]struct{})
	for id := 1; id <= numRanges; id++ {
		ranges[roachpb.RangeID(id)] = struct{}{}
	}

	// Collect range IDs in the same way as raftTickLoop does, such that the
	// performance is comparable.
	batch := s.NewEnqueueBatch()
	defer batch.Close()
	getRangeIDs := func() raftSchedulerBatch {
		batch.Reset()
		for id := range ranges {
			batch.Add(id)
		}
		return batch
	}
	ids := getRangeIDs()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if collect {
			ids = getRangeIDs()
		}
		s.EnqueueRaftTicks(ids)

		// Flush the queue. We haven't started any workers that pull from it, so we
		// just clear it out.
		for _, shard := range s.shards {
			shard.queue = rangeIDQueue{}
		}
	}
}
