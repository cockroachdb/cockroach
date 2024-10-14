// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
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

type testProcessor struct {
	mu struct {
		syncutil.Mutex
		raftReady               map[roachpb.RangeID]int
		raftRequest             map[roachpb.RangeID]int
		raftTick                map[roachpb.RangeID]int
		rac2PiggybackedAdmitted map[roachpb.RangeID]int
		rac2RangeController     map[roachpb.RangeID]int
		ready                   func(roachpb.RangeID)
	}
}

func newTestProcessor() *testProcessor {
	p := &testProcessor{}
	p.mu.raftReady = make(map[roachpb.RangeID]int)
	p.mu.raftRequest = make(map[roachpb.RangeID]int)
	p.mu.raftTick = make(map[roachpb.RangeID]int)
	p.mu.rac2PiggybackedAdmitted = make(map[roachpb.RangeID]int)
	p.mu.rac2RangeController = make(map[roachpb.RangeID]int)
	return p
}

func (p *testProcessor) onReady(f func(roachpb.RangeID)) {
	p.mu.Lock()
	p.mu.ready = f
	p.mu.Unlock()
}

func (p *testProcessor) processReady(rangeID roachpb.RangeID) {
	p.mu.Lock()
	p.mu.raftReady[rangeID]++
	onReady := p.mu.ready
	p.mu.ready = nil
	p.mu.Unlock()
	if onReady != nil {
		onReady(rangeID)
	}
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

func (p *testProcessor) processRACv2PiggybackedAdmitted(
	_ context.Context, rangeID roachpb.RangeID,
) {
	p.mu.Lock()
	p.mu.rac2PiggybackedAdmitted[rangeID]++
	p.mu.Unlock()
}

func (p *testProcessor) processRACv2RangeController(_ context.Context, rangeID roachpb.RangeID) {
	p.mu.Lock()
	p.mu.rac2RangeController[rangeID]++
	p.mu.Unlock()
}

func (p *testProcessor) readyCount(rangeID roachpb.RangeID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.raftReady[rangeID]
}

func (p *testProcessor) countsLocked(m map[roachpb.RangeID]int) string {
	var ids []roachpb.RangeID
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
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
	return fmt.Sprintf("ready=%s request=%s tick=%s piggybacked-admitted=%s range-controller=%s",
		p.countsLocked(p.mu.raftReady),
		p.countsLocked(p.mu.raftRequest),
		p.countsLocked(p.mu.raftTick),
		p.countsLocked(p.mu.rac2PiggybackedAdmitted),
		p.countsLocked(p.mu.rac2RangeController))
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
	s := newRaftScheduler(log.MakeTestingAmbientContext(stopper.Tracer()), m, p, 1, 1, 1, 1)
	s.Start(stopper)

	batch := s.NewEnqueueBatch()
	defer batch.Close()
	batch.Add(1)
	batch.Add(2)
	batch.Add(3)
	s.EnqueueRaftTicks(batch)

	testutils.SucceedsSoon(t, func() error {
		const expected = "ready=[] request=[] tick=[1:1,2:1,3:1] piggybacked-admitted=[] range-controller=[]"
		if s := p.String(); expected != s {
			return errors.Errorf("expected %s, but got %s", expected, s)
		}
		return nil
	})

	count, _ := m.RaftSchedulerLatency.CumulativeSnapshot().Total()
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
	s := newRaftScheduler(log.MakeTestingAmbientContext(stopper.Tracer()), m, p, 1, 1, 1, 5)
	s.Start(stopper)

	testCases := []struct {
		flag  raftScheduleFlags
		slow  bool // true if Ready processing is slow
		ticks int
		want  string
	}{
		{flag: stateRaftReady,
			want: "ready=[1:1] request=[] tick=[] piggybacked-admitted=[] range-controller=[]"},
		{flag: stateRaftRequest,
			want: "ready=[1:1] request=[1:1] tick=[] piggybacked-admitted=[] range-controller=[]"},
		{flag: stateRaftTick,
			want: "ready=[1:1] request=[1:1] tick=[1:5] piggybacked-admitted=[] range-controller=[]"},
		{flag: stateRaftReady | stateRaftRequest | stateRaftTick,
			want: "ready=[1:2] request=[1:2] tick=[1:10] piggybacked-admitted=[] range-controller=[]"},
		{flag: stateRaftTick,
			want: "ready=[1:2] request=[1:2] tick=[1:15] piggybacked-admitted=[] range-controller=[]"},
		// All 4 ticks are processed.
		{flag: 0, ticks: 4,
			want: "ready=[1:2] request=[1:2] tick=[1:19] piggybacked-admitted=[] range-controller=[]"},
		// Only 5/10 ticks are buffered while Raft processing is slow.
		{flag: stateRaftReady, slow: true, ticks: 10,
			want: "ready=[1:3] request=[1:2] tick=[1:24] piggybacked-admitted=[] range-controller=[]"},
		// All 3 ticks are processed even if processing is slow.
		{flag: stateRaftReady, slow: true, ticks: 3,
			want: "ready=[1:4] request=[1:2] tick=[1:27] piggybacked-admitted=[] range-controller=[]"},
		{flag: stateRACv2PiggybackedAdmitted,
			want: "ready=[1:4] request=[1:2] tick=[1:27] piggybacked-admitted=[1:1] range-controller=[]"},
		{flag: stateRACv2RangeController,
			want: "ready=[1:4] request=[1:2] tick=[1:27] piggybacked-admitted=[1:1] range-controller=[1:1]"},
	}

	for _, c := range testCases {
		var started, done chan struct{}
		if c.slow {
			started, done = make(chan struct{}), make(chan struct{})
			p.onReady(func(roachpb.RangeID) {
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

	pri := 2 // priority workers

	testcases := []struct {
		priorityWorkers int
		workers         int
		shardSize       int
		expectShards    []int
	}{
		// We always assign at least 1 priority worker to the priority shard.
		{-1, 1, 1, []int{1, 1}},
		{0, 1, 1, []int{1, 1}},
		{2, 1, 1, []int{2, 1}},

		// We balance workers across shards instead of filling up shards. We assume
		// ranges are evenly distributed across shards, and want ranges to have
		// about the same number of workers available on average.
		//
		// Shard 0 is reserved for priority ranges, with a constant worker count.
		{pri, -1, -1, []int{pri, 1}},
		{pri, 0, 0, []int{pri, 1}},
		{pri, 1, -1, []int{pri, 1}},
		{pri, 1, 0, []int{pri, 1}},
		{pri, 1, 1, []int{pri, 1}},
		{pri, 1, 2, []int{pri, 1}},
		{pri, 2, 2, []int{pri, 2}},
		{pri, 3, 2, []int{pri, 2, 1}},
		{pri, 1, 3, []int{pri, 1}},
		{pri, 2, 3, []int{pri, 2}},
		{pri, 3, 3, []int{pri, 3}},
		{pri, 4, 3, []int{pri, 2, 2}},
		{pri, 5, 3, []int{pri, 3, 2}},
		{pri, 6, 3, []int{pri, 3, 3}},
		{pri, 7, 3, []int{pri, 3, 2, 2}},
		{pri, 8, 3, []int{pri, 3, 3, 2}},
		{pri, 9, 3, []int{pri, 3, 3, 3}},
		{pri, 10, 3, []int{pri, 3, 3, 2, 2}},
		{pri, 11, 3, []int{pri, 3, 3, 3, 2}},
		{pri, 12, 3, []int{pri, 3, 3, 3, 3}},

		// Typical examples, using 8 workers per CPU core. Note that we cap workers
		// at 128 by default.
		{pri, 1 * 8, 16, []int{pri, 8}},
		{pri, 2 * 8, 16, []int{pri, 16}},
		{pri, 3 * 8, 16, []int{pri, 12, 12}},
		{pri, 4 * 8, 16, []int{pri, 16, 16}},
		{pri, 6 * 8, 16, []int{pri, 16, 16, 16}},
		{pri, 8 * 8, 16, []int{pri, 16, 16, 16, 16}},
		{pri, 12 * 8, 16, []int{pri, 16, 16, 16, 16, 16, 16}},
		{pri, 16 * 8, 16, []int{pri, 16, 16, 16, 16, 16, 16, 16, 16}}, // 128 workers
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("workers=%d/shardSize=%d", tc.workers, tc.shardSize), func(t *testing.T) {
			m := newStoreMetrics(metric.TestSampleInterval)
			p := newTestProcessor()
			s := newRaftScheduler(log.MakeTestingAmbientContext(nil), m, p,
				tc.workers, tc.shardSize, tc.priorityWorkers, 5)

			var shardWorkers []int
			for _, shard := range s.shards {
				shardWorkers = append(shardWorkers, shard.numWorkers)
			}
			require.Equal(t, tc.expectShards, shardWorkers)
		})
	}
}

// TestSchedulerPriority tests that range prioritization is correctly
// updated and applied.
func TestSchedulerPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up a test scheduler with 1 regular non-priority worker.
	stopper := stop.NewStopper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer stopper.Stop(ctx)

	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(log.MakeTestingAmbientContext(nil), m, p, 1, 1, 1, 5)
	s.Start(stopper)
	require.Empty(t, s.PriorityIDs())

	// We use 3 ranges: r1 has priority, r2 blocks, r3 starves due to r2.
	const (
		priorityID = 1
		blockedID  = 2
		starvedID  = 3
	)
	s.AddPriorityID(priorityID)
	require.Equal(t, []roachpb.RangeID{priorityID}, s.PriorityIDs())

	// Enqueue r2 and wait for it to block.
	blockedC := make(chan chan struct{}, 1)
	p.onReady(func(rangeID roachpb.RangeID) {
		if rangeID == blockedID {
			unblockC := make(chan struct{})
			blockedC <- unblockC
			select {
			case <-unblockC:
			case <-ctx.Done():
			}
		}
	})
	s.EnqueueRaftReady(blockedID)

	var unblockC chan struct{}
	select {
	case unblockC = <-blockedC:
	case <-ctx.Done():
		return
	}

	// r3 should get starved.
	s.EnqueueRaftReady(starvedID)
	time.Sleep(time.Second)
	require.Zero(t, p.readyCount(starvedID))

	// r1 should get scheduled.
	s.EnqueueRaftReady(priorityID)
	require.Eventually(t, func() bool {
		return p.readyCount(priorityID) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Remove r1's priority. It should now starve as well.
	s.RemovePriorityID(priorityID)
	require.Empty(t, s.PriorityIDs())

	s.EnqueueRaftReady(priorityID)
	time.Sleep(time.Second)
	require.Equal(t, 1, p.readyCount(priorityID))

	// Unblock r2. r3 and r1 should now both get scheduled.
	close(unblockC)
	require.Eventually(t, func() bool {
		return p.readyCount(starvedID) == 1
	}, 10*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		return p.readyCount(priorityID) == 2
	}, 10*time.Second, 100*time.Millisecond)
}

// TestSchedulerPrioritizesLivenessAndMeta tests that the meta and liveness
// ranges are prioritized in the Raft scheduler.
func TestSchedulerPrioritizesLivenessAndMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	iter, err := s.RangeDescIteratorFactory().(rangedesc.IteratorFactory).
		NewIterator(ctx, keys.EverythingSpan)
	require.NoError(t, err)

	expectPrioritySpans := []roachpb.Span{keys.NodeLivenessSpan, keys.MetaSpan}
	expectPriorityIDs := []roachpb.RangeID{}
	for ; iter.Valid(); iter.Next() {
		desc := iter.CurRangeDescriptor()
		for _, span := range expectPrioritySpans {
			rspan, err := keys.SpanAddr(span)
			require.NoError(t, err)
			if _, err := desc.RSpan().Intersect(rspan); err == nil {
				expectPriorityIDs = append(expectPriorityIDs, desc.RangeID)
			}
		}
	}
	require.NotEmpty(t, expectPriorityIDs)
	require.ElementsMatch(t, expectPriorityIDs, store.RaftSchedulerPriorityIDs())
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
		for _, priority := range []bool{false, true} {
			for _, numRanges := range []int{1, 10, 100, 1000, 10000, 100000} {
				for _, numWorkers := range []int{8, 16, 32, 64, 128} {
					b.Run(fmt.Sprintf("collect=%t/priority=%t/ranges=%d/workers=%d", collect, priority, numRanges, numWorkers),
						func(b *testing.B) {
							runSchedulerEnqueueRaftTicks(ctx, b, collect, priority, numRanges, numWorkers)
						},
					)
				}
			}
		}
	}
}

func runSchedulerEnqueueRaftTicks(
	ctx context.Context, b *testing.B, collect bool, priority bool, numRanges, numWorkers int,
) {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	a := log.MakeTestingAmbientContext(stopper.Tracer())
	m := newStoreMetrics(metric.TestSampleInterval)
	p := newTestProcessor()
	s := newRaftScheduler(
		a, m, p, numWorkers, defaultRaftSchedulerShardSize, defaultRaftSchedulerPriorityShardSize, 5)

	// If requested, add a prioritized range corresponding to e.g. the liveness
	// range.
	if priority {
		s.AddPriorityID(1)
	}

	// raftTickLoop keeps unquiesced ranges in a map, so we do the same.
	ranges := make(map[roachpb.RangeID]struct{})
	for id := 1; id <= numRanges; id++ {
		ranges[roachpb.RangeID(id)] = struct{}{}
	}

	// Collect range IDs in the same way as raftTickLoop does, such that the
	// performance is comparable.
	getRangeIDs := func() *raftSchedulerBatch {
		batch := s.NewEnqueueBatch()
		for id := range ranges {
			batch.Add(id)
		}
		return batch
	}
	ids := getRangeIDs()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if collect {
			ids.Close()
			ids = getRangeIDs()
		}
		s.EnqueueRaftTicks(ids)

		// Flush the queue. We haven't started any workers that pull from it, so we
		// just clear it out.
		for _, shard := range s.shards {
			shard.queue = rangeIDQueue{}
		}
	}
	ids.Close()
}
