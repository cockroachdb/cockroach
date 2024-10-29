// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
)

const rangeIDChunkSize = 1000

type rangeIDChunk struct {
	// Valid contents are buf[rd:wr], read at buf[rd], write at buf[wr].
	buf    [rangeIDChunkSize]roachpb.RangeID
	rd, wr int
}

func (c *rangeIDChunk) PushBack(id roachpb.RangeID) bool {
	if c.WriteCap() == 0 {
		return false
	}
	c.buf[c.wr] = id
	c.wr++
	return true
}

func (c *rangeIDChunk) PopFront() (roachpb.RangeID, bool) {
	if c.Len() == 0 {
		return 0, false
	}
	id := c.buf[c.rd]
	c.rd++
	return id, true
}

func (c *rangeIDChunk) WriteCap() int {
	return len(c.buf) - c.wr
}

func (c *rangeIDChunk) Len() int {
	return c.wr - c.rd
}

// rangeIDQueue is a chunked queue of range IDs. Instead of a separate list
// element for every range ID, it uses a rangeIDChunk to hold many range IDs,
// amortizing the allocation/GC cost. Using a chunk queue avoids any copying
// that would occur if a slice were used (the copying would occur on slice
// reallocation).
//
// The queue implements a FIFO queueing policy with no prioritization of some
// ranges over others.
type rangeIDQueue struct {
	len    int
	chunks list.List
}

func (q *rangeIDQueue) Push(id roachpb.RangeID) {
	q.len++
	if q.chunks.Len() == 0 || q.back().WriteCap() == 0 {
		q.chunks.PushBack(&rangeIDChunk{})
	}
	if !q.back().PushBack(id) {
		panic(fmt.Sprintf(
			"unable to push rangeID to chunk: len=%d, cap=%d",
			q.back().Len(), q.back().WriteCap()))
	}
}

func (q *rangeIDQueue) PopFront() (roachpb.RangeID, bool) {
	if q.len == 0 {
		return 0, false
	}
	q.len--
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*rangeIDChunk)
	id, ok := front.PopFront()
	if !ok {
		panic("encountered empty chunk")
	}
	if front.Len() == 0 && front.WriteCap() == 0 {
		q.chunks.Remove(frontElem)
	}
	return id, true
}

func (q *rangeIDQueue) Len() int {
	return q.len
}

func (q *rangeIDQueue) back() *rangeIDChunk {
	return q.chunks.Back().Value.(*rangeIDChunk)
}

type raftProcessor interface {
	// Process a raft.Ready struct containing entries and messages that are
	// ready to read, be saved to stable storage, committed, or sent to other
	// peers.
	//
	// This method does not take a ctx; the implementation is expected to use a
	// ctx annotated with the range information, according to RangeID.
	processReady(roachpb.RangeID)
	// Process all queued messages for the specified range.
	// Return true if the range should be queued for ready processing.
	processRequestQueue(context.Context, roachpb.RangeID) bool
	// Process a raft tick for the specified range.
	// Return true if the range should be queued for ready processing.
	processTick(context.Context, roachpb.RangeID) bool
	// Process piggybacked admitted vectors that may advance admitted state for
	// the given range's peer replicas. Used for RACv2.
	processRACv2PiggybackedAdmitted(ctx context.Context, id roachpb.RangeID)
	// Process the RACv2 RangeController.
	processRACv2RangeController(ctx context.Context, id roachpb.RangeID)
}

type raftScheduleFlags int

const (
	stateQueued raftScheduleFlags = 1 << iota
	stateRaftReady
	stateRaftRequest
	stateRaftTick
	stateRACv2PiggybackedAdmitted
	stateRACv2RangeController
)

type raftScheduleState struct {
	flags raftScheduleFlags
	begin crtime.Mono

	// The number of ticks queued. Usually it's 0 or 1, but may go above if the
	// scheduling or processing is slow. It is limited by raftScheduler.maxTicks,
	// so that the cost of processing all the ticks doesn't grow uncontrollably.
	// If ticks consistently reaches maxTicks, the node/range is too slow, and it
	// is safer to not deliver all the ticks as it may cause a cascading effect
	// (the range events take longer and longer to process).
	// TODO(pavelkalinnikov): add a node health metric for the ticks.
	//
	// INVARIANT: flags&stateRaftTick == 0 iff ticks == 0.
	ticks int64
}

var raftSchedulerBatchPool = sync.Pool{
	New: func() interface{} {
		return new(raftSchedulerBatch)
	},
}

// raftSchedulerBatch is a batch of range IDs to enqueue. It enables
// efficient per-shard enqueueing.
type raftSchedulerBatch struct {
	rangeIDs    [][]roachpb.RangeID // by shard
	priorityIDs map[roachpb.RangeID]bool
}

func newRaftSchedulerBatch(
	numShards int, priorityIDs *syncutil.Set[roachpb.RangeID],
) *raftSchedulerBatch {
	b := raftSchedulerBatchPool.Get().(*raftSchedulerBatch)
	if cap(b.rangeIDs) >= numShards {
		b.rangeIDs = b.rangeIDs[:numShards]
	} else {
		b.rangeIDs = make([][]roachpb.RangeID, numShards)
	}
	if b.priorityIDs == nil {
		b.priorityIDs = make(map[roachpb.RangeID]bool, 8) // expect few ranges, if any
	}
	// Cache the priority range IDs in an owned map, since we expect this to be
	// very small or empty and we do a lookup for every Add() call.
	priorityIDs.Range(func(id roachpb.RangeID) bool {
		b.priorityIDs[id] = true
		return true
	})
	return b
}

func (b *raftSchedulerBatch) Add(id roachpb.RangeID) {
	shardIdx := shardIndex(id, len(b.rangeIDs), b.priorityIDs[id])
	b.rangeIDs[shardIdx] = append(b.rangeIDs[shardIdx], id)
}

func (b *raftSchedulerBatch) Close() {
	for i := range b.rangeIDs {
		b.rangeIDs[i] = b.rangeIDs[i][:0]
	}
	for i := range b.priorityIDs {
		delete(b.priorityIDs, i)
	}
	raftSchedulerBatchPool.Put(b)
}

// shardIndex returns the raftScheduler shard index of the given range ID based
// on the shard count and the range's priority. Priority ranges are assigned to
// the reserved shard 0, other ranges are modulo range ID (ignoring shard 0).
// numShards will always be 2 or more (1 priority, 1 regular).
func shardIndex(id roachpb.RangeID, numShards int, priority bool) int {
	if priority {
		return 0
	}
	return 1 + int(int64(id)%int64(numShards-1)) // int64s to avoid overflow
}

type raftScheduler struct {
	ambientContext log.AmbientContext
	processor      raftProcessor
	metrics        *StoreMetrics
	// shards contains scheduler shards. Ranges and workers are allocated to
	// separate shards to reduce contention at high worker counts. Allocation
	// is modulo range ID, with shard 0 reserved for priority ranges.
	shards      []*raftSchedulerShard // 1 + RangeID % (len(shards) - 1)
	priorityIDs syncutil.Set[roachpb.RangeID]
	done        sync.WaitGroup
}

type raftSchedulerShard struct {
	syncutil.Mutex
	cond       *sync.Cond
	queue      rangeIDQueue
	state      map[roachpb.RangeID]raftScheduleState
	numWorkers int
	maxTicks   int64
	stopped    bool
}

func newRaftScheduler(
	ambient log.AmbientContext,
	metrics *StoreMetrics,
	processor raftProcessor,
	numWorkers int,
	shardSize int,
	priorityWorkers int,
	maxTicks int64,
) *raftScheduler {
	s := &raftScheduler{
		ambientContext: ambient,
		processor:      processor,
		metrics:        metrics,
	}

	// Priority shard at index 0.
	if priorityWorkers <= 0 {
		priorityWorkers = 1
	}
	s.shards = append(s.shards, newRaftSchedulerShard(priorityWorkers, maxTicks))

	// Regular shards, excluding priority shard.
	numShards := 1
	if shardSize > 0 && numWorkers > shardSize {
		numShards = (numWorkers-1)/shardSize + 1 // ceiling division
	}
	for i := 0; i < numShards; i++ {
		shardWorkers := numWorkers / numShards
		if i < numWorkers%numShards { // distribute remainder
			shardWorkers++
		}
		if shardWorkers <= 0 {
			shardWorkers = 1 // ensure we always have a worker
		}
		s.shards = append(s.shards, newRaftSchedulerShard(shardWorkers, maxTicks))
	}
	return s
}

func newRaftSchedulerShard(numWorkers int, maxTicks int64) *raftSchedulerShard {
	shard := &raftSchedulerShard{
		state:      map[roachpb.RangeID]raftScheduleState{},
		numWorkers: numWorkers,
		maxTicks:   maxTicks,
	}
	shard.cond = sync.NewCond(&shard.Mutex)
	return shard
}

func (s *raftScheduler) Start(stopper *stop.Stopper) {
	ctx := s.ambientContext.AnnotateCtx(context.Background())
	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		for _, shard := range s.shards {
			shard.Lock()
			shard.stopped = true
			shard.Unlock()
			shard.cond.Broadcast()
		}
	}
	if err := stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "raftsched-wait-quiesce",
			// This task doesn't reference a parent because it runs for the server's
			// lifetime.
			SpanOpt: stop.SterileRootSpan,
		},
		waitQuiesce); err != nil {
		waitQuiesce(ctx)
	}

	for _, shard := range s.shards {
		s.done.Add(shard.numWorkers)
		for i := 0; i < shard.numWorkers; i++ {
			if err := stopper.RunAsyncTaskEx(ctx,
				stop.TaskOpts{
					TaskName: "raft-worker",
					// This task doesn't reference a parent because it runs for the server's
					// lifetime.
					SpanOpt: stop.SterileRootSpan,
				},
				func(ctx context.Context) {
					shard.worker(ctx, s.processor, s.metrics)
					s.done.Done()
				},
			); err != nil {
				s.done.Done()
			}
		}
	}
}

func (s *raftScheduler) Wait(context.Context) {
	s.done.Wait()
}

// AddPriorityID adds the given range ID to the set of priority ranges.
func (s *raftScheduler) AddPriorityID(rangeID roachpb.RangeID) {
	s.priorityIDs.Add(rangeID)
}

// RemovePriorityID removes the given range ID from the set of priority ranges.
func (s *raftScheduler) RemovePriorityID(rangeID roachpb.RangeID) {
	s.priorityIDs.Remove(rangeID)
}

// PriorityIDs returns the current priority ranges.
func (s *raftScheduler) PriorityIDs() []roachpb.RangeID {
	var priorityIDs []roachpb.RangeID
	s.priorityIDs.Range(func(id roachpb.RangeID) bool {
		priorityIDs = append(priorityIDs, id)
		return true
	})
	return priorityIDs
}

func (ss *raftSchedulerShard) worker(
	ctx context.Context, processor raftProcessor, metrics *StoreMetrics,
) {

	// We use a sync.Cond for worker notification instead of a buffered
	// channel. Buffered channels have internal overhead for maintaining the
	// buffer even when the elements are empty. And the buffer isn't necessary as
	// the raftScheduler work is already buffered on the internal queue. Lastly,
	// signaling a sync.Cond is significantly faster than selecting and sending
	// on a buffered channel.

	ss.Lock()
	for {
		var id roachpb.RangeID
		for {
			if ss.stopped {
				ss.Unlock()
				return
			}
			var ok bool
			if id, ok = ss.queue.PopFront(); ok {
				break
			}
			ss.cond.Wait()
		}

		// Grab and clear the existing state for the range ID. Note that we leave
		// the range ID marked as "queued" so that a concurrent Enqueue* will not
		// queue the range ID again.
		state := ss.state[id]
		ss.state[id] = raftScheduleState{flags: stateQueued}
		ss.Unlock()

		// Record the scheduling latency for the range.
		lat := state.begin.Elapsed()
		metrics.RaftSchedulerLatency.RecordValue(int64(lat))

		// Process requests first. This avoids a scenario where a tick and a
		// "quiesce" message are processed in the same iteration and intervening
		// raft ready processing unquiesces the replica because the tick triggers
		// an election.
		if state.flags&stateRaftRequest != 0 {
			// processRequestQueue returns true if the range should perform ready
			// processing. Do not reorder this below the call to processReady.
			if processor.processRequestQueue(ctx, id) {
				state.flags |= stateRaftReady
			}
		}
		if util.RaceEnabled { // assert the ticks invariant
			if tick := state.flags&stateRaftTick != 0; tick != (state.ticks != 0) {
				log.Fatalf(ctx, "stateRaftTick is %v with ticks %v", tick, state.ticks)
			}
		}
		if state.flags&stateRaftTick != 0 {
			for t := state.ticks; t > 0; t-- {
				// processRaftTick returns true if the range should perform ready
				// processing. Do not reorder this below the call to processReady.
				if processor.processTick(ctx, id) {
					state.flags |= stateRaftReady
				}
			}
		}
		if state.flags&stateRACv2PiggybackedAdmitted != 0 {
			processor.processRACv2PiggybackedAdmitted(ctx, id)
		}
		if state.flags&stateRaftReady != 0 {
			processor.processReady(id)
		}
		if state.flags&stateRACv2RangeController != 0 {
			processor.processRACv2RangeController(ctx, id)
		}

		ss.Lock()
		state = ss.state[id]
		if state.flags == stateQueued {
			// No further processing required by the range ID, clear it from the
			// state map.
			delete(ss.state, id)
		} else {
			// There was a concurrent call to one of the Enqueue* methods. Queue
			// the range ID for further processing.
			//
			// Even though the Enqueue* method did not signal after detecting
			// that the range was being processed, there also is no need for us
			// to signal the condition variable. This is because this worker
			// goroutine will loop back around and continue working without ever
			// going back to sleep.
			//
			// We can prove this out through a short derivation.
			// - For optimal concurrency, we want:
			//     awake_workers = min(max_workers, num_ranges)
			// - The condition variable / mutex structure ensures that:
			//     awake_workers = cur_awake_workers + num_signals
			// - So we need the following number of signals for optimal concurrency:
			//     num_signals = min(max_workers, num_ranges) - cur_awake_workers
			// - If we re-enqueue a range that's currently being processed, the
			//   num_ranges does not change once the current iteration completes
			//   and the worker does not go back to sleep between the current
			//   iteration and the next iteration, so no change to num_signals
			//   is needed.
			ss.queue.Push(id)
		}
	}
}

// NewEnqueueBatch creates a new range ID batch for enqueueing via
// EnqueueRaft(Ticks|Requests). The caller must call Close() on the batch when
// done.
func (s *raftScheduler) NewEnqueueBatch() *raftSchedulerBatch {
	return newRaftSchedulerBatch(len(s.shards), &s.priorityIDs)
}

func (ss *raftSchedulerShard) enqueue1Locked(
	addFlags raftScheduleFlags, id roachpb.RangeID, now crtime.Mono,
) int {
	ticks := int64((addFlags & stateRaftTick) / stateRaftTick) // 0 or 1

	prevState := ss.state[id]
	if prevState.flags&addFlags == addFlags && ticks == 0 {
		return 0
	}
	var queued int
	newState := prevState
	newState.flags = newState.flags | addFlags
	newState.ticks += ticks
	if newState.ticks > ss.maxTicks {
		newState.ticks = ss.maxTicks
	}
	if newState.flags&stateQueued == 0 {
		newState.flags |= stateQueued
		queued++
		ss.queue.Push(id)
	}
	if newState.begin == 0 {
		newState.begin = now
	}
	ss.state[id] = newState
	return queued
}

func (s *raftScheduler) enqueue1(addFlags raftScheduleFlags, id roachpb.RangeID) {
	now := crtime.NowMono()
	hasPriority := s.priorityIDs.Contains(id)
	shardIdx := shardIndex(id, len(s.shards), hasPriority)
	shard := s.shards[shardIdx]
	shard.Lock()
	n := shard.enqueue1Locked(addFlags, id, now)
	shard.Unlock()
	shard.signal(n)
}

func (ss *raftSchedulerShard) enqueueN(addFlags raftScheduleFlags, ids ...roachpb.RangeID) int {
	// Enqueue the ids in chunks to avoid holding mutex for too long.
	const enqueueChunkSize = 128

	// Avoid locking for 0 new ranges.
	if len(ids) == 0 {
		return 0
	}

	now := crtime.NowMono()
	ss.Lock()
	var count int
	for i, id := range ids {
		count += ss.enqueue1Locked(addFlags, id, now)
		if (i+1)%enqueueChunkSize == 0 {
			ss.Unlock()
			now = crtime.NowMono()
			ss.Lock()
		}
	}
	ss.Unlock()
	return count
}

func (s *raftScheduler) enqueueBatch(addFlags raftScheduleFlags, batch *raftSchedulerBatch) {
	for shardIdx, ids := range batch.rangeIDs {
		count := s.shards[shardIdx].enqueueN(addFlags, ids...)
		s.shards[shardIdx].signal(count)
	}
}

func (ss *raftSchedulerShard) signal(count int) {
	if count >= ss.numWorkers {
		ss.cond.Broadcast()
	} else {
		for i := 0; i < count; i++ {
			ss.cond.Signal()
		}
	}
}

func (s *raftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	s.enqueue1(stateRaftReady, id)
}

func (s *raftScheduler) EnqueueRaftRequest(id roachpb.RangeID) {
	s.enqueue1(stateRaftRequest, id)
}

func (s *raftScheduler) EnqueueRaftRequests(batch *raftSchedulerBatch) {
	s.enqueueBatch(stateRaftRequest, batch)
}

func (s *raftScheduler) EnqueueRaftTicks(batch *raftSchedulerBatch) {
	s.enqueueBatch(stateRaftTick, batch)
}

func (s *raftScheduler) EnqueueRACv2PiggybackAdmitted(id roachpb.RangeID) {
	s.enqueue1(stateRACv2PiggybackedAdmitted, id)
}

func (s *raftScheduler) EnqueueRACv2RangeController(id roachpb.RangeID) {
	s.enqueue1(stateRACv2RangeController, id)
}

type racV2Scheduler raftScheduler

var _ rac2.Scheduler = &racV2Scheduler{}

// ScheduleControllerEvent implements rac2.Scheduler.
func (s *racV2Scheduler) ScheduleControllerEvent(rangeID roachpb.RangeID) {
	(*raftScheduler)(s).EnqueueRACv2RangeController(rangeID)
}
