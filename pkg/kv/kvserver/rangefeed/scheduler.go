// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Scheduler is used by rangefeed processors to schedule work to a pool of
// workers instead of running individual goroutines per range. Store will run
// a single scheduler for all its rangefeeds.
//
// When processor is started it registers a callback with scheduler.
// After that processor can enqueue work for itself by telling scheduler what
// types of events it plans to process.
//
// Scheduler maintains a queue of processor ids that wish to do processing and
// notifies callbacks in order passing them union of event types that were
// enqueued since last notification.

// processorEventType is a mask for pending events for the processor. All event
// types that were enqueued between two callback invocations are coalesced into
// a single value.
type processorEventType int

const (
	// Queued is an internal event type that indicate that there's already a
	// pending work for processor and it is already scheduled for execution.
	// When more events types come in, they should just be added to existing
	// pending value.
	Queued processorEventType = 1 << iota
	// Stopped is an event that indicates that there would be no more events
	// scheduled for the processor. once it is enqueued, all subsequent events
	// are rejected. processor should perform any cleanup when receiving this
	// event that it needs to perform within callback context.
	Stopped
	// EventQueued is scheduled when event is put into rangefeed queue for
	// processing.
	EventQueued
	// RequestQueued is scheduled when request closure is put into rangefeed
	// request queue.
	RequestQueued
	// PushTxnQueued is scheduled externally on ranges to push transaction with intents
	// that block resolved timestamp advancing.
	PushTxnQueued
	// numProcessorEventTypes is total number of event types.
	numProcessorEventTypes int = iota
)

var eventNames = map[processorEventType]string{
	Queued:        "Queued",
	Stopped:       "Stopped",
	EventQueued:   "Event",
	RequestQueued: "Request",
	PushTxnQueued: "PushTxn",
}

func (e processorEventType) String() string {
	var evts []string
	for i := 0; i < numProcessorEventTypes; i++ {
		if eventType := processorEventType(1 << i); eventType&e != 0 {
			evts = append(evts, eventNames[eventType])
		}
	}
	return strings.Join(evts, " | ")
}

// enqueueBulkMaxChunk is max number of event enqueued in one go while holding
// scheduler lock.
const enqueueBulkMaxChunk = 100

// schedulerLatencyHistogramCollectionFrequency defines how frequently scheduler
// will update histogram. we choose a prime number to reduce changes of periodic
// events skewing sampling.
const schedulerLatencyHistogramCollectionFrequency = 13

// Callback is a callback to perform work set by processor. Event is a
// combination of all event types scheduled since last callback invocation.
//
// Once callback returns, event types considered to be processed. If a processor
// decided not to process everything, it can return remaining types which would
// instruct scheduler to re-enqueue processor.
//
// This mechanism allows processors to throttle processing if it has too much
// pending data to process in one go without blocking other processors.
type Callback func(event processorEventType) (remaining processorEventType)

// SchedulerConfig contains configurable scheduler parameters.
type SchedulerConfig struct {
	// Workers is the number of pool workers for scheduler to use.
	Workers int
	// PriorityWorkers is the number of workers to use for the priority shard.
	PriorityWorkers int
	// ShardSize is the maximum number of workers per scheduler shard. Once a
	// shard is full, new shards are split off, and workers are evently distribued
	// across all shards.
	ShardSize int
	// BulkChunkSize is number of ids that would be enqueued in a single bulk
	// enqueue operation. Chunking is done to avoid holding locks for too long
	// as it will interfere with enqueue operations.
	BulkChunkSize int

	// Metrics for scheduler performance monitoring
	Metrics *SchedulerMetrics
	// histogramFrequency sets how frequently wait latency is recorded for metrics
	// purposes.
	HistogramFrequency int64
}

// shardIndex returns the shard index of the given processor ID based on the
// shard count and processor priority. Priority processors are assigned to the
// reserved shard 0, other ranges are modulo ID (ignoring shard 0). numShards
// will always be 2 or more (1 priority, 1 regular).
// gcassert:inline
func shardIndex(id int64, numShards int, priority bool) int {
	if priority {
		return 0
	}
	return 1 + int(id%int64(numShards-1))
}

// Scheduler is a simple scheduler that allows work to be scheduler
// against number of processors. Each processor is represented by unique id and
// a callback.
//
// Work is enqueued in a form of event type using processor id.
// Processors callback is then called by worker thread with all combined pending
// events.
//
// Each event is represented as a bit mask and multiple pending events could be
// ORed together before being delivered to processor.
type Scheduler struct {
	nextID atomic.Int64
	// shards contains scheduler shards. Processors and workers are allocated to
	// separate shards to reduce mutex contention. Allocation is modulo
	// processors, with shard 0 reserved for priority processors.
	shards      []*schedulerShard // 1 + id%(len(shards)-1)
	priorityIDs syncutil.Set[int64]
	wg          sync.WaitGroup
}

// schedulerShard is a mutex shard, which reduces contention: workers in a shard
// share a mutex for scheduling bookkeeping, and this mutex becomes highly
// contended without sharding. Processors are assigned round-robin to a shard
// when registered, see shardIndex().
type schedulerShard struct {
	syncutil.Mutex
	numWorkers    int
	bulkChunkSize int
	cond          *sync.Cond
	procs         map[int64]Callback
	status        map[int64]processorEventType
	queue         *idQueue
	// No more new registrations allowed. Workers are winding down.
	quiescing bool

	metrics *ShardMetrics
	// histogramFrequency determines frequency of histogram collection
	histogramFrequency int64
	nextLatencyCheck   int64
}

// NewScheduler will instantiate an idle scheduler based on provided config.
// Scheduler needs to be started to become operational.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	bulkChunkSize := cfg.BulkChunkSize
	if bulkChunkSize == 0 {
		bulkChunkSize = enqueueBulkMaxChunk
	}
	histogramFrequency := cfg.HistogramFrequency
	if histogramFrequency == 0 {
		histogramFrequency = schedulerLatencyHistogramCollectionFrequency
	}

	s := &Scheduler{}

	// Priority shard at index 0.
	priorityWorkers := 1
	if cfg.PriorityWorkers > 0 {
		priorityWorkers = cfg.PriorityWorkers
	}
	s.shards = append(s.shards,
		newSchedulerShard(priorityWorkers, bulkChunkSize, cfg.Metrics.SystemPriority, histogramFrequency))

	// Regular shards, excluding priority shard.
	numShards := 1
	if cfg.ShardSize > 0 && cfg.Workers > cfg.ShardSize {
		numShards = (cfg.Workers-1)/cfg.ShardSize + 1 // ceiling division
	}
	for i := 0; i < numShards; i++ {
		shardWorkers := cfg.Workers / numShards
		if i < cfg.Workers%numShards { // distribute remainder
			shardWorkers++
		}
		if shardWorkers <= 0 {
			shardWorkers = 1 // ensure we always have a worker
		}
		s.shards = append(s.shards,
			newSchedulerShard(shardWorkers, bulkChunkSize, cfg.Metrics.NormalPriority, histogramFrequency))
	}

	return s
}

// newSchedulerShard creates a new shard with the given number of workers.
func newSchedulerShard(
	numWorkers, bulkChunkSize int, metrics *ShardMetrics, histogramFrequency int64,
) *schedulerShard {
	ss := &schedulerShard{
		numWorkers:         numWorkers,
		bulkChunkSize:      bulkChunkSize,
		procs:              map[int64]Callback{},
		status:             map[int64]processorEventType{},
		queue:              newIDQueue(),
		metrics:            metrics,
		histogramFrequency: histogramFrequency,
	}
	ss.cond = sync.NewCond(&ss.Mutex)
	return ss
}

// Start scheduler workers.
func (s *Scheduler) Start(ctx context.Context, stopper *stop.Stopper) error {
	// Start each shard.
	for shardID, shard := range s.shards {
		// Start the shard's workers.
		for workerID := 0; workerID < shard.numWorkers; workerID++ {
			s.wg.Add(1)

			if err := stopper.RunAsyncTask(ctx,
				fmt.Sprintf("rangefeed-scheduler-worker-shard%d-%d", shardID, workerID),
				func(ctx context.Context) {
					defer s.wg.Done()
					log.VEventf(ctx, 3, "scheduler worker %d:%d started", shardID, workerID)
					shard.processEvents(ctx)
					log.VEventf(ctx, 3, "scheduler worker %d:%d finished", shardID, workerID)
				},
			); err != nil {
				s.wg.Done()
				s.Stop()
				return err
			}
		}
	}

	if err := stopper.RunAsyncTask(ctx, "terminate scheduler",
		func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			log.VEvent(ctx, 2, "scheduler quiescing")
			s.Stop()
		}); err != nil {
		s.Stop()
		return err
	}
	return nil
}

// register callback to be able to schedule work. Returns error if id is already
// registered or if Scheduler is stopped.  If priority is true, the range is
// allocated to a separate priority shard with dedicated workers (intended for a
// small number of system ranges). Returns error if Scheduler is stopped.
func (s *Scheduler) register(id int64, f Callback, priority bool) error {
	// Make sure we register the priority ID before registering the callback,
	// since we can otherwise race with enqueues, using the wrong shard.
	if priority {
		s.priorityIDs.Add(id)
	}
	if err := s.shards[shardIndex(id, len(s.shards), priority)].register(id, f); err != nil {
		s.priorityIDs.Remove(id)
		return err
	}
	return nil
}

// unregister removed the processor callback from scheduler. If processor is
// currently processing event it will finish processing.
//
// Processor won't receive Stopped event if it wasn't explicitly sent.
// To make sure processor performs cleanup, it is easier to send it Stopped
// event first and let it remove itself from registration during event handling.
// Any attempts to enqueue events for processor after this call will return an
// error.
func (s *Scheduler) unregister(id int64) {
	priority := s.priorityIDs.Contains(id)
	s.shards[shardIndex(id, len(s.shards), priority)].unregister(id)
	s.priorityIDs.Remove(id)
}

func (s *Scheduler) Stop() {
	// Stop all shard workers.
	for _, shard := range s.shards {
		shard.quiesce()
	}
	s.wg.Wait()

	// Synchronously notify processors about stop.
	for _, shard := range s.shards {
		shard.stop()
	}
}

// stopProcessor instructs processor to stop gracefully by sending it Stopped event.
// Once stop is called all subsequent Schedule calls for this id will return
// error.
func (s *Scheduler) stopProcessor(id int64) {
	s.enqueue(id, Stopped)
}

// Enqueue event for existing callback. The event is ignored if the processor
// does not exist.
func (s *Scheduler) enqueue(id int64, evt processorEventType) {
	priority := s.priorityIDs.Contains(id)
	s.shards[shardIndex(id, len(s.shards), priority)].enqueue(id, evt)
}

// EnqueueBatch enqueues an event for a set of processors across all shards.
// Using a batch allows efficient enqueueing with minimal lock contention.
func (s *Scheduler) EnqueueBatch(batch *SchedulerBatch, evt processorEventType) {
	for shardIdx, ids := range batch.ids {
		if len(ids) > 0 {
			s.shards[shardIdx].enqueueN(ids, evt)
		}
	}
}

// NewEnqueueBatch creates a new batch that can be used to efficiently enqueue
// events for multiple processors via EnqueueBatch(). The batch should be closed
// when done by calling Close().
func (s *Scheduler) NewEnqueueBatch() *SchedulerBatch {
	return newSchedulerBatch(len(s.shards), &s.priorityIDs)
}

// register registers a callback with the shard. The caller must not hold
// the shard lock.
func (ss *schedulerShard) register(id int64, f Callback) error {
	ss.Lock()
	defer ss.Unlock()
	if ss.quiescing {
		// Don't accept new registrations if quiesced.
		return errors.New("server stopping")
	}
	if _, registered := ss.procs[id]; registered {
		return errors.Newf("callback is already registered with id %d", id)
	}
	ss.procs[id] = f
	return nil
}

// unregister unregisters a callback with the shard. The caller must not
// hold the shard lock.
func (ss *schedulerShard) unregister(id int64) {
	ss.Lock()
	defer ss.Unlock()
	delete(ss.procs, id)
	delete(ss.status, id)
}

// enqueue enqueues a single event for a given processor in this shard, and wakes
// up a worker to process it. The caller must not hold the shard lock.
func (ss *schedulerShard) enqueue(id int64, evt processorEventType) {
	// We get time outside of lock to get more realistic delay in case there's a
	// scheduler contention.
	now := ss.maybeEnqueueStartTime()
	ss.Lock()
	defer ss.Unlock()
	if ss.enqueueLocked(queueEntry{id: id, startTime: now}, evt) {
		// Wake up potential waiting worker.
		// We are allowed to do this under cond lock.
		ss.cond.Signal()
		ss.metrics.QueueSize.Inc(1)
	}
}

// maybeEnqueueStartTime returns now in nanos or 0. 0 means event will have no start
// time and hence won't be included in histogram. This is done to throttle
// histogram collection by only recording every n-th event to reduce CPU waste.
func (ss *schedulerShard) maybeEnqueueStartTime() int64 {
	var now int64
	if v := atomic.AddInt64(&ss.nextLatencyCheck, -1); v < 1 && (-v%ss.histogramFrequency) == 0 {
		now = timeutil.Now().UnixNano()
		atomic.AddInt64(&ss.nextLatencyCheck, ss.histogramFrequency)
	}
	return now
}

// enqueueLocked enqueues a single event for a given processor in this shard.
// Does not wake up a worker to process it.
func (ss *schedulerShard) enqueueLocked(entry queueEntry, evt processorEventType) bool {
	if _, ok := ss.procs[entry.id]; !ok {
		return false
	}
	pending := ss.status[entry.id]
	if pending&Stopped != 0 {
		return false
	}
	if pending == 0 {
		// Enqueue if processor was idle.
		ss.queue.pushBack(entry)
	}
	update := pending | evt | Queued
	if update != pending {
		// Only update if event actually changed.
		ss.status[entry.id] = update
	}
	return pending == 0
}

// enqueueN enqueues an event for multiple processors on this shard, and wakes
// up workers to process them. The caller must not hold the shard lock.
func (ss *schedulerShard) enqueueN(ids []int64, evt processorEventType) int {
	// Avoid locking for 0 new processors.
	if len(ids) == 0 {
		return 0
	}

	// For bulk insertions we just apply sampling frequency within the batch
	// to reduce contention.
	now := timeutil.Now().UnixNano()
	ss.Lock()
	var count int
	for i, id := range ids {
		time := int64(0)
		if int64(i)%ss.histogramFrequency == 0 {
			time = now
		}
		if ss.enqueueLocked(queueEntry{id: id, startTime: time}, evt) {
			count++
		}
		if (i+1)%ss.bulkChunkSize == 0 {
			ss.Unlock()
			ss.Lock()
		}
	}
	ss.Unlock()
	ss.metrics.QueueSize.Inc(int64(count))

	if count >= ss.numWorkers {
		ss.cond.Broadcast()
	} else {
		for i := 0; i < count; i++ {
			ss.cond.Signal()
		}
	}
	return count
}

// processEvents is a main worker method of a scheduler pool. each one should
// be launched in separate goroutine and will loop until scheduler is stopped.
func (ss *schedulerShard) processEvents(ctx context.Context) {
	for {
		var entry queueEntry
		ss.Lock()
		for {
			if ss.quiescing {
				ss.Unlock()
				return
			}
			var ok bool
			if entry, ok = ss.queue.popFront(); ok {
				break
			}
			ss.cond.Wait()
		}

		cb := ss.procs[entry.id]
		e := ss.status[entry.id]
		// Keep Queued status and preserve Stopped to block any more events.
		ss.status[entry.id] = Queued | (e & Stopped)
		ss.Unlock()

		if entry.startTime != 0 {
			delay := timeutil.Now().UnixNano() - entry.startTime
			ss.metrics.QueueTime.RecordValue(delay)
		}
		ss.metrics.QueueSize.Dec(1)

		procEventType := Queued ^ e
		remaining := cb(procEventType)

		if remaining != 0 && buildutil.CrdbTestBuild {
			if (remaining^procEventType)&remaining != 0 {
				log.Fatalf(ctx,
					"rangefeed processor attempted to reschedule event type %s that was not present in original event set %s",
					procEventType, remaining)
			}
		}

		if e&Stopped != 0 {
			if remaining != 0 {
				log.VWarningf(ctx, 5,
					"rangefeed processor %d didn't process all events on close", entry.id)
			}
			// We'll keep Stopped state to avoid calling stopped processor again
			// on scheduler shutdown.
			ss.Lock()
			ss.status[entry.id] = Stopped
			ss.Unlock()
			continue
		}

		ss.Lock()
		pendingStatus, ok := ss.status[entry.id]
		if !ok {
			ss.Unlock()
			continue
		}
		newStatus := pendingStatus | remaining
		if newStatus == Queued {
			// If no events arrived, get rid of id.
			delete(ss.status, entry.id)
		} else {
			// Since more events arrived during processing, reschedule.
			ss.queue.pushBack(queueEntry{id: entry.id, startTime: ss.maybeEnqueueStartTime()})
			// If remaining work was returned and not already planned, then update
			// pending status to reflect that.
			if newStatus != pendingStatus {
				ss.status[entry.id] = newStatus
			}
			ss.metrics.QueueSize.Inc(1)
		}
		ss.Unlock()
	}
}

// quiesce asks shard workers to terminate and stops accepting new work.
func (ss *schedulerShard) quiesce() {
	ss.Lock()
	ss.quiescing = true
	ss.Unlock()
	ss.cond.Broadcast()
}

// stop synchronously stops processors by submitting and processing a stopped
// event and any other pending work. quiesce() must be called first to stop
// shard workers.
func (ss *schedulerShard) stop() {
	ss.Lock()
	defer ss.Unlock()
	for id, p := range ss.procs {
		pending := ss.status[id]
		// Ignore processors that already processed their stopped event.
		if pending == Stopped {
			continue
		}
		// Add stopped event on top of what was pending and remove queued.
		pending = (^Queued & pending) | Stopped
		ss.Unlock()
		p(pending)
		ss.Lock()
	}
}

var schedulerBatchPool = sync.Pool{
	New: func() interface{} {
		return new(SchedulerBatch)
	},
}

// SchedulerBatch is a batch of IDs to enqueue. It enables efficient per-shard
// enqueueing, by pre-sharding the IDs and only locking a single shard at a time
// while bulk-enqueueing.
type SchedulerBatch struct {
	ids         [][]int64 // by shard
	priorityIDs map[int64]bool
}

func newSchedulerBatch(numShards int, priorityIDs *syncutil.Set[int64]) *SchedulerBatch {
	b := schedulerBatchPool.Get().(*SchedulerBatch)
	if cap(b.ids) >= numShards {
		b.ids = b.ids[:numShards]
	} else {
		b.ids = make([][]int64, numShards)
	}
	if b.priorityIDs == nil {
		b.priorityIDs = make(map[int64]bool, 8) // expect few ranges, if any
	}
	// Cache the priority range IDs in an owned map, since we expect this to be
	// very small or empty and we do a lookup for every Add() call.
	priorityIDs.Range(func(id int64) bool {
		b.priorityIDs[id] = true
		return true
	})
	return b
}

// Add adds a processor ID to the batch.
func (b *SchedulerBatch) Add(id int64) {
	shardIdx := shardIndex(id, len(b.ids), b.priorityIDs[id])
	b.ids[shardIdx] = append(b.ids[shardIdx], id)
}

// Close returns the batch to the pool for reuse.
func (b *SchedulerBatch) Close() {
	for i := range b.ids {
		b.ids[i] = b.ids[i][:0]
	}
	for id := range b.priorityIDs {
		delete(b.priorityIDs, id)
	}
	schedulerBatchPool.Put(b)
}

// ClientScheduler is a wrapper on top of scheduler that could be passed to a
// processor to be able to register itself with a pre-configured ID, enqueue
// events and terminate as needed.
type ClientScheduler struct {
	id int64
	s  *Scheduler
}

// NewClientScheduler creates an instance of ClientScheduler for next available
// id.
// It is safe to use it as value as it is immutable and delegates all work to
// underlying scheduler.
func (s *Scheduler) NewClientScheduler() ClientScheduler {
	return ClientScheduler{
		s:  s,
		id: s.nextID.Add(1),
	}
}

// ID returns underlying callback id used to schedule work.
func (cs *ClientScheduler) ID() int64 {
	return cs.id
}

// Register registers processing callback in scheduler. Error is returned if
// callback was already registered for this ClientScheduler or if scheduler is
// already quiescing.
func (cs *ClientScheduler) Register(cb Callback, priority bool) error {
	return cs.s.register(cs.id, cb, priority)
}

// Enqueue schedules callback execution for event.
func (cs *ClientScheduler) Enqueue(event processorEventType) {
	cs.s.enqueue(cs.id, event)
}

// StopProcessor instructs processor to stop gracefully by sending it Stopped event.
// Once stop is called all subsequent Schedule calls will return error.
func (cs *ClientScheduler) StopProcessor() {
	cs.s.stopProcessor(cs.id)
}

// Unregister will remove callback associated with this processor. No stopped
// event will be scheduled. See Scheduler.Unregister for details.
func (cs *ClientScheduler) Unregister() {
	cs.s.unregister(cs.id)
}

// Number of queue elements allocated at once to amortize queue allocations.
const idQueueChunkSize = 8000

type queueEntry struct {
	id        int64
	startTime int64
}

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type idQueueChunk struct {
	data      [idQueueChunkSize]queueEntry
	nextChunk *idQueueChunk
}

var sharedIDQueueChunkSyncPool = sync.Pool{
	New: func() interface{} {
		return new(idQueueChunk)
	},
}

func getPooledIDQueueChunk() *idQueueChunk {
	return sharedIDQueueChunkSyncPool.Get().(*idQueueChunk)
}

func putPooledIDQueueChunk(e *idQueueChunk) {
	// Don't need to cleanup chunk as it is an array of values.
	e.nextChunk = nil
	sharedIDQueueChunkSyncPool.Put(e)
}

// idQueue stores pending processor ID's. Internally data is stored in
// idQueueChunkSize sized arrays that are added as needed and discarded once
// reader and writers finish working with it. Since we only have a single
// scheduler per store, we don't use a pool as only reuse could happen within
// the same queue and in that case we can just increase chunk size.
type idQueue struct {
	first, last *idQueueChunk
	read, write int
	size        int
}

func newIDQueue() *idQueue {
	chunk := getPooledIDQueueChunk()
	return &idQueue{
		first: chunk,
		last:  chunk,
		read:  0,
		size:  0,
	}
}

func (q *idQueue) pushBack(id queueEntry) {
	if q.write == idQueueChunkSize {
		nexChunk := getPooledIDQueueChunk()
		q.last.nextChunk = nexChunk
		q.last = nexChunk
		q.write = 0
	}
	q.last.data[q.write] = id
	q.write++
	q.size++
}

func (q *idQueue) popFront() (queueEntry, bool) {
	if q.size == 0 {
		return queueEntry{}, false
	}
	if q.read == idQueueChunkSize {
		removed := q.first
		q.first = q.first.nextChunk
		putPooledIDQueueChunk(removed)
		q.read = 0
	}
	res := q.first.data[q.read]
	q.read++
	q.size--
	return res, true
}

func (q *idQueue) Len() int {
	return q.size
}
