// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Scheduler is used by rangefeed processors to schedule work to a pool of
// workers instead of running individual goroutines per range. Store will run
// a single scheduler for all its rangefeeds.
// When processor is started it registers a callback with scheduler.
// After that processor can enqueue work for itself by telling scheduler what
// types of events it plans to process.
// Scheduler maintains a queue of processor ids that wish to do processing and
// notifies callbacks in order passing them union of event types that were
// enqueued since last notification.

// processorEventType is a mask for pending events for the processor. All event
// types that were enqueued between two callback invocations are coalesced into
// a single value.
type processorEventType int

const (
	// queued is an internal event type that indicate that there's already a
	// pending work for processor and it is already scheduled for execution.
	// When more events types come in, they should just be added to existing
	// pending value.
	queued processorEventType = 1 << 0
	// stopped is an event that indicates that there would be no more events
	// scheduled for the processor. once it is enqueued, all subsequent events
	// are rejected. processor should perform any cleanup when receiving this
	// event that it needs to perform within callback context.
	stopped processorEventType = 1 << 1
)

var eventNames = map[processorEventType]string{
	queued:  "Queued",
	stopped: "Stopped",
}

func (e processorEventType) String() string {
	var evts []string
	for m := queued; m <= stopped; m = m << 1 {
		if m&e != 0 {
			evts = append(evts, eventNames[m])
		}
	}
	return strings.Join(evts, " | ")
}

// enqueueBulkMaxChunk is max number of event enqueued in one go while holding
// scheduler lock.
const enqueueBulkMaxChunk = 100

// Callback is a callback to perform work set by processor. Event is a
// combination of all event types scheduled since last callback invocation.
// Once callback returns, event types considered to be processed. If a processor
// decided not to process everything, it can return remaining types which would
// instruct scheduler to re-enqueue processor. This mechanism allows processors
// to throttle processing if it has too much pending data to process in one go
// without blocking other processors.
type Callback func(event processorEventType) (remaining processorEventType)

// SchedulerConfig contains configurable scheduler parameters.
type SchedulerConfig struct {
	// Name a name to be given to async workers for observability purposes.
	Name string
	// Workers is the number of pool workers for scheduler to use.
	Workers int
	// BulkChunkSize is number of ids that would be enqueued in a single bulk
	// enqueue operation. Chunking is done to avoid holding locks for too long
	// as it will interfere with enqueue operations.
	BulkChunkSize int
}

// Scheduler is a simple scheduler that allows work to be scheduler
// against number of processors. Each processor is represented by unique id and
// a callback.
// Work is enqueued in a form of event type using processor id.
// Processors callback is then called by worker thread with all combined pending
// events.
// Each event is represented as a bit mask and multiple pending events could be
// ORed together before being delivered to processor.
type Scheduler struct {
	SchedulerConfig

	mu struct {
		syncutil.Mutex
		procs  map[int64]Callback
		status map[int64]processorEventType
		queue  *idQueue
		// No more new registrations allowed.
		quiescing bool
		// Once raised, workers will terminate as soon as current callback is
		// finished.
		stopped bool
	}
	cond *sync.Cond
	wg   sync.WaitGroup
	// Chan to wait for all workers in pool to terminate.
	drained chan interface{}
}

// NewScheduler will instantiate an idle scheduler based on provided config.
// Scheduler needs to be started to become operational.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.BulkChunkSize == 0 {
		cfg.BulkChunkSize = enqueueBulkMaxChunk
	}
	s := &Scheduler{
		SchedulerConfig: cfg,
		wg:              sync.WaitGroup{},
		drained:         make(chan interface{}),
	}
	s.mu.procs = make(map[int64]Callback)
	s.mu.status = make(map[int64]processorEventType)
	s.mu.queue = newIDQueue()
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Start scheduler workers.
func (s *Scheduler) Start(ctx context.Context, stopper *stop.Stopper) error {
	for i := 0; i < s.Workers; i++ {
		s.wg.Add(1)
		workerID := i
		if err := stopper.RunAsyncTask(ctx, fmt.Sprintf("%s: worker %d", s.Name, workerID),
			func(ctx context.Context) {
				log.VEventf(ctx, 3, "%s/%d scheduler worker started", s.Name, workerID)
				defer s.wg.Done()
				s.processEvents(ctx)
				log.VEventf(ctx, 3, "%s/%d scheduler worker finished", s.Name, workerID)
			}); err != nil {
			s.wg.Done()
			s.Close()
			return err
		}
	}
	if err := stopper.RunAsyncTask(context.Background(),
		fmt.Sprintf("%s: terminate scheduler", s.Name),
		func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			log.VEventf(ctx, 2, "%s scheduler quiescing", s.Name)
			s.Close()
		}); err != nil {
		s.Close()
		return err
	}
	return nil
}

// Register callback to be able to schedule work. Returns error if Scheduler
// is stopped or if provided id is already registered. Once unregistered this
// id should not be reused to avoid any uncertainty.
func (s *Scheduler) Register(id int64, f Callback) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.quiescing {
		// Don't accept new registrations if quiesced.
		return errors.New("server stopping")
	}
	if _, ok := s.mu.procs[id]; ok {
		return errors.Newf("callback is already registered for id %d", id)
	}
	s.mu.procs[id] = f
	return nil
}

// Enqueue event for existing callback. Returns error if callback was not
// registered for the id or if processor is stopping. Error doesn't guarantee
// that processor actually handled stopped event it may either be pending or
// processed.
func (s *Scheduler) Enqueue(id int64, evt processorEventType) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mu.procs[id]; !ok {
		return errors.Errorf("unknown processor %d", id)
	}
	newWork, err := s.enqueueInternalLocked(id, evt)
	if newWork {
		// Wake up potential waiting worker.
		// We are allowed to do this under cond lock.
		s.cond.Signal()
	}
	return err
}

func (s *Scheduler) enqueueInternalLocked(id int64, evt processorEventType) (bool, error) {
	pending := s.mu.status[id]
	if pending&stopped != 0 {
		return false, errors.Errorf("attempt to enqueue data for already stopped processor %d", id)
	}
	if pending == 0 {
		// Enqueue if processor was idle.
		s.mu.queue.pushBack(id)
	}
	update := pending | evt | queued
	if update != pending {
		// Only update if event actually changed.
		s.mu.status[id] = update
	}
	return pending == 0, nil
}

// EnqueueAll enqueues event for all existing non-stopped id's. Enqueueing is
// done in chunks to avoid holding lock for too long and interfering with other
// enqueue operations.
// If id is not known or already stopped it is ignored.
func (s *Scheduler) EnqueueAll(ids []int64, evt processorEventType) {
	pos := 0
	total := len(ids)
	scheduleChunk := func() (int, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		wake := 0
		last := pos + s.BulkChunkSize
		if last > total {
			last = total
		}
		for ; pos < last; pos++ {
			id := ids[pos]
			if _, ok := s.mu.procs[id]; ok {
				if newWork, _ := s.enqueueInternalLocked(id, evt); newWork {
					wake++
				}
			}
		}
		return wake, last == total
	}
	wake := 0
	for {
		added, done := scheduleChunk()
		wake += added
		if done {
			break
		}
	}
	// Wake up potential waiting workers. We wake all of them as we expect more
	// than total number of workers.
	if wake >= s.Workers {
		s.cond.Broadcast()
	} else {
		for ; wake > 0; wake-- {
			s.cond.Signal()
		}
	}
}

// Stop instructs processor to stop gracefully by sending it stopped event.
// Once stop is called all subsequent Schedule calls for this id will return
// error.
func (s *Scheduler) Stop(id int64) {
	_ = s.Enqueue(id, stopped)
}

// processEvents is a main worker method of a scheduler pool. each one should
// be launched in separate goroutine and will loop until scheduler is stopped.
func (s *Scheduler) processEvents(ctx context.Context) {
	for {
		var id int64
		s.mu.Lock()
		for {
			if s.mu.stopped {
				s.mu.Unlock()
				return
			}
			var ok bool
			if id, ok = s.mu.queue.popFront(); ok {
				break
			}
			s.cond.Wait()
		}

		cb := s.mu.procs[id]
		e := s.mu.status[id]
		// Keep queued status and preserve stopped to block any more events.
		s.mu.status[id] = queued | (e & stopped)
		s.mu.Unlock()

		remaining := cb(queued ^ e)

		if e&stopped != 0 {
			if remaining != 0 {
				log.VWarningf(ctx, 5,
					"worker %s, processor %d didn't process all events on close", s.Name, id)
			}
			// We don't remove queued and stopped status from processor to prevent
			// any further events from being enqueued.
			continue
		}

		s.mu.Lock()
		pendingStatus, ok := s.mu.status[id]
		if !ok {
			s.mu.Unlock()
			continue
		}
		newStatus := pendingStatus | remaining
		if newStatus == queued {
			// If no events arrived, get rid of id.
			delete(s.mu.status, id)
		} else {
			// Since more events arrived during processing, reschedule.
			s.mu.queue.pushBack(id)
			// If remaining work was returned and not already planned, then update
			// pending status to reflect that.
			if newStatus != pendingStatus {
				s.mu.status[id] = newStatus
			}
		}
		s.mu.Unlock()
	}
}

// Unregister a processor. This function is removing processor callback and
// status from scheduler. If processor is currently processing event it will
// finish processing.
// Processor won't receive stopped event if it wasn't explicitly sent.
// To make sure processor performs cleanup, it is easier to send it stopped
// event first and let it remove itself from registration during event handling.
// Any attempts to enqueue events for processor after this call will return an
// error.
func (s *Scheduler) Unregister(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.mu.procs, id)
	delete(s.mu.status, id)

	// If we are quiescing and removed last processor then notify that work is
	// drained.
	if s.mu.quiescing && len(s.mu.procs) == 0 {
		close(s.drained)
	}
}

// Close implements Scheduler
func (s *Scheduler) Close() {
	// Drain all processors by enqueueing stopped to all queues.
	s.mu.Lock()
	if !s.mu.quiescing {
		// On first close attempt trigger termination of all unfinished callbacks,
		// we only need to do that once to avoid closing s.drained channel multiple
		// times.
		s.mu.quiescing = true
		if len(s.mu.procs) == 0 {
			// Workers will never wake up, we need to force drained status under the
			// lock ourselves.
			close(s.drained)
		} else {
			for id := range s.mu.procs {
				_, _ = s.enqueueInternalLocked(id, stopped)
			}
		}
	}
	s.mu.Unlock()
	s.cond.Broadcast()

	// Now wait for all processors to drain and unregister.
	func() {
		for {
			select {
			case <-s.drained:
				return
			case <-time.After(10 * time.Second):
				s.mu.Lock()
				registered := len(s.mu.procs)
				s.mu.Unlock()
				log.Infof(context.Background(), "schedule shutdown: active processors count: %d", registered)
			}
		}
	}()

	// Stop all worker threads.
	s.mu.Lock()
	s.mu.stopped = true
	s.mu.Unlock()
	s.cond.Broadcast()

	s.wg.Wait()
}

// ClientScheduler is a wrapper on top of scheduler that could be passed to a
// processor to be able to register itself with a pre-configured ID, enqueue
// events and terminate as needed.
type ClientScheduler struct {
	id int64
	s  *Scheduler
}

// NewClientScheduler creates an instance of ClientScheduler for specific id.
// It is safe to use it as value as it is immutable and delegates all work to
// underlying scheduler.
func NewClientScheduler(id int64, s *Scheduler) ClientScheduler {
	return ClientScheduler{
		id: id,
		s:  s,
	}
}

// ID returns underlying callback id used to schedule work.
func (cs *ClientScheduler) ID() int64 {
	return cs.id
}

// Register registers processing callback in scheduler. Error is returned if
// callback was already registered for this ClientScheduler or if scheduler is
// already quiescing.
func (cs *ClientScheduler) Register(cb Callback) error {
	return cs.s.Register(cs.id, cb)
}

// Schedule schedules callback for event. Error is returned if client callback
// wasn't registered prior to this call.
func (cs *ClientScheduler) Schedule(event processorEventType) error {
	return cs.s.Enqueue(cs.id, event)
}

// Stop instructs processor to stop gracefully by sending it stopped event.
// Once stop is called all subsequent Schedule calls will return error.
func (cs *ClientScheduler) Stop() {
	cs.s.Stop(cs.id)
}

// Unregister will remove callback associated with this processor. No stopped
// event will be scheduled. See Scheduler.Unregister for details.
func (cs *ClientScheduler) Unregister() {
	cs.s.Unregister(cs.id)
}

// Number of queue elements allocated at once to amortize queue allocations.
const idQueueChunkSize = 1000

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type idQueueChunk struct {
	data [idQueueChunkSize]int64
	next *idQueueChunk
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
	e.next = nil
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

func (q *idQueue) pushBack(id int64) {
	if q.write == idQueueChunkSize {
		nexChunk := getPooledIDQueueChunk()
		q.last.next = nexChunk
		q.last = nexChunk
		q.write = 0
	}
	q.last.data[q.write] = id
	q.write++
	q.size++
}

func (q *idQueue) popFront() (int64, bool) {
	if q.size == 0 {
		return 0, false
	}
	if q.read == idQueueChunkSize {
		removed := q.first
		q.first = q.first.next
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
