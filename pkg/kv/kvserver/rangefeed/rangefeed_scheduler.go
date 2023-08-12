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
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	// Queued is special event type that can't be scheduled explicitly. Its
	// presence means that consumer is either pending or currently processing
	// other events.
	Queued  processorEvent = 1 << 0
	Stopped processorEvent = 1 << 1
)

// enqueueBulkMaxChunk is max number of event enqueued in one go while holding
// scheduler lock.
const enqueueBulkMaxChunk = 100

// Callback is a callback to perform work. If some of the work is left
// unprocessed, it is returned as a remaining event.
type Callback func(event processorEvent, events chunkSnapshot) (
	remaining processorEvent, consumedEvents int,
)

type ReplicaInfo struct {
	process   Callback
	events    *eventQueue
	capacity  int
	capWaiter *sync.Cond
}

type SConfig struct {
	Name          string
	Workers       int
	BulkChunkSize int
}

// Scheduler is a simple scheduler that allows work to be scheduler
// against number of consumers. Each consumer is represented by id and a
// callback.
// Work is enqueued in a form of event using consumer id.
// Consumer callback is then called by worker thread with all combined pending
// events.
// Each event is represented as a bit mask and multiple pending events could be
// ORed together before being delivered to consumer.
type Scheduler struct {
	SConfig

	mu struct {
		syncutil.Mutex
		// TODO(oleg): consider sync.Map as write once read many.
		fs     map[int64]ReplicaInfo
		status map[int64]processorEvent
		// TODO(oleg): would be nice to shrink this once unused.
		queue ring.Buffer[int64]
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
func NewScheduler(cfg SConfig) *Scheduler {
	if cfg.BulkChunkSize == 0 {
		cfg.BulkChunkSize = enqueueBulkMaxChunk
	}
	s := &Scheduler{
		SConfig:  cfg,
		wg:      sync.WaitGroup{},
		drained: make(chan interface{}),
	}
	s.mu.fs = make(map[int64]ReplicaInfo)
	s.mu.status = make(map[int64]processorEvent)
	s.mu.queue = ring.MakeBuffer(make([]int64, 100))
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Start scheduler workers.
func (s *Scheduler) Start(stopper *stop.Stopper) error {
	for i := 0; i < s.Workers; i++ {
		s.wg.Add(1)
		workerID := i
		if err := stopper.RunAsyncTask(context.Background(),
			fmt.Sprintf("%s: worker %d", s.Name, workerID),
			func(ctx context.Context) {
				log.VEventf(ctx, 3, "%s/%d scheduler worker started", s.Name, workerID)
				defer s.wg.Done()
				s.processEvents()
				log.VEventf(ctx, 3, "%s/%d scheduler worker finished", s.Name, workerID)
			}); err != nil {
			s.wg.Done()
			// Rely on subsequent RunAsyncTask to trigger cleanup.
			break
		}
	}
	if err := stopper.RunAsyncTask(context.Background(),
		fmt.Sprintf("%s: terminate scheduler", s.Name),
		func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			log.VEventf(ctx, 2, "%s scheduler quiescing", s.Name)
			_ = s.Close(time.Minute)
		}); err != nil {
		_ = s.Close(time.Minute)
		return err
	}
	return nil
}

// Register callback to be able to schedule work. Returns error if Scheduler
// is stopped or if provided id is already registered.
func (s *Scheduler) Register(id int64, f Callback) error {
	s.mu.Lock()
	if s.mu.quiescing {
		// Don't accept new registrations if quiesced.
		return errors.New("server stopping")
	}
	defer s.mu.Unlock()
	if _, ok := s.mu.fs[id]; ok {
		return errors.Newf("callback is already registered for id %d", id)
	}
	s.mu.fs[id] = ReplicaInfo{
		process:   f,
		events:    &eventQueue{},
		capacity:  1000,
		capWaiter: sync.NewCond(&s.mu),
	}
	return nil
}

// EnqueueEvent for existing callback. Returns error if callback was not
// registered for the id or if callback is stopping.
func (s *Scheduler) EnqueueEvent(ctx context.Context, id int64, evt *event, timeout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.mu.fs[id]
	if !ok {
		return errors.Errorf("unknown consumer %d", id)
	}
	if c.events.Len() >= c.capacity {
		timedOut := false
		if timeout > 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		for c.events.Len() >= c.capacity && !timedOut {
			// First check if we were stopped before going to sleep, otherwise we
			// might wait forever on discarded status.
			pending := s.mu.status[id]
			if pending&Stopped != 0 {
				return errors.Errorf("already stopped consumer %d", id)
			}
			if ctxutil.WhenDone(ctx, func(err error) {
				// TODO(oleg):
				// I think this is a race if done is called directly when context is
				// already cancelled, need to think through.
				timedOut = true
				c.capWaiter.Signal()
			}) {
				c.capWaiter.Wait()
			}
		}
		if timedOut {
			return kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER)
		}
	}

	// Recheck stopped status before putting into the queue.
	pending := s.mu.status[id]
	if pending&Stopped != 0 {
		return errors.Errorf("already stopped consumer %d", id)
	}
	c.events.Push(evt)

	if pending == 0 {
		// Enqueue for processing only if consumer was idle.
		s.mu.queue.AddLast(id)
	}
	update := pending | queueData | Queued
	if update != pending {
		// Only update if event actually changed.
		s.mu.status[id] = update
	}
	if pending == 0 {
		// Wake up potential waiting workers.
		// We are allowed to do this under cond lock.
		s.cond.Signal()
	}
	return nil
}

// Enqueue event for existing callback. Returns error if callback was not
// registered for the id or if callback is stopping.
func (s *Scheduler) Enqueue(id int64, evt processorEvent) error {
	if evt&Queued != 0 {
		panic("can't use queued status for custom statuses")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mu.fs[id]; !ok {
		return errors.Errorf("unknown consumer %d", id)
	}
	err, newWork := s.enqueueInternalLocked(id, evt)
	if newWork {
		// Wake up potential waiting workers.
		// We are allowed to do this under cond lock.
		s.cond.Signal()
	}
	return err
}

func (s *Scheduler) enqueueInternalLocked(id int64, evt processorEvent) (error, bool) {
	pending := s.mu.status[id]
	if pending&Stopped != 0 {
		return errors.Errorf("already stopped consumer %d", id), false
	}
	if pending == 0 {
		// Enqueue if consumer was idle.
		s.mu.queue.AddLast(id)
	}
	update := pending | evt | Queued
	if update != pending {
		// Only update if event actually changed.
		s.mu.status[id] = update
	}
	return nil, pending == 0
}

// EnqueueAll enqueues event for all existing non-stopped id's. Enqueueing is
// done in chunks to avoid holding lock for too long and interfering with other
// enqueue operations.
// If id is not known or already stopped it is ignored.
func (s *Scheduler) EnqueueAll(ids []int64, evt processorEvent) {
	if evt&Queued != 0 {
		panic("can't use queued status for custom statuses")
	}
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
			if _, ok := s.mu.fs[id]; ok {
				if _, newWork := s.enqueueInternalLocked(id, evt); newWork {
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

// processEvents is a main worker method of a scheduler pool. each one should
// be launched in separate goroutine and will loop until scheduler is stopped.
func (s *Scheduler) processEvents() {
	for {
		s.mu.Lock()
		for {
			if s.mu.stopped {
				s.mu.Unlock()
				return
			}
			if s.mu.queue.Len() > 0 {
				// Some work to do.
				break
			}
			s.cond.Wait()
		}
		id := s.mu.queue.GetFirst()
		s.mu.queue.RemoveFirst()

		c := s.mu.fs[id]
		e := s.mu.status[id]
		events := c.events.FrontChunk()
		// Keep queued status and preserve stopped to block any more events.
		s.mu.status[id] = Queued | (e & Stopped)
		s.mu.Unlock()

		remaining, consumed := c.process(Queued ^ e, events)

		if e&Stopped != 0 {
			if remaining != 0 {
				log.VWarningf(context.Background(), 5,
					"worker %s, consumer %d didn't process all events on close", s.Name, id)
			}
			s.mu.Lock()
			// Cleanup registration and status.
			delete(s.mu.fs, id)
			delete(s.mu.status, id)
			// If we are quiescing and removed last consumer then notify that work is
			// drained.
			if s.mu.quiescing && len(s.mu.fs) == 0 {
				close(s.drained)
			}
			s.mu.Unlock()
			continue
		}

		s.mu.Lock()
		pendingStatus := s.mu.status[id]
		newStatus := pendingStatus | remaining
		if consumed > 0 {
			c.events.RemoveHeadN(consumed)
			// If we didn't process all data, read range to the queue even if consumer
			// didn't ask.
			if c.events.Len() > 0 {
				newStatus |= queueData
			}
			// Wake up potential writers if we are in overflow state.
			c.capWaiter.Signal()
		}

		if newStatus == Queued {
			// If no events arrived, get rid of id.
			delete(s.mu.status, id)
		} else {
			// Since more events arrived during processing, reschedule.
			s.mu.queue.AddLast(id)
			// If remaining work was returned and not already planned, then update
			// pending status to reflect that.
			if newStatus != pendingStatus {
				s.mu.status[id] = newStatus
			}
		}
		s.mu.Unlock()
	}
}

// Stop and unregister a consumer. This function is not waiting for consumer
// to process stop event, but only enqueues stop request.
func (s *Scheduler) Stop(id int64) {
	// Enqueue stopped status, that would notify consumer and delete callback
	// after.
	_ = s.Enqueue(id, Stopped)
}

// Close implements Scheduler
func (s *Scheduler) Close(wait time.Duration) error {
	// Drain all consumers. After that terminate all workers.
	s.mu.Lock()
	if !s.mu.quiescing {
		// On first close attempt trigger termination of all unfinished callbacks,
		// we only need to do that once to avoid closing s.drained channel multiple
		// times.
		s.mu.quiescing = true
		if len(s.mu.fs) == 0 {
			// Workers will never wake up, we need to force drained status under the
			// lock ourselves.
			close(s.drained)
		} else {
			for id := range s.mu.fs {
				_, _ = s.enqueueInternalLocked(id, Stopped)
			}
		}
	}
	s.mu.Unlock()
	s.cond.Broadcast()

	// Now wait for all workers to drain.
	var err error
	select {
	case <-s.drained:
	case <-time.After(wait):
		err = errors.Errorf("failed to drain all consumers within %s", wait)
	}

	// Stop all worker threads.
	s.mu.Lock()
	s.mu.stopped = true
	s.mu.Unlock()
	s.cond.Broadcast()

	if err == nil {
		// Only wait for workers if they were not blocked by consumers. Otherwise
		// workgroup might never complete.
		s.wg.Wait()
	}
	return err
}

// ClientScheduler is a wrapper on top of scheduler that could be passed to a
// consumer to be able to register itself with a pre-configured ID, enqueue
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
func (cs *ClientScheduler) Schedule(event processorEvent) error {
	return cs.s.Enqueue(cs.id, event)
}

// ScheduleEvent schedules callback for event. Error is returned if client callback
// wasn't registered prior to this call.
func (cs *ClientScheduler) ScheduleEvent(ctx context.Context, e *event, timeout time.Duration) error {
	return cs.s.EnqueueEvent(ctx, cs.id, e, timeout)
}

// Stop posts stop event onto callback if it was previously registered. This
// will cause callback to be executed with Stopped event in due course and
// callback will be unregistered when processing is complete.
func (cs *ClientScheduler) Stop() {
	cs.s.Stop(cs.id)
}

const eventChunks = 100

type eventQueueChunk struct {
	// Valid contents are buf[rd:wr], read at buf[rd], write at buf[wr].
	buf    [eventChunks]*event
	rd, wr int
}

func (c *eventQueueChunk) PushBack(e *event) bool {
	if c.WriteCap() == 0 {
		return false
	}
	c.buf[c.wr] = e
	c.wr++
	return true
}

// func (c *eventQueueChunk) PopFront() (*event, bool) {
// 	if c.Len() == 0 {
// 		return nil, false
// 	}
// 	e := c.buf[c.rd]
// 	c.rd++
// 	return e, true
// }

func (c *eventQueueChunk) WriteCap() int {
	return len(c.buf) - c.wr
}

func (c *eventQueueChunk) Len() int {
	return c.wr - c.rd
}

// eventQueue is a chunked queue of range IDs. Instead of a separate list
// element for every range ID, it uses a eventQueueChunk to hold many range IDs,
// amortizing the allocation/GC cost. Using a chunk queue avoids any copying
// that would occur if a slice were used (the copying would occur on slice
// reallocation).
//
// The queue implements a FIFO queueing policy with no prioritization of some
// ranges over others.
type eventQueue struct {
	len    int
	chunks list.List
}

func (q *eventQueue) Push(e *event) {
	q.len++
	if q.chunks.Len() == 0 || q.back().WriteCap() == 0 {
		q.chunks.PushBack(&eventQueueChunk{})
	}
	if !q.back().PushBack(e) {
		panic(fmt.Sprintf(
			"unable to push rangeID to chunk: len=%d, cap=%d",
			q.back().Len(), q.back().WriteCap()))
	}
}

// func (q *eventQueue) PopFront() (*event, bool) {
// 	if q.len == 0 {
// 		return nil, false
// 	}
// 	q.len--
// 	frontElem := q.chunks.Front()
// 	front := frontElem.Value.(*eventQueueChunk)
// 	e, ok := front.PopFront()
// 	if !ok {
// 		panic("encountered empty chunk")
// 	}
// 	if front.Len() == 0 && front.WriteCap() == 0 {
// 		q.chunks.Remove(frontElem)
// 	}
// 	return e, true
// }

type chunkSnapshot struct {
	buf *[eventChunks]*event
	// Write snapshot and read pointer.
	rd, wr int
}

func (c *chunkSnapshot) Next() (*event, bool) {
	if c.rd == c.wr {
		return nil, false
	}
	e := c.buf[c.rd]
	c.rd++
	return e, true
}

func (q *eventQueue) FrontChunk() chunkSnapshot {
	if q.len == 0 {
		return chunkSnapshot{}
	}
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*eventQueueChunk)
	return chunkSnapshot{
		buf: &front.buf,
		rd:  front.rd,
		wr:  front.wr,
	}
}

func (q *eventQueue) RemoveHeadN(count int) {
	if q.len < count {
		panic("consumer read more data that provided in queue")
	}
	q.len -= count
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*eventQueueChunk)
	if front.Len() < count {
		panic("consumer read more data that provided in chunk")
	}
	front.rd+=count
	if front.Len() == 0 && front.WriteCap() == 0 {
		q.chunks.Remove(frontElem)
	}
}

func (q *eventQueue) Len() int {
	return q.len
}

func (q *eventQueue) back() *eventQueueChunk {
	return q.chunks.Back().Value.(*eventQueueChunk)
}
