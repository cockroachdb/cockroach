// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sched

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	Queued  int = 1 << 0
	Stopped int = 1 << 1
)

// enqueueBulkMaxChunk is max number of event enqueued in one go while holding
// scheduler lock.
const enqueueBulkMaxChunk = 100

// Callback is a callback to perform work. If some of the work is left
// unprocessed, it is returned as a remaining event.
type Callback func(event int) (remaining int)

type Config struct {
	Name          string
	Workers       int
	BulkChunkSize int
}

// PooledScheduler is a simple scheduler that allows work to be scheduler
// against number of consumers. Each consumer is represented by id and a
// callback.
// Work is enqueued in a form of event using consumer id.
// Consumer callback is then called by worker thread with all combined pending
// events.
// Each event is represented as a bit mask and multiple pending events could be
// ORed together before being delivered to consumer.
type PooledScheduler struct {
	Config

	mu struct {
		syncutil.Mutex
		// TODO(oleg): consider sync.Map as write once read many.
		fs     map[int64]Callback
		status map[int64]int
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
func NewScheduler(cfg Config) *PooledScheduler {
	if cfg.BulkChunkSize == 0 {
		cfg.BulkChunkSize = enqueueBulkMaxChunk
	}
	s := &PooledScheduler{
		Config:  cfg,
		wg:      sync.WaitGroup{},
		drained: make(chan interface{}),
	}
	s.mu.fs = make(map[int64]Callback)
	s.mu.status = make(map[int64]int)
	s.mu.queue = ring.MakeBuffer(make([]int64, 100))
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Start scheduler workers.
func (s *PooledScheduler) Start(stopper *stop.Stopper) error {
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
func (s *PooledScheduler) Register(id int64, f Callback) error {
	s.mu.Lock()
	if s.mu.quiescing {
		// Don't accept new registrations if quiesced.
		return errors.New("server stopping")
	}
	defer s.mu.Unlock()
	if _, ok := s.mu.fs[id]; ok {
		return errors.Newf("callback is already registered for id %d", id)
	}
	s.mu.fs[id] = f
	return nil
}

// Enqueue event for existing callback. Returns error if callback was not
// registered for the id or if callback is stopping.
func (s *PooledScheduler) Enqueue(id int64, evt int) error {
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

func (s *PooledScheduler) enqueueInternalLocked(id int64, evt int) (error, bool) {
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
func (s *PooledScheduler) EnqueueAll(ids []int64, evt int) {
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
func (s *PooledScheduler) processEvents() {
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

		cb := s.mu.fs[id]
		e := s.mu.status[id]
		// Keep queued status and preserve stopped to block any more events.
		s.mu.status[id] = Queued | (e & Stopped)
		s.mu.Unlock()

		remaining := cb(Queued ^ e)

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
func (s *PooledScheduler) Stop(id int64) {
	// Enqueue stopped status, that would notify consumer and delete callback
	// after.
	_ = s.Enqueue(id, Stopped)
}

// Close implements Scheduler
func (s *PooledScheduler) Close(wait time.Duration) error {
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
	s  *PooledScheduler
}

// NewClientScheduler creates an instance of ClientScheduler for specific id.
// It is safe to use it as value as it is immutable and delegates all work to
// underlying scheduler.
func NewClientScheduler(id int64, s *PooledScheduler) ClientScheduler {
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
func (cs *ClientScheduler) Schedule(event int) error {
	return cs.s.Enqueue(cs.id, event)
}

// Stop posts stop event onto callback if it was previously registered. This
// will cause callback to be executed with Stopped event in due course and
// callback will be unregistered when processing is complete.
func (cs *ClientScheduler) Stop() {
	cs.s.Stop(cs.id)
}
