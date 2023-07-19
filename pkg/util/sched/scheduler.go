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
	Queued  int = 1 << 0
	Stopped int = 1 << 1
)

// Callback is a callback to perform work. If some of the work is left
// unprocessed, it is returned as a remaining event.
type Callback func(event int) (remaining int)

type Scheduler interface {
	// Register callback to be able to schedule work.
	Register(id int64, f Callback) error
	// Enqueue event for existing callback.
	Enqueue(id int64, evt int) error
	// Stop and unregister a consumer.
	Stop(id int64)
}

type SchedConfig struct {
	Name    string
	Workers int
}

type PooledScheduler struct {
	SchedConfig

	mu struct {
		syncutil.Mutex
		// TODO(oleg): consider sync.Map as write once read many
		fs     map[int64]Callback
		status map[int64]int
		// TODO(oleg): need to be able to shrink the queue somehow.
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

func NewScheduler(cfg SchedConfig) *PooledScheduler {
	s := &PooledScheduler{
		SchedConfig: cfg,
		wg:          sync.WaitGroup{},
		drained:     make(chan interface{}),
	}
	s.mu.fs = make(map[int64]Callback)
	s.mu.status = make(map[int64]int)
	s.mu.queue = ring.MakeBuffer(make([]int64, 100))
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *PooledScheduler) Start(stopper *stop.Stopper) error {
	// TODO(oleg): maybe add sanity check here for already started scheduler.
	for i := 0; i < s.Workers; i++ {
		s.wg.Add(1)
		workerID := i
		if err := stopper.RunAsyncTask(context.Background(), fmt.Sprintf("%s: worker %d", s.Name, workerID),
			func(ctx context.Context) {
				log.VEventf(ctx, 3, "%s/%d scheduler worker started", s.Name, workerID)
				defer s.wg.Done()
				s.processEvents()
				log.VEventf(ctx, 3, "%s/%d scheduler worker finished", s.Name, workerID)
			}); err != nil {
			// Rely on subsequent RunAsyncTask to trigger cleanup.
			break
		}
	}
	if err := stopper.RunAsyncTask(context.Background(),
		fmt.Sprintf("%s: terminate scheduler", s.Name),
		func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			log.Infof(ctx, "%s scheduler quiescing", s.Name)
			_ = s.Close(time.Minute)
		}); err != nil {
		_ = s.Close(time.Minute)
	}
	return nil
}

func (s *PooledScheduler) Register(id int64, f Callback) error {
	defer func() {
		// TODO(oleg): remove this logging
		log.VEventf(context.Background(), 3, "registered callback with id %d: %v", id, f)
	}()
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

func (s *PooledScheduler) Enqueue(id int64, evt int) error {
	if evt&Queued != 0 {
		panic("can't use queued status for custom statuses")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mu.fs[id]; !ok {
		return errors.Errorf("unknown resource %d", id)
	}
	if s.enqueueInternalLocked(id, evt) {
		// Wake up potential waiting workers.
		// We are allowed to do this under cond lock.
		s.cond.Signal()
	}
	return nil
}

func (s *PooledScheduler) enqueueInternalLocked(id int64, evt int) bool {
	pending := s.mu.status[id]
	if pending == 0 {
		// Enqueue if consumer was idle.
		s.mu.queue.AddLast(id)
	}
	update := pending | evt | Queued
	if update != pending {
		// Only update if event actually changed.
		s.mu.status[id] = update
	}
	return pending == 0
}

// ProcessEvents adds processor to the pool.
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
			if log.V(5) {
				log.Infof(context.Background(), "worker waiting on queue condition")
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

// TODO(oleg): do we need this explicit method on this type, we can just use
// event directly since we plan to use wrapper inside client anyways.
func (s *PooledScheduler) Stop(id int64) {
	// Enqueue stopped status, that would notify consumer and delete callback
	// after.
	_ = s.Enqueue(id, Stopped)
}

func (s *PooledScheduler) Close(wait time.Duration) error {
	// Drain all consumers. After that terminate all workers.
	s.mu.Lock()
	s.mu.quiescing = true
	if len(s.mu.fs) == 0 {
		// Workers will never wake up, we need to force drained status under the
		// lock ourselves.
		close(s.drained)
	} else {
		for id := range s.mu.fs {
			s.enqueueInternalLocked(id, Stopped)
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
	s  Scheduler
}

func NewClientScheduler(id int64, s Scheduler) ClientScheduler {
	return ClientScheduler{
		id: id,
		s:  s,
	}
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
