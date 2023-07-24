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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	Queued int = 1 << 0
	Stopped int = 1 << 1
)

type Scheduler[E int, ID int64] struct {
	mu struct {
		syncutil.Mutex
		status map[ID]E
		queue  ring.Buffer[ID]
		stopped bool
	}
	cond *sync.Cond
}

func NewScheduler[E int, ID int64]() *Scheduler[E, ID] {
	s := &Scheduler[E, ID]{}
	s.mu.status = make(map[ID]E, 100)
	s.mu.queue = ring.MakeBuffer(make([]ID, 100))
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Enqueue
// Note that Different event types would be squashed into a single notification.
func (s *Scheduler[E, ID]) Enqueue(id ID, evt E) {
	if evt & E(Queued) != 0 {
		panic("can't use queued status for custom statuses")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := s.mu.status[id]
	if pending == 0 {
		// Enqueue if consumer was idle.
		s.mu.queue.AddLast(id)
	}
	update := pending | evt | E(Queued)
	if update != pending {
		// Only update if event actually changed.
		s.mu.status[id] = update
	}
	if pending == 0 {
		// Wake up potential waiting workers.
		// We are allowed to do this under cond lock.
		s.cond.Signal()
	}
}

// ProcessEvents adds processor to the pool.
func (s *Scheduler[E, ID]) ProcessEvents(f func(event E, id ID) error) {
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

		e := s.mu.status[id]
		s.mu.status[id] = E(Queued)
		s.mu.Unlock()

		if err := f(E(Queued) ^ e, id); err != nil {
			if log.V(5) {
				log.Infof(context.Background(), "queue processing failed: %s", err)
			}
		}

		s.mu.Lock()
		if s.mu.status[id] == E(Queued) {
			// If no events arrived, get rid of id.
			delete(s.mu.status, id)
		} else {
			// Since more events arrived during processing, reschedule.
			s.mu.queue.AddLast(id)
		}
		s.mu.Unlock()
	}
}

// Stop will abort scheduler immediately.
func (s *Scheduler[E, ID]) Stop() {
	s.mu.Lock()
	s.mu.stopped = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Processing callback. If true, remove registration after process is done.
type Callback func(event int) (bool, error)

// CallbackScheduler manages lifecycle of callbacks and workers in the scheduler.
type CallbackScheduler struct {
	name    string
	workers int

	sched *Scheduler[int, int64]

	wg sync.WaitGroup
	mu struct {
		// TODO: consider sync.Map as write once read many
		syncutil.RWMutex
		fs        map[int64]Callback
		quiescing bool
	}
	drained chan interface{}
}

// TODO(oleg): maybe add ourselves to Closer-Stopper?
func NewCallbackScheduler(name string, sched *Scheduler[int, int64], workers int) *CallbackScheduler {
	r := &CallbackScheduler{
		name:    name,
		workers: workers,
		sched:   sched,
		drained: make(chan interface{}),
	}
	r.mu.fs = make(map[int64]Callback, 10)
	return r
}

func (r *CallbackScheduler) Start(stopper *stop.Stopper) {
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		workerID := i
		if err := stopper.RunAsyncTask(context.Background(), fmt.Sprintf("%s: worker %d", r.name, workerID),
			func(ctx context.Context) {
				log.VEventf(ctx, 3, "%s/%d scheduler worker started", r.name, workerID)
				defer r.wg.Done()
				r.sched.ProcessEvents(func(event int, id int64) error {
					return r.invoke(id, event)
				})
				log.VEventf(ctx, 3, "%s/%d scheduler worker finished", r.name, workerID)
			}); err != nil {
			// Rely on subsequent RunAsyncTask to trigger cleanup.
			break
		}
	}
	if err := stopper.RunAsyncTask(context.Background(),
		fmt.Sprintf("%s: terminate scheduler", r.name),
		func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			log.Infof(ctx, "%s scheduler quiescing", r.name)
			r.Close()
		}); err != nil {
		r.Close()
	}
}

func (r *CallbackScheduler) Enqueue(id int64, evt int)  {
	r.sched.Enqueue(id, evt)
}

func (r *CallbackScheduler) Register(id int64, f Callback) error {
	defer func() {
		// TODO: remove this logging
		log.VEventf(context.Background(), 3, "registered rangefeed callback for range r%d: %v", id, f)
	}()
	r.mu.Lock()
	if r.mu.quiescing {
		// Don't accept new registrations if quiesced.
		return errors.New("server stopping")
	}
	defer r.mu.Unlock()
	r.mu.fs[id] = f
	return nil
}

func (r *CallbackScheduler) invoke(id int64, ev int) error {
	r.mu.RLock()
	c := r.mu.fs[id]
	r.mu.RUnlock()
	if c == nil {
		log.Errorf(context.Background(), "discarding event for r%d, %d", id, ev)
		return nil
	}
	remove, err := c(ev)
	if remove {
		r.Unregister(id)
	}
	return err
}

func (r *CallbackScheduler) Unregister(id int64) {
	defer func() {
		// TODO: remove this logging
		log.VEventf(context.Background(), 3, "scheduler callback unregistered for r%d", id)
	}()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mu.fs[id]; !ok {
		return
	}
	delete(r.mu.fs, id)
	if r.mu.quiescing && len(r.mu.fs) == 0 {
		close(r.drained)
	}
}

func (r *CallbackScheduler) quiesce() {
	// On quiesce send stopped to all registered callbacks and wait for all registrations to drain.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.quiescing = true
	if len(r.mu.fs) > 0 {
		for k := range r.mu.fs {
			r.sched.Enqueue(k, Stopped)
		}
	} else {
		// If we don't need to wait any registrations, then signal drained immediately,
		// otherwise wait for all callbacks to report that they are stopped.
		select {
		case <-r.drained:
		default:
			close(r.drained)
		}
	}
}

func (r *CallbackScheduler) Close() {
	log.Infof(context.Background(), "quiescing registrations")
	r.quiesce()
	log.Infof(context.Background(), "waiting registrations to drain")
	<-r.drained
	log.Infof(context.Background(), "signaling scheduler to stop")
	r.sched.Stop()
	log.Infof(context.Background(), "waiting for scheduler workers to stop")
	r.wg.Wait()
}
