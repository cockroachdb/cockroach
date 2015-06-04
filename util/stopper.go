// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"sync"
)

// Closer is an interface for objects to attach to the stopper to
// be closed once the stopper completes.
type Closer interface {
	Close()
}

// A Stopper provides a channel-based mechanism to stop an arbitrary
// array of workers. Each worker is registered with the stopper via
// the AddWorker() method. The system further tracks each task which
// is outstanding by calling StartTask() when a task is started and
// FinishTask() when completed.
//
// Stopping occurs in two phases: the first is the request to stop,
// which moves the stopper into a draining phase. While draining,
// calls to StartTask() return false, meaning the system is draining
// and new tasks should not be accepted. When all outstanding tasks
// have been completed via calls to FinishTask(), the stopper closes
// its stopper channel, which signals all live workers that it's safe
// to shut down. Once shutdown, each worker invokes SetStopped(). When
// all workers have shutdown, the stopper is complete.
//
// An arbitrary list of objects implementing the Closer interface may
// be added to the stopper via AddCloser(), to be closed after the
// stopper has stopped.
type Stopper struct {
	stopper  chan struct{}  // Closed when stopping
	stopped  chan struct{}  // Closed when stopped completely
	stop     sync.WaitGroup // Incremented for outstanding workers
	mu       sync.Mutex     // Protects the fields below
	drain    *sync.Cond     // Conditional variable to wait for outstanding tasks
	draining bool           // true when Stop() has been called
	numTasks int            // number of outstanding tasks
	closers  []Closer
}

// NewStopper returns an instance of Stopper.
func NewStopper() *Stopper {
	s := &Stopper{
		stopper: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	s.drain = sync.NewCond(&s.mu)
	return s
}

// RunWorker runs the supplied function as a "worker" to be stopped
// by the stopper. The function <f> is run in a goroutine.
func (s *Stopper) RunWorker(f func()) {
	s.AddWorker()
	go func() {
		defer s.SetStopped()
		f()
	}()
}

// AddWorker adds a worker to the stopper.
func (s *Stopper) AddWorker() {
	s.stop.Add(1)
}

// AddCloser adds an object to close after the stopper has been stopped.
func (s *Stopper) AddCloser(c Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closers = append(s.closers, c)
}

// StartTask adds one to the count of tasks left to drain in the
// system. Any worker which is a "first mover" when starting tasks
// must call this method before starting work on a new task and must
// subsequently invoke FinishTask() when the task is complete.
// First movers include goroutines launched to do periodic work and
// the kv/db.go gateway which accepts external client
// requests.
//
// Returns true if the task can be launched or false to indicate the
// system is currently draining and the task should be refused.
func (s *Stopper) StartTask() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.numTasks++
	return true
}

// FinishTask removes one from the count of tasks left to drain in the
// system. This function must be invoked for every call to StartTask().
func (s *Stopper) FinishTask() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numTasks--
	s.drain.Broadcast()
}

// NumTasks returns the number of active tasks.
func (s *Stopper) NumTasks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numTasks
}

// Stop signals all live workers to stop and then waits for each to
// confirm it has stopped (workers do this by calling SetStopped()).
func (s *Stopper) Stop() {
	s.Quiesce()
	close(s.stopper)
	s.stop.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.closers {
		c.Close()
	}
	close(s.stopped)
}

// ShouldStop returns a channel which will be closed when Stop() has been
// invoked and outstanding tasks have drained. SetStopped() should be called
// to confirm.
func (s *Stopper) ShouldStop() <-chan struct{} {
	if s == nil {
		// A nil stopper will never signal ShouldStop, but will also never panic.
		return nil
	}
	return s.stopper
}

// IsStopped returns a channel which will be closed after Stop() has
// been invoked to full completion, meaning all workers have completed
// and all closers have been closed.
func (s *Stopper) IsStopped() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.stopped
}

// SetStopped should be called after the ShouldStop() channel has
// been closed to confirm the worker has stopped.
func (s *Stopper) SetStopped() {
	if s != nil {
		s.stop.Done()
	}
}

// Quiesce moves the stopper to state draining and waits until all
// tasks complete. This is used from Stop() and unittests.
func (s *Stopper) Quiesce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
	for s.numTasks > 0 {
		// Unlock s.mu, wait for the signal, and lock s.mu.
		s.drain.Wait()
	}
}
