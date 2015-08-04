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

package stop

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/util/caller"
)

// Closer is an interface for objects to attach to the stopper to
// be closed once the stopper completes.
type Closer interface {
	Close()
}

// A Stopper provides a channel-based mechanism to stop an arbitrary
// array of workers. Each worker is registered with the stopper via
// the RunWorker() method. The system further allows execution of functions
// through RunTask() and RunAsyncTask().
//
// Stopping occurs in two phases: the first is the request to stop, which moves
// the stopper into a draining phase. While draining, calls to RunTask() &
// RunAsyncTask() don't execute the function passed in and return false.
// When all outstanding tasks have been completed, the stopper
// closes its stopper channel, which signals all live workers that it's safe to
// shut down. When all workers have shutdown, the stopper is complete.
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
	tasks    map[string]int
	closers  []Closer
}

// NewStopper returns an instance of Stopper.
func NewStopper() *Stopper {
	s := &Stopper{
		stopper: make(chan struct{}),
		stopped: make(chan struct{}),
		tasks:   map[string]int{},
	}
	s.drain = sync.NewCond(&s.mu)
	return s
}

// RunWorker runs the supplied function as a "worker" to be stopped
// by the stopper. The function <f> is run in a goroutine.
func (s *Stopper) RunWorker(f func()) {
	s.stop.Add(1)
	go func() {
		defer s.stop.Done()
		f()
	}()
}

// AddCloser adds an object to close after the stopper has been stopped.
func (s *Stopper) AddCloser(c Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closers = append(s.closers, c)
}

// RunTask adds one to the count of tasks left to drain in the system. Any
// worker which is a "first mover" when starting tasks must call this method
// before starting work on a new task. First movers include
// goroutines launched to do periodic work and the kv/db.go gateway which
// accepts external client requests.
//
// Returns false to indicate that the system is currently draining and
// function f was not called.
func (s *Stopper) RunTask(f func()) bool {
	file, line, _ := caller.Lookup(1)
	taskKey := fmt.Sprintf("%s:%d", file, line)
	if !s.runPrelude(taskKey) {
		return false
	}
	// Call f.
	f()
	s.runPostlude(taskKey)
	return true
}

// RunAsyncTask runs function f in a goroutine. It returns false when the
// Stopper is draining and the function is not executed.
func (s *Stopper) RunAsyncTask(f func()) bool {
	file, line, _ := caller.Lookup(1)
	taskKey := fmt.Sprintf("%s:%d", file, line)
	if !s.runPrelude(taskKey) {
		return false
	}
	// Call f.
	go func() {
		f()
		s.runPostlude(taskKey)
	}()
	return true
}

func (s *Stopper) runPrelude(taskKey string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.numTasks++
	s.tasks[taskKey]++
	return true
}

func (s *Stopper) runPostlude(taskKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numTasks--
	s.tasks[taskKey]--
	s.drain.Broadcast()
}

// NumTasks returns the number of active tasks.
func (s *Stopper) NumTasks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numTasks
}

// A TaskMap is returned by RunningTasks().
type TaskMap map[string]int

// String implements fmt.Stringer and returns a sorted multi-line listing of
// the TaskMap.
func (tm TaskMap) String() string {
	var totalNum int
	var lines []string
	for location, num := range tm {
		lines = append(lines, fmt.Sprintf("%-6d %s", num, location))
		totalNum += num
	}
	sort.Sort(sort.Reverse(sort.StringSlice(lines)))
	return strings.Join(lines, "\n")
}

// RunningTasks returns a map containing the count of running tasks keyed by
// callsite.
func (s *Stopper) RunningTasks() TaskMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runningTasksLocked()
}

func (s *Stopper) runningTasksLocked() TaskMap {
	m := map[string]int{}
	for k := range s.tasks {
		if s.tasks[k] == 0 {
			continue
		}
		m[k] = s.tasks[k]
	}
	return m
}

// Stop signals all live workers to stop and then waits for each to
// confirm it has stopped.
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
// invoked and outstanding tasks have drained.
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

// Quiesce moves the stopper to state draining and waits until all
// tasks complete. This is used from Stop() and unittests.
func (s *Stopper) Quiesce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
	for s.numTasks > 0 {
		log.Print("draining; tasks left:\n", s.runningTasksLocked())
		// Unlock s.mu, wait for the signal, and lock s.mu.
		s.drain.Wait()
	}
}
