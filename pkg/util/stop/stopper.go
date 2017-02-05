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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package stop

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ErrThrottled is returned from RunLimitedAsyncTask in the event that there
// is no more capacity for async tasks, as limited by the semaphore.
var ErrThrottled = errors.New("throttled on async limiting semaphore")

var errUnavailable = &roachpb.NodeUnavailableError{}

func register(s *Stopper) {
	trackedStoppers.Lock()
	trackedStoppers.stoppers = append(trackedStoppers.stoppers, s)
	trackedStoppers.Unlock()
}

func unregister(s *Stopper) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	sl := trackedStoppers.stoppers
	for i, tracked := range sl {
		if tracked == s {
			trackedStoppers.stoppers = sl[:i+copy(sl[i:], sl[i+1:])]
			return
		}
	}
	panic("attempt to unregister untracked stopper")
}

var trackedStoppers struct {
	syncutil.Mutex
	stoppers []*Stopper
}

func handleDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	for _, s := range trackedStoppers.stoppers {
		s.mu.Lock()
		fmt.Fprintf(w, "%p: %d tasks\n%s", s, s.mu.numTasks, s.runningTasksLocked())
		s.mu.Unlock()
	}
}

func init() {
	http.Handle("/debug/stopper", http.HandlerFunc(handleDebug))
}

// Closer is an interface for objects to attach to the stopper to
// be closed once the stopper completes.
type Closer interface {
	Close()
}

// CloserFn is type that allows any function to be a Closer.
type CloserFn func()

// Close implements the Closer interface.
func (f CloserFn) Close() {
	f()
}

type taskKey struct {
	file string
	line int
}

func (k taskKey) String() string {
	return fmt.Sprintf("%s:%d", k.file, k.line)
}

// A Stopper provides a channel-based mechanism to stop an arbitrary
// array of workers. Each worker is registered with the stopper via
// the RunWorker() method. The system further allows execution of functions
// through RunTask() and RunAsyncTask().
//
// Stopping occurs in two phases: the first is the request to stop, which moves
// the stopper into a quiescing phase. While quiescing, calls to RunTask() &
// RunAsyncTask() don't execute the function passed in and return errUnavailable.
// When all outstanding tasks have been completed, the stopper
// closes its stopper channel, which signals all live workers that it's safe to
// shut down. When all workers have shutdown, the stopper is complete.
//
// An arbitrary list of objects implementing the Closer interface may
// be added to the stopper via AddCloser(), to be closed after the
// stopper has stopped.
type Stopper struct {
	quiescer   chan struct{}     // Closed when quiescing
	stopper    chan struct{}     // Closed when stopping
	stopped    chan struct{}     // Closed when stopped completely
	onPanic    func(interface{}) // called with recover() on panic on any goroutine
	trackTasks bool              // Should task call sites be tracked
	stop       sync.WaitGroup    // Incremented for outstanding workers
	mu         struct {
		syncutil.Mutex
		quiesce   *sync.Cond // Conditional variable to wait for outstanding tasks
		quiescing bool       // true when Stop() has been called
		numTasks  int        // number of outstanding tasks
		tasks     map[taskKey]int
		closers   []Closer
		cancels   []func()
	}
}

// An Option can be passed to NewStopper.
type Option interface {
	apply(*Stopper)
}

type optionPanicHandler func(interface{})

func (oph optionPanicHandler) apply(stopper *Stopper) {
	stopper.onPanic = oph
}

// OnPanic is an option which lets the Stopper recover from all panics using
// the provided panic handler.
//
// When Stop() is invoked during stack unwinding, OnPanic is also invoked, but
// Stop() may not have carried out its duties.
func OnPanic(handler func(interface{})) Option {
	return optionPanicHandler(handler)
}

type optionTrackTasks bool

func (ott optionTrackTasks) apply(stopper *Stopper) {
	stopper.trackTasks = bool(ott)
}

// TrackTasks is an option which allows tracking of tasks to be disabled.
func TrackTasks(enabled bool) Option {
	return optionTrackTasks(enabled)
}

// NewStopper returns an instance of Stopper.
func NewStopper(options ...Option) *Stopper {
	s := &Stopper{
		quiescer:   make(chan struct{}),
		stopper:    make(chan struct{}),
		stopped:    make(chan struct{}),
		trackTasks: true,
	}

	s.mu.tasks = map[taskKey]int{}

	for _, opt := range options {
		opt.apply(s)
	}

	s.mu.quiesce = sync.NewCond(&s.mu)
	register(s)
	return s
}

// Recover is used internally by Stopper to provide a hook for recovery of
// panics on goroutines started by the Stopper. It can also be invoked
// explicitly (via "defer s.Recover()") on goroutines that are created outside
// of Stopper.
func (s *Stopper) Recover() {
	if r := recover(); r != nil {
		if s.onPanic != nil {
			s.onPanic(r)
			return
		}
		panic(r)
	}
}

// RunWorker runs the supplied function as a "worker" to be stopped
// by the stopper. The function <f> is run in a goroutine.
func (s *Stopper) RunWorker(f func()) {
	s.stop.Add(1)
	go func() {
		defer s.Recover()
		defer s.stop.Done()
		f()
	}()
}

// AddCloser adds an object to close after the stopper has been stopped.
func (s *Stopper) AddCloser(c Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.closers = append(s.mu.closers, c)
}

// RunTask adds one to the count of tasks left to quiesce in the system. Any
// worker which is a "first mover" when starting tasks must call this method
// before starting work on a new task. First movers include
// goroutines launched to do periodic work and the kv/db.go gateway which
// accepts external client requests.
//
// Returns an error to indicate that the system is currently quiescing and
// function f was not called.
func (s *Stopper) RunTask(f func()) error {
	key := taskKey{"???", 1}
	if s.trackTasks {
		key.file, key.line, _ = caller.Lookup(1)
	}
	if !s.runPrelude(key) {
		return errUnavailable
	}
	// Call f.
	defer s.Recover()
	defer s.runPostlude(key)
	f()
	return nil
}

// RunTaskWithErr adds one to the count of tasks left to quiesce in the system.
// Any worker which is a "first mover" when starting tasks must call this method
// before starting work on a new task. First movers include goroutines launched
// to do periodic work and the kv/db.go gateway which accepts external client
// requests.
//
// If the system is currently quiescing and function f was not called, returns
// an error indicating this condition. Otherwise, returns whatever f returns.
func (s *Stopper) RunTaskWithErr(f func() error) error {
	key := taskKey{"???", 1}
	if s.trackTasks {
		key.file, key.line, _ = caller.Lookup(1)
	}
	if !s.runPrelude(key) {
		return errUnavailable
	}
	// Call f.
	defer s.Recover()
	defer s.runPostlude(key)
	return f()
}

// RunAsyncTask runs function f in a goroutine. It returns an error when the
// Stopper is quiescing, in which case the function is not executed.
func (s *Stopper) RunAsyncTask(ctx context.Context, f func(context.Context)) error {
	key := taskKey{"???", 1}
	if s.trackTasks {
		key.file, key.line, _ = caller.Lookup(1)
	}
	if !s.runPrelude(key) {
		return errUnavailable
	}

	ctx, span := tracing.ForkCtxSpan(ctx, key.String())

	// Call f.
	go func() {
		defer s.Recover()
		defer s.runPostlude(key)
		defer tracing.FinishSpan(span)
		f(ctx)
	}()
	return nil
}

// RunLimitedAsyncTask runs function f in a goroutine, using the given
// channel as a semaphore to limit the number of tasks that are run
// concurrently to the channel's capacity. If wait is true, blocks
// until the semaphore is available in order to push back on callers
// that may be trying to create many tasks. If wait is false, returns
// immediately with an error if the semaphore is not
// available. Returns an error if the Stopper is quiescing, in which
// case the function is not executed.
func (s *Stopper) RunLimitedAsyncTask(
	ctx context.Context, sem chan struct{}, wait bool, f func(context.Context),
) error {
	key := taskKey{"???", 1}
	if s.trackTasks {
		key.file, key.line, _ = caller.Lookup(1)
	}

	// Wait for permission to run from the semaphore.
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ShouldQuiesce():
		return errUnavailable
	default:
		if !wait {
			return ErrThrottled
		}
		log.Infof(context.TODO(), "stopper throttling task from %s due to semaphore", key)
		// Retry the select without the default.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ShouldQuiesce():
			return errUnavailable
		}
	}

	// Check for canceled context: it's possible to get the semaphore even
	// if the context is canceled.
	select {
	case <-ctx.Done():
		<-sem
		return ctx.Err()
	default:
	}

	if !s.runPrelude(key) {
		<-sem
		return errUnavailable
	}

	ctx, span := tracing.ForkCtxSpan(ctx, key.String())

	go func() {
		defer s.Recover()
		defer s.runPostlude(key)
		defer func() { <-sem }()
		defer tracing.FinishSpan(span)
		f(ctx)
	}()
	return nil
}

func (s *Stopper) runPrelude(key taskKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.quiescing {
		return false
	}
	s.mu.numTasks++
	s.mu.tasks[key]++
	return true
}

func (s *Stopper) runPostlude(key taskKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.numTasks--
	s.mu.tasks[key]--
	s.mu.quiesce.Broadcast()
}

// NumTasks returns the number of active tasks.
func (s *Stopper) NumTasks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numTasks
}

// A TaskMap is returned by RunningTasks().
type TaskMap map[string]int

// String implements fmt.Stringer and returns a sorted multi-line listing of
// the TaskMap.
func (tm TaskMap) String() string {
	var lines []string
	for location, num := range tm {
		lines = append(lines, fmt.Sprintf("%-6d %s", num, location))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(lines)))
	return strings.Join(lines, "\n")
}

// RunningTasks returns a map containing the count of running tasks keyed by
// call site.
func (s *Stopper) RunningTasks() TaskMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runningTasksLocked()
}

func (s *Stopper) runningTasksLocked() TaskMap {
	m := map[string]int{}
	for k := range s.mu.tasks {
		if s.mu.tasks[k] == 0 {
			continue
		}
		m[k.String()] = s.mu.tasks[k]
	}
	return m
}

// Stop signals all live workers to stop and then waits for each to
// confirm it has stopped.
func (s *Stopper) Stop() {
	defer s.Recover()
	defer unregister(s)

	if log.V(1) {
		file, line, _ := caller.Lookup(1)
		log.Infof(context.TODO(),
			"stop has been called from %s:%d, stopping or quiescing all running tasks", file, line)
	}
	// Don't bother doing stuff cleanly if we're panicking, that would likely
	// block. Instead, best effort only. This cleans up the stack traces,
	// avoids stalls and helps some tests in `./cli` finish cleanly (where
	// panics happen on purpose).
	if r := recover(); r != nil {
		go s.Quiesce()
		close(s.stopper)
		close(s.stopped)
		s.mu.Lock()
		for _, c := range s.mu.closers {
			go c.Close()
		}
		s.mu.Unlock()
		panic(r)
	}

	s.Quiesce()
	close(s.stopper)
	s.stop.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.mu.closers {
		c.Close()
	}
	close(s.stopped)
}

// ShouldQuiesce returns a channel which will be closed when Stop() has been
// invoked and outstanding tasks should begin to quiesce.
func (s *Stopper) ShouldQuiesce() <-chan struct{} {
	if s == nil {
		// A nil stopper will never signal ShouldQuiesce, but will also never panic.
		return nil
	}
	return s.quiescer
}

// ShouldStop returns a channel which will be closed when Stop() has been
// invoked and outstanding tasks have quiesced.
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

// Quiesce moves the stopper to state quiescing and waits until all
// tasks complete. This is used from Stop() and unittests.
func (s *Stopper) Quiesce() {
	defer s.Recover()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cancel := range s.mu.cancels {
		cancel()
	}
	if !s.mu.quiescing {
		s.mu.quiescing = true
		close(s.quiescer)
	}
	for s.mu.numTasks > 0 {
		log.Infof(context.TODO(), "quiescing; tasks left:\n%s", s.runningTasksLocked())
		// Unlock s.mu, wait for the signal, and lock s.mu.
		s.mu.quiesce.Wait()
	}
}

// WithCancel returns a child context which is cancelled when the Stopper
// begins to quiesce.
func (s *Stopper) WithCancel(ctx context.Context) context.Context {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.cancels = append(s.mu.cancels, cancel)
	return ctx
}
