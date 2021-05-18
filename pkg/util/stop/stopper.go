// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stop

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func init() {
	leaktest.PrintLeakedStoppers = PrintLeakedStoppers
}

const asyncTaskNamePrefix = "[async] "

// ErrThrottled is returned from RunLimitedAsyncTask in the event that there
// is no more capacity for async tasks, as limited by the semaphore.
var ErrThrottled = errors.New("throttled on async limiting semaphore")

// ErrUnavailable indicates that the server is quiescing and is unable to
// process new work.
var ErrUnavailable = &roachpb.NodeUnavailableError{}

func register(s *Stopper) {
	trackedStoppers.Lock()
	trackedStoppers.stoppers = append(trackedStoppers.stoppers,
		stopperWithStack{s: s, createdAt: string(debug.Stack())})
	trackedStoppers.Unlock()
}

func unregister(s *Stopper) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	sl := trackedStoppers.stoppers
	for i, tracked := range sl {
		if tracked.s == s {
			trackedStoppers.stoppers = append(sl[:i], sl[i+1:]...)
			return
		}
	}
	panic("attempt to unregister untracked stopper")
}

type stopperWithStack struct {
	s         *Stopper
	createdAt string // stack from NewStopper()
}

var trackedStoppers struct {
	syncutil.Mutex
	stoppers []stopperWithStack
}

// HandleDebug responds with the list of stopper tasks actively running.
func HandleDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	for _, ss := range trackedStoppers.stoppers {
		s := ss.s
		fmt.Fprintf(w, "%p: %d tasks", s, s.NumTasks())
	}
}

// PrintLeakedStoppers prints (using `t`) the creation site of each Stopper
// for which `.Stop()` has not yet been called.
func PrintLeakedStoppers(t testing.TB) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	for _, tracked := range trackedStoppers.stoppers {
		t.Errorf("leaked stopper, created at:\n%s", tracked.createdAt)
	}
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

// A Stopper provides control over the lifecycle of goroutines started
// through it via its RunTask, RunAsyncTask, and other similar methods.
//
// When Stop is invoked, the Stopper
//
// - it invokes Quiesce, which causes the Stopper to refuse new work
//   (that is, its Run* family of methods starts returning ErrUnavailable),
//   closes the channel returned by ShouldQuiesce, and blocks until
//   until no more tasks are tracked, then
// - it runs all of the methods supplied to AddCloser, then
// - closes the IsStopped channel.
//
// When ErrUnavailable is returned from a task, the caller needs
// to handle it appropriately by terminating any work that it had
// hoped to defer to the task (which is guaranteed to never have been
// invoked). A simple example of this can be seen in the below snippet:
//
//     var wg sync.WaitGroup
//     wg.Add(1)
//     if err := s.RunAsyncTask("foo", func(ctx context.Context) {
//       defer wg.Done()
//     }); err != nil {
//       // Task never ran.
//       wg.Done()
//     }
//
// To ensure that tasks that do get started are sensitive to Quiesce,
// they need to observe the ShouldQuiesce channel similar to how they
// are expected to observe context cancellation:
//
//     func x() {
//       select {
//       case <-s.ShouldQuiesce:
//         return
//       case <-ctx.Done():
//         return
//       case <-someChan:
//         // Do work.
//       }
//     }
//
// TODO(tbg): many improvements here are possible:
// - propagate quiescing via context cancellation
// - better API around refused tasks
// - all the other things mentioned in:
//     https://github.com/cockroachdb/cockroach/issues/58164
type Stopper struct {
	quiescer chan struct{}     // Closed when quiescing
	stopped  chan struct{}     // Closed when stopped completely
	onPanic  func(interface{}) // called with recover() on panic on any goroutine

	mu struct {
		syncutil.RWMutex
		// _numTasks is the number of active tasks. It is incremented atomically via
		// addTask() under the read lock for task acquisition. We need the read lock
		// to ensure task creation is prohibited atomically with the quiescing or
		// stopping bools set below. When simply reading or decrementing the number
		// of tasks, the lock is not necessary.
		_numTasks int32
		// quiescing and stopping are set in Quiesce and Stop (which calls
		// Quiesce). When either is set, no new tasks are allowed and closers
		// should execute immediately.
		quiescing, stopping bool
		closers             []Closer

		// idAlloc is incremented atomically under the read lock when adding a
		// context to be canceled.
		idAlloc  int64 // allocates index into qCancels
		qCancels sync.Map
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

// NewStopper returns an instance of Stopper.
func NewStopper(options ...Option) *Stopper {
	s := &Stopper{
		quiescer: make(chan struct{}),
		stopped:  make(chan struct{}),
	}

	for _, opt := range options {
		opt.apply(s)
	}

	register(s)
	return s
}

// Recover is used internally by Stopper to provide a hook for recovery of
// panics on goroutines started by the Stopper. It can also be invoked
// explicitly (via "defer s.Recover()") on goroutines that are created outside
// of Stopper.
func (s *Stopper) Recover(ctx context.Context) {
	if r := recover(); r != nil {
		if s.onPanic != nil {
			s.onPanic(r)
			return
		}
		if sv := settings.TODO(); sv != nil {
			logcrash.ReportPanic(ctx, sv, r, 1)
		}
		panic(r)
	}
}

func (s *Stopper) addTask(delta int32) (updated int32) {
	return atomic.AddInt32(&s.mu._numTasks, delta)
}

// refuseRLocked returns true if the stopper refuses new tasks. This
// means that the stopper is either quiescing or stopping.
func (s *Stopper) refuseRLocked() bool {
	return s.mu.stopping || s.mu.quiescing
}

// AddCloser adds an object to close after the stopper has been stopped.
//
// WARNING: memory resources acquired by this method will stay around for
// the lifetime of the Stopper. Use with care to avoid leaking memory.
//
// A closer that is added after Stop has already been called will be
// called immediately.
func (s *Stopper) AddCloser(c Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refuseRLocked() {
		c.Close()
		return
	}
	s.mu.closers = append(s.mu.closers, c)
}

// WithCancelOnQuiesce returns a child context which is canceled when the
// returned cancel function is called or when the Stopper begins to quiesce,
// whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func (s *Stopper) WithCancelOnQuiesce(ctx context.Context) (context.Context, func()) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.refuseRLocked() {
		cancel()
		return ctx, func() {}
	}
	id := atomic.AddInt64(&s.mu.idAlloc, 1)
	s.mu.qCancels.Store(id, cancel)
	return ctx, func() {
		cancel()
		s.mu.qCancels.Delete(id)
	}
}

// RunTask adds one to the count of tasks left to quiesce in the system.
// Any worker which is a "first mover" when starting tasks must call this method
// before starting work on a new task. First movers include goroutines launched
// to do periodic work and the kv/db.go gateway which accepts external client
// requests.
//
// taskName is used as the "operation" field of the span opened for this task
// and is visible in traces. It's also part of reports printed by stoppers
// waiting to stop. The convention is
// <package name>.<struct name>: <succinct description of the task's action>
//
// Returns an error to indicate that the system is currently quiescing and
// function f was not called.
func (s *Stopper) RunTask(ctx context.Context, taskName string, f func(context.Context)) error {
	if !s.runPrelude() {
		return ErrUnavailable
	}

	// Call f.
	defer s.Recover(ctx)
	defer s.runPostlude()

	f(ctx)
	return nil
}

// RunTaskWithErr is like RunTask(), but takes in a callback that can return an
// error. The error is returned to the caller.
func (s *Stopper) RunTaskWithErr(
	ctx context.Context, taskName string, f func(context.Context) error,
) error {
	if !s.runPrelude() {
		return ErrUnavailable
	}

	// Call f.
	defer s.Recover(ctx)
	defer s.runPostlude()

	return f(ctx)
}

// RunAsyncTask is like RunTask, except the callback is run in a goroutine. The
// method doesn't block for the callback to finish execution.
func (s *Stopper) RunAsyncTask(
	ctx context.Context, taskName string, f func(context.Context),
) error {
	taskName = asyncTaskNamePrefix + taskName
	if !s.runPrelude() {
		return ErrUnavailable
	}

	ctx, span := tracing.ForkSpan(ctx, taskName)

	// Call f.
	go func() {
		defer s.Recover(ctx)
		defer s.runPostlude()
		defer span.Finish()

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
// available. It is the caller's responsibility to ensure that sem is
// closed when the stopper is quiesced. For quotapools which live for the
// lifetime of the stopper, it is generally best to register the sem with the
// stopper using AddCloser.
func (s *Stopper) RunLimitedAsyncTask(
	ctx context.Context, taskName string, sem *quotapool.IntPool, wait bool, f func(context.Context),
) (err error) {
	// Wait for permission to run from the semaphore.
	var alloc *quotapool.IntAlloc
	if wait {
		alloc, err = sem.Acquire(ctx, 1)
	} else {
		alloc, err = sem.TryAcquire(ctx, 1)
	}
	if errors.Is(err, quotapool.ErrNotEnoughQuota) {
		err = ErrThrottled
	} else if quotapool.HasErrClosed(err) {
		err = ErrUnavailable
	}
	if err != nil {
		return err
	}
	defer func() {
		// If the err is non-nil then we know that we did not start the async task
		// and thus we need to release the acquired quota. If it is nil then we
		// did start the task and it will release the quota.
		if err != nil {
			alloc.Release()
		}
	}()

	// Check for canceled context: it's possible to get the semaphore even
	// if the context is canceled.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !s.runPrelude() {
		return ErrUnavailable
	}

	ctx, span := tracing.ForkSpan(ctx, taskName)

	go func() {
		defer s.Recover(ctx)
		defer s.runPostlude()
		defer alloc.Release()
		defer span.Finish()

		f(ctx)
	}()
	return nil
}

func (s *Stopper) runPrelude() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.refuseRLocked() {
		return false
	}
	// NB: we run this under the read lock to ensure that `refuseRLocked()` cannot
	// change until the task is registered. If we didn't do this, we'd run the
	// risk of starting a task after a successful call to Stop().
	s.addTask(1)
	return true
}

func (s *Stopper) runPostlude() {
	s.addTask(-1)
}

// NumTasks returns the number of active tasks.
func (s *Stopper) NumTasks() int {
	return int(atomic.LoadInt32(&s.mu._numTasks))
}

// Stop signals all live workers to stop and then waits for each to
// confirm it has stopped.
//
// Stop is idempotent; concurrent calls will block on each other.
func (s *Stopper) Stop(ctx context.Context) {
	s.mu.Lock()
	stopCalled := s.mu.stopping
	s.mu.stopping = true
	s.mu.Unlock()

	if stopCalled {
		// Wait for the concurrent Stop() to complete.
		<-s.stopped
		return
	}

	defer func() {
		s.Recover(ctx)
		unregister(s)
		close(s.stopped)
	}()

	// Don't bother doing stuff cleanly if we're panicking, that would likely
	// block. Instead, best effort only. This cleans up the stack traces,
	// avoids stalls and helps some tests in `./cli` finish cleanly (where
	// panics happen on purpose).
	if r := recover(); r != nil {
		go s.Quiesce(ctx)
		s.mu.Lock()
		for _, c := range s.mu.closers {
			go c.Close()
		}
		s.mu.Unlock()
		panic(r)
	}

	s.Quiesce(ctx)

	// Run the closers without holding s.mu. There's no concern around new
	// closers being added; we've marked this stopper as `stopping` above, so
	// any attempts to do so will be refused.
	for _, c := range s.mu.closers {
		c.Close()
	}
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
func (s *Stopper) Quiesce(ctx context.Context) {
	defer time.AfterFunc(5*time.Second, func() {
		log.Infof(ctx, "quiescing...")
	}).Stop()
	defer s.Recover(ctx)

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mu.quiescing {
			s.mu.quiescing = true
			close(s.quiescer)
		}

		s.mu.qCancels.Range(func(k, v interface{}) (wantMore bool) {
			cancel := v.(func())
			cancel()
			s.mu.qCancels.Delete(k)
			return true
		})
	}()

	for s.NumTasks() > 0 {
		time.Sleep(5 * time.Millisecond)
	}
}
