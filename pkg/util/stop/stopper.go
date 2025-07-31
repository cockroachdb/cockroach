// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stop

import (
	"context"
	"fmt"
	"net/http"
	"runtime/trace"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/growstack"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func init() {
	leaktest.PrintLeakedStoppers = PrintLeakedStoppers
}

// ErrThrottled is returned from RunAsyncTaskEx in the event that there
// is no more capacity for async tasks, as limited by the semaphore.
var ErrThrottled = errors.New("throttled on async limiting semaphore")

// ErrUnavailable indicates that the server is quiescing and is unable to
// process new work.
var ErrUnavailable = &kvpb.NodeUnavailableError{}

func register(s *Stopper) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	trackedStoppers.stoppers = append(trackedStoppers.stoppers,
		stopperWithStack{s: s, createdAt: debugutil.Stack()})
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
	createdAt debugutil.SafeStack // stack from NewStopper()
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
		fmt.Fprintf(w, "%p: %d tasks\n", s, s.NumTasks())
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
// # When Stop is invoked, the Stopper
//
//   - it invokes Quiesce, which causes the Stopper to refuse new work
//     (that is, its Run* family of methods starts returning ErrUnavailable),
//     closes the channel returned by ShouldQuiesce, and blocks until
//     until no more tasks are tracked, then
//   - it runs all of the methods supplied to AddCloser, then
//   - closes the IsStopped channel.
//
// When ErrUnavailable is returned from a task, the caller needs
// to handle it appropriately by terminating any work that it had
// hoped to defer to the task (which is guaranteed to never have been
// invoked). A simple example of this can be seen in the below snippet:
//
//	var wg sync.WaitGroup
//	wg.Add(1)
//	if err := s.RunAsyncTask("foo", func(ctx context.Context) {
//	  defer wg.Done()
//	}); err != nil {
//	  // Task never ran.
//	  wg.Done()
//	}
//
// To ensure that tasks that do get started are sensitive to Quiesce,
// they need to observe the ShouldQuiesce channel similar to how they
// are expected to observe context cancellation:
//
//	func x() {
//	  select {
//	  case <-s.ShouldQuiesce:
//	    return
//	  case <-ctx.Done():
//	    return
//	  case <-someChan:
//	    // Do work.
//	  }
//	}
//
// TODO(tbg): many improvements here are possible:
//   - propagate quiescing via context cancellation
//   - better API around refused tasks
//   - all the other things mentioned in:
//     https://github.com/cockroachdb/cockroach/issues/58164
type Stopper struct {
	quiescer chan struct{}   // Closed when quiescing
	stopped  chan struct{}   // Closed when stopped completely
	tracer   *tracing.Tracer // tracer used to create spans for tasks

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
		quiescers           []func()

		// idAlloc is incremented atomically under the read lock when adding a
		// context to be canceled.
		idAlloc  int64 // allocates index into qCancels
		qCancels syncutil.Map[int64, context.CancelFunc]
	}
}

// An Option can be passed to NewStopper.
type Option interface {
	apply(*Stopper)
}

type withTracer struct {
	tr *tracing.Tracer
}

var _ Option = withTracer{}

func (o withTracer) apply(stopper *Stopper) {
	stopper.tracer = o.tr
}

// WithTracer is an option for NewStopper() supplying the Tracer to use for
// creating spans for tasks. Note that for tasks asking for a child span, the
// parent's tracer is used instead of this one.
func WithTracer(t *tracing.Tracer) Option {
	return withTracer{t}
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
	if s.tracer == nil {
		s.tracer = tracing.NewTracer()
	}

	register(s)
	return s
}

// recover reports the current panic, if any, and panics again.
//
// Note: this function _must_ be called with `defer s.recover()`, otherwise
// the panic recovery won't work.
func (s *Stopper) recover(ctx context.Context) {
	if r := recover(); r != nil {
		logcrash.ReportPanicWithGlobalSettings(ctx, r, 1)
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

// OnQuiesce is like AddCloser, but invokes on quiesce.
// If the Stopper is already quiescing, the function will be invoked
// synchronously.
func (s *Stopper) OnQuiesce(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refuseRLocked() {
		f()
		return
	}
	s.mu.quiescers = append(s.mu.quiescers, f)
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
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.refuseRLocked() {
		cancel()
		return ctx, func() {}
	}
	id := atomic.AddInt64(&s.mu.idAlloc, 1)
	s.mu.qCancels.Store(id, &cancel)
	return ctx, func() {
		cancel()
		s.mu.qCancels.Delete(id)
	}
}

// RunTask adds one to the count of tasks left to quiesce in the system.
// Any worker which is a "first mover" when starting tasks must call this method
// before starting work on a new task. First movers include goroutines launched
// to do periodic work and the kv/isql_db.go gateway which accepts external client
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
	defer s.recover(ctx)
	defer s.runPostlude()
	defer s.startRegion(ctx, taskName).End()

	f(ctx)
	return nil
}

type region interface {
	End()
}

type noopRegion struct{}

func (n noopRegion) End() {}

func (s *Stopper) startRegion(ctx context.Context, taskName string) region {
	if !trace.IsEnabled() {
		return noopRegion{}
	}
	return trace.StartRegion(ctx, taskName)
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
	defer s.recover(ctx)
	defer s.runPostlude()
	defer s.startRegion(ctx, taskName).End()

	return f(ctx)
}

// RunAsyncTask is like RunTask, except the callback is run in a goroutine. The
// method doesn't block for the callback to finish execution.
//
// See also RunAsyncTaskEx for a version with more options.
func (s *Stopper) RunAsyncTask(
	ctx context.Context, taskName string, f func(context.Context),
) error {
	return s.RunAsyncTaskEx(ctx,
		TaskOpts{
			TaskName:   taskName,
			SpanOpt:    FollowsFromSpan,
			Sem:        nil,
			WaitForSem: false,
		},
		f)
}

// SpanOption specifies the type of tracing span that a task will run in.
type SpanOption int

const (
	// FollowsFromSpan makes the task run in a span that's not included in the
	// caller's recording (if any). For external tracers, the task's span will
	// still reference the caller's span through a FollowsFrom relationship. If
	// the caller doesn't have a span, then the task will execute in a root span.
	//
	// Use this when the caller will not wait for the task to finish, but a
	// relationship between the caller and the task might still be useful to
	// visualize in a trace collector.
	FollowsFromSpan SpanOption = iota

	// ChildSpan makes the task run in a span that's a child of the caller's span
	// (if any). The child is included in the parent's recording. For external
	// tracers, the child references the parent through a ChildOf relationship.
	// If the caller doesn't have a span, then the task will execute in a root
	// span.
	//
	// ChildSpan has consequences on memory usage: the memory lifetime of
	// the task's span becomes tied to the lifetime of the parent. Generally
	// ChildSpan should be used when the parent usually waits for the task to
	// complete, and the parent is not a long-running process.
	ChildSpan

	// SterileRootSpan makes the task run in a root span that doesn't get any
	// children. Anybody trying to create a child of the task's span will get a
	// root span. This is suitable for long-running tasks: connecting children to
	// these tasks would lead to infinitely-long traces, and connecting the
	// long-running task to its parent is also problematic because of the
	// different lifetimes.
	SterileRootSpan
)

// TaskOpts groups the task execution options for RunAsyncTaskEx.
type TaskOpts struct {
	// TaskName is a human-readable name for the operation. Used as the name of
	// the tracing span.
	TaskName string

	// SpanOpt controls the kind of span that the task will run in.
	SpanOpt SpanOption

	// If set, Sem is used as a semaphore limiting the concurrency (each task has
	// weight 1).
	//
	// It is the caller's responsibility to ensure that Sem is closed when the
	// stopper is quiesced. For quotapools which live for the lifetime of the
	// stopper, it is generally best to register the sem with the stopper using
	// AddCloser.
	Sem *quotapool.IntPool
	// If Sem is not nil, WaitForSem specifies whether the call blocks or not when
	// the semaphore is full. If true, the call blocks until the semaphore is
	// available in order to push back on callers that may be trying to create
	// many tasks. If false, returns immediately with an error if the semaphore is
	// not available.
	WaitForSem bool
}

// RunAsyncTaskEx is like RunTask, except the callback f is run in a goroutine.
// The call doesn't block for the callback to finish execution.
func (s *Stopper) RunAsyncTaskEx(ctx context.Context, opt TaskOpts, f func(context.Context)) error {
	var alloc *quotapool.IntAlloc
	taskStarted := false
	if opt.Sem != nil {
		// Wait for permission to run from the semaphore.
		var err error
		if opt.WaitForSem {
			alloc, err = opt.Sem.Acquire(ctx, 1)
		} else {
			alloc, err = opt.Sem.TryAcquire(ctx, 1)
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
			// If the task is started, the alloc will be released async.
			if !taskStarted {
				alloc.Release()
			}
		}()

		// Check for canceled context: it's possible to get the semaphore even
		// if the context is canceled.
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	if !s.runPrelude() {
		return ErrUnavailable
	}

	// If the caller has a span, the task gets a child span.
	//
	// Note that we have to create the child in this parent goroutine; we can't
	// defer the creation to the spawned async goroutine since the parent span
	// might get Finish()ed by then. However, we'll update the child's goroutine
	// ID.
	var sp *tracing.Span
	switch opt.SpanOpt {
	case FollowsFromSpan:
		ctx, sp = tracing.ForkSpan(ctx, opt.TaskName)
	case ChildSpan:
		ctx, sp = tracing.ChildSpan(ctx, opt.TaskName)
	case SterileRootSpan:
		ctx, sp = s.tracer.StartSpanCtx(ctx, opt.TaskName, tracing.WithSterile())
	default:
		panic(fmt.Sprintf("unsupported SpanOption: %v", opt.SpanOpt))
	}

	// Call f on another goroutine.
	taskStarted = true // Another goroutine now takes ownership of the alloc, if any.
	go func(taskName string) {
		growstack.Grow()
		defer s.runPostlude()
		defer s.startRegion(ctx, taskName).End()
		defer sp.Finish()
		defer s.recover(ctx)
		if alloc != nil {
			defer alloc.Release()
		}

		sp.UpdateGoroutineIDToCurrent()
		f(ctx)
	}(opt.TaskName)
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
	defer s.recover(ctx)

	// Don't bother doing any cleanup if we're panicking; the process is expected
	// to terminate.
	if r := recover(); r != nil {
		panic(r)
	}

	stopCalled := func() (stopCalled bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		stopCalled = s.mu.stopping
		s.mu.stopping = true
		return stopCalled
	}()

	if stopCalled {
		// Wait for the concurrent Stop() to complete.
		<-s.stopped
		return
	}

	defer func() {
		unregister(s)
		close(s.stopped)
	}()

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
	defer s.recover(ctx)

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mu.quiescing {
			s.mu.quiescing = true
			close(s.quiescer)
		}

		s.mu.qCancels.Range(func(id int64, cancel *context.CancelFunc) (wantMore bool) {
			(*cancel)()
			s.mu.qCancels.Delete(id)
			return true
		})
		for _, f := range s.mu.quiescers {
			f()
		}
	}()

	start := timeutil.Now()
	var loggedQuiescing, loggedSlowQuiescing bool
	for s.NumTasks() > 0 {
		since := timeutil.Since(start)
		if !loggedQuiescing && since > 5*time.Second {
			log.Infof(ctx, "quiescing...")
			loggedQuiescing = true
		}
		if !loggedSlowQuiescing && since > 2*time.Minute {
			log.DumpStacks(ctx, "slow quiesce")
			loggedSlowQuiescing = true
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// SetTracer sets the tracer to be used for task spans. This cannot be called
// concurrently with starting tasks.
//
// Note that for tasks asking for a child span, the parent's tracer is used
// instead of this one.
//
// When possible, prefer supplying the tracer to the ctor through WithTracer.
func (s *Stopper) SetTracer(tr *tracing.Tracer) {
	s.tracer = tr
}

// Tracer returns the Tracer that the Stopper will use for tasks.
func (s *Stopper) Tracer() *tracing.Tracer {
	return s.tracer
}
