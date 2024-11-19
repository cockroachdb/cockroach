// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
)

// Future represents a value which will become ready in the future.
// A zero initialized Future is ready to be used, and it represents a promise
// that the caller will produce value in the future.
// The caller may arrange for a callback to be invoked when the future
// becomes ready.
// Use AwaitableFuture if the caller needs to wait for the future.
type Future[T any] struct {
	// value contains the actual value, along with any optional callbacks to
	// execute when value becomes ready.
	value atomic.Pointer[futureValue[T]]
}

// futureValue holds value, along with any callbacks that
// need to run when value becomes available.
type futureValue[T any] struct {
	v         *T
	whenReady []func(v T)

	// stack is the stack information of a go routine that prepared this future.
	// Usually nil, but can be set under certain conditions (e.g. race).
	stack []uintptr
}

// ErrorFuture is a handy alias for a future that returns an error.
type ErrorFuture = Future[error]

// AwaitableFuture is a future that can be waited on.
type AwaitableFuture[T any] struct {
	*Future[T]
	done <-chan struct{}
}

// MakeCompletedFuture constructs future which returns specified value.
func MakeCompletedFuture[T any](v T) *Future[T] {
	f := Make[T]()
	f.Set(v)
	return f
}

// Make constructs the future.
func Make[T any]() *Future[T] {
	return &Future[T]{}
}

// MakeCompletedErrorFuture constructs completed ErrorFuture.
func MakeCompletedErrorFuture(err error) *ErrorFuture {
	f := ErrorFuture{}
	f.Set(err)
	return &f
}

// MakeAwaitableFuture wraps underlying future so that the callers can wait
// for future to become available.
func MakeAwaitableFuture[T any](f *Future[T]) AwaitableFuture[T] {
	if ptr := f.value.Load(); ptr != nil && ptr.v != nil {
		// Future already prepared; no need to wait for it to become ready.
		return AwaitableFuture[T]{Future: f, done: closedChan}
	}

	done := make(chan struct{})
	f.WhenReady(func(v T) {
		close(done)
	})

	return AwaitableFuture[T]{Future: f, done: done}
}

// Wait is a helper function that waits until future becomes ready and returns
// its value. Returns context error.
func Wait[T any](ctx context.Context, f *Future[T]) (T, error) {
	a := MakeAwaitableFuture(f)
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case <-a.done:
		return a.Get(), nil
	}
}

// Get returns future value if it's ready, or a zero value otherwise.
// Use Done() method to select on in order to wait for future completion.
// Alternatively, use future.Wait() method to wait for future completion,
// while respecting context cancellation.
func (f AwaitableFuture[T]) Get() T {
	if v := f.value.Load(); v != nil && v.v != nil {
		return *v.v
	}
	var zero T
	return zero
}

// Done returns a channel that can selected on until future becomes ready.
func (f AwaitableFuture[T]) Done() <-chan struct{} {
	return f.done
}

// WhenReady invokes provided callback when the value becomes available.
// Multiple WhenReady callbacks may be set, in which case they are executed
// sequentially.
//
// NB: callback executes on whatever thread called this function if the
// future already completed, or on whatever thread completed this future.
// Thus, it is important for the caller to keep this in mind particular when
// dealing with locking and/or re-entrant fn() calls.  For example:
//
//		 mu.Lock()
//		 f := FunctionReturningFuture()  // Note: f maybe completed already
//		 f.WhenCompleted(func(value T, err error) {
//		      mu.Lock()
//		      ...
//	  })
//
// Clearly, in the above example, the call to WhenCompleted will deadlock
// because we attempt to execute callback function with the lock held.
// The caller, of course, is free to specify function that spins up a go
// routine to work around locking issues.
func (f *Future[T]) WhenReady(fn func(v T)) {
	for {
		old := f.value.Load()
		if old != nil && old.v != nil {
			// Future already here; execute function immediately.
			fn(*old.v)
			return
		}

		// Add the function to the list of  to run when future becomes available.
		fv := &futureValue[T]{}
		if old != nil {
			fv.whenReady = append(fv.whenReady, old.whenReady...)
		}
		fv.whenReady = append(fv.whenReady, fn)
		if f.value.CompareAndSwap(old, fv) {
			return
		}
	}
}

// Set fulfils the promise and sets the value of the future if the
// future has not been completed yet.  Only the first value Set kept.
// Subsequent attempts to Set future value ignored.  See MustSet
// when the caller needs to ensure that value must be set.
// Returns boolean indicating if the value was set.
func (f *Future[T]) Set(v T) (wasSet bool) {
	return f.prepare(v) == nil
}

func shouldCaptureCallerStack() bool {
	return util.RaceEnabled || log.V(2)
}

// MustSet sets future value, and returns an error indicating if the
// value was set.
// Under certain conditions (shouldCaptureCallerStack()), the error can contain
// the stack trace of the go routine that produced the first future value.
func MustSet[T any](f *Future[T], v T) error {
	old := f.prepare(v)
	if old == nil {
		return nil
	}
	if len(old.stack) == 0 {
		return errors.New("future value ignored; it was already set before")
	}
	return errors.Newf("future value ignored; it was already set at\n%+v", callerStack(old.stack))
}

// prepare completes this future and returns previously prepared value, if any.
func (f *Future[T]) prepare(v T) *futureValue[T] {
	fv := &futureValue[T]{v: &v}
	if shouldCaptureCallerStack() {
		fv.stack = callers(2)
	}

	for {
		old := f.value.Load()
		if old != nil && old.v != nil {
			return old
		}

		// Attempt to prepare the future.
		if f.value.CompareAndSwap(old, fv) {
			// If we succeeded in producing value, run any
			// callbacks that were configured with it.
			if old != nil {
				for _, fn := range old.whenReady {
					fn(v)
				}
			}
			return nil
		}
	}
}

func callers(skip int) []uintptr {
	const depth = 32
	var stack [depth]uintptr
	n := runtime.Callers(skip+1, stack[:])
	return stack[:n]
}

// callerStack returns the stack trace of the go routine
// that produced this future value.
// Only useful if shouldCaptureCallerStack() was true at the time
// when the future was produced.
func callerStack(stack []uintptr) errbase.StackTrace {
	frames := make([]errbase.StackFrame, len(stack))
	for i := range frames {
		frames[i] = errbase.StackFrame(stack[i])
	}
	return frames
}

var closedChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()
