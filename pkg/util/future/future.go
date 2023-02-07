// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package future

import "sync/atomic"

// Future represents a value which will become ready in the future.
// A zero initialized Future is ready to be used, and it represents a promise
// that the caller will produce value in the future.
// The caller may arrange for a callback to be invoked when the future
// becomes ready.
// Use AwaitableFuture if the caller needs to wait for the future.
type Future[T any] struct {
	value atomic.Pointer[futureValue[T]]
}

// futureValue holds value, along with any callbacks that
// need to run when value becomes available.
type futureValue[T any] struct {
	v         *T
	whenReady []func(v T)
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
	f := MakePromise[T]()
	f.Set(v)
	return f
}

// MakePromise constructs future where the caller promises to set the value or
// an error.
func MakePromise[T any]() *Future[T] {
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
	if f.isReady() {
		return AwaitableFuture[T]{Future: f, done: closedChan}
	}
	done := make(chan struct{})
	f.WhenReady(func(v T) {
		close(done)
	})

	return AwaitableFuture[T]{Future: f, done: done}
}

// Get blocks until future is ready and returns it.
// This method blocks unconditionally.  If the caller needs
// to respect context cancellation, use Done() method to select
// on, followed by Get.
func (f AwaitableFuture[T]) Get() T {
	<-f.done
	return *f.value.Load().v
}

// Done returns a channel that can selected on until future becomes ready.
func (f AwaitableFuture[T]) Done() <-chan struct{} {
	return f.done
}

// WhenReady invokes provided callback when the value (or error) becomes
// available. Multiple WhenReady callbacks may be set, in which case they are
// executed sequentially.
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
	f.runOrDefer(fn)
}

// Set fulfils the promise and sets the value of the future.
// Only the first value kept.
func (f *Future[T]) Set(v T) {
	f.prepare(v)
}

// runOrDefer executes fn if future is ready, or queues it in the queue.
func (f *Future[T]) runOrDefer(fn func(v T)) {
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
			fv.whenReady = old.whenReady
		}
		fv.whenReady = append(fv.whenReady, fn)
		if f.value.CompareAndSwap(old, fv) {
			return
		}
	}
}

// prepare marks this future as ready.
func (f *Future[T]) prepare(v T) {
	for {
		old := f.value.Load()
		if old != nil && old.v != nil {
			// Future already prepared.
			return
		}

		// Attempt to prepare the future.
		if f.value.CompareAndSwap(old, &futureValue[T]{v: &v}) {
			// If we succeeded in producing value, run any
			// callbacks that were configured with it.
			if old != nil {
				for _, fn := range old.whenReady {
					fn(v)
				}
			}
		}
	}
}

func (f *Future[T]) isReady() bool {
	ptr := f.value.Load()
	return ptr != nil && ptr.v != nil
}

var closedChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()
