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

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// Future represents a value or an error which will become ready in the future.
// A zero initialized Future is ready to be used, and it represents a promise
// that the caller will fulfill (i.e. set a value or an error) in the future.
type Future[T any] struct {
	await

	// value or err valid only after await has been signaled.
	value T
	err   error
}

// ErrorFuture is a future that produces an error result.
// A zero initialized ErrorFuture is ready to be used.
type ErrorFuture struct {
	await
	err error // valid only after await has been signaled.
}

// MakeErrFuture constructs future which returns an error.
func MakeErrFuture[T any](err error) *Future[T] {
	f := MakePromise[T]()
	f.SetErr(err)
	return f
}

// MakeCompletedFuture constructs future which returns specified value.
func MakeCompletedFuture[T any](v T) *Future[T] {
	f := MakePromise[T]()
	f.SetValue(v)
	return f
}

// MakePromise constructs future where the caller promises to set the value or
// an error.
func MakePromise[T any]() *Future[T] {
	var f Future[T]
	return &f
}

// MakeCompletedErrorFuture constructs completed ErrorFuture.
func MakeCompletedErrorFuture(err error) *ErrorFuture {
	f := ErrorFuture{}
	f.SetErr(err)
	return &f
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
func (f *Future[T]) WhenReady(fn func(v T, err error)) {
	f.runOrDefer(func() { fn(f.value, f.err) })
}

// WhenReady invokes specified function when error future becomes ready.
func (e *ErrorFuture) WhenReady(fn func(err error)) {
	e.runOrDefer(func() { fn(e.err) })
}

// SetValue fulfils the promise and sets the value of the future.
// Only the first value kept.
func (f *Future[T]) SetValue(v T) {
	f.markReady(func() { f.value = v })
}

// SetErr fulfils the promise and sets the error value of the future.
// Only the first error kept.
func (f *Future[T]) SetErr(err error) {
	f.markReady(func() { f.err = err })
}

// Get blocks until future is ready and returns it.
// This method blocks unconditionally.  If the caller needs
// to respect context cancellation, use Done() method to select
// on, followed by Get.
func (f *Future[T]) Get() (T, error) {
	<-f.whenReady()
	return f.value, f.err
}

// SetErr fulfils the promise and sets the error value of the future.
// Only the first error kept.
func (e *ErrorFuture) SetErr(err error) {
	e.markReady(func() { e.err = err })
}

// Get blocks until future is ready and returns it.
// This method blocks unconditionally.  If the caller needs
// to respect context cancellation, use Done() method to select
// on, followed by Get.
func (e *ErrorFuture) Get() error {
	<-e.whenReady()
	return e.err
}

// await is a synchronization helper struct.
type await struct {
	mu struct {
		syncutil.Mutex
		ready     bool
		done      chan struct{} // created lazily.
		whenReady []func()
	}
}

// runOrDefer executes fn if future is ready, or queues it in the queue.
func (w *await) runOrDefer(fn func()) {
	w.mu.Lock()

	if w.mu.done == closedChan {
		w.mu.Unlock()
		runClosures(fn)
	} else {
		w.mu.whenReady = append(w.mu.whenReady, fn)
		w.mu.Unlock()
	}
}

// Done returns a channel that can selected on until future becomes ready.
func (w *await) Done() <-chan struct{} {
	return w.whenReady()
}

// ready marks this future as ready.
// If await is not ready yet, invokes ready function (once), which lets
// the caller update its state.
func (w *await) markReady(readyFn func()) {
	w.mu.Lock()

	if w.mu.ready {
		w.mu.Unlock()
		return
	}

	w.mu.ready = true
	readyFn()

	if w.mu.done == nil {
		w.mu.done = closedChan
	} else {
		close(w.mu.done)
	}

	readyFns := w.mu.whenReady
	w.mu.whenReady = nil
	w.mu.Unlock()

	runClosures(readyFns...)
}

// runClosures is a helper to execute list of closures either inline or on
// another go routine.
func runClosures(closures ...func()) {
	for _, fn := range closures {
		fn()
	}
}

// whenReady returns the channel that signals the future, creating
// it if necessary.
func (w *await) whenReady() chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.done == nil {
		w.mu.done = make(chan struct{})
	}
	return w.mu.done
}

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()
