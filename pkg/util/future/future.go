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

// Future represents a value which will become ready in the future.
type Future[T any] struct {
	// A bit of defensive type embedding in order to have all constructors return
	// value type so that it is more difficult to make a mistake of returning
	// 'nil' future.
	*future[T]
}

type future[T any] struct {
	mu struct {
		syncutil.Mutex
		ready     bool
		done      chan struct{} // created lazily
		whenReady []resultCallback[T]
	}

	// Valid only after done channel signaled.
	value T
	err   error
}

type resultCallback[T any] func(v T, err error)

// MakeErrFuture constructs future which returns an error.
func MakeErrFuture[T any](err error) Future[T] {
	f := MakePromise[T]()
	f.SetErr(err)
	return f
}

// MakeCompletedFuture constructs future which returns specified value.
func MakeCompletedFuture[T any](v T) Future[T] {
	f := MakePromise[T]()
	f.SetValue(v)
	return f
}

// MakePromise constructs future where the caller promises to set the value or
// an error.
func MakePromise[T any]() Future[T] {
	return Future[T]{&future[T]{}}
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
//	 mu.Lock()
//	 f := FunctionReturningFuture()  // Note: f maybe completed already
//	 f.WhenCompleted(func(value T, err error) {
//	      mu.Lock()
//	      ...
//	 })
//	Clearly, in the above example, the call to WhenCompleted will deadlock
//	because we attempt to execute callback function with the lock held.
//	If it is too unwieldy for the caller to guarantee lock safety,
//	use WhenReadyAsync instead.
func (f *future[T]) WhenReady(fn func(v T, err error)) {
	f.runOrDefer(fn)
}

// runOrDefer executes fn if future is ready, or queues it in the queue.
func (f *future[T]) runOrDefer(fn func(v T, err error)) {
	f.mu.Lock()

	if f.mu.done == closedChan {
		f.mu.Unlock()
		f.runClosures(fn)
	} else {
		f.mu.whenReady = append(f.mu.whenReady, fn)
		f.mu.Unlock()
	}
}

// SetValue fulfils the promise and sets the value of the future.
// Only the first value kept.
func (f *future[T]) SetValue(v T) {
	f.ready(v, nil)
}

// SetErr fulfils the promise and sets the error value of the future.
// Only the first error kept.
func (f *Future[T]) SetErr(err error) {
	var v T
	f.ready(v, err)
}

// Get blocks until future is ready and returns it.
// This method blocks unconditionally.  If the caller needs
// to respect context cancellation, use Done() method to select
// on, followed by Get.
func (f *future[T]) Get() (T, error) {
	<-f.whenReady()
	return f.value, f.err
}

// Done returns a channel that can selected on until future becomes ready.
func (f *future[T]) Done() <-chan struct{} {
	return f.whenReady()
}

// ready marks this future as ready.
func (f *future[T]) ready(v T, err error) {
	f.mu.Lock()

	if f.mu.ready {
		f.mu.Unlock()
		return
	}

	f.mu.ready = true
	f.value = v
	f.err = err

	if f.mu.done == nil {
		f.mu.done = closedChan
	} else {
		close(f.mu.done)
	}

	whenReady := f.mu.whenReady
	f.mu.whenReady = nil
	f.mu.Unlock()

	f.runClosures(whenReady...)
}

// runClosures invokes the closures synchronously, one by one.
func (f *future[T]) runClosures(closures ...resultCallback[T]) {
	for _, fn := range closures {
		fn(f.value, f.err)
	}
}

// whenReady returns the channel that signals the future, creating
// it if necessary.
func (f *future[T]) whenReady() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.done == nil {
		f.mu.done = make(chan struct{})
	}
	return f.mu.done
}

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()
