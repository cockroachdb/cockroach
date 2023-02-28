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

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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

type resultCallback[T any] struct {
	fn    func(v T, err error)
	async bool
}

// MakeErrFuture constructs future which returns an error.
func MakeErrFuture[T any](err error) Future[T] {
	var zero T
	f := &future[T]{}
	f.ready(zero, err)
	return Future[T]{f}
}

// MakeCompletedFuture constructs future which returns specified value.
func MakeCompletedFuture[T any](v T) Future[T] {
	f := &future[T]{}
	f.ready(v, nil)
	return Future[T]{f}
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
	f.runOrDefer(fn, false)
}

// WhenReadyAsync is similar to WhenReady but executes provided callback on a
// separate go routine.
func (f *future[T]) WhenReadyAsync(fn func(v T, err error)) {
	f.runOrDefer(fn, true)
}

// runOrDefer executes fn if future is ready, or queues it in the queue.
func (f *future[T]) runOrDefer(fn func(v T, err error), async bool) {
	f.mu.Lock()
	cb := resultCallback[T]{fn: fn, async: async}

	if f.mu.done == closedChan {
		f.mu.Unlock()
		f.runClosures(cb)
	} else {
		f.mu.whenReady = append(f.mu.whenReady, cb)
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

// runClosures is a helper to execute list of closures either inline or on
// another go routine.
func (f *future[T]) runClosures(closures ...resultCallback[T]) {
	if len(closures) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, cb := range closures {
		if cb.async {
			wg.Add(1)
			go func(fn func(v T, err error)) {
				defer wg.Done()
				fn(f.value, f.err)
			}(cb.fn)
		} else {
			cb.fn(f.value, f.err)
		}
	}
	wg.Wait()
}

// whenReady returns a channel that can be selected on to wait
// for future to become ready.
func (f *future[T]) whenReady() chan struct{} {
	f.mu.Lock()
	if f.mu.done == nil {
		f.mu.done = make(chan struct{})
	}
	defer f.mu.Unlock()
	return f.mu.done
}

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()
