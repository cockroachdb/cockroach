// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package future

import (
	"sync"
	"sync/atomic"

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
		done      atomic.Value // of chan struct{}, created lazily.
		whenReady []func()
		cleanup   []func()
	}

	// Valid only after done channel signaled.
	value T
	err   error
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

// Join performs cleanup steps.
// Join does not wait for the promise to be fulfilled.
// WhenReady callbacks are not executed, however cleanup callbacks are.
func (f *future[T]) Join() {
	f.mu.Lock()
	cleanup := f.mu.cleanup
	f.mu.cleanup = nil
	f.mu.whenReady = nil
	f.mu.ready = true // Ready or not, we are done.
	f.mu.Unlock()

	runClosures(cleanup...)
}

// WhenReady invokes provided callback when the value (or error) becomes
// available. Multiple WhenReady callbacks may be set, in which case they are
// executed sequentially.
//
// Callbacks execute on a separate Go routine.
func (f *future[T]) WhenReady(fn func(v T, err error)) {
	f.runOrDefer(func() { fn(f.value, f.err) }, &f.mu.whenReady)
}

// Defer is like a defer statement attached to Future[T].  It will
// execute when future becomes ready, or when it is joined to release whatever
// resources maybe have been acquired.
// Callbacks execute on a separate Go routine.
func (f *future[T]) Defer(cleanup func()) {
	f.runOrDefer(cleanup, &f.mu.cleanup)
}

// runOrDefer executes fn if future is ready, or queues it in the queue.
func (f *future[T]) runOrDefer(fn func(), queue *[]func()) {
	f.mu.Lock()
	c, _ := f.mu.done.Load().(chan struct{})
	if c == closedChan {
		f.mu.Unlock()
		runClosures(fn)
	} else {
		*queue = append(*queue, fn)
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

	d, _ := f.mu.done.Load().(chan struct{})
	if d == nil {
		f.mu.done.Store(closedChan)
	} else {
		close(d)
	}
	// Grab whenReady and cleanup callbacks, if any.
	// We shouldn't run those with the lock held since they
	// may issue a call (e.g. Join) to this future.
	whenReady, cleanup := f.mu.whenReady, f.mu.cleanup
	f.mu.whenReady, f.mu.cleanup = nil, nil
	f.mu.Unlock()

	runClosures(whenReady...)
	runClosures(cleanup...)
}

// runClosures is a helper to execute list of closures on a separate go routine, and
// wait for the completion.
func runClosures(closures ...func()) {
	if len(closures) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, fn := range closures {
			fn()
		}
	}()
	wg.Wait()
}

// whenReady returns a channel that can be selected on to wait
// for future to become ready.
func (f *future[T]) whenReady() chan struct{} {
	f.mu.Lock()
	d, _ := f.mu.done.Load().(chan struct{})
	if d == nil {
		d = make(chan struct{})
		f.mu.done.Store(d)
	}
	f.mu.Unlock()
	return d
}

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()
