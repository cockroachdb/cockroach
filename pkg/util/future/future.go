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
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Future represents a value which will become ready in the future.
type Future[T any] struct {
	// A bit of defensive type embedding in order to have all constructors return
	// value type so that it is more difficult to make a mistake of returning
	// 'nil' future.
	*future[T]
}

type future[T any] struct {
	joiner func() // optional function to invoke when joining.

	mu struct {
		syncutil.Mutex
		ready     bool
		done      atomic.Value // of chan struct{}, created lazily.
		whenReady []func(v T, err error)
	}

	// Valid only after done channel signaled.
	value T
	err   error
}

// Make creates Future which will produce the value returned by the produce function.
// Produce function runs on separate Go routine.  The passed in context should be
// respected by produce function.
func Make[T any](ctx context.Context, produce func(ctx context.Context) (T, error)) Future[T] {
	f := &future[T]{}
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	var joinOnce sync.Once
	f.joiner = func() {
		joinOnce.Do(func() {
			cancel()
			wg.Wait()
		})
	}

	go func() {
		ctx, restore := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
		var v T
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = errors.Newf("panic recovered: %v", r)
			}
			f.ready(v, err)
			wg.Done()
			restore()
		}()

		v, err = produce(ctx)
	}()

	return Future[T]{f}
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
// Callbacks may execute on the calling thread if the future already completed.
// If the future has not completed, the callback will execute on the thread
// which fulfills this promise.
//
// NB: fn should be careful not to block.  Because this function executes inline,
// care must be taken to ensure that no deadlocks can occur. Thread management
// (i.e. ability to execute this function on some other thread) is left to the
// caller: the caller may arrange for this function to write to a (non-blocking)
// channel, and arrange for appropriate Go routine to read that channel.
func (f *future[T]) WhenReady(fn func(v T, err error)) {
	f.mu.Lock()

	c, _ := f.mu.done.Load().(chan struct{})
	if c == closedChan {
		// Future is here already.
		fn(f.value, f.err)
	} else {
		f.mu.whenReady = append(f.mu.whenReady, fn)
	}

	f.mu.Unlock()
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

// Join performs a cleanup step.
func (f *future[T]) Join() {
	if f.joiner != nil {
		f.joiner()
	}
}

// ready marks this future as ready.
func (f *future[T]) ready(v T, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.ready {
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
	for _, readyFn := range f.mu.whenReady {
		readyFn(f.value, f.err)
	}
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
