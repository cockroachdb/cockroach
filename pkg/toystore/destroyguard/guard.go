// Copyright 2019 The Cockroach Authors.
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

package destroyguard

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util"
)

// A Guard tracks a reference count (usually associated to some object to be
// guarded) while offering an API that allows tearing down the guard and
// instructing all reference holders to relinquish their references in a timely
// manner.
//
// A Guard is in either of three states, through which it moves in the following
// order:
// - active: Teardown has not been called; Acquire() returns success.
// - tearing down: Teardown has been called, but the refcount hasn't dropped to
//   zero; Acquire returns the error passed to Teardown.
// - deadCh: Teardown has been called and the refcount is zero; the Dead() channel
//   is closed and Acquire continues to return the same error.
//
// The zero Guard is ready to use. A Guard must not be copied by value. All
// methods on Guard are safe to call concurrently.
type Guard struct {
	// TODO(tbg): use int32 here to assert Release() without Acquire().
	ref uint32 // updated atomically by Acquire() and Release()

	initOnce sync.Once // sets up channels when needed

	done   uint32        // for idempotency in Teardown
	doneCh chan struct{} // Done(), closed on Teardown
	err    error         // Err(), set on Teardown

	dead   uint32        // atomically set by Teardown(), cleared by maybeKill()
	deadCh chan struct{} // Dead(); closed when ref hits zero

	util.NoCopy
}

// flagDestroyed is set on the ref count to indicate that Teardown has been
// called.
const flagDestroyed = uint32(1) << 31

func (d *Guard) maybeInit() {
	d.initOnce.Do(func() {
		d.doneCh = make(chan struct{})
		d.deadCh = make(chan struct{})
	})
}

// Acquire a reference on the Guard. If Teardown has been called, the acquisition
// will return with an error (and without having acquired a reference). If no
// error is returned, the caller is free to proceed and use the reference, though
// Done() should be consulted regularly (usually when carrying out a blocking
// operation, such as sending on or receiving from a channel). References can
// in principle be long-lived,
func (d *Guard) Acquire() error {
	n := atomic.AddUint32(&d.ref, 1)

	if (n & flagDestroyed) != 0 {
		d.Release()
		return d.err
	}

	return nil
}

func (d *Guard) maybeKill(ref uint32) {
	// If the Guard is tearing down and we just released the last reference,
	// kill the guard.
	if ref == flagDestroyed {
		if atomic.CompareAndSwapUint32(&d.dead, 0, 1) {
			close(d.deadCh)
		}
	}
}

// Release a reference. This must match a previous successful call to Acquire.
func (d *Guard) Release() {
	d.maybeKill(atomic.AddUint32(&d.ref, ^uint32(0) /* decrement by one */))
}

// Teardown instructs the Guard to refuse new references and to signal the
// Done() channel. The supplied error will be returned from Done(). Teardown
// does not block; the caller can consult Dead() to block until all references
// have been relinquished. In particular, it's perfectly fine to call Teardown
// from a goroutine that also holds a reference.
//
// Multiple calls to Teardown are allowed, but only the error from the first
// call will be used.
func (d *Guard) Teardown(err error) {
	if atomic.AddUint32(&d.done, 1) == 1 {
		d.maybeInit()
		d.err = err
		atomic.AddUint32(&d.ref, flagDestroyed)
		close(d.doneCh)
		d.maybeKill(atomic.LoadUint32(&d.ref))
	}
}

// Dead returns a channel that is closed when the reference count has dropped to
// zero after a call to Teardown. Consequently it must not be blocked on from a
// goroutine that is holding a reference, for that would constitute a deadlock.
// Additionally, blocking on this channel makes little sense unless it is known
// that Teardown() has been called.
func (d *Guard) Dead() <-chan struct{} {
	d.maybeInit()
	return d.deadCh
}

// Done is closed when Teardown is called.
func (d *Guard) Done() <-chan struct{} {
	d.maybeInit()
	return d.doneCh
}

// Err returns the error passed to Teardown. A call to Err() must be preceded
// by a read of the Done() channel.
func (d *Guard) Err() error {
	return d.err
}
