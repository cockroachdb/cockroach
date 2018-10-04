// Copyright 2014 The Cockroach Authors.
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

package main

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// resourceGovernor manages a pool of resources (GCP CPUs) and dispenses them to
// clients.
//
// Sample usage:
//
//   rg := newResourceGovernor(10)
//   for i := 0; i < 100; i++ {
//     go func() {
//       l, err := rg.Lock()
//       if err != nil {
//       	return err
//       }
//       defer l.Unlock()  // Protect against early return.
//       // Loop until I get my resources.
//       var alloc resourceAllocation
//       for {
//         alloc, err := l.AllocateCPU(resources_i_need)
//         // TODO: handle err
//         if alloc == (resourceAllocation{}) {
//           // Not enough resources for me. Wait until I can ask for resources.
//           if err := l.Wait(); err != nil {
//           	return err
//           }
//         } else {
//           // Release the resources at the end.
//           defer alloc.Release()
//           break
//         }
//       }
//       l.Unlock()
//       // Do good work with my acquired resources.
//     }()
//   }
//
// NOTE: This pattern of locking the governor and then asking for resources, as
// opposed to asking it directly, was introduced to facilitate uses where a
// client has different work items that it could perform depending on what
// resources are available at the moment. If none of the work items can be
// currently accommodated then the client wants to sleep until more resources
// become available. Without a way for explicitly locking the governor, it's
// unclear how the client would know when to continue trying to acquire and when
// to sleep.
type resourceGovernor struct {
	quota int
	mu    struct {
		syncutil.Mutex
		// closed is set if Close() was called. All Lock() requests return an error.
		closed bool
		// closerMsg is set if closed is set and represents a message to be
		// delivered to all subsequent users of the resourceGovernor. This allows
		// one actor that's Close()ing the governor to inform others about why it
		// was called (so that logs make more sense).
		closerMsg    string
		availableCPU int
		// cond is signaled when resources are released to the governor.
		cond *sync.Cond
	}
}

func newResourceGovernor(cpu int) *resourceGovernor {
	rg := &resourceGovernor{quota: cpu}
	rg.mu.availableCPU = cpu
	rg.mu.cond = sync.NewCond(&rg.mu.Mutex)
	return rg
}

// resourceLock is the result of a resourceGovernor.Lock() call. It's a mutex
// and condition variable in one.
// It allows a client to ask the resourceGovernor for resources. If there's not
// enough resources at the moment to satisfy the request(s), it can be used to
// block until more resources become available.
type resourceLock struct {
	// rg is the parent resourceGovernor. It's set to nil once `Unlock()` has been
	// called.
	rg *resourceGovernor
}

// Close makes all current resourceLock.Wait() calls, as subsequent Lock()
// calls, fail with a message that includes msg.
func (r *resourceGovernor) Close(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.closed {
		return
	}
	r.mu.closed = true
	r.mu.closerMsg = msg
	// Wake up everybody blocked in resourceLock.Wait().
	r.mu.cond.Broadcast()
}

type errGovernorClosed struct {
	closerMsg string
}

func (err errGovernorClosed) Error() string {
	return "resourceGovernor closed: " + err.closerMsg
}

func newErrGovernorClosed(closerMsg string) error {
	return errGovernorClosed{closerMsg: closerMsg}
}

// Lock prevents others from acquiring or releasing resources until
// resourceLock.Unlock() is called.
func (r *resourceGovernor) Lock() (*resourceLock, error) {
	r.mu.Lock() // Unlocked by ResourceLock.Unlock()/Wait().
	if r.mu.closed {
		r.mu.Unlock()
		return nil, newErrGovernorClosed(r.mu.closerMsg)
	}
	return &resourceLock{rg: r}, nil
}

func (r *resourceGovernor) releaseCPU(cpu int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.availableCPU += cpu
	// Wake up everybody blocked in resourceLock.Wait().
	r.mu.cond.Broadcast()
}

// Unlock releases the lock.
// It can be called multiple times; subsequent calls are no-ops. This allows a
// usage pattern where `defer l.Unlock()` is called immediately after the lock
// is acquired (so that early return code paths are covered), even if code also
// calls Unlock() sooner on the regular path.
func (l *resourceLock) Unlock() {
	if l.rg == nil {
		return
	}
	l.rg.mu.Unlock()
	l.rg = nil
}

func (l *resourceLock) AllocateCPU(cpu int) (resourceAllocation, error) {
	l.rg.mu.Mutex.AssertHeld()
	if cpu > l.rg.quota {
		return resourceAllocation{}, fmt.Errorf(
			"requested resources (%d) over quota (%d)", cpu, l.rg.quota)
	}
	if l.rg.mu.availableCPU >= cpu {
		l.rg.mu.availableCPU -= cpu
		return resourceAllocation{rg: l.rg, cpu: cpu}, nil
	}
	return resourceAllocation{}, nil
}

// Wait unlocks the resourceGovernor and blocks until some resources are
// released. Once some resources are releases, all waiters wake up and re-lock
// the governor in turn (so, when Wait() returns, the governor is locked). All
// new resources might be gone by the time Wait() returns; the caller cannot
// assume anything but the fact that there's now a chance that allocations that
// previously failed will succeed.
func (l *resourceLock) Wait() error {
	l.rg.mu.AssertHeld()
	l.rg.mu.cond.Wait()
	if l.rg.mu.closed {
		return newErrGovernorClosed(l.rg.mu.closerMsg)
	}
	return nil
}

type resourceAllocation struct {
	rg  *resourceGovernor
	cpu int
}

// Release releases the resources to the resourceGovernor.
//
// This can be called on the resouceAllocation zero value, and it will be a
// no-op.
func (a resourceAllocation) Release() {
	if a.rg == nil {
		return
	}
	a.rg.releaseCPU(a.cpu)
}
