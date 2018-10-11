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
//       l := rg.Lock()
//       // Loop until I get my resources.
//       var alloc resourceAllocation
//       for {
//         alloc, err := l.AllocateCPU(resources_i_need)
//         // TODO: handle err
//         if alloc == (resourceAllocation{}) {
//           // Not enough resources for me. Wait until I can ask for resources.
//           l.Wait()
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
	rg *resourceGovernor
}

// Lock prevents others from acquiring or releasing resources until
// resourceLock.Unlock() is called.
func (r *resourceGovernor) Lock() resourceLock {
	r.mu.Lock() // Unlocked by ResourceLock.Unlock()/Wait().
	return resourceLock{rg: r}
}

func (r *resourceGovernor) releaseCPU(cpu int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.availableCPU += cpu
	// Wake up everybody blocked in resourceLock.Wait().
	r.mu.cond.Broadcast()
}

func (l resourceLock) Unlock() {
	l.rg.mu.Unlock()
}

func (l *resourceLock) AllocateCPU(cpu int) (resourceAllocation, error) {
	l.rg.mu.Mutex.AssertHeld()
	if cpu > l.rg.quota {
		return resourceAllocation{}, fmt.Errorf(
			"requsted resources (%d) over quota (%d)", cpu, l.rg.quota)
	}
	if l.rg.mu.availableCPU > cpu {
		l.rg.mu.availableCPU -= cpu
		return resourceAllocation{rg: l.rg, cpu: cpu}, nil
	}
	return resourceAllocation{}, nil
}

// Wait unlocks the resourceGovernor and blocks until some resources are
// released. Once some resources are releases, all waiters wake up and re-lock
// the governor in turns. All new resources might be gone by the time Wait()
// returns; the caller cannot assume anything but the fact that there's now a
// chance that allocations that previously failed will succeed.
func (l resourceLock) Wait() {
	l.rg.mu.AssertHeld()
	l.rg.mu.cond.Wait()
	l.rg.mu.Unlock()
}

type resourceAllocation struct {
	rg  *resourceGovernor
	cpu int
}

func (a resourceAllocation) Release() {
	a.rg.releaseCPU(a.cpu)
}
