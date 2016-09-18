// Copyright 2016 The Cockroach Authors.
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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package syncutil

import "sync/atomic"

// Once is an object that will perform exactly one action.
//
// This object behaves the same as sync.Once from the standard library, except
// that it holds a reference to a Mutex instead of holding the Mutex by value.
// This means that it can be used in conjunction with external synchronization
// that uses the same Mutex.
type Once struct {
	m    *Mutex
	done uint32
}

// SetMutex sets the mutex that the Once object should use for synchronization.
func (o *Once) SetMutex(m *Mutex) {
	o.m = m
}

// Do calls the function f if and only if Do is being called for the
// first time for this instance of Once.
func (o *Once) Do(f func()) {
	if atomic.LoadUint32(&o.done) == 1 {
		return
	}
	// Slow-path.
	o.m.Lock()
	defer o.m.Unlock()
	o.DoLocked(f)
}

// DoLocked is like Do, except it requires the Mutex to be held.
func (o *Once) DoLocked(f func()) {
	if o.done == 0 {
		defer atomic.StoreUint32(&o.done, 1)
		f()
	}
}

// IsDone checks if the Once object has completed its task. It should
// be called if the Mutex is not already held.
func (o *Once) IsDone() bool {
	if atomic.LoadUint32(&o.done) == 1 {
		return true
	}
	// Slow-path.
	o.m.Lock()
	defer o.m.Unlock()
	return o.IsDoneLocked()
}

// Avoid unused go vet warning. The method is useful enough to justify its existence.
var _ = (&Once{}).IsDone

// IsDoneLocked checks if the Once object has completed its task. It should
// be called if the Mutex is already held.
func (o *Once) IsDoneLocked() bool {
	return o.done == 1
}
