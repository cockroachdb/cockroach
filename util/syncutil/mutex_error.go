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

package syncutil

func init() {
	// Silence TestUnused.
	var rw RWMutexWithError
	_ = rw.Wrapped()
	_ = rw.Destroyed()
	rw.Destroy(nil)
	var w MutexWithError
	w.Destroy(nil)
}

// RWMutexWithError .
type RWMutexWithError struct {
	internalMutex RWMutex
	destroyed     error
}

// Wrapped .
func (mu *RWMutexWithError) Wrapped() *RWMutex {
	return &mu.internalMutex
}

// Destroy .
func (mu *RWMutexWithError) Destroy(err error) {
	mu.internalMutex.Lock()
	defer mu.internalMutex.Unlock()
	mu.DestroyLocked(err)
}

// DestroyLocked .
func (mu *RWMutexWithError) DestroyLocked(err error) {
	mu.destroyed = err
}

// Destroyed .
func (mu *RWMutexWithError) Destroyed() error {
	mu.internalMutex.Lock()
	defer mu.internalMutex.Unlock()
	return mu.DestroyedLocked()
}

// DestroyedLocked .
func (mu *RWMutexWithError) DestroyedLocked() error {
	return mu.destroyed
}

// RLock .
func (mu *RWMutexWithError) RLock() error {
	mu.internalMutex.RLock()
	if mu.destroyed != nil {
		mu.internalMutex.RUnlock()
		return mu.destroyed
	}
	return nil
}

// RUnlock .
func (mu *RWMutexWithError) RUnlock() {
	mu.internalMutex.RUnlock()
}

// Lock .
func (mu *RWMutexWithError) Lock() error {
	mu.internalMutex.Lock()
	if mu.destroyed != nil {
		mu.internalMutex.Unlock()
		return mu.destroyed
	}
	return nil
}

// Unlock .
func (mu *RWMutexWithError) Unlock() {
	mu.internalMutex.Unlock()
}

// MutexWithError .
type MutexWithError struct {
	internalMutex Mutex
	destroyed     error
}

// Destroyed .
func (mu *MutexWithError) Destroyed() error {
	mu.internalMutex.Lock()
	defer mu.internalMutex.Unlock()
	return mu.DestroyedLocked()
}

// DestroyedLocked .
func (mu *MutexWithError) DestroyedLocked() error {
	return mu.destroyed
}

// Wrapped .
func (mu *MutexWithError) Wrapped() *Mutex {
	return &mu.internalMutex
}

// Destroy .
func (mu *MutexWithError) Destroy(err error) {
	mu.internalMutex.Lock()
	defer mu.internalMutex.Unlock()
	mu.DestroyLocked(err)
}

// DestroyLocked .
func (mu *MutexWithError) DestroyLocked(err error) {
	mu.destroyed = err
}

// MustLock .
func (mu *MutexWithError) MustLock() {
	if err := mu.Lock(); err != nil {
		panic(err)
	}
}

// Lock .
func (mu *MutexWithError) Lock() error {
	mu.internalMutex.Lock()
	if mu.destroyed != nil {
		mu.internalMutex.Unlock()
		return mu.destroyed
	}
	return nil
}

// Unlock .
func (mu *MutexWithError) Unlock() {
	mu.internalMutex.Unlock()
}
