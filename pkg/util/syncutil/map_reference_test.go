// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's sync package.

package syncutil

import "sync/atomic"

// This file contains reference map implementations for unit-tests.

// mapInterface is the interface Map implements.
type mapInterface[K comparable, V any] interface {
	Load(K) (*V, bool)
	Store(key K, value *V)
	LoadOrStore(key K, value *V) (actual *V, loaded bool)
	LoadAndDelete(key K) (value *V, loaded bool)
	Delete(K)
	Range(func(key K, value *V) (shouldContinue bool))
}

// RWMutexMap is an implementation of mapInterface using a RWMutex.
type RWMutexMap[K comparable, V any] struct {
	mu    RWMutex
	dirty map[K]*V
}

func (m *RWMutexMap[K, V]) Load(key K) (value *V, ok bool) {
	m.mu.RLock()
	value, ok = m.dirty[key]
	m.mu.RUnlock()
	return
}

func (m *RWMutexMap[K, V]) Store(key K, value *V) {
	m.mu.Lock()
	if m.dirty == nil {
		m.dirty = make(map[K]*V)
	}
	m.dirty[key] = value
	m.mu.Unlock()
}

func (m *RWMutexMap[K, V]) LoadOrStore(key K, value *V) (actual *V, loaded bool) {
	m.mu.Lock()
	actual, loaded = m.dirty[key]
	if !loaded {
		actual = value
		if m.dirty == nil {
			m.dirty = make(map[K]*V)
		}
		m.dirty[key] = value
	}
	m.mu.Unlock()
	return actual, loaded
}

func (m *RWMutexMap[K, V]) LoadAndDelete(key K) (value *V, loaded bool) {
	m.mu.Lock()
	value, loaded = m.dirty[key]
	if !loaded {
		m.mu.Unlock()
		return nil, false
	}
	delete(m.dirty, key)
	m.mu.Unlock()
	return value, loaded
}

func (m *RWMutexMap[K, V]) Delete(key K) {
	m.mu.Lock()
	delete(m.dirty, key)
	m.mu.Unlock()
}

func (m *RWMutexMap[K, V]) Range(f func(key K, value *V) (shouldContinue bool)) {
	m.mu.RLock()
	keys := make([]K, 0, len(m.dirty))
	for k := range m.dirty {
		keys = append(keys, k)
	}
	m.mu.RUnlock()

	for _, k := range keys {
		v, ok := m.Load(k)
		if !ok {
			continue
		}
		if !f(k, v) {
			break
		}
	}
}

// DeepCopyMap is an implementation of mapInterface using a Mutex and
// atomic.Value.  It makes deep copies of the map on every write to avoid
// acquiring the Mutex in Load.
type DeepCopyMap[K comparable, V any] struct {
	mu    Mutex
	clean atomic.Value
}

func (m *DeepCopyMap[K, V]) Load(key K) (value *V, ok bool) {
	clean, _ := m.clean.Load().(map[K]*V)
	value, ok = clean[key]
	return value, ok
}

func (m *DeepCopyMap[K, V]) Store(key K, value *V) {
	m.mu.Lock()
	dirty := m.dirty()
	dirty[key] = value
	m.clean.Store(dirty)
	m.mu.Unlock()
}

func (m *DeepCopyMap[K, V]) LoadOrStore(key K, value *V) (actual *V, loaded bool) {
	clean, _ := m.clean.Load().(map[K]*V)
	actual, loaded = clean[key]
	if loaded {
		return actual, loaded
	}

	m.mu.Lock()
	// Reload clean in case it changed while we were waiting on m.mu.
	clean, _ = m.clean.Load().(map[K]*V)
	actual, loaded = clean[key]
	if !loaded {
		dirty := m.dirty()
		dirty[key] = value
		actual = value
		m.clean.Store(dirty)
	}
	m.mu.Unlock()
	return actual, loaded
}

func (m *DeepCopyMap[K, V]) LoadAndDelete(key K) (value *V, loaded bool) {
	m.mu.Lock()
	dirty := m.dirty()
	value, loaded = dirty[key]
	delete(dirty, key)
	m.clean.Store(dirty)
	m.mu.Unlock()
	return
}

func (m *DeepCopyMap[K, V]) Delete(key K) {
	m.mu.Lock()
	dirty := m.dirty()
	delete(dirty, key)
	m.clean.Store(dirty)
	m.mu.Unlock()
}

func (m *DeepCopyMap[K, V]) Range(f func(key K, value *V) (shouldContinue bool)) {
	clean, _ := m.clean.Load().(map[K]*V)
	for k, v := range clean {
		if !f(k, v) {
			break
		}
	}
}

func (m *DeepCopyMap[K, V]) dirty() map[K]*V {
	clean, _ := m.clean.Load().(map[K]*V)
	dirty := make(map[K]*V, len(clean)+1)
	for k, v := range clean {
		dirty[k] = v
	}
	return dirty
}
