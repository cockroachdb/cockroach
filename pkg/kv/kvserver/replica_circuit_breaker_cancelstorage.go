// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// A MapCancelStorage implements CancelStorage via a mutex-protected map.
type MapCancelStorage struct {
	syncutil.Mutex
	m map[context.Context]func()
}

// Reset implements CancelStorage.
func (m *MapCancelStorage) Reset() {
	m.Lock()
	m.m = map[context.Context]func(){}
	m.Unlock()
}

// Set implements CancelStorage.
func (m *MapCancelStorage) Set(ctx context.Context, cancel func()) {
	m.Lock()
	m.m[ctx] = cancel
	m.Unlock()
}

// Del implements CancelStorage.
func (m *MapCancelStorage) Del(ctx context.Context) {
	m.Lock()
	delete(m.m, ctx)
	m.Unlock()
}

// Visit implements CancelStorage.
func (m *MapCancelStorage) Visit(fn func(context.Context, func()) (remove bool)) {
	m.Lock()
	for ctx, cancel := range m.m {
		if fn(ctx, cancel) {
			delete(m.m, ctx)
		}
	}
	m.Unlock()
}

// A SyncMapCancelStorage implements CancelStorage via a sync.Map.
type SyncMapCancelStorage struct {
	m *sync.Map
}

// Reset implements CancelStorage.
func (s *SyncMapCancelStorage) Reset() {
	s.m = &sync.Map{}
}

// Set implements CancelStorage.
func (s *SyncMapCancelStorage) Set(ctx context.Context, f func()) {
	s.m.Store(ctx, f)
}

// Del implements CancelStorage.
func (s *SyncMapCancelStorage) Del(ctx context.Context) {
	s.m.Delete(ctx)
}

// Visit implements CancelStorage.
func (s *SyncMapCancelStorage) Visit(f func(context.Context, func()) (remove bool)) {
	var rm []context.Context
	s.m.Range(func(ctxi, cancel interface{}) bool {
		ctx := ctxi.(context.Context)
		if f(ctx, cancel.(func())) {
			rm = append(rm, ctx)
		}
		return true // more
	})
	for _, ctx := range rm {
		s.Del(ctx)
	}
}

// TODO(tbg): if we really want to go all in, we should pick something that
// has a per-P (i.e. per-CPU) map. For example, we could create 4*GOMAXPROCS()
// mutex-protected maps, and "abuse" a sync.Pool to get per-P affinity to a
// single mutex most of the time. It's unclear that that's worth it.
