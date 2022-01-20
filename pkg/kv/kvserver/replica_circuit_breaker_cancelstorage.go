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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// CancelStorage implements tracking of context cancellation functions
// for use by Replica circuit breakers.
type CancelStorage interface {
	// Reset initializes the storage. Not thread safe.
	Reset()
	// Set adds context and associated cancel func to the storage. Returns a token
	// that can be passed to Del.
	//
	// Set is thread-safe.
	Set(_ context.Context, cancel func()) (token interface{})
	// Del removes a cancel func, as identified by the token returned from Set.
	//
	// Del is thread-safe.
	Del(token interface{})
	// Visit invokes the provided closure with each (context,cancel) pair currently
	// present in the storage. Items for which the visitor returns true are removed
	// from the storage.
	//
	// Visit is thread-safe, but it is illegal to invoke methods of the
	// CancelStorage from within the visitor.
	Visit(func(context.Context, func()) (remove bool))
}

type cancelToken struct {
	ctx context.Context
}

func (tok *cancelToken) fasthash() int {
	// From https://github.com/taylorza/go-lfsr/blob/7ec2b93980f950da1e36c6682771e6fe14c144c2/lfsr.go#L46-L48.
	s := int(uintptr(unsafe.Pointer(tok)))
	b := (s >> 0) ^ (s >> 2) ^ (s >> 3) ^ (s >> 4)
	return (s >> 1) | (b << 7)
}

var cancelTokenPool = sync.Pool{
	New: func() interface{} { return &cancelToken{} },
}

type mapCancelShard struct {
	syncutil.Mutex
	m map[*cancelToken]func()
}

// A MapCancelStorage implements CancelStorage via shards of mutex-protected
// maps.
type MapCancelStorage struct {
	NumShards int
	sl        []*mapCancelShard
}

// Reset implements CancelStorage.
func (m *MapCancelStorage) Reset() {
	if m.NumShards == 0 {
		m.NumShards = 1
	}
	m.sl = make([]*mapCancelShard, m.NumShards)
	for i := range m.sl {
		s := &mapCancelShard{}
		s.m = map[*cancelToken]func(){}
		m.sl[i] = s
	}
}

// Set implements CancelStorage.
func (m *MapCancelStorage) Set(ctx context.Context, cancel func()) interface{} {
	tok := cancelTokenPool.Get().(*cancelToken)
	tok.ctx = ctx
	shard := m.sl[tok.fasthash()%len(m.sl)]
	shard.Lock()
	shard.m[tok] = cancel
	shard.Unlock()
	return tok
}

// Del implements CancelStorage.
func (m *MapCancelStorage) Del(tok interface{}) {
	ttok := tok.(*cancelToken)
	shard := m.sl[ttok.fasthash()%len(m.sl)]
	shard.Lock()
	delete(shard.m, tok.(*cancelToken))
	shard.Unlock()
}

// Visit implements CancelStorage.
func (m *MapCancelStorage) Visit(fn func(context.Context, func()) (remove bool)) {
	for _, shard := range m.sl {
		shard.Lock()
		for tok, cancel := range shard.m {
			if fn(tok.ctx, cancel) {
				delete(shard.m, tok)
			}
		}
		shard.Unlock()
	}
}
