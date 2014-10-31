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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

// A LocalSender provides methods to access a collection of local stores.
type LocalSender struct {
	mu       sync.RWMutex             // Protects storeMap and addrs
	storeMap map[int32]*storage.Store // Map from StoreID to Store
}

// NewLocalSender returns a local-only sender which directly accesses
// a collection of stores.
func NewLocalSender() *LocalSender {
	return &LocalSender{storeMap: map[int32]*storage.Store{}}
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *LocalSender) GetStoreCount() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return len(ls.storeMap)
}

// HasStore returns true if the specified store is owned by this LocalSender.
func (ls *LocalSender) HasStore(storeID int32) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	_, ok := ls.storeMap[storeID]
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *LocalSender) GetStore(storeID int32) (*storage.Store, error) {
	ls.mu.RLock()
	store, ok := ls.storeMap[storeID]
	ls.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store %d not found", storeID)
	}
	return store, nil
}

// AddStore adds the specified store to the store map.
func (ls *LocalSender) AddStore(s *storage.Store) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if _, ok := ls.storeMap[s.Ident.StoreID]; ok {
		panic(fmt.Sprintf("cannot add store twice to local db: %+v", s.Ident))
	}
	ls.storeMap[s.Ident.StoreID] = s
}

// VisitStores implements a visitor pattern over stores in the storeMap.
// The specified function is invoked with each store in turn. Stores are
// visited in a random order.
func (ls *LocalSender) VisitStores(visitor func(s *storage.Store) error) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	for _, s := range ls.storeMap {
		if err := visitor(s); err != nil {
			return err
		}
	}
	return nil
}

// Send implements the client.KVSender interface. The store is looked
// up from the store map if specified by header.Replica; otherwise,
// the command is being executed locally, and the replica is
// determined via lookup through each store's LookupRange method.
func (ls *LocalSender) Send(call *client.Call) {
	// Instant retry with max two attempts to handle the case of a
	// range split, which is exposed here as a RangeKeyMismatchError.
	// If we fail with two in a row, pass the error up to caller and
	// let the remote client requery range metadata.
	retryOpts := util.RetryOptions{
		Tag:         fmt.Sprintf("routing %s locally", call.Method),
		MaxAttempts: 2,
	}
	util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		var err error
		var store *storage.Store

		// If we aren't given a Replica, then a little bending over
		// backwards here. We need to find the Store, but all we have is the
		// Key. So find its Range locally. This lets us use the same
		// codepath below (store.ExecuteCmd) for both locally and remotely
		// originated commands.
		header := call.Args.Header()
		if header.Replica.StoreID == 0 {
			var repl *proto.Replica
			repl, err = ls.lookupReplica(header.Key, header.EndKey)
			if err == nil {
				header.Replica = *repl
			}
		}
		if err == nil {
			store, err = ls.GetStore(header.Replica.StoreID)
		}
		if err != nil {
			call.Reply.Header().SetGoError(err)
		} else {
			if err = store.ExecuteCmd(call.Method, call.Args, call.Reply); err != nil {
				// Check for range key mismatch error (this could happen if
				// range was split between lookup and execution). In this case,
				// reset header.Replica and engage retry loop.
				switch err.(type) {
				case *proto.RangeKeyMismatchError:
					header.Replica = proto.Replica{}
					return util.RetryContinue, nil
				}
				call.Reply.Header().SetGoError(err)
			} else {
				if err = call.Reply.Verify(call.Args); err != nil {
					call.Reply.Header().SetGoError(err)
				}
			}
		}
		return util.RetryBreak, nil
	})
}

// Close implements the client.KVSender interface. Close closes all
// stores.
func (ls *LocalSender) Close() {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	for _, store := range ls.storeMap {
		store.Close()
	}
}

// lookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupRange(key).
func (ls *LocalSender) lookupReplica(start, end proto.Key) (*proto.Replica, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	for _, store := range ls.storeMap {
		if rng := store.LookupRange(start, end); rng != nil {
			return rng.GetReplica(), nil
		}
	}
	return nil, proto.NewRangeKeyMismatchError(start, end, nil)
}
