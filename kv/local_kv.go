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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// A LocalKV provides methods to access a collection of local stores.
type LocalKV struct {
	mu       sync.RWMutex             // Protects storeMap and addrs
	storeMap map[int32]*storage.Store // Map from StoreID to Store
}

// NewLocalKV returns a local-only KV DB for direct access to a store.
func NewLocalKV() *LocalKV {
	return &LocalKV{storeMap: map[int32]*storage.Store{}}
}

// GetStoreCount returns the number of stores this node is exporting.
func (kv *LocalKV) GetStoreCount() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.storeMap)
}

// HasStore returns true if the specified store is owned by this LocalKV.
func (kv *LocalKV) HasStore(storeID int32) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	_, ok := kv.storeMap[storeID]
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (kv *LocalKV) GetStore(storeID int32) (*storage.Store, error) {
	kv.mu.RLock()
	store, ok := kv.storeMap[storeID]
	kv.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store %d not found", storeID)
	}
	return store, nil
}

// AddStore adds the specified store to the store map.
func (kv *LocalKV) AddStore(s *storage.Store) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.storeMap[s.Ident.StoreID]; ok {
		panic(fmt.Sprintf("cannot add store twice to local db: %+v", s.Ident))
	}
	kv.storeMap[s.Ident.StoreID] = s
}

// VisitStores implements a visitor pattern over stores in the storeMap.
// The specified function is invoked with each store in turn. Stores are
// visited in a random order.
func (kv *LocalKV) VisitStores(visitor func(s *storage.Store) error) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, s := range kv.storeMap {
		if err := visitor(s); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteCmd synchronously runs Store.ExecuteCmd. The store is looked
// up from the store map if specified by header.Replica; otherwise,
// the command is being executed locally, and the replica is
// determined via lookup through each of the stores.
func (kv *LocalKV) ExecuteCmd(method string, args proto.Request, replyChan interface{}) {
	for {
		var err error
		var store *storage.Store

		// If we aren't given a Replica, then a little bending over
		// backwards here. We need to find the Store, but all we have is the
		// Key. So find its Range locally. This lets us use the same
		// codepath below (store.ExecuteCmd) for both locally and remotely
		// originated commands.
		header := args.Header()
		if header.Replica.StoreID == 0 {
			var repl *proto.Replica
			repl, err = kv.lookupReplica(header.Key, header.EndKey)
			if err == nil {
				header.Replica = *repl
			}
		}
		if err == nil {
			store, err = kv.GetStore(header.Replica.StoreID)
		}
		reply := proto.NewReply(replyChan)
		if err != nil {
			reply.Header().SetGoError(err)
		} else {
			if err = store.ExecuteCmd(method, args, reply); err != nil {
				reply.Header().SetGoError(err)
			}
			if reply.Header().GoError() == nil {
				if err = reply.Verify(args); err != nil {
					reply.Header().SetGoError(err)
				}
			} else {
				// Check for case of range splitting to continue and retry the
				// local replica lookup.
				switch reply.Header().GoError().(type) {
				case *proto.RangeKeyMismatchError:
					header.Replica = proto.Replica{}
					continue
				}
			}
		}
		proto.SendReply(replyChan, reply)
		return
	}
}

// Close closes all stores.
func (kv *LocalKV) Close() {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for _, store := range kv.storeMap {
		store.Close()
	}
}

// lookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupRange(key).
func (kv *LocalKV) lookupReplica(start, end engine.Key) (*proto.Replica, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for _, store := range kv.storeMap {
		if rng := store.LookupRange(start, end); rng != nil {
			return rng.GetReplica(), nil
		}
	}
	return nil, proto.NewRangeKeyMismatchError(start, end, nil)
}
