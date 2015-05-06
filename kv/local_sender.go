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
	mu       sync.RWMutex                     // Protects storeMap and addrs
	storeMap map[proto.StoreID]*storage.Store // Map from StoreID to Store
}

// NewLocalSender returns a local-only sender which directly accesses
// a collection of stores.
func NewLocalSender() *LocalSender {
	return &LocalSender{
		storeMap: map[proto.StoreID]*storage.Store{},
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *LocalSender) GetStoreCount() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return len(ls.storeMap)
}

// HasStore returns true if the specified store is owned by this LocalSender.
func (ls *LocalSender) HasStore(storeID proto.StoreID) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	_, ok := ls.storeMap[storeID]
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *LocalSender) GetStore(storeID proto.StoreID) (*storage.Store, error) {
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

// RemoveStore removes the specified store from the store map.
func (ls *LocalSender) RemoveStore(s *storage.Store) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	delete(ls.storeMap, s.Ident.StoreID)
}

// VisitStores implements a visitor pattern over stores in the storeMap.
// The specified function is invoked with each store in turn. Stores are
// visited in a random order.
func (ls *LocalSender) VisitStores(visitor func(s *storage.Store) error) error {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	for _, s := range ls.storeMap {
		if err := visitor(s); err != nil {
			return err
		}
	}
	return nil
}

// GetStoreIDs returns all the current store ids in a random order.
func (ls *LocalSender) GetStoreIDs() []proto.StoreID {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	storeIDs := make([]proto.StoreID, 0, len(ls.storeMap))
	for storeID := range ls.storeMap {
		storeIDs = append(storeIDs, storeID)
	}
	return storeIDs
}

// Send implements the client.KVSender interface. The store is looked
// up from the store map if specified by header.Replica; otherwise,
// the command is being executed locally, and the replica is
// determined via lookup through each store's LookupRange method.
func (ls *LocalSender) Send(call client.Call) {
	var err error
	var store *storage.Store

	// If we aren't given a Replica, then a little bending over
	// backwards here. This case applies exclusively to unittests.
	header := call.Args.Header()
	if header.RaftID == 0 || header.Replica.StoreID == 0 {
		var repl *proto.Replica
		var raftID int64
		raftID, repl, err = ls.lookupReplica(header.Key, header.EndKey)
		if err == nil {
			header.RaftID = raftID
			header.Replica = *repl
		}
	}
	if err == nil {
		store, err = ls.GetStore(header.Replica.StoreID)
	}
	if err != nil {
		call.Reply.Header().SetGoError(err)
	} else {
		// For calls that read data within a txn, we can avoid uncertainty
		// related retries in certain situations. If the node is in
		// "CertainNodes", we need not worry about uncertain reads any
		// more. Setting MaxTimestamp=Timestamp for the operation
		// accomplishes that. See proto.Transaction.CertainNodes for details.
		if header.Txn != nil && header.Txn.CertainNodes.Contains(header.Replica.NodeID) {
			// MaxTimestamp = Timestamp corresponds to no clock uncertainty.
			header.Txn.MaxTimestamp = header.Txn.Timestamp
		}
		store.ExecuteCmd(client.Call{Args: call.Args, Reply: call.Reply})
	}
}

// lookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupRange(key).
// Returns RaftID and replica on success; RangeKeyMismatch error
// if not found.
// TODO(tschottdorf) with a very large number of stores, the LocalSender
// may want to avoid scanning the whole map of stores on each invocation.
func (ls *LocalSender) lookupReplica(start, end proto.Key) (raftID int64, replica *proto.Replica, err error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	var rng *storage.Range
	for _, store := range ls.storeMap {
		rng = store.LookupRange(start, end)
		if rng == nil {
			continue
		}
		if replica == nil {
			raftID = rng.Desc().RaftID
			replica = rng.GetReplica()
			continue
		}
		// Should never happen outside of tests.
		return 0, nil, util.Errorf(
			"range %+v exists on additional store: %+v", rng, store)
	}
	if replica == nil {
		err = proto.NewRangeKeyMismatchError(start, end, nil)
	}
	return raftID, replica, err
}
