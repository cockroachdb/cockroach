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
	"bytes"
	"fmt"
	"reflect"
	"sort"
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
	ranges   storage.RangeSlice       // *Range slice sorted by end key
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

// GetStore looks up the store by Replica.StoreID. Returns an error
// if not found.
func (kv *LocalKV) GetStore(r *proto.Replica) (*storage.Store, error) {
	kv.mu.RLock()
	store, ok := kv.storeMap[r.StoreID]
	kv.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store for replica %+v not found", r)
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

	// Maintain a slice of ranges ordered by StartKey.
	kv.ranges = append(kv.ranges, s.GetRanges()...)
	sort.Sort(kv.ranges)
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
// determined via lookup of header.Key in the ranges slice.
func (kv *LocalKV) ExecuteCmd(method string, args proto.Request, replyChan interface{}) {
	// If the replica isn't specified in the header, look it up.
	var err error
	var store *storage.Store
	// If we aren't given a Replica, then a little bending over
	// backwards here. We need to find the Store, but all we have is the
	// Key. So find its Range locally, and pull out its Replica which we
	// use to find the Store. This lets us use the same codepath below
	// (store.ExecuteCmd) for both locally and remotely originated
	// commands.
	header := args.Header()
	if header.Replica.NodeID == 0 {
		if repl := kv.lookupReplica(header.Key); repl != nil {
			header.Replica = *repl
		} else {
			err = util.Errorf("unable to lookup range replica for key %q", string(header.Key))
		}
	}
	if err == nil {
		store, err = kv.GetStore(&header.Replica)
	}
	reply := reflect.New(reflect.TypeOf(replyChan).Elem().Elem()).Interface().(proto.Response)
	if err != nil {
		reply.Header().SetGoError(err)
	} else {
		store.ExecuteCmd(method, args, reply)
	}
	reflect.ValueOf(replyChan).Send(reflect.ValueOf(reply))
}

// Close closes all stores.
func (kv *LocalKV) Close() {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for _, store := range kv.storeMap {
		store.Close()
	}
}

// lookupReplica looks up a replica by key. Lookups are done via
// binary search over the "ranges" RangeSlice. Returns nil if no range
// is found for the specified key.
func (kv *LocalKV) lookupReplica(key engine.Key) *proto.Replica {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	n := sort.Search(len(kv.ranges), func(i int) bool {
		return bytes.Compare(key, kv.ranges[i].Meta.EndKey) < 0
	})
	if n >= len(kv.ranges) || bytes.Compare(key, kv.ranges[n].Meta.EndKey) >= 0 {
		return nil
	}

	// Search the Replicas for the one that references our local Range. See executeCmd() as well.
	for i, repl := range kv.ranges[n].Meta.Replicas {
		if repl.RangeID == kv.ranges[n].Meta.RangeID {
			return &kv.ranges[n].Meta.Replicas[i]
		}
	}
	return nil
}
