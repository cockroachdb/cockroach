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
// implied.  See the License for the specific language governing
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

	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

// A LocalDB provides methods to access a collection of local stores.
type LocalDB struct {
	mu       sync.RWMutex             // Protects storeMap and addrs
	storeMap map[int32]*storage.Store // Map from StoreID to Store
	ranges   storage.RangeSlice       // *Range slice sorted by end key
}

// NewLocalDB returns a local-only KV DB for direct access to a store.
func NewLocalDB() *LocalDB {
	return &LocalDB{
		storeMap: make(map[int32]*storage.Store),
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (db *LocalDB) GetStoreCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.storeMap)
}

// HasStore returns true if the specified store is owned by this LocalDB.
func (db *LocalDB) HasStore(storeID int32) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.storeMap[storeID]
	return ok
}

// GetStore looks up the store by Replica.StoreID. Returns an error
// if not found.
func (db *LocalDB) GetStore(r *storage.Replica) (*storage.Store, error) {
	db.mu.RLock()
	store, ok := db.storeMap[r.StoreID]
	db.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store for replica %+v not found", r)
	}
	return store, nil
}

// AddStore adds the specified store to the store map.
func (db *LocalDB) AddStore(s *storage.Store) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.storeMap[s.Ident.StoreID]; ok {
		panic(fmt.Sprintf("cannot add store twice to local db: %+v", s.Ident))
	}
	db.storeMap[s.Ident.StoreID] = s

	// Maintain a slice of ranges ordered by StartKey.
	db.ranges = append(db.ranges, s.GetRanges()...)
	sort.Sort(db.ranges)
}

// VisitStores implements a visitor pattern over stores in the storeMap.
// The specified function is invoked with each store in turn. Stores are
// visited in a random order.
func (db *LocalDB) VisitStores(visitor func(s *storage.Store) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, s := range db.storeMap {
		if err := visitor(s); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all stores.
func (db *LocalDB) Close() {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, store := range db.storeMap {
		store.Close()
	}
}

// lookupReplica looks up a replica by key. Lookups are done via
// binary search over the "ranges" RangeSlice. Returns nil if no range
// is found for the specified key.
func (db *LocalDB) lookupReplica(key storage.Key) *storage.Replica {
	db.mu.RLock()
	defer db.mu.RUnlock()
	n := sort.Search(len(db.ranges), func(i int) bool {
		return bytes.Compare(key, db.ranges[i].Meta.EndKey) < 0
	})
	if n >= len(db.ranges) || bytes.Compare(key, db.ranges[n].Meta.EndKey) >= 0 {
		return nil
	}

	// Search the Replicas for the one that references our local Range. See executeCmd() as well.
	for i, repl := range db.ranges[n].Meta.Replicas {
		if repl.RangeID == db.ranges[n].Meta.RangeID {
			return &db.ranges[n].Meta.Replicas[i]
		}
	}
	return nil
}

// executeCmd runs Store.ExecuteCmd in a goroutine. A channel with
// element type equal to the reply type is created and returned
// immediately. The reply is sent to the channel once the cmd has been
// executed by the store. The store is looked up from the store map
// if specified by header.Replica; otherwise, the command is being
// executed locally, and the replica is determined via lookup of
// header.Key in the ranges slice.
func (db *LocalDB) executeCmd(method string, args storage.Request, reply storage.Response) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)
	replyVal := reflect.ValueOf(reply)
	go func() {
		// If the replica isn't specified in the header, look it up.
		var err error

		var store *storage.Store
		// If we aren't given a Replica, then a little bending over backwards here. We need to find the Store, but all
		// we have is the Key. So find its Range locally, and pull out its Replica which we use to find the Store.
		// This lets us use the same codepath below (store.ExecuteCmd) for both locally and remotely originated
		// commands.
		header := args.Header()
		if header.Replica.NodeID == 0 {
			if repl := db.lookupReplica(header.Key); repl != nil {
				header.Replica = *repl
			} else {
				err = util.Errorf("unable to lookup range replica for key %q", string(header.Key))
			}
		}
		if err == nil {
			store, err = db.GetStore(&header.Replica)
		}
		if err != nil {
			reply.Header().Error = err
		} else {
			store.ExecuteCmd(method, args, reply)
		}
		chanVal.Send(replyVal)
	}()
	return chanVal.Interface()
}

// Contains passes through to local range.
func (db *LocalDB) Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse {
	return db.executeCmd(storage.Contains,
		args, &storage.ContainsResponse{}).(chan *storage.ContainsResponse)
}

// Get passes through to local range.
func (db *LocalDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.executeCmd(storage.Get,
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

// Put passes through to local range.
func (db *LocalDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.executeCmd(storage.Put,
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}

// ConditionalPut passes through to local range.
func (db *LocalDB) ConditionalPut(args *storage.ConditionalPutRequest) <-chan *storage.ConditionalPutResponse {
	return db.executeCmd(storage.ConditionalPut,
		args, &storage.ConditionalPutResponse{}).(chan *storage.ConditionalPutResponse)
}

// Increment passes through to local range.
func (db *LocalDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.executeCmd(storage.Increment,
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}

// Delete passes through to local range.
func (db *LocalDB) Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse {
	return db.executeCmd(storage.Delete,
		args, &storage.DeleteResponse{}).(chan *storage.DeleteResponse)
}

// DeleteRange passes through to local range.
func (db *LocalDB) DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse {
	return db.executeCmd(storage.DeleteRange,
		args, &storage.DeleteRangeResponse{}).(chan *storage.DeleteRangeResponse)
}

// Scan passes through to local range.
func (db *LocalDB) Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse {
	return db.executeCmd(storage.Scan,
		args, &storage.ScanResponse{}).(chan *storage.ScanResponse)
}

// EndTransaction passes through to local range.
func (db *LocalDB) EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse {
	return db.executeCmd(storage.EndTransaction,
		args, &storage.EndTransactionResponse{}).(chan *storage.EndTransactionResponse)
}

// AccumulateTS passes through to local range.
func (db *LocalDB) AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse {
	return db.executeCmd(storage.AccumulateTS,
		args, &storage.AccumulateTSResponse{}).(chan *storage.AccumulateTSResponse)
}

// ReapQueue passes through to local range.
func (db *LocalDB) ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse {
	return db.executeCmd(storage.ReapQueue,
		args, &storage.ReapQueueResponse{}).(chan *storage.ReapQueueResponse)
}

// EnqueueUpdate passes through to local range.
func (db *LocalDB) EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse {
	return db.executeCmd(storage.EnqueueUpdate,
		args, &storage.EnqueueUpdateResponse{}).(chan *storage.EnqueueUpdateResponse)
}

// EnqueueMessage passes through to local range.
func (db *LocalDB) EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse {
	return db.executeCmd(storage.EnqueueMessage,
		args, &storage.EnqueueMessageResponse{}).(chan *storage.EnqueueMessageResponse)
}
