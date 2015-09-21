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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracer"
)

// A LocalSender provides methods to access a collection of local stores.
type LocalSender struct {
	mu       sync.RWMutex                     // Protects storeMap and addrs
	storeMap map[proto.StoreID]*storage.Store // Map from StoreID to Store
}

var _ client.Sender = &LocalSender{}
var _ client.BatchSender = &LocalSender{}
var _ rangeDescriptorDB = &LocalSender{}

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

// SendBatch implements batch.Sender.
func (ls *LocalSender) SendBatch(ctx context.Context, ba proto.BatchRequest) (*proto.BatchResponse, error) {
	trace := tracer.FromCtx(ctx)
	var store *storage.Store
	var err error

	// If we aren't given a Replica, then a little bending over
	// backwards here. This case applies exclusively to unittests.
	if ba.RangeID == 0 || ba.Replica.StoreID == 0 {
		var repl *proto.Replica
		var rangeID proto.RangeID
		rangeID, repl, err = ls.lookupReplica(ba.Key, ba.EndKey)
		if err == nil {
			ba.RangeID = rangeID
			ba.Replica = *repl
		}
	}

	ctx = log.Add(ctx,
		log.Method, ba.Method(), // TODO(tschottdorf): Method() always `Batch`.
		log.Key, ba.Key,
		log.RangeID, ba.RangeID)

	if err == nil {
		store, err = ls.GetStore(ba.Replica.StoreID)
	}

	var br *proto.BatchResponse
	if err == nil {
		// For calls that read data within a txn, we can avoid uncertainty
		// related retries in certain situations. If the node is in
		// "CertainNodes", we need not worry about uncertain reads any
		// more. Setting MaxTimestamp=Timestamp for the operation
		// accomplishes that. See proto.Transaction.CertainNodes for details.
		if ba.Txn != nil && ba.Txn.CertainNodes.Contains(ba.Replica.NodeID) {
			// MaxTimestamp = Timestamp corresponds to no clock uncertainty.
			trace.Event("read has no clock uncertainty")
			ba.Txn.MaxTimestamp = ba.Txn.Timestamp
		}
		{
			var tmpR proto.Response
			var tmpErr *proto.Error
			// TODO(tschottdorf): &ba -> ba
			tmpR, tmpErr = store.ExecuteCmd(ctx, &ba)
			// TODO(tschottdorf): remove this dance once BatchResponse is returned.
			if tmpR != nil {
				br = tmpR.(*proto.BatchResponse)
				if br.Error != nil {
					panic(proto.ErrorUnexpectedlySet)
				}
			}
			err = tmpErr.GoError()
		}
	}
	// TODO(tschottdorf): Later error needs to be associated to an index
	// and ideally individual requests don't even have an error in their
	// header. See #1891.
	return br, err
}

// Send implements the client.Sender interface. The store is looked
// up from the store map if specified by header.Replica; otherwise,
// the command is being executed locally, and the replica is
// determined via lookup through each store's LookupRange method.
func (ls *LocalSender) Send(ctx context.Context, call proto.Call) {
	client.SendCallConverted(ls, ctx, call)
	return
}

// lookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupRange(key).
// Returns RangeID and replica on success; RangeKeyMismatch error
// if not found.
// This is only for testing usage; performance doesn't matter.
func (ls *LocalSender) lookupReplica(start, end proto.Key) (rangeID proto.RangeID, replica *proto.Replica, err error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	var rng *storage.Replica
	for _, store := range ls.storeMap {
		rng = store.LookupReplica(start, end)
		if rng == nil {
			if tmpRng := store.LookupReplica(start, nil); tmpRng != nil {
				log.Warningf(fmt.Sprintf("range not contained in one range: [%s,%s), but have [%s,%s)", start, end, tmpRng.Desc().StartKey, tmpRng.Desc().EndKey))
			}
			continue
		}
		if replica == nil {
			rangeID = rng.Desc().RangeID
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
	return rangeID, replica, err
}

// firstRange implements the rangeDescriptorDB interface. It returns the
// range descriptor which contains KeyMin.
func (ls *LocalSender) firstRange() (*proto.RangeDescriptor, error) {
	_, replica, err := ls.lookupReplica(proto.KeyMin, nil)
	if err != nil {
		return nil, err
	}
	store, err := ls.GetStore(replica.StoreID)
	if err != nil {
		return nil, err
	}

	rpl := store.LookupReplica(proto.KeyMin, nil)
	if rpl == nil {
		panic("firstRange found no first range")
	}
	return rpl.Desc(), nil
}

// rangeLookup implements the rangeDescriptorDB interface. It looks up
// the descriptors for the given (meta) key.
func (ls *LocalSender) rangeLookup(key proto.Key, options lookupOptions, _ *proto.RangeDescriptor) ([]proto.RangeDescriptor, error) {
	ba, unwrap := client.MaybeWrap(&proto.RangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:             key,
			ReadConsistency: proto.INCONSISTENT,
		},
		MaxRanges:       1,
		ConsiderIntents: options.considerIntents,
		Reverse:         options.useReverseScan,
	})
	br, err := ls.SendBatch(context.Background(), *ba)
	if err != nil {
		return nil, err
	}
	return unwrap(br).(*proto.RangeLookupResponse).Ranges, nil
}
