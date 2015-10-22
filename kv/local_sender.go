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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracer"
)

// A LocalSender provides methods to access a collection of local stores.
type LocalSender struct {
	mu       sync.RWMutex                       // Protects storeMap and addrs
	storeMap map[roachpb.StoreID]*storage.Store // Map from StoreID to Store
}

var _ client.Sender = &LocalSender{}
var _ rangeDescriptorDB = &LocalSender{}

// NewLocalSender returns a local-only sender which directly accesses
// a collection of stores.
func NewLocalSender() *LocalSender {
	return &LocalSender{
		storeMap: map[roachpb.StoreID]*storage.Store{},
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *LocalSender) GetStoreCount() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return len(ls.storeMap)
}

// HasStore returns true if the specified store is owned by this LocalSender.
func (ls *LocalSender) HasStore(storeID roachpb.StoreID) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	_, ok := ls.storeMap[storeID]
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *LocalSender) GetStore(storeID roachpb.StoreID) (*storage.Store, error) {
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

// Send implements the client.Sender interface. The store is looked up from the
// store map if specified by the request; otherwise, the command is being
// executed locally, and the replica is determined via lookup through each
// store's LookupRange method. The latter path is taken only by unit tests.
func (ls *LocalSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	trace := tracer.FromCtx(ctx)
	var store *storage.Store
	var err error

	// If we aren't given a Replica, then a little bending over
	// backwards here. This case applies exclusively to unittests.
	if ba.RangeID == 0 || ba.Replica.StoreID == 0 {
		var repl *roachpb.ReplicaDescriptor
		var rangeID roachpb.RangeID
		rs := keys.Range(ba)
		rangeID, repl, err = ls.lookupReplica(rs.Key, rs.EndKey)
		if err == nil {
			ba.RangeID = rangeID
			ba.Replica = *repl
		}
	}

	ctx = log.Add(ctx,
		log.RangeID, ba.RangeID)

	if err == nil {
		store, err = ls.GetStore(ba.Replica.StoreID)
	}

	var br *roachpb.BatchResponse
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	// For calls that read data within a txn, we can avoid uncertainty
	// related retries in certain situations. If the node is in
	// "CertainNodes", we need not worry about uncertain reads any
	// more. Setting MaxTimestamp=Timestamp for the operation
	// accomplishes that. See roachpb.Transaction.CertainNodes for details.
	if ba.Txn != nil && ba.Txn.CertainNodes.Contains(ba.Replica.NodeID) {
		// MaxTimestamp = Timestamp corresponds to no clock uncertainty.
		trace.Event("read has no clock uncertainty")
		ba.Txn.MaxTimestamp = ba.Txn.Timestamp
	}
	br, pErr := store.Send(ctx, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(store, br))
	}
	return br, pErr
}

// lookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupRange(key).
// Returns RangeID and replica on success; RangeKeyMismatch error
// if not found.
// This is only for testing usage; performance doesn't matter.
func (ls *LocalSender) lookupReplica(start, end roachpb.RKey) (rangeID roachpb.RangeID, replica *roachpb.ReplicaDescriptor, err error) {
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
		err = roachpb.NewRangeKeyMismatchError(start.AsRawKey(), end.AsRawKey(), nil)
	}
	return rangeID, replica, err
}

// firstRange implements the rangeDescriptorDB interface. It returns the
// range descriptor which contains KeyMin.
func (ls *LocalSender) firstRange() (*roachpb.RangeDescriptor, error) {
	_, replica, err := ls.lookupReplica(roachpb.RKeyMin, nil)
	if err != nil {
		return nil, err
	}
	store, err := ls.GetStore(replica.StoreID)
	if err != nil {
		return nil, err
	}

	rpl := store.LookupReplica(roachpb.RKeyMin, nil)
	if rpl == nil {
		panic("firstRange found no first range")
	}
	return rpl.Desc(), nil
}

// rangeLookup implements the rangeDescriptorDB interface. It looks up
// the descriptors for the given (meta) key.
func (ls *LocalSender) rangeLookup(key roachpb.RKey, options lookupOptions, _ *roachpb.RangeDescriptor) ([]roachpb.RangeDescriptor, error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Add(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			// key is a meta key, so it's guaranteed not local-prefixed.
			Key: key.AsRawKey(),
		},
		MaxRanges:       1,
		ConsiderIntents: options.considerIntents,
		Reverse:         options.useReverseScan,
	})
	br, pErr := ls.Send(context.Background(), ba)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	return br.Responses[0].GetInner().(*roachpb.RangeLookupResponse).Ranges, nil
}
