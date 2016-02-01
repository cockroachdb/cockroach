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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracer"
)

// A Stores provides methods to access a collection of stores. There's
// a visitor pattern and also an implementation of the client.Sender
// interface which directs a call to the appropriate store based on
// the call's key range. Stores also implements the gossip.Storage
// interface, which allows gossip bootstrap information to be
// persisted consistently to every store and the most recent bootstrap
// information to be read at node startup.
type Stores struct {
	clock      *hlc.Clock
	mu         sync.RWMutex               // Protects storeMap and addrs
	storeMap   map[roachpb.StoreID]*Store // Map from StoreID to Store
	biLatestTS roachpb.Timestamp          // Timestamp of gossip bootstrap info
}

var _ client.Sender = &Stores{}  // Stores implements the client.Sender interface
var _ gossip.Storage = &Stores{} // Stores implements the gossip.Storage interface

// NewStores returns a local-only sender which directly accesses
// a collection of stores.
func NewStores(clock *hlc.Clock) *Stores {
	return &Stores{
		clock:    clock,
		storeMap: map[roachpb.StoreID]*Store{},
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *Stores) GetStoreCount() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return len(ls.storeMap)
}

// HasStore returns true if the specified store is owned by this Stores.
func (ls *Stores) HasStore(storeID roachpb.StoreID) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	_, ok := ls.storeMap[storeID]
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *Stores) GetStore(storeID roachpb.StoreID) (*Store, *roachpb.Error) {
	ls.mu.RLock()
	store, ok := ls.storeMap[storeID]
	ls.mu.RUnlock()
	if !ok {
		return nil, roachpb.NewErrorf("store %d not found", storeID)
	}
	return store, nil
}

// AddStore adds the specified store to the store map.
func (ls *Stores) AddStore(s *Store) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if _, ok := ls.storeMap[s.Ident.StoreID]; ok {
		panic(fmt.Sprintf("cannot add store twice: %+v", s.Ident))
	}
	ls.storeMap[s.Ident.StoreID] = s
	// If we've already read the gossip bootstrap info, ensure that
	// all stores have the most recent values.
	if !ls.biLatestTS.Equal(roachpb.ZeroTimestamp) {
		// ReadBootstrapInfo calls updateAllBootstrapInfos.
		if err := ls.readBootstrapInfoLocked(&gossip.BootstrapInfo{}); err != nil {
			log.Errorf("failed to update bootstrap info on stores: %s", err)
		}
	}
}

// RemoveStore removes the specified store from the store map.
func (ls *Stores) RemoveStore(s *Store) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	delete(ls.storeMap, s.Ident.StoreID)
}

// VisitStores implements a visitor pattern over stores in the storeMap.
// The specified function is invoked with each store in turn. Stores are
// visited in a random order.
func (ls *Stores) VisitStores(visitor func(s *Store) error) error {
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
func (ls *Stores) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	trace := tracer.SpanFromContext(ctx)
	var store *Store
	var pErr *roachpb.Error

	// If we aren't given a Replica, then a little bending over
	// backwards here. This case applies exclusively to unittests.
	if ba.RangeID == 0 || ba.Replica.StoreID == 0 {
		var repl *roachpb.ReplicaDescriptor
		var rangeID roachpb.RangeID
		rs := keys.Range(ba)
		rangeID, repl, pErr = ls.lookupReplica(rs.Key, rs.EndKey)
		if pErr == nil {
			ba.RangeID = rangeID
			ba.Replica = *repl
		}
	}

	ctx = log.Add(ctx,
		log.RangeID, ba.RangeID)

	if pErr == nil {
		store, pErr = ls.GetStore(ba.Replica.StoreID)
	}

	var br *roachpb.BatchResponse
	if pErr != nil {
		return nil, pErr
	}
	// For calls that read data within a txn, we can avoid uncertainty
	// related retries in certain situations. If the node is in
	// "CertainNodes", we need not worry about uncertain reads any
	// more. Setting MaxTimestamp=Timestamp for the operation
	// accomplishes that. See roachpb.Transaction.CertainNodes for details.
	if ba.Txn != nil && ba.Txn.CertainNodes.Contains(ba.Replica.NodeID) {
		// MaxTimestamp = Timestamp corresponds to no clock uncertainty.
		trace.LogEvent("read has no clock uncertainty")
		ba.Txn.MaxTimestamp = ba.Txn.Timestamp
	}
	br, pErr = store.Send(ctx, ba)
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
func (ls *Stores) lookupReplica(start, end roachpb.RKey) (rangeID roachpb.RangeID, replica *roachpb.ReplicaDescriptor, pErr *roachpb.Error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	var rng *Replica
	for _, store := range ls.storeMap {
		rng = store.LookupReplica(start, end)
		if rng == nil {
			if tmpRng := store.LookupReplica(start, nil); tmpRng != nil {
				log.Warningf("range not contained in one range: [%s,%s), but have [%s,%s)",
					start, end, tmpRng.Desc().StartKey, tmpRng.Desc().EndKey)
			}
			continue
		}
		if replica == nil {
			rangeID = rng.RangeID
			replica = rng.GetReplica()
			continue
		}
		// Should never happen outside of tests.
		return 0, nil, roachpb.NewErrorf(
			"range %+v exists on additional store: %+v", rng, store)
	}
	if replica == nil {
		pErr = roachpb.NewError(roachpb.NewRangeKeyMismatchError(start.AsRawKey(), end.AsRawKey(), nil))
	}
	return rangeID, replica, pErr
}

// FirstRange implements the RangeDescriptorDB interface. It returns the
// range descriptor which contains KeyMin.
func (ls *Stores) FirstRange() (*roachpb.RangeDescriptor, *roachpb.Error) {
	_, replica, pErr := ls.lookupReplica(roachpb.RKeyMin, nil)
	if pErr != nil {
		return nil, pErr
	}
	store, pErr := ls.GetStore(replica.StoreID)
	if pErr != nil {
		return nil, pErr
	}

	rpl := store.LookupReplica(roachpb.RKeyMin, nil)
	if rpl == nil {
		panic("firstRange found no first range")
	}
	return rpl.Desc(), nil
}

// RangeLookup implements the RangeDescriptorDB interface. It looks up
// the descriptors for the given (meta) key.
func (ls *Stores) RangeLookup(key roachpb.RKey, _ *roachpb.RangeDescriptor, considerIntents, useReverseScan bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Add(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			// key is a meta key, so it's guaranteed not local-prefixed.
			Key: key.AsRawKey(),
		},
		MaxRanges:       1,
		ConsiderIntents: considerIntents,
		Reverse:         useReverseScan,
	})
	br, pErr := ls.Send(context.Background(), ba)
	if pErr != nil {
		return nil, pErr
	}
	return br.Responses[0].GetInner().(*roachpb.RangeLookupResponse).Ranges, nil
}

// ReadBootstrapInfo implements the gossip.Storage interface. Read
// attempts to read gossip bootstrap info from every known store and
// finds the most recent from all stores to initialize the bootstrap
// info argument. Returns an error on any issues reading data for the
// stores (but excluding the case in which no data has been persisted
// yet).
func (ls *Stores) ReadBootstrapInfo(bi *gossip.BootstrapInfo) error {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.readBootstrapInfoLocked(bi)
}

func (ls *Stores) readBootstrapInfoLocked(bi *gossip.BootstrapInfo) error {
	latestTS := roachpb.ZeroTimestamp
	timestamps := map[roachpb.StoreID]roachpb.Timestamp{}

	// Find the most recent bootstrap info, collecting timestamps for
	// each store along the way.
	for id, s := range ls.storeMap {
		var storeBI gossip.BootstrapInfo
		ok, err := engine.MVCCGetProto(s.engine, keys.StoreGossipKey(), roachpb.ZeroTimestamp, true, nil, &storeBI)
		if err != nil {
			return err
		}
		timestamps[id] = storeBI.Timestamp
		if ok && latestTS.Less(storeBI.Timestamp) {
			latestTS = storeBI.Timestamp
			*bi = storeBI
		}
	}

	// Update all stores with an earlier timestamp.
	for id, s := range ls.storeMap {
		if timestamps[id].Less(latestTS) {
			if err := engine.MVCCPutProto(s.engine, nil, keys.StoreGossipKey(), roachpb.ZeroTimestamp, nil, bi); err != nil {
				return err
			}
			log.Infof("updated gossip bootstrap info to %s", s)
		}
	}

	ls.biLatestTS = latestTS
	return nil
}

// WriteBootstrapInfo implements the gossip.Storage interface. Write
// persists the supplied bootstrap info to every known store. Returns
// nil on success; otherwise returns first error encountered writing
// to the stores.
func (ls *Stores) WriteBootstrapInfo(bi *gossip.BootstrapInfo) error {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	ls.biLatestTS = ls.clock.Now()
	bi.Timestamp = ls.biLatestTS
	for _, s := range ls.storeMap {
		if err := engine.MVCCPutProto(s.engine, nil, keys.StoreGossipKey(), roachpb.ZeroTimestamp, nil, bi); err != nil {
			return err
		}
		log.Infof("wrote gossip bootstrap info to %s", s)
	}
	return nil
}
