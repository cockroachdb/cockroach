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
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
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
	latestBI   *gossip.BootstrapInfo      // Latest cached bootstrap info
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
func (ls *Stores) GetStore(storeID roachpb.StoreID) (*Store, error) {
	ls.mu.RLock()
	store, ok := ls.storeMap[storeID]
	ls.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store %d not found", storeID)
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
		if err := ls.updateBootstrapInfo(ls.latestBI); err != nil {
			log.Errorf("failed to update bootstrap info on newly added store: %s", err)
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
	// If we aren't given a Replica, then a little bending over
	// backwards here. This case applies exclusively to unittests.
	if ba.RangeID == 0 || ba.Replica.StoreID == 0 {
		rs, err := keys.Range(ba)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		rangeID, repl, err := ls.lookupReplica(rs.Key, rs.EndKey)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		ba.RangeID = rangeID
		ba.Replica = *repl
	}

	ctx = log.Add(ctx,
		log.RangeID, ba.RangeID)

	store, err := ls.GetStore(ba.Replica.StoreID)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if ba.Txn != nil {
		// For calls that read data within a txn, we keep track of timestamps
		// observed from the various participating nodes' HLC clocks. If we have
		// a timestamp on file for this Node which is smaller than MaxTimestamp,
		// we can lower MaxTimestamp accordingly. If MaxTimestamp drops below
		// OrigTimestamp, we effectively can't see uncertainty restarts any
		// more.
		// Note that it's not an issue if MaxTimestamp propagates back out to
		// the client via a returned Transaction update - when updating a Txn
		// from another, the larger MaxTimestamp wins.
		if maxTS, ok := ba.Txn.GetObservedTimestamp(ba.Replica.NodeID); ok && maxTS.Less(ba.Txn.MaxTimestamp) {
			// Copy-on-write to protect others we might be sharing the Txn with.
			shallowTxn := *ba.Txn
			// The uncertainty window is [OrigTimestamp, maxTS), so if that window
			// is empty, there won't be any uncertainty restarts.
			if !ba.Txn.OrigTimestamp.Less(maxTS) {
				log.Trace(ctx, "read has no clock uncertainty")
			}
			shallowTxn.MaxTimestamp.Backward(maxTS)
			ba.Txn = &shallowTxn
		}
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
func (ls *Stores) lookupReplica(start, end roachpb.RKey) (rangeID roachpb.RangeID, replica *roachpb.ReplicaDescriptor, err error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	var rng *Replica
	var partialDesc *roachpb.RangeDescriptor
	for _, store := range ls.storeMap {
		rng = store.LookupReplica(start, end)
		if rng == nil {
			if tmpRng := store.LookupReplica(start, nil); tmpRng != nil {
				log.Warningf("range not contained in one range: [%s,%s), but have [%s,%s)",
					start, end, tmpRng.Desc().StartKey, tmpRng.Desc().EndKey)
				partialDesc = tmpRng.Desc()
				break
			}
			continue
		}
		if replica == nil {
			rangeID = rng.RangeID
			replica = rng.GetReplica()
			continue
		}
		// Should never happen outside of tests.
		return 0, nil, util.Errorf(
			"range %+v exists on additional store: %+v", rng, store)
	}
	if replica == nil {
		err = roachpb.NewRangeKeyMismatchError(start.AsRawKey(), end.AsRawKey(), partialDesc)
	}
	return rangeID, replica, err
}

// FirstRange implements the RangeDescriptorDB interface. It returns the
// range descriptor which contains KeyMin.
func (ls *Stores) FirstRange() (*roachpb.RangeDescriptor, *roachpb.Error) {
	_, replica, err := ls.lookupReplica(roachpb.RKeyMin, nil)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	store, err := ls.GetStore(replica.StoreID)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	rpl := store.LookupReplica(roachpb.RKeyMin, nil)
	if rpl == nil {
		panic("firstRange found no first range")
	}
	return rpl.Desc(), nil
}

// RangeLookup implements the RangeDescriptorDB interface. It looks up
// the descriptors for the given (meta) key.
func (ls *Stores) RangeLookup(
	key roachpb.RKey, _ *roachpb.RangeDescriptor, considerIntents, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
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
	br, pErr := ls.Send(context.TODO(), ba)
	if pErr != nil {
		return nil, nil, pErr
	}
	resp := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	return resp.Ranges, resp.PrefetchedRanges, nil
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
	latestTS := roachpb.ZeroTimestamp

	// Find the most recent bootstrap info.
	for _, s := range ls.storeMap {
		var storeBI gossip.BootstrapInfo
		ok, err := engine.MVCCGetProto(context.Background(), s.engine, keys.StoreGossipKey(), roachpb.ZeroTimestamp, true, nil, &storeBI)
		if err != nil {
			return err
		}
		if ok && latestTS.Less(storeBI.Timestamp) {
			latestTS = storeBI.Timestamp
			*bi = storeBI
		}
	}
	log.Infof("read %d node addresses from persistent storage", len(bi.Addresses))
	return ls.updateBootstrapInfo(bi)
}

// WriteBootstrapInfo implements the gossip.Storage interface. Write
// persists the supplied bootstrap info to every known store. Returns
// nil on success; otherwise returns first error encountered writing
// to the stores.
func (ls *Stores) WriteBootstrapInfo(bi *gossip.BootstrapInfo) error {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	bi.Timestamp = ls.clock.Now()
	if err := ls.updateBootstrapInfo(bi); err != nil {
		return err
	}
	log.Infof("wrote %d node addresses to persistent storage", len(bi.Addresses))
	return nil
}

func (ls *Stores) updateBootstrapInfo(bi *gossip.BootstrapInfo) error {
	if bi.Timestamp.Less(ls.biLatestTS) {
		return nil
	}
	// Update the latest timestamp and set cached version.
	ls.biLatestTS = bi.Timestamp
	ls.latestBI = protoutil.Clone(bi).(*gossip.BootstrapInfo)
	// Update all stores.
	for _, s := range ls.storeMap {
		if err := engine.MVCCPutProto(context.Background(), s.engine, nil, keys.StoreGossipKey(), roachpb.ZeroTimestamp, nil, bi); err != nil {
			return err
		}
	}
	return nil
}
