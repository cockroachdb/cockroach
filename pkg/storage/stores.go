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

package storage

import (
	"fmt"

	"golang.org/x/net/context"
	"golang.org/x/sync/syncmap"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// Stores provides methods to access a collection of stores. There's
// a visitor pattern and also an implementation of the client.Sender
// interface which directs a call to the appropriate store based on
// the call's key range. Stores also implements the gossip.Storage
// interface, which allows gossip bootstrap information to be
// persisted consistently to every store and the most recent bootstrap
// information to be read at node startup.
type Stores struct {
	log.AmbientContext
	clock    *hlc.Clock
	storeMap syncmap.Map // map[roachpb.StoreID]*Store
	mu       struct {
		syncutil.Mutex
		biLatestTS hlc.Timestamp         // Timestamp of gossip bootstrap info
		latestBI   *gossip.BootstrapInfo // Latest cached bootstrap info
	}
}

var _ client.Sender = &Stores{}  // Stores implements the client.Sender interface
var _ gossip.Storage = &Stores{} // Stores implements the gossip.Storage interface

// NewStores returns a local-only sender which directly accesses
// a collection of stores.
func NewStores(ambient log.AmbientContext, clock *hlc.Clock) *Stores {
	return &Stores{
		AmbientContext: ambient,
		clock:          clock,
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *Stores) GetStoreCount() int {
	var count int
	ls.storeMap.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

// HasStore returns true if the specified store is owned by this Stores.
func (ls *Stores) HasStore(storeID roachpb.StoreID) bool {
	_, ok := ls.storeMap.Load(storeID)
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *Stores) GetStore(storeID roachpb.StoreID) (*Store, error) {
	if value, ok := ls.storeMap.Load(storeID); ok {
		return value.(*Store), nil
	}
	return nil, roachpb.NewStoreNotFoundError(storeID)
}

// AddStore adds the specified store to the store map.
func (ls *Stores) AddStore(s *Store) {
	if _, loaded := ls.storeMap.LoadOrStore(s.Ident.StoreID, s); loaded {
		panic(fmt.Sprintf("cannot add store twice: %+v", s.Ident))
	}
	// If we've already read the gossip bootstrap info, ensure that
	// all stores have the most recent values.
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.mu.biLatestTS != (hlc.Timestamp{}) {
		if err := ls.updateBootstrapInfoLocked(ls.mu.latestBI); err != nil {
			ctx := ls.AnnotateCtx(context.TODO())
			log.Errorf(ctx, "failed to update bootstrap info on newly added store: %s", err)
		}
	}
}

// RemoveStore removes the specified store from the store map.
func (ls *Stores) RemoveStore(s *Store) {
	ls.storeMap.Delete(s.Ident.StoreID)
}

// VisitStores implements a visitor pattern over stores in the
// storeMap. The specified function is invoked with each store in
// turn. Care is taken to invoke the visitor func without the lock
// held to avoid inconsistent lock orderings, as some visitor
// functions may call back into the Stores object. Stores are visited
// in random order.
func (ls *Stores) VisitStores(visitor func(s *Store) error) error {
	var err error
	ls.storeMap.Range(func(k, v interface{}) bool {
		err = visitor(v.(*Store))
		return err == nil
	})
	return err
}

// Send implements the client.Sender interface. The store is looked up from the
// store map if specified by the request; otherwise, the command is being
// executed locally, and the replica is determined via lookup through each
// store's LookupRange method. The latter path is taken only by unit tests.
func (ls *Stores) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If we aren't given a Replica, then a little bending over
	// backwards here. This case applies exclusively to unittests.
	if ba.RangeID == 0 || ba.Replica.StoreID == 0 {
		rs, err := keys.Range(ba)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		rangeID, repDesc, err := ls.LookupReplica(rs.Key, rs.EndKey)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		ba.RangeID = rangeID
		ba.Replica = repDesc
	}

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
				log.Event(ctx, "read has no clock uncertainty")
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

// LookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupReplica(key).
// Returns RangeID and replica on success; RangeKeyMismatch error
// if not found.
// If end is nil, a replica containing start is looked up.
// This is only for testing usage; performance doesn't matter.
func (ls *Stores) LookupReplica(
	start, end roachpb.RKey,
) (roachpb.RangeID, roachpb.ReplicaDescriptor, error) {
	var rangeID roachpb.RangeID
	var repDesc roachpb.ReplicaDescriptor
	var repDescFound bool
	var err error
	ls.storeMap.Range(func(k, v interface{}) bool {
		store := v.(*Store)
		replica := store.LookupReplica(start, nil)
		if replica == nil {
			return true
		}

		// Verify that the descriptor contains the entire range.
		if desc := replica.Desc(); !desc.ContainsKeyRange(start, end) {
			ctx := ls.AnnotateCtx(context.TODO())
			log.Warningf(ctx, "range not contained in one range: [%s,%s), but have [%s,%s)",
				start, end, desc.StartKey, desc.EndKey)
			err = roachpb.NewRangeKeyMismatchError(start.AsRawKey(), end.AsRawKey(), desc)
			return false
		}

		rangeID = replica.RangeID

		repDesc, err = replica.GetReplicaDescriptor()
		if err != nil {
			if _, ok := err.(*roachpb.RangeNotFoundError); ok {
				// We are not holding a lock across this block; the replica could have
				// been removed from the range (via down-replication) between the
				// LookupReplica and the GetReplicaDescriptor calls. In this case just
				// ignore this replica.
				err = nil
			}
			return err == nil
		}

		if repDescFound {
			// We already found the range; this should never happen outside of tests.
			err = errors.Errorf("range %+v exists on additional store: %+v", replica, store)
			return false
		}

		repDescFound = true
		return true // loop to see if another store also contains the replica
	})
	if err != nil {
		return 0, roachpb.ReplicaDescriptor{}, err
	}
	if !repDescFound {
		return 0, roachpb.ReplicaDescriptor{}, roachpb.NewRangeNotFoundError(0)
	}
	return rangeID, repDesc, nil
}

// FirstRange implements the RangeDescriptorDB interface. It returns the
// range descriptor which contains KeyMin.
func (ls *Stores) FirstRange() (*roachpb.RangeDescriptor, error) {
	_, repDesc, err := ls.LookupReplica(roachpb.RKeyMin, nil)
	if err != nil {
		return nil, err
	}
	store, err := ls.GetStore(repDesc.StoreID)
	if err != nil {
		return nil, err
	}

	rpl := store.LookupReplica(roachpb.RKeyMin, nil)
	if rpl == nil {
		panic("firstRange found no first range")
	}
	return rpl.Desc(), nil
}

// ReadBootstrapInfo implements the gossip.Storage interface. Read
// attempts to read gossip bootstrap info from every known store and
// finds the most recent from all stores to initialize the bootstrap
// info argument. Returns an error on any issues reading data for the
// stores (but excluding the case in which no data has been persisted
// yet).
func (ls *Stores) ReadBootstrapInfo(bi *gossip.BootstrapInfo) error {
	var latestTS hlc.Timestamp

	ctx := ls.AnnotateCtx(context.TODO())
	var err error

	// Find the most recent bootstrap info.
	ls.storeMap.Range(func(k, v interface{}) bool {
		s := v.(*Store)
		var storeBI gossip.BootstrapInfo
		var ok bool
		ok, err = engine.MVCCGetProto(ctx, s.engine, keys.StoreGossipKey(), hlc.Timestamp{}, true, nil, &storeBI)
		if err != nil {
			return false
		}
		if ok && latestTS.Less(storeBI.Timestamp) {
			latestTS = storeBI.Timestamp
			*bi = storeBI
		}
		return true
	})
	if err != nil {
		return err
	}
	log.Infof(ctx, "read %d node addresses from persistent storage", len(bi.Addresses))

	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.updateBootstrapInfoLocked(bi)
}

// WriteBootstrapInfo implements the gossip.Storage interface. Write
// persists the supplied bootstrap info to every known store. Returns
// nil on success; otherwise returns first error encountered writing
// to the stores.
func (ls *Stores) WriteBootstrapInfo(bi *gossip.BootstrapInfo) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	bi.Timestamp = ls.clock.Now()
	if err := ls.updateBootstrapInfoLocked(bi); err != nil {
		return err
	}
	ctx := ls.AnnotateCtx(context.TODO())
	log.Infof(ctx, "wrote %d node addresses to persistent storage", len(bi.Addresses))
	return nil
}

func (ls *Stores) updateBootstrapInfoLocked(bi *gossip.BootstrapInfo) error {
	if bi.Timestamp.Less(ls.mu.biLatestTS) {
		return nil
	}
	ctx := ls.AnnotateCtx(context.TODO())
	// Update the latest timestamp and set cached version.
	ls.mu.biLatestTS = bi.Timestamp
	ls.mu.latestBI = protoutil.Clone(bi).(*gossip.BootstrapInfo)
	// Update all stores.
	var err error
	ls.storeMap.Range(func(k, v interface{}) bool {
		s := v.(*Store)
		err = engine.MVCCPutProto(ctx, s.engine, nil, keys.StoreGossipKey(), hlc.Timestamp{}, nil, bi)
		return err == nil
	})
	return err
}
