// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
	storeMap syncutil.Map[roachpb.StoreID, Store]

	mu struct {
		syncutil.Mutex
		biLatestTS hlc.Timestamp         // Timestamp of gossip bootstrap info
		latestBI   *gossip.BootstrapInfo // Latest cached bootstrap info
	}
}

var _ kv.Sender = &Stores{}      // Stores implements the client.Sender interface
var _ gossip.Storage = &Stores{} // Stores implements the gossip.Storage interface

// NewStores returns a local-only sender which directly accesses
// a collection of stores.
func NewStores(ambient log.AmbientContext, clock *hlc.Clock) *Stores {
	return &Stores{
		AmbientContext: ambient,
		clock:          clock,
	}
}

// IsMeta1Leaseholder returns whether the specified stores owns
// the meta1 lease. Returns an error if any.
func (ls *Stores) IsMeta1Leaseholder(ctx context.Context, now hlc.ClockTimestamp) (bool, error) {
	repl, _, err := ls.GetReplicaForRangeID(ctx, 1)
	if kvpb.IsRangeNotFoundError(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return repl.OwnsValidLease(ctx, now), nil
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *Stores) GetStoreCount() int {
	var count int
	ls.storeMap.Range(func(_ roachpb.StoreID, _ *Store) bool {
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
		return value, nil
	}
	return nil, kvpb.NewStoreNotFoundError(storeID)
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
	if !ls.mu.biLatestTS.IsEmpty() {
		if err := ls.updateBootstrapInfoLocked(ls.mu.latestBI); err != nil {
			ctx := ls.AnnotateCtx(context.TODO())
			log.Errorf(ctx, "failed to update bootstrap info on newly added store: %+v", err)
		}
	}
}

// RemoveStore removes the specified store from the store map.
func (ls *Stores) RemoveStore(s *Store) {
	ls.storeMap.Delete(s.Ident.StoreID)
}

// ForwardSideTransportClosedTimestampForRange forwards the side-transport
// closed timestamp for the local replicas of the given range.
func (ls *Stores) ForwardSideTransportClosedTimestampForRange(
	ctx context.Context, rangeID roachpb.RangeID, closedTS hlc.Timestamp, lai kvpb.LeaseAppliedIndex,
) {
	if err := ls.VisitStores(func(s *Store) error {
		r := s.GetReplicaIfExists(rangeID)
		if r != nil {
			r.ForwardSideTransportClosedTimestamp(ctx, closedTS, lai)
		}
		return nil
	}); err != nil {
		log.Fatalf(ctx, "unexpected error: %s", err)
	}
}

// VisitStores implements a visitor pattern over stores in the
// storeMap. The specified function is invoked with each store in
// turn. Care is taken to invoke the visitor func without the lock
// held to avoid inconsistent lock orderings, as some visitor
// functions may call back into the Stores object. Stores are visited
// in random order.
func (ls *Stores) VisitStores(visitor func(s *Store) error) error {
	var err error
	ls.storeMap.Range(func(k roachpb.StoreID, v *Store) bool {
		err = visitor(v)
		return err == nil
	})
	return err
}

// GetReplicaForRangeID returns the replica and store which contains the
// specified range. If the replica is not found on any store then
// kvpb.RangeNotFoundError will be returned.
func (ls *Stores) GetReplicaForRangeID(
	ctx context.Context, rangeID roachpb.RangeID,
) (*Replica, *Store, error) {
	var replica *Replica
	var store *Store
	if err := ls.VisitStores(func(s *Store) error {
		r := s.GetReplicaIfExists(rangeID)
		if r != nil {
			replica, store = r, s
		}
		return nil
	}); err != nil {
		log.Fatalf(ctx, "unexpected error: %s", err)
	}
	if replica == nil {
		return nil, nil, kvpb.NewRangeNotFoundError(rangeID, 0)
	}
	return replica, store, nil
}

// Send implements the client.Sender interface. The store is looked up from the
// store map using the ID specified in the request.
func (ls *Stores) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	br, writeBytes, pErr := ls.SendWithWriteBytes(ctx, ba)
	writeBytes.Release()
	return br, pErr
}

// SendWithWriteBytes is the implementation of Send with an additional
// *StoreWriteBytes return value.
func (ls *Stores) SendWithWriteBytes(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvadmission.StoreWriteBytes, *kvpb.Error) {
	if err := ba.ValidateForEvaluation(); err != nil {
		return nil, nil, kvpb.NewError(errors.Wrapf(err, "invalid batch (%s)", ba))
	}

	store, err := ls.GetStore(ba.Replica.StoreID)
	if err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	br, writeBytes, pErr := store.SendWithWriteBytes(ctx, ba)
	if br != nil && br.Error != nil {
		panic(kvpb.ErrorUnexpectedlySet(store, br))
	}
	return br, writeBytes, pErr
}

// RangeFeed registers a rangefeed over the specified span. It sends
// updates to the provided stream and returns a future with an optional error
// when the rangefeed is complete.
func (ls *Stores) RangeFeed(
	streamCtx context.Context,
	args *kvpb.RangeFeedRequest,
	stream rangefeed.Stream,
	perConsumerCatchupLimiter *limit.ConcurrentRequestLimiter,
) (rangefeed.Disconnector, error) {
	if args.RangeID == 0 {
		log.Fatal(streamCtx, "rangefeed request missing range ID")
	} else if args.Replica.StoreID == 0 {
		log.Fatal(streamCtx, "rangefeed request missing store ID")
	}

	store, err := ls.GetStore(args.Replica.StoreID)
	if err != nil {
		return nil, err
	}

	return store.RangeFeed(streamCtx, args, stream, perConsumerCatchupLimiter)
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
	ls.storeMap.Range(func(_ roachpb.StoreID, s *Store) bool {
		var storeBI gossip.BootstrapInfo
		var ok bool
		// TODO(sep-raft-log): probably state engine since it's random data
		// with no durability guarantees.
		ok, err = storage.MVCCGetProto(ctx, s.TODOEngine(), keys.StoreGossipKey(), hlc.Timestamp{}, &storeBI,
			storage.MVCCGetOptions{})
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
	ls.storeMap.Range(func(_ roachpb.StoreID, s *Store) bool {
		// TODO(sep-raft-log): see ReadBootstrapInfo.
		err = storage.MVCCPutProto(ctx, s.TODOEngine(), keys.StoreGossipKey(), hlc.Timestamp{}, bi, storage.MVCCWriteOptions{})
		return err == nil
	})
	return err
}

// DiskStatsMonitor abstracts disk.Monitor for testing purposes.
type DiskStatsMonitor interface {
	CumulativeStats() (disk.Stats, error)
	Clone() *disk.Monitor
	Close()
}

// RegisterDiskMonitors injects a monitor into each store to track an
// individual disk's stats.
func (ls *Stores) RegisterDiskMonitors(diskMonitors map[roachpb.StoreID]DiskStatsMonitor) error {
	return ls.VisitStores(func(s *Store) error {
		if monitor, ok := diskMonitors[s.StoreID()]; ok {
			// The disk monitor is not thread-safe, so we create a clone that can be
			// queried in parallel for per-store stats.
			s.diskMonitor = monitor.Clone()
		}
		return nil
	})
}

func (ls *Stores) CloseDiskMonitors() {
	_ = ls.VisitStores(func(s *Store) error {
		if s.diskMonitor != nil {
			s.diskMonitor.Close()
		}
		return nil
	})
}

// GetStoreMetricRegistry returns the metric registry of the provided store ID.
func (ls *Stores) GetStoreMetricRegistry(storeID roachpb.StoreID) *metric.Registry {
	if s, ok := ls.storeMap.Load(storeID); ok {
		return s.Registry()
	}
	return nil
}
