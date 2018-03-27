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
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	storeMap syncutil.IntMap // map[roachpb.StoreID]*Store
	// These two versions are usually cluster.BinaryMinimumSupportedVersion and
	// cluster.BinaryServerVersion, respectively. They are changed in some
	// tests.
	minSupportedVersion roachpb.Version
	serverVersion       roachpb.Version
	mu                  struct {
		syncutil.Mutex
		biLatestTS hlc.Timestamp         // Timestamp of gossip bootstrap info
		latestBI   *gossip.BootstrapInfo // Latest cached bootstrap info
	}
}

var _ client.Sender = &Stores{}  // Stores implements the client.Sender interface
var _ gossip.Storage = &Stores{} // Stores implements the gossip.Storage interface

// NewStores returns a local-only sender which directly accesses
// a collection of stores.
func NewStores(
	ambient log.AmbientContext, clock *hlc.Clock, minVersion, serverVersion roachpb.Version,
) *Stores {
	return &Stores{
		AmbientContext:      ambient,
		clock:               clock,
		minSupportedVersion: minVersion,
		serverVersion:       serverVersion,
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *Stores) GetStoreCount() int {
	var count int
	ls.storeMap.Range(func(_ int64, _ unsafe.Pointer) bool {
		count++
		return true
	})
	return count
}

// HasStore returns true if the specified store is owned by this Stores.
func (ls *Stores) HasStore(storeID roachpb.StoreID) bool {
	_, ok := ls.storeMap.Load(int64(storeID))
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *Stores) GetStore(storeID roachpb.StoreID) (*Store, error) {
	if value, ok := ls.storeMap.Load(int64(storeID)); ok {
		return (*Store)(value), nil
	}
	return nil, roachpb.NewStoreNotFoundError(storeID)
}

// AddStore adds the specified store to the store map.
func (ls *Stores) AddStore(s *Store) {
	if _, loaded := ls.storeMap.LoadOrStore(int64(s.Ident.StoreID), unsafe.Pointer(s)); loaded {
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
	ls.storeMap.Delete(int64(s.Ident.StoreID))
}

// VisitStores implements a visitor pattern over stores in the
// storeMap. The specified function is invoked with each store in
// turn. Care is taken to invoke the visitor func without the lock
// held to avoid inconsistent lock orderings, as some visitor
// functions may call back into the Stores object. Stores are visited
// in random order.
func (ls *Stores) VisitStores(visitor func(s *Store) error) error {
	var err error
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		err = visitor((*Store)(v))
		return err == nil
	})
	return err
}

// GetReplicaForRangeID returns the replica which contains the specified range,
// or nil if it's not found.
func (ls *Stores) GetReplicaForRangeID(rangeID roachpb.RangeID) (*Replica, error) {
	var replica *Replica

	err := ls.VisitStores(func(store *Store) error {
		replicaFromStore, err := store.GetReplica(rangeID)

		switch err.(type) {
		case nil:
			replica = replicaFromStore
		case *roachpb.RangeNotFoundError:
			return nil
		default:
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	if replica == nil {
		return nil, roachpb.NewRangeNotFoundError(rangeID)
	}
	return replica, nil
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
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		store := (*Store)(v)
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
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		s := (*Store)(v)
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
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		s := (*Store)(v)
		err = engine.MVCCPutProto(ctx, s.engine, nil, keys.StoreGossipKey(), hlc.Timestamp{}, nil, bi)
		return err == nil
	})
	return err
}

// ReadVersionFromEngineOrDefault reads the persisted cluster version from the
// engine, falling back to v1.0 if no version is specified on the engine.
func ReadVersionFromEngineOrDefault(
	ctx context.Context, e engine.Engine,
) (cluster.ClusterVersion, error) {
	var cv cluster.ClusterVersion
	cv, err := ReadClusterVersion(ctx, e)
	if err != nil {
		return cluster.ClusterVersion{}, err
	}

	// These values should always exist in 1.1-initialized clusters, but may
	// not on 1.0.x; we synthesize the missing version.
	if cv.UseVersion == (roachpb.Version{}) {
		cv.UseVersion = cluster.VersionByKey(cluster.VersionBase)
	}
	if cv.MinimumVersion == (roachpb.Version{}) {
		cv.MinimumVersion = cluster.VersionByKey(cluster.VersionBase)
	}
	return cv, nil
}

// WriteClusterVersionToEngines writes the given version to the given engines,
// without any sanity checks.
func WriteClusterVersionToEngines(
	ctx context.Context, engines []engine.Engine, cv cluster.ClusterVersion,
) error {
	for _, eng := range engines {
		if err := WriteClusterVersion(ctx, eng, cv); err != nil {
			return errors.Wrapf(err, "error writing version to engine %s", eng)
		}
	}
	return nil
}

// SynthesizeClusterVersionFromEngines implements the core of (*Stores).SynthesizeClusterVersion.
func SynthesizeClusterVersionFromEngines(
	ctx context.Context, engines []engine.Engine, minSupportedVersion, serverVersion roachpb.Version,
) (cluster.ClusterVersion, error) {
	// Find the most recent bootstrap info.
	type originVersion struct {
		roachpb.Version
		origin string
	}

	// FIXME(tschottdorf): If we don't find anything, this should return v1.0, but
	// we have to guarantee first that you always find something (should be OK).
	maxMinVersion := originVersion{
		Version: minSupportedVersion,
		origin:  "(no store)",
	}

	maxPossibleVersion := roachpb.Version{Major: 999999} // sort above any real version
	minUseVersion := originVersion{
		Version: maxPossibleVersion,
		origin:  "(no store)",
	}

	// We run this twice because only after having seen all the versions, we
	// can decide whether the node catches a version error. However, we also
	// want to name at least one engine that violates the version
	// constraints, which at the latest the second loop will achieve
	// (because then minUseVersion and maxMinVersion don't change any more).
	for _, eng := range engines {
		var cv cluster.ClusterVersion
		cv, err := ReadVersionFromEngineOrDefault(ctx, eng)
		if err != nil {
			return cluster.ClusterVersion{}, err
		}

		// Avoid running a binary with a store that is too new. For example,
		// restarting into 1.1 after having upgraded to 1.2 doesn't work.
		for _, v := range []roachpb.Version{cv.MinimumVersion, cv.UseVersion} {
			if serverVersion.Less(v) {
				return cluster.ClusterVersion{}, errors.Errorf("cockroach version v%s is incompatible with data in store %s; use version v%s or later",
					serverVersion, eng, v)
			}
		}

		// Track the highest minimum version encountered.
		if maxMinVersion.Version.Less(cv.MinimumVersion) {
			maxMinVersion.Version = cv.MinimumVersion
			maxMinVersion.origin = fmt.Sprint(eng)

		}
		// Track smallest use version encountered.
		if cv.UseVersion.Less(minUseVersion.Version) {
			minUseVersion.Version = cv.UseVersion
			minUseVersion.origin = fmt.Sprint(eng)
		}
	}

	// If no use version was found, fall back to our
	// minSupportedVersion. This is the case when a brand new node is
	// joining an existing cluster (which may be on any older version
	// this binary supports).
	if minUseVersion.Version == maxPossibleVersion {
		minUseVersion.Version = minSupportedVersion
	}

	cv := cluster.ClusterVersion{
		UseVersion:     minUseVersion.Version,
		MinimumVersion: maxMinVersion.Version,
	}
	log.Eventf(ctx, "read ClusterVersion %+v", cv)

	for _, v := range []originVersion{minUseVersion, maxMinVersion} {
		// Avoid running a binary too new for this store. This is what you'd catch
		// if, say, you restarted directly from 1.0 into 1.2 (bumping the min
		// version) without going through 1.1 first. It would also what you catch if
		// you are starting 1.1 for the first time (after 1.0), but it crashes
		// half-way through the startup sequence (so now some stores have 1.1, but
		// some 1.0), in which case you are expected to run 1.1 again (hopefully
		// without the crash this time) which would then rewrite all the stores.
		//
		// We only verify this now because as we iterate through the stores, we
		// may not yet have picked up the final versions we're actually planning
		// to use.
		if v.Version.Less(minSupportedVersion) {
			return cluster.ClusterVersion{}, errors.Errorf("store %s, last used with cockroach version v%s, "+
				"is too old for running version v%s (which requires data from v%s or later)",
				v.origin, v.Version, serverVersion, minSupportedVersion)
		}
	}
	// Write the "actual" version back to all stores. This is almost always a
	// no-op, but will backfill the information for 1.0.x clusters, and also
	// smoothens out inconsistent state that can crop up during an ill-timed
	// crash or when new stores are being added.
	return cv, WriteClusterVersionToEngines(ctx, engines, cv)
}

// SynthesizeClusterVersion reads and returns the ClusterVersion protobuf
// (written to any of the configured stores (all of which are bootstrapped)).
// The returned value is also replicated to all stores for consistency, in case
// a new store was added or an old store re-configured. In case of non-identical
// versions across the stores, returns a version that carries the largest
// MinVersion and the smallest UseVersion.
//
// If there aren't any stores, returns a ClusterVersion with MinSupportedVersion
// and UseVersion set to the minimum supported version and server version of the
// build, respectively.
func (ls *Stores) SynthesizeClusterVersion(ctx context.Context) (cluster.ClusterVersion, error) {
	var engines []engine.Engine
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		engines = append(engines, (*Store)(v).engine)
		return true // want more
	})
	cv, err := SynthesizeClusterVersionFromEngines(ctx, engines, ls.minSupportedVersion, ls.serverVersion)
	if err != nil {
		return cluster.ClusterVersion{}, err
	}
	return cv, nil
}

// WriteClusterVersion persists the supplied ClusterVersion to every
// configured store. Returns nil on success; otherwise returns first
// error encountered writing to the stores.
//
// WriteClusterVersion makes no attempt to validate the supplied version.
func (ls *Stores) WriteClusterVersion(ctx context.Context, cv cluster.ClusterVersion) error {
	// Update all stores.
	engines := ls.engines()
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		engines = append(engines, (*Store)(v).Engine())
		return true // want more
	})
	return WriteClusterVersionToEngines(ctx, engines, cv)
}

func (ls *Stores) engines() []engine.Engine {
	var engines []engine.Engine
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		engines = append(engines, (*Store)(v).Engine())
		return true // want more
	})
	return engines
}

// OnClusterVersionChange is invoked when the running node receives a notification
// indicating that the cluster version has changed.
func (ls *Stores) OnClusterVersionChange(ctx context.Context, cv cluster.ClusterVersion) error {
	if err := ls.WriteClusterVersion(ctx, cv); err != nil {
		return errors.Wrap(err, "writing cluster version")
	}

	return nil
}
