// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/raft/v3/raftpb"
)

// FirstNodeID is the NodeID assigned to the node bootstrapping a new cluster.
const FirstNodeID = roachpb.NodeID(1)

// FirstStoreID is the StoreID assigned to the first store on the node with ID
// FirstNodeID.
const FirstStoreID = roachpb.StoreID(1)

// InitEngine writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty save for a cluster version, which must
// already have been persisted to it. Returns an error if this is not
// the case.
func InitEngine(ctx context.Context, eng storage.Engine, ident roachpb.StoreIdent) error {
	exIdent, err := ReadStoreIdent(ctx, eng)
	if err == nil {
		return errors.Errorf("engine %s is already initialized with ident %s", eng, exIdent.String())
	}
	if !errors.HasType(err, (*NotBootstrappedError)(nil)) {
		return err
	}

	if err := checkCanInitializeEngine(ctx, eng); err != nil {
		return errors.Wrap(err, "while trying to initialize engine")
	}

	batch := eng.NewBatch()
	if err := storage.MVCCPutProto(
		ctx,
		batch,
		nil,
		keys.StoreIdentKey(),
		hlc.Timestamp{},
		hlc.ClockTimestamp{},
		nil,
		&ident,
	); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(true /* sync */); err != nil {
		return errors.Wrap(err, "persisting engine initialization data")
	}

	return nil
}

// checkCanInitializeEngine ensures that the engine is empty except for a
// cluster version, which must be present.
func checkCanInitializeEngine(ctx context.Context, eng storage.Engine) error {
	// See if this is an already-bootstrapped store.
	ident, err := ReadStoreIdent(ctx, eng)
	if err == nil {
		return errors.Errorf("engine already initialized as %s", ident.String())
	} else if !errors.HasType(err, (*NotBootstrappedError)(nil)) {
		return errors.Wrap(err, "unable to read store ident")
	}
	// Engine is not bootstrapped yet (i.e. no StoreIdent). Does it contain a
	// cluster version, cached settings and nothing else? Note that there is one
	// cluster version key and many cached settings key, and the cluster version
	// key precedes the cached settings.
	//
	// We use an EngineIterator to ensure that there are no keys that cannot be
	// parsed as MVCCKeys (e.g. lock table keys) in the engine.
	iter := eng.NewEngineIterator(storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		UpperBound: roachpb.KeyMax,
	})
	defer iter.Close()
	valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: roachpb.KeyMin})
	if !valid {
		if err == nil {
			return errors.New("no cluster version found on uninitialized engine")
		}
		return err
	}
	getMVCCKey := func() (storage.MVCCKey, error) {
		if _, hasRange := iter.HasPointAndRange(); hasRange {
			bounds, err := iter.EngineRangeBounds()
			if err != nil {
				return storage.MVCCKey{}, err
			}
			return storage.MVCCKey{}, errors.Errorf("found mvcc range key: %s", bounds)
		}
		var k storage.EngineKey
		k, err = iter.EngineKey()
		if err != nil {
			return storage.MVCCKey{}, err
		}
		if !k.IsMVCCKey() {
			return storage.MVCCKey{}, errors.Errorf("found non-mvcc key: %s", k)
		}
		return k.ToMVCCKey()
	}
	var k storage.MVCCKey
	if k, err = getMVCCKey(); err != nil {
		return err
	}
	if !k.Key.Equal(keys.StoreClusterVersionKey()) {
		return errors.New("no cluster version found on uninitialized engine")
	}
	valid, err = iter.NextEngineKey()
	for valid {
		// Only allowed to find cached cluster settings on an uninitialized
		// engine.
		if k, err = getMVCCKey(); err != nil {
			return err
		}
		if _, err := keys.DecodeStoreCachedSettingsKey(k.Key); err != nil {
			return errors.Errorf("engine cannot be bootstrapped, contains key:\n%s", k.String())
		}
		// There may be more cached cluster settings, so continue iterating.
		valid, err = iter.NextEngineKey()
	}
	return err
}

// IterateIDPrefixKeys helps visit system keys that use RangeID prefixing (such
// as RaftHardStateKey, RangeTombstoneKey, and many others). Such keys could in
// principle exist at any RangeID, and this helper efficiently discovers all the
// keys of the desired type (as specified by the supplied `keyFn`) and, for each
// key-value pair discovered, unmarshals it into `msg` and then invokes `f`.
//
// Iteration stops on the first error (and will pass through that error).
func IterateIDPrefixKeys(
	ctx context.Context,
	reader storage.Reader,
	keyFn func(roachpb.RangeID) roachpb.Key,
	msg protoutil.Message,
	f func(_ roachpb.RangeID) error,
) error {
	rangeID := roachpb.RangeID(1)
	// NB: Range-ID local keys have no versions and no intents.
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: keys.LocalRangeIDPrefix.PrefixEnd().AsRawKey(),
	})
	defer iter.Close()

	for {
		bumped := false
		mvccKey := storage.MakeMVCCMetadataKey(keyFn(rangeID))
		iter.SeekGE(mvccKey)

		if ok, err := iter.Valid(); !ok {
			return err
		}

		unsafeKey := iter.UnsafeKey()

		if !bytes.HasPrefix(unsafeKey.Key, keys.LocalRangeIDPrefix) {
			// Left the local keyspace, so we're done.
			return nil
		}

		curRangeID, _, _, _, err := keys.DecodeRangeIDKey(unsafeKey.Key)
		if err != nil {
			return err
		}

		if curRangeID > rangeID {
			// `bumped` is always `false` here, but let's be explicit.
			if !bumped {
				rangeID = curRangeID
				bumped = true
			}
			mvccKey = storage.MakeMVCCMetadataKey(keyFn(rangeID))
		}

		if !unsafeKey.Key.Equal(mvccKey.Key) {
			if !bumped {
				// Don't increment the rangeID if it has already been incremented
				// above, or we could skip past a value we ought to see.
				rangeID++
				bumped = true // for completeness' sake; continuing below anyway
			}
			continue
		}

		ok, err := storage.MVCCGetProto(
			ctx, reader, unsafeKey.Key, hlc.Timestamp{}, msg, storage.MVCCGetOptions{})
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("unable to unmarshal %s into %T", unsafeKey.Key, msg)
		}

		if err := f(rangeID); err != nil {
			return iterutil.Map(err)
		}
		rangeID++
	}
}

// ReadStoreIdent reads the StoreIdent from the store.
// It returns *NotBootstrappedError if the ident is missing (meaning that the
// store needs to be bootstrapped).
func ReadStoreIdent(ctx context.Context, eng storage.Engine) (roachpb.StoreIdent, error) {
	var ident roachpb.StoreIdent
	ok, err := storage.MVCCGetProto(
		ctx, eng, keys.StoreIdentKey(), hlc.Timestamp{}, &ident, storage.MVCCGetOptions{})
	if err != nil {
		return roachpb.StoreIdent{}, err
	} else if !ok {
		return roachpb.StoreIdent{}, &NotBootstrappedError{}
	}
	return ident, err
}

// IterateRangeDescriptorsFromDisk discovers the initialized replicas and calls
// the provided function with each such descriptor from the provided Engine. The
// return values of this method and fn have semantics similar to
// storage.MVCCIterate.
func IterateRangeDescriptorsFromDisk(
	ctx context.Context, reader storage.Reader, fn func(desc roachpb.RangeDescriptor) error,
) error {
	log.Event(ctx, "beginning range descriptor iteration")
	// MVCCIterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	allCount := 0
	matchCount := 0
	bySuffix := make(map[redact.RedactableString]int)
	kvToDesc := func(kv roachpb.KeyValue) error {
		allCount++
		// Only consider range metadata entries; ignore others.
		startKey, suffix, _, err := keys.DecodeRangeKey(kv.Key)
		if err != nil {
			return err
		}
		bySuffix[redact.RedactableString(suffix)]++
		if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
			return nil
		}
		var desc roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&desc); err != nil {
			return err
		}
		// Descriptor for range `[a,z)` must be found at `/rdsc/a`.
		if !startKey.Equal(desc.StartKey.AsRawKey()) {
			return errors.AssertionFailedf("descriptor stored at %s but has StartKey %s",
				kv.Key, desc.StartKey)
		}
		if !desc.IsInitialized() {
			return errors.AssertionFailedf("uninitialized descriptor: %s", desc)
		}
		matchCount++
		err = fn(desc)
		if err == nil {
			return nil
		}
		if iterutil.Map(err) == nil {
			return iterutil.StopIteration()
		}
		return err
	}

	_, err := storage.MVCCIterate(ctx, reader, start, end, hlc.MaxTimestamp,
		storage.MVCCScanOptions{Inconsistent: true}, kvToDesc)
	log.Eventf(ctx, "iterated over %d keys to find %d range descriptors (by suffix: %v)",
		allCount, matchCount, bySuffix)
	return err
}

type EngineReplicas struct {
	Uninitialized map[storage.FullReplicaID]struct{}
	Initialized   map[storage.FullReplicaID]*roachpb.RangeDescriptor
}

// loadFullReplicaIDsFromDisk discovers all Replicas on this Store.
// There will only be one entry in the map for a given RangeID.
//
// Replicas which were created before the RaftReplicaID was
// introduced will be returned with a zero ReplicaID.
//
// TODO(sep-raft-log): the reader here is for the log engine.
func loadFullReplicaIDsFromDisk(
	ctx context.Context, reader storage.Reader,
) (map[storage.FullReplicaID]struct{}, error) {
	m := map[storage.FullReplicaID]struct{}{}
	mBackfill := map[roachpb.RangeID]raftpb.HardState{}
	var msg roachpb.RaftReplicaID
	if err := IterateIDPrefixKeys(ctx, reader, func(rangeID roachpb.RangeID) roachpb.Key {
		return keys.RaftReplicaIDKey(rangeID)
	}, &msg, func(rangeID roachpb.RangeID) error {
		m[storage.FullReplicaID{RangeID: rangeID, ReplicaID: msg.ReplicaID}] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}

	var hs raftpb.HardState
	if err := IterateIDPrefixKeys(ctx, reader, func(rangeID roachpb.RangeID) roachpb.Key {
		return keys.RaftHardStateKey(rangeID)
	}, &hs, func(rangeID roachpb.RangeID) error {
		mBackfill[rangeID] = hs
		return nil
	}); err != nil {
		return nil, err
	}

	// mBackfill := mBackfill - m, i.e. keep only those entries in mBackfill for which
	// we didn't have a persisted RaftReplicaID.
	for id := range m {
		delete(mBackfill, id.RangeID)
	}

	// Patch the resulting mBackfill back into m, with a zero ReplicaID.
	for rangeID := range mBackfill {
		m[storage.FullReplicaID{RangeID: rangeID}] = struct{}{}
	}

	// TODO(sep-raft-log): if there is any other data that we mandate is present here
	// (like a HardState), validate that here.

	return m, nil
}

// LoadAndReconcileReplicas loads the Replicas present on this
// store. It reconciles inconsistent state and runs validation checks.
//
// TOOD(sep-raft-log): consider a callback-visitor pattern here.
func LoadAndReconcileReplicas(ctx context.Context, eng storage.Engine) (*EngineReplicas, error) {
	ident, err := ReadStoreIdent(ctx, eng)
	if err != nil {
		return nil, err
	}

	initM := map[storage.FullReplicaID]*roachpb.RangeDescriptor{}
	// INVARIANT: the latest visible committed version of the RangeDescriptor
	// (which is what IterateRangeDescriptorsFromDisk returns) is the one reflecting
	// the state of the Replica.
	// INVARIANT: the descriptor for range [a,z) is located at RangeDescriptorKey(a).
	// This is checked in IterateRangeDescriptorsFromDisk.
	var sg roachpb.SpanGroup
	if err := IterateRangeDescriptorsFromDisk(
		ctx, eng, func(desc roachpb.RangeDescriptor) error {
			// INVARIANT: a Replica's RangeDescriptor always contains the local Store,
			// i.e. a Store is a member of all of its local Replicas.
			repDesc, found := desc.GetReplicaDescriptor(ident.StoreID)
			if !found {
				return errors.AssertionFailedf(
					"RangeDescriptor does not contain local s%d: %s",
					ident.StoreID, desc)
			}

			sp := desc.RSpan().AsRawSpanWithNoLocals()
			if sg.Sub(sp) {
				// `sp` overlapped `sg`.
				for _, other := range initM {
					if sp.Overlaps(other.RSpan().AsRawSpanWithNoLocals()) {
						return errors.AssertionFailedf("%s overlaps %s", desc, other)
					}
				}
				// If we hit this it's basically a bug in either SpanGroup or
				// the above `for` loop.
				return errors.AssertionFailedf("%s overlaps unknown other descriptor", desc)
			}
			sg.Add(sp)

			initM[storage.FullReplicaID{
				RangeID:   desc.RangeID,
				ReplicaID: repDesc.ReplicaID,
			}] = &desc
			return nil
		}); err != nil {
		return nil, err
	}

	// INVARIANT: all replicas have a persisted full replicaID (i.e. a "replicaID from disk").
	//
	// This invariant is true for replicas created in 22.2. Without further action, it
	// maybe be violated for clusters that originated before 22.2.
	//
	// loadFullReplicaIDsFromDisk returns a zero ReplicaID when none is found.
	//
	// In this method, we migrate to always satisfy the invariant:
	//
	// 1. for initialized replicas, use ReplicaID from RangeDescriptor as RaftReplicaID
	// 2. remove uninitialized replicas lacking a RaftReplicaID.
	//
	// The migration can be removed when the KV host cluster MinSupportedVersion
	// matches or exceeds 23.1 (i.e. once we know that a store has definitely
	// started up on 23.1 at least once).

	allM, err := loadFullReplicaIDsFromDisk(ctx, eng)
	if err != nil {
		return nil, err
	}

	for id := range allM {
		if id.ReplicaID != 0 {
			// RaftReplicaID was present, no need to backfill.
			continue
		}

	}

	for id := range initM {
		// `allM` will be our map of uninitialized replicas when this loop is done.
		//
		// A replica is "uninitialized" if it's not in initM (i.e. is at log position
		// zero and has no visible RangeDescriptor).

		// If a ReplicaRaftID was not persisted yet, allM will have a zero ReplicaID
		// in its key, but we must find a match either with the ReplicaID or zero or
		// something is awry.
		idZero := storage.FullReplicaID{RangeID: id.RangeID, ReplicaID: 0}
		_, foundWithID := allM[id]
		_, foundWithZero := allM[idZero]
		if !foundWithID && !foundWithZero {
			return nil, errors.AssertionFailedf("initialized replica %s not present in allM", id)
		}
		if foundWithID && foundWithZero {
			return nil, errors.AssertionFailedf("initialized replica %s duplicated in allM", allM)
		}
		// Now we know that foundWithID xor foundWithZero holds.

		// INVARIANT: all replicas have a persisted full replicaID (i.e. a "replicaID from disk").
		if foundWithZero {
			// Backfill RaftReplicaID.
			if err := logstore.NewStateLoader(id.RangeID).SetRaftReplicaID(ctx, eng, id.ReplicaID); err != nil {
				return nil, errors.Wrapf(err, "backfilling replicaID for r%d", id.RangeID)
			}
			log.Eventf(ctx, "backfilled replicaID for initialized replica %s", id)
			// `allM` contains `idZero` in this case, and we want to make sure this range
			// isn't accidentally considered an uninitialized replica that needs to be
			// removed. `initM` was already keyed with `id`, so nothing to do there.
			delete(allM, idZero)
		} // else foundWithID==true, so nothing to backfill

		// allM will be our uninitM, so delete all initialized replicas from it.
		delete(allM, id)
	}

	uninitM := allM

	var removedLegacy []roachpb.RangeID // for deterministic logging
	for id := range uninitM {
		// We've backfilled RaftReplicaID for initialized replicas above, but there
		// can be uninitialized ones, too. We don't have a RaftReplicaID for them so
		// instead of making one up we remove this replica. This is theoretically
		// unsound - after all, it could have voted - but in practice this won't
		// matter.
		if id.ReplicaID != 0 {
			continue
		}
		// TODO(tbg): if clearRangeData were in this package we could destroy more
		// effectively even if for some reason we had in the past written state
		// other than the HardState here.
		if err := eng.ClearUnversioned(logstore.NewStateLoader(id.RangeID).RaftHardStateKey()); err != nil {
			return nil, errors.Wrapf(err, "removing HardState for r%d", id.RangeID)
		}
		removedLegacy = append(removedLegacy, id.RangeID)
		delete(uninitM, id)
	}
	sort.Slice(removedLegacy, func(i, j int) bool {
		return removedLegacy[i] < removedLegacy[j]
	})
	for _, rangeID := range removedLegacy {
		log.Eventf(ctx, "removed legacy uninitialized replica for r%s", rangeID)
	}

	return &EngineReplicas{
		Initialized:   initM,
		Uninitialized: uninitM,
	}, nil
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}
