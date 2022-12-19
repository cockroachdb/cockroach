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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

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
	bySuffix := make(map[string]int)
	kvToDesc := func(kv roachpb.KeyValue) error {
		allCount++
		// Only consider range metadata entries; ignore others.
		startKey, suffix, _, err := keys.DecodeRangeKey(kv.Key)
		if err != nil {
			return err
		}
		bySuffix[string(suffix)]++
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
// TODO(sep-raft-log): the reader here is for the log engine.
func loadFullReplicaIDsFromDisk(
	ctx context.Context, reader storage.Reader,
) (map[storage.FullReplicaID]struct{}, error) {
	m := map[storage.FullReplicaID]struct{}{}
	var msg roachpb.RaftReplicaID
	if err := IterateIDPrefixKeys(ctx, reader, func(rangeID roachpb.RangeID) roachpb.Key {
		return keys.RaftReplicaIDKey(rangeID)
	}, &msg, func(rangeID roachpb.RangeID) error {
		m[storage.FullReplicaID{RangeID: rangeID, ReplicaID: msg.ReplicaID}] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
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

			initM[storage.FullReplicaID{
				RangeID:   desc.RangeID,
				ReplicaID: repDesc.ReplicaID,
			}] = &desc
			return nil
		}); err != nil {
		return nil, err
	}

	allM, err := loadFullReplicaIDsFromDisk(ctx, eng)
	if err != nil {
		return nil, err
	}

	for id := range initM {
		if _, ok := allM[id]; !ok {
			// INVARIANT: all replicas have a persisted full replicaID (i.e. a "replicaID from disk").
			//
			// This invariant is true for replicas created in 22.2, but no migration
			// was ever written. So we backfill the replicaID here (as of 23.1) and
			// remove this code in the future (the follow-up release, assuming it is
			// forced to migrate through 23.1, otherwise later).
			if buildutil.CrdbTestBuild {
				return nil, errors.AssertionFailedf("%s has no persisted replicaID", initM[id])
			}
			if err := logstore.NewStateLoader(id.RangeID).SetRaftReplicaID(ctx, eng, id.ReplicaID); err != nil {
				return nil, errors.Wrapf(err, "backfilling replicaID for r%d", id.RangeID)
			}
			log.Eventf(ctx, "backfilled replicaID for %s", id)
		}
		// `allM` will be our map of uninitialized replicas.
		//
		// A replica is "uninitialized" if it's not in initM (i.e. is at log position
		// zero and has no visible RangeDescriptor).
		delete(allM, id)
	}

	return &EngineReplicas{
		Initialized:   initM,
		Uninitialized: allM, // NB: init'ed ones were deleted earlier
	}, nil
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}
