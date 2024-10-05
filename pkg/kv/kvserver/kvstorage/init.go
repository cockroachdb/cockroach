// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		keys.StoreIdentKey(),
		hlc.Timestamp{},
		&ident,
		storage.MVCCWriteOptions{},
	); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(true /* sync */); err != nil {
		return errors.Wrap(err, "persisting engine initialization data")
	}
	// We will set the store ID during start, but we want to set it as quickly as
	// possible after creating a store (to initialize the shared object creator
	// ID).
	if err := eng.SetStoreID(ctx, int32(ident.StoreID)); err != nil {
		return errors.Wrap(err, "setting store ID")
	}

	return nil
}

// checkCanInitializeEngine ensures that the engine is empty except possibly for
// cluster version or cached cluster settings.
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
	iter, err := eng.NewEngineIterator(storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		UpperBound: roachpb.KeyMax,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: roachpb.KeyMin})
	if err != nil {
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

	for valid {
		var k storage.MVCCKey
		if k, err = getMVCCKey(); err != nil {
			return err
		}
		// Only allowed to find cluster version and cached cluster settings on an
		// uninitialized engine.
		if !k.Key.Equal(keys.DeprecatedStoreClusterVersionKey()) {
			if _, err := keys.DecodeStoreCachedSettingsKey(k.Key); err != nil {
				return errors.Errorf("engine cannot be bootstrapped, contains key:\n%s", k.String())
			}
		}
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
	iter, err := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: keys.LocalRangeIDPrefix.PrefixEnd().AsRawKey(),
	})
	if err != nil {
		return err
	}
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
	log.Info(ctx, "beginning range descriptor iteration")
	// MVCCIterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	allCount := 0
	matchCount := 0
	bySuffix := make(map[redact.RedactableString]int)

	var scanStats kvpb.ScanStats
	opts := storage.MVCCScanOptions{
		Inconsistent: true,
		ScanStats:    &scanStats,
	}
	lastReportTime := timeutil.Now()
	kvToDesc := func(kv roachpb.KeyValue) error {
		const reportPeriod = 15 * time.Second
		if timeutil.Since(lastReportTime) >= reportPeriod {
			// Note: MVCCIterate scans and buffers 1000 keys at a time which could
			// make the scan stats confusing. However, because this callback can't
			// take a long time, it's very unlikely that we will log twice for the
			// same batch of keys.
			log.Infof(ctx, "range descriptor iteration in progress: %d keys, %d range descriptors (by suffix: %v); %v",
				allCount, matchCount, bySuffix, &scanStats)
			lastReportTime = timeutil.Now()
		}

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
		// Descriptor for range `[a,z)` must be found at `/Local/Range/a/RangeDescriptor`.
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

	_, err := storage.MVCCIterate(ctx, reader, start, end, hlc.MaxTimestamp, opts, kvToDesc)
	log.Infof(ctx, "range descriptor iteration done: %d keys, %d range descriptors (by suffix: %v); %s",
		allCount, matchCount, bySuffix, &scanStats)
	return err
}

// A Replica references a CockroachDB Replica. The data in this struct does not
// represent the data of the Replica but is sufficient to access all of its
// contents via additional calls to the storage engine.
type Replica struct {
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
	Desc      *roachpb.RangeDescriptor // nil for uninitialized Replica

	hardState raftpb.HardState // internal to kvstorage, see migration in LoadAndReconcileReplicas
}

// ID returns the FullReplicaID.
func (r Replica) ID() storage.FullReplicaID {
	return storage.FullReplicaID{
		RangeID:   r.RangeID,
		ReplicaID: r.ReplicaID,
	}
}

// Load loads the state necessary to instantiate a replica in memory.
func (r Replica) Load(
	ctx context.Context, eng storage.Reader, storeID roachpb.StoreID,
) (LoadedReplicaState, error) {
	ls := LoadedReplicaState{
		ReplicaID: r.ReplicaID,
		hardState: r.hardState,
	}
	sl := stateloader.Make(r.Desc.RangeID)
	var err error
	if ls.LastIndex, err = sl.LoadLastIndex(ctx, eng); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.ReplState, err = sl.Load(ctx, eng, r.Desc); err != nil {
		return LoadedReplicaState{}, err
	}

	if err := ls.check(storeID); err != nil {
		return LoadedReplicaState{}, err
	}
	return ls, nil
}

// A replicaMap organizes a set of Replicas with unique RangeIDs.
type replicaMap map[roachpb.RangeID]Replica

func (m replicaMap) getOrMake(rangeID roachpb.RangeID) Replica {
	ent := m[rangeID]
	ent.RangeID = rangeID
	return ent
}

func (m replicaMap) setReplicaID(rangeID roachpb.RangeID, replicaID roachpb.ReplicaID) {
	ent := m.getOrMake(rangeID)
	ent.ReplicaID = replicaID
	m[rangeID] = ent
}

func (m replicaMap) setHardState(rangeID roachpb.RangeID, hs raftpb.HardState) {
	ent := m.getOrMake(rangeID)
	ent.hardState = hs
	m[rangeID] = ent
}

func (m replicaMap) setDesc(rangeID roachpb.RangeID, desc roachpb.RangeDescriptor) error {
	ent := m.getOrMake(rangeID)
	if ent.Desc != nil {
		return errors.AssertionFailedf("overlapping descriptors %v and %v", ent.Desc, &desc)
	}
	ent.Desc = &desc
	m[rangeID] = ent
	return nil
}

func loadReplicas(ctx context.Context, eng storage.Engine) ([]Replica, error) {
	s := replicaMap{}

	// INVARIANT: the latest visible committed version of the RangeDescriptor
	// (which is what IterateRangeDescriptorsFromDisk returns) is the one reflecting
	// the state of the Replica.
	// INVARIANT: the descriptor for range [a,z) is located at RangeDescriptorKey(a).
	// This is checked in IterateRangeDescriptorsFromDisk.
	{
		var lastDesc roachpb.RangeDescriptor
		if err := IterateRangeDescriptorsFromDisk(
			ctx, eng, func(desc roachpb.RangeDescriptor) error {
				if lastDesc.RangeID != 0 && desc.StartKey.Less(lastDesc.EndKey) {
					return errors.AssertionFailedf("overlapping descriptors %s and %s", lastDesc, desc)
				}
				lastDesc = desc
				return s.setDesc(desc.RangeID, desc)
			},
		); err != nil {
			return nil, err
		}
	}

	// INVARIANT: all replicas have a persisted full ReplicaID (i.e. a "ReplicaID from disk").
	//
	// This invariant is true for replicas created in 22.1. Without further action, it
	// would be violated for clusters that originated before 22.1. In this method, we
	// backfill the ReplicaID (for initialized replicas) and we remove uninitialized
	// replicas lacking a ReplicaID (see below for rationale).
	//
	// The migration can be removed when the KV host cluster MinSupportedVersion
	// matches or exceeds 23.1 (i.e. once we know that a store has definitely
	// started up on >=23.1 at least once).

	// Collect all the RangeIDs that either have a RaftReplicaID or HardState. For
	// unmigrated replicas we see only the HardState - that is how we detect
	// replicas that still need to be migrated.
	//
	// TODO(tbg): tighten up the case where we see a RaftReplicaID but no HardState.
	// This leads to the general desire to validate the internal consistency of the
	// entire raft state (i.e. HardState, TruncatedState, Log).
	{
		var msg kvserverpb.RaftReplicaID
		if err := IterateIDPrefixKeys(ctx, eng, func(rangeID roachpb.RangeID) roachpb.Key {
			return keys.RaftReplicaIDKey(rangeID)
		}, &msg, func(rangeID roachpb.RangeID) error {
			s.setReplicaID(rangeID, msg.ReplicaID)
			return nil
		}); err != nil {
			return nil, err
		}

		var hs raftpb.HardState
		if err := IterateIDPrefixKeys(ctx, eng, func(rangeID roachpb.RangeID) roachpb.Key {
			return keys.RaftHardStateKey(rangeID)
		}, &hs, func(rangeID roachpb.RangeID) error {
			s.setHardState(rangeID, hs)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	sl := make([]Replica, 0, len(s))
	for _, repl := range s {
		sl = append(sl, repl)
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].RangeID < sl[j].RangeID
	})
	return sl, nil
}

// LoadAndReconcileReplicas loads the Replicas present on this
// store. It reconciles inconsistent state and runs validation checks.
// The returned slice is sorted by ReplicaID.
//
// TODO(sep-raft-log): consider a callback-visitor pattern here.
func LoadAndReconcileReplicas(ctx context.Context, eng storage.Engine) ([]Replica, error) {
	ident, err := ReadStoreIdent(ctx, eng)
	if err != nil {
		return nil, err
	}

	sl, err := loadReplicas(ctx, eng)
	if err != nil {
		return nil, err
	}

	// Check invariants.
	//
	// Migrate into RaftReplicaID for all replicas that need it.
	var newIdx int
	for _, repl := range sl {
		var descReplicaID roachpb.ReplicaID
		if repl.Desc != nil {
			// INVARIANT: a Replica's RangeDescriptor always contains the local Store,
			// i.e. a Store is a member of all of its local initialized Replicas.
			replDesc, found := repl.Desc.GetReplicaDescriptor(ident.StoreID)
			if !found {
				return nil, errors.AssertionFailedf("s%d not found in %s", ident.StoreID, repl.Desc)
			}
			if repl.ReplicaID != 0 && replDesc.ReplicaID != repl.ReplicaID {
				return nil, errors.AssertionFailedf("conflicting RaftReplicaID %d for %s", repl.ReplicaID, repl.Desc)
			}
			descReplicaID = replDesc.ReplicaID
		}

		if repl.ReplicaID != 0 {
			sl[newIdx] = repl
			newIdx++
			// RaftReplicaID present, no need to migrate.
			continue
		}

		// Migrate into RaftReplicaID. This migration can be removed once the
		// BinaryMinSupportedVersion is >= 23.1, and we can assert that
		// repl.ReplicaID != 0 always holds.

		if descReplicaID != 0 {
			// Backfill RaftReplicaID for an initialized Replica.
			if err := logstore.NewStateLoader(repl.RangeID).SetRaftReplicaID(ctx, eng, descReplicaID); err != nil {
				return nil, errors.Wrapf(err, "backfilling ReplicaID for r%d", repl.RangeID)
			}
			repl.ReplicaID = descReplicaID
			sl[newIdx] = repl
			newIdx++
			log.Eventf(ctx, "backfilled replicaID for initialized replica %s", repl.ID())
		} else {
			// We found an uninitialized replica that did not have a persisted
			// ReplicaID. We can't determine the ReplicaID now, so we migrate by
			// removing this uninitialized replica. This technically violates raft
			// invariants if this replica has cast a vote, but the conditions under
			// which this matters are extremely unlikely.
			//
			// TODO(tbg): if clearRangeData were in this package we could destroy more
			// effectively even if for some reason we had in the past written state
			// other than the HardState here (not supposed to happen, but still).
			if err := eng.ClearUnversioned(logstore.NewStateLoader(repl.RangeID).RaftHardStateKey(), storage.ClearOptions{}); err != nil {
				return nil, errors.Wrapf(err, "removing HardState for r%d", repl.RangeID)
			}
			log.Eventf(ctx, "removed legacy uninitialized replica for r%s", repl.RangeID)
			// NB: removed from `sl` since we're not incrementing `newIdx`.
		}
	}

	return sl[:newIdx], nil
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}
