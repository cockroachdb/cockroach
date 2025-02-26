// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	iter, err := eng.NewEngineIterator(ctx, storage.IterOptions{
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
	iter, err := reader.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
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
// the provided function with each such descriptor from the provided Engine. If
// the function returns an error, IterateRangeDescriptorsFromDisk fails with
// that error.
func IterateRangeDescriptorsFromDisk(
	ctx context.Context, reader storage.Reader, fn func(desc roachpb.RangeDescriptor) error,
) error {
	log.Info(ctx, "beginning range descriptor iteration")

	// We are going to find all range descriptor keys. This code is equivalent to
	// using MVCCIterate on all range-local keys in Inconsistent mode and with
	// hlc.MaxTimestamp, but is faster in that it completely skips over areas with
	// transaction records (these areas can contain large numbers of LSM
	// tombstones).
	iter, err := reader.NewMVCCIterator(ctx, storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		UpperBound: keys.LocalRangeMax,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	var descriptorCount, intentCount, tombstoneCount int
	lastReportTime := timeutil.Now()

	iter.SeekGE(storage.MVCCKey{Key: keys.LocalRangePrefix})
	var keyBuf []byte
	for {
		if valid, err := iter.Valid(); err != nil {
			return err
		} else if !valid {
			break
		}

		const reportPeriod = 10 * time.Second
		if timeutil.Since(lastReportTime) >= reportPeriod {
			stats := iter.Stats().Stats
			log.Infof(ctx, "range descriptor iteration in progress: %d range descriptors, %d intents, %d tombstones; stats: %s",
				descriptorCount, intentCount, tombstoneCount, stats.String())
		}

		key := iter.UnsafeKey()
		startKey, suffix, _, err := keys.DecodeRangeKey(key.Key)
		if err != nil {
			return err
		}

		if suffixCmp := bytes.Compare(suffix, keys.LocalRangeDescriptorSuffix); suffixCmp != 0 {
			if suffixCmp < 0 {
				// Seek to the range descriptor key for this range.
				//
				// Note that inside Pebble, SeekGE will try to iterate through the next
				// few keys so it's ok to seek even if there are very few keys before the
				// descriptor.
				iter.SeekGE(storage.MVCCKey{Key: keys.RangeDescriptorKey(keys.MustAddr(startKey))})
			} else {
				// This case shouldn't happen in practice: we have a key that isn't
				// associated with any range descriptor.
				if buildutil.CrdbTestBuild {
					panic(errors.AssertionFailedf("range local key %s outside of a known range", key.Key))
				}
				iter.NextKey()
			}
			continue
		}

		// We are at a descriptor key.
		rawValue, err := iter.UnsafeValue()
		if err != nil {
			return err
		}

		if key.Timestamp.IsEmpty() {
			// This is an intent. We want to get its timestamp and read the latest
			// version before that timestamp. This is consistent with what MVCCIterate
			// does in Inconsistent mode.
			intentCount++
			var meta enginepb.MVCCMetadata
			if err := protoutil.Unmarshal(rawValue, &meta); err != nil {
				return err
			}
			metaTS := meta.Timestamp.ToTimestamp()
			if metaTS.IsEmpty() {
				return errors.AssertionFailedf("range key has intent with no timestamp")
			}
			// Seek to the latest value below the intent timestamp.
			// We cannot pass to SeekGE a key that was returned to us from the iterator; make a copy.
			keyBuf = append(keyBuf[:0], key.Key...)
			iter.SeekGE(storage.MVCCKey{Key: keyBuf, Timestamp: metaTS.Prev()})
			continue
		}

		value, err := storage.DecodeValueFromMVCCValue(rawValue)
		if err != nil {
			return errors.Wrap(err, "decoding range descriptor MVCC value")
		}
		if len(value.RawBytes) == 0 {
			// This is a tombstone, so this key no longer exists; skip over any older
			// versions of this key.
			tombstoneCount++
			iter.NextKey()
			continue
		}

		// This is what we are looking for: the latest version of a range
		// descriptor key.
		var desc roachpb.RangeDescriptor
		if err := value.GetProto(&desc); err != nil {
			return errors.Wrap(err, "decoding range descriptor proto")
		}
		// Descriptor for range `[a,z)` must be found at `/Local/Range/a/RangeDescriptor`.
		if !startKey.Equal(desc.StartKey.AsRawKey()) {
			return errors.AssertionFailedf("descriptor stored at %q but has StartKey %q",
				key.Key, desc.StartKey)
		}
		// We should never be writing out uninitialized descriptors.
		if !desc.IsInitialized() {
			return errors.AssertionFailedf("uninitialized descriptor: %s", desc)
		}

		descriptorCount++
		nextRangeDescKey := keys.RangeDescriptorKey(desc.EndKey)
		if err := fn(desc); err != nil {
			return err
		}
		// Seek to the next possible descriptor key. This seek is important as it
		// can skip over a large amount of txn record keys or tombstones.
		iter.SeekGE(storage.MVCCKey{Key: nextRangeDescKey})
	}

	stats := iter.Stats().Stats
	log.Infof(ctx, "range descriptor iteration done: %d range descriptors, %d intents, %d tombstones; stats: %s",
		descriptorCount, intentCount, tombstoneCount, stats.String())
	return nil
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
	if ls.TruncState, err = sl.LoadRaftTruncatedState(ctx, eng); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.LastEntryID, err = sl.LoadLastEntryID(ctx, eng, ls.TruncState); err != nil {
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

	// Load replicas from disk based on their RaftReplicaID and HardState.
	//
	// INVARIANT: all replicas have a persisted full ReplicaID (i.e. a "ReplicaID from disk").
	//
	// TODO(tbg): tighten up the case where we see a RaftReplicaID but no HardState.
	// This leads to the general desire to validate the internal consistency of the
	// entire raft state (i.e. HardState, TruncatedState, Log).
	{
		logEvery := log.Every(10 * time.Second)
		var i int
		var msg kvserverpb.RaftReplicaID
		if err := IterateIDPrefixKeys(ctx, eng, func(rangeID roachpb.RangeID) roachpb.Key {
			return keys.RaftReplicaIDKey(rangeID)
		}, &msg, func(rangeID roachpb.RangeID) error {
			if logEvery.ShouldLog() && i > 0 { // only log if slow
				log.Infof(ctx, "loaded replica ID for %d/%d replicas", i, len(s))
			}
			i++
			s.setReplicaID(rangeID, msg.ReplicaID)
			return nil
		}); err != nil {
			return nil, err
		}
		log.Infof(ctx, "loaded replica ID for %d/%d replicas", len(s), len(s))

		logEvery = log.Every(10 * time.Second)
		i = 0
		var hs raftpb.HardState
		if err := IterateIDPrefixKeys(ctx, eng, func(rangeID roachpb.RangeID) roachpb.Key {
			return keys.RaftHardStateKey(rangeID)
		}, &hs, func(rangeID roachpb.RangeID) error {
			if logEvery.ShouldLog() && i > 0 { // only log if slow
				log.Infof(ctx, "loaded Raft state for %d/%d replicas", i, len(s))
			}
			i++
			s.setHardState(rangeID, hs)
			return nil
		}); err != nil {
			return nil, err
		}
		log.Infof(ctx, "loaded Raft state for %d/%d replicas", len(s), len(s))
	}
	sl := make([]Replica, 0, len(s))
	for _, repl := range s {
		sl = append(sl, repl)
	}
	slices.SortFunc(sl, func(a, b Replica) int {
		return cmp.Compare(a.RangeID, b.RangeID)
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
	log.Infof(ctx, "loaded %d replicas", len(sl))

	// Check invariants.
	//
	// TODO(erikgrinaker): consider moving this logic into loadReplicas.
	logEvery := log.Every(10 * time.Second)
	for i, repl := range sl {
		// Log progress regularly, but not for the first replica (we only want to
		// log when this is slow). The last replica is logged after iteration.
		if logEvery.ShouldLog() && i > 0 {
			log.Infof(ctx, "verified %d/%d replicas", i, len(sl))
		}

		// INVARIANT: a Replica always has a replica ID.
		if repl.ReplicaID == 0 {
			return nil, errors.AssertionFailedf("no RaftReplicaID for %s", repl.Desc)
		}

		if repl.Desc != nil {
			// INVARIANT: a Replica's RangeDescriptor always contains the local Store,
			// i.e. a Store is a member of all of its local initialized Replicas.
			replDesc, found := repl.Desc.GetReplicaDescriptor(ident.StoreID)
			if !found {
				return nil, errors.AssertionFailedf("s%d not found in %s", ident.StoreID, repl.Desc)
			}
			// INVARIANT: a Replica's ID always matches the descriptor.
			if replDesc.ReplicaID != repl.ReplicaID {
				return nil, errors.AssertionFailedf("conflicting RaftReplicaID %d for %s", repl.ReplicaID, repl.Desc)
			}
		}
	}
	log.Infof(ctx, "verified %d/%d replicas", len(sl), len(sl))

	return sl, nil
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}
