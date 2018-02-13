// Copyright 2016 The Cockroach Authors.
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

package stateloader

import (
	"bytes"
	"context"
	"math"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// StateLoader contains accessor methods to read or write the
// fields of storagebase.ReplicaState. It contains an internal buffer
// which is reused to avoid an allocation on frequently-accessed code
// paths.
//
// Because of this internal buffer, this struct is not safe for
// concurrent use, and the return values of methods that return keys
// are invalidated the next time any method is called.
//
// It is safe to have multiple replicaStateLoaders for the same
// Replica. Reusable replicaStateLoaders are typically found in a
// struct with a mutex, and temporary loaders may be created when
// locking is less desirable than an allocation.
type StateLoader struct {
	st *cluster.Settings
	keys.RangeIDPrefixBuf
}

// Make creates an Instance.
func Make(st *cluster.Settings, rangeID roachpb.RangeID) StateLoader {
	rsl := StateLoader{
		st:               st,
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
	return rsl
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (rsl StateLoader) Load(
	ctx context.Context, reader engine.Reader, desc *roachpb.RangeDescriptor,
) (storagebase.ReplicaState, error) {
	var s storagebase.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	lease, err := rsl.LoadLease(ctx, reader)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.Lease = &lease

	if s.GCThreshold, err = rsl.LoadGCThreshold(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.TxnSpanGCThreshold, err = rsl.LoadTxnSpanGCThreshold(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.RaftAppliedIndex, s.LeaseAppliedIndex, err = rsl.LoadAppliedIndex(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	ms, err := rsl.LoadMVCCStats(ctx, reader)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.Stats = &ms

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := rsl.LoadTruncatedState(ctx, reader)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	return s, nil
}

// Save persists the given ReplicaState to disk. It assumes that the contained
// Stats are up-to-date and returns the stats which result from writing the
// updated State.
//
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): test and assert that none of the optional values are
// missing whenever save is called. Optional values should be reserved
// strictly for use in Result. Do before merge.
func (rsl StateLoader) Save(
	ctx context.Context, eng engine.ReadWriter, state storagebase.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms := state.Stats
	if err := rsl.SetLease(ctx, eng, ms, *state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetAppliedIndex(
		ctx, eng, ms, state.RaftAppliedIndex, state.LeaseAppliedIndex,
	); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetGCThreshold(ctx, eng, ms, state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetTxnSpanGCThreshold(ctx, eng, ms, state.TxnSpanGCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetTruncatedState(ctx, eng, ms, state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetMVCCStats(ctx, eng, ms); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return *ms, nil
}

// LoadLease loads the lease.
func (rsl StateLoader) LoadLease(ctx context.Context, reader engine.Reader) (roachpb.Lease, error) {
	var lease roachpb.Lease
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, true, nil, &lease)
	return lease, err
}

// SetLease persists a lease.
func (rsl StateLoader) SetLease(
	ctx context.Context, eng engine.ReadWriter, ms *enginepb.MVCCStats, lease roachpb.Lease,
) error {
	return engine.MVCCPutProto(ctx, eng, ms, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, nil, &lease)
}

// LoadAppliedIndex returns the Raft applied index and the lease applied index.
func (rsl StateLoader) LoadAppliedIndex(
	ctx context.Context, reader engine.Reader,
) (uint64, uint64, error) {
	var appliedIndex uint64
	v, _, err := engine.MVCCGet(ctx, reader, rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{}, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64AppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		appliedIndex = uint64(int64AppliedIndex)
	}
	// TODO(tschottdorf): code duplication.
	var leaseAppliedIndex uint64
	v, _, err = engine.MVCCGet(ctx, reader, rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{}, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64LeaseAppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		leaseAppliedIndex = uint64(int64LeaseAppliedIndex)
	}

	return appliedIndex, leaseAppliedIndex, nil
}

// SetAppliedIndex sets the {raft,lease} applied index values, properly
// accounting for existing keys in the returned stats.
func (rsl StateLoader) SetAppliedIndex(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	appliedIndex,
	leaseAppliedIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))
	if err := engine.MVCCPut(ctx, eng, ms,
		rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return engine.MVCCPut(ctx, eng, ms,
		rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */)
}

// SetAppliedIndexBlind sets the {raft,lease} applied index values using a
// "blind" put which ignores any existing keys. This is identical to
// setAppliedIndex but is used to optimize the writing of the applied index
// values during write operations where we definitively know the size of the
// previous values.
func (rsl StateLoader) SetAppliedIndexBlind(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	appliedIndex,
	leaseAppliedIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))
	if err := engine.MVCCBlindPut(ctx, eng, ms,
		rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return engine.MVCCBlindPut(ctx, eng, ms,
		rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */)
}

func inlineValueIntEncodedSize(v int64) int {
	var value roachpb.Value
	value.SetInt(v)
	meta := enginepb.MVCCMetadata{RawBytes: value.RawBytes}
	return meta.Size()
}

// CalcAppliedIndexSysBytes calculates the size (MVCCStats.SysBytes) of the {raft,lease} applied
// index keys/values.
func (rsl StateLoader) CalcAppliedIndexSysBytes(appliedIndex, leaseAppliedIndex uint64) int64 {
	return int64(engine.MakeMVCCMetadataKey(rsl.RaftAppliedIndexKey()).EncodedSize() +
		engine.MakeMVCCMetadataKey(rsl.LeaseAppliedIndexKey()).EncodedSize() +
		inlineValueIntEncodedSize(int64(appliedIndex)) +
		inlineValueIntEncodedSize(int64(leaseAppliedIndex)))
}

// LoadTruncatedState loads the truncated state.
func (rsl StateLoader) LoadTruncatedState(
	ctx context.Context, reader engine.Reader,
) (roachpb.RaftTruncatedState, error) {
	var truncState roachpb.RaftTruncatedState
	if _, err := engine.MVCCGetProto(ctx, reader,
		rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, true,
		nil, &truncState); err != nil {
		return roachpb.RaftTruncatedState{}, err
	}
	return truncState, nil
}

// SetTruncatedState overwrites the truncated state.
func (rsl StateLoader) SetTruncatedState(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	truncState *roachpb.RaftTruncatedState,
) error {
	if (*truncState == roachpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}
	return engine.MVCCPutProto(ctx, eng, ms,
		rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, nil, truncState)
}

// LoadGCThreshold loads the GC threshold.
func (rsl StateLoader) LoadGCThreshold(
	ctx context.Context, reader engine.Reader,
) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeLastGCKey(),
		hlc.Timestamp{}, true, nil, &t)
	return &t, err
}

// SetGCThreshold sets the GC threshold.
func (rsl StateLoader) SetGCThreshold(
	ctx context.Context, eng engine.ReadWriter, ms *enginepb.MVCCStats, threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return engine.MVCCPutProto(ctx, eng, ms,
		rsl.RangeLastGCKey(), hlc.Timestamp{}, nil, threshold)
}

// LoadTxnSpanGCThreshold loads the transaction GC threshold.
func (rsl StateLoader) LoadTxnSpanGCThreshold(
	ctx context.Context, reader engine.Reader,
) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeTxnSpanGCThresholdKey(),
		hlc.Timestamp{}, true, nil, &t)
	return &t, err
}

// SetTxnSpanGCThreshold overwrites the transaction GC threshold.
func (rsl StateLoader) SetTxnSpanGCThreshold(
	ctx context.Context, eng engine.ReadWriter, ms *enginepb.MVCCStats, threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil TxnSpanGCThreshold")
	}

	return engine.MVCCPutProto(ctx, eng, ms,
		rsl.RangeTxnSpanGCThresholdKey(), hlc.Timestamp{}, nil, threshold)
}

// LoadMVCCStats loads the MVCC stats.
func (rsl StateLoader) LoadMVCCStats(
	ctx context.Context, reader engine.Reader,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeStatsKey(), hlc.Timestamp{}, true, nil, &ms)
	return ms, err
}

// SetMVCCStats overwrites the MVCC stats.
func (rsl StateLoader) SetMVCCStats(
	ctx context.Context, eng engine.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	return engine.MVCCPutProto(ctx, eng, nil, rsl.RangeStatsKey(), hlc.Timestamp{}, nil, newMS)
}

// The rest is not technically part of ReplicaState.
// TODO(tschottdorf): more consolidation of ad-hoc structures: last index and
// hard state. These are closely coupled with ReplicaState (and in particular
// with its TruncatedState) but are different in that they are not consistently
// updated through Raft.

// LoadLastIndex loads the last index.
func (rsl StateLoader) LoadLastIndex(ctx context.Context, reader engine.Reader) (uint64, error) {
	iter := reader.NewIterator(false)
	defer iter.Close()

	var lastIndex uint64
	iter.SeekReverse(engine.MakeMVCCMetadataKey(rsl.RaftLogKey(math.MaxUint64)))
	if ok, _ := iter.Valid(); ok {
		key := iter.Key()
		prefix := rsl.RaftLogPrefix()
		if bytes.HasPrefix(key.Key, prefix) {
			var err error
			_, lastIndex, err = encoding.DecodeUint64Ascending(key.Key[len(prefix):])
			if err != nil {
				log.Fatalf(ctx, "unable to decode Raft log index key: %s", key)
			}
		}
	}

	if lastIndex == 0 {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, err := rsl.LoadTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

// SetLastIndex overwrites the last index.
func (rsl StateLoader) SetLastIndex(
	ctx context.Context, eng engine.ReadWriter, lastIndex uint64,
) error {
	if rsl.st.Version.IsActive(cluster.VersionRaftLastIndex) {
		return nil
	}
	var value roachpb.Value
	value.SetInt(int64(lastIndex))
	return engine.MVCCPut(ctx, eng, nil, rsl.RaftLastIndexKey(),
		hlc.Timestamp{}, value, nil /* txn */)
}

// LoadReplicaDestroyedError loads the replica destroyed error for the specified
// range. If there is no error, nil is returned.
func (rsl StateLoader) LoadReplicaDestroyedError(
	ctx context.Context, reader engine.Reader,
) (*roachpb.Error, error) {
	var v roachpb.Error
	found, err := engine.MVCCGetProto(ctx, reader,
		rsl.RangeReplicaDestroyedErrorKey(),
		hlc.Timestamp{}, true /* consistent */, nil, &v)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &v, nil
}

// SetReplicaDestroyedError sets an error indicating that the replica has been
// destroyed.
func (rsl StateLoader) SetReplicaDestroyedError(
	ctx context.Context, eng engine.ReadWriter, err *roachpb.Error,
) error {
	return engine.MVCCPutProto(ctx, eng, nil,
		rsl.RangeReplicaDestroyedErrorKey(), hlc.Timestamp{}, nil /* txn */, err)
}

// LoadHardState loads the HardState.
func (rsl StateLoader) LoadHardState(
	ctx context.Context, reader engine.Reader,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := engine.MVCCGetProto(ctx, reader,
		rsl.RaftHardStateKey(),
		hlc.Timestamp{}, true, nil, &hs)

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

// SetHardState overwrites the HardState.
func (rsl StateLoader) SetHardState(
	ctx context.Context, batch engine.ReadWriter, st raftpb.HardState,
) error {
	return engine.MVCCPutProto(ctx, batch, nil,
		rsl.RaftHardStateKey(), hlc.Timestamp{}, nil, &st)
}

// SynthesizeRaftState creates a Raft state which synthesizes both a HardState
// and a lastIndex from pre-seeded data in the engine (typically created via
// writeInitialReplicaState and, on a split, perhaps the activity of an
// uninitialized Raft group)
func (rsl StateLoader) SynthesizeRaftState(ctx context.Context, eng engine.ReadWriter) error {
	hs, err := rsl.LoadHardState(ctx, eng)
	if err != nil {
		return err
	}
	truncState, err := rsl.LoadTruncatedState(ctx, eng)
	if err != nil {
		return err
	}
	raftAppliedIndex, _, err := rsl.LoadAppliedIndex(ctx, eng)
	if err != nil {
		return err
	}
	if err := rsl.SynthesizeHardState(ctx, eng, hs, truncState, raftAppliedIndex); err != nil {
		return err
	}
	return rsl.SetLastIndex(ctx, eng, truncState.Index)
}

// SynthesizeHardState synthesizes an on-disk HardState from the given input,
// taking care that a HardState compatible with the existing data is written.
func (rsl StateLoader) SynthesizeHardState(
	ctx context.Context,
	eng engine.ReadWriter,
	oldHS raftpb.HardState,
	truncState roachpb.RaftTruncatedState,
	raftAppliedIndex uint64,
) error {
	newHS := raftpb.HardState{
		Term: truncState.Term,
		// Note that when applying a Raft snapshot, the applied index is
		// equal to the Commit index represented by the snapshot.
		Commit: raftAppliedIndex,
	}

	if oldHS.Commit > newHS.Commit {
		return errors.Errorf("can't decrease HardState.Commit from %d to %d",
			oldHS.Commit, newHS.Commit)
	}
	if oldHS.Term > newHS.Term {
		// The existing HardState is allowed to be ahead of us, which is
		// relevant in practice for the split trigger. We already checked above
		// that we're not rewinding the acknowledged index, and we haven't
		// updated votes yet.
		newHS.Term = oldHS.Term
	}
	// If the existing HardState voted in this term, remember that.
	if oldHS.Term == newHS.Term {
		newHS.Vote = oldHS.Vote
	}
	err := rsl.SetHardState(ctx, eng, newHS)
	return errors.Wrapf(err, "writing HardState %+v", &newHS)
}
