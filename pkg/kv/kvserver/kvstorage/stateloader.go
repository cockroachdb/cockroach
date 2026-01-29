// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// StateLoader contains accessor methods to read or write the
// fields of kvserverbase.ReplicaState. It contains an internal buffer
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
	logstore.StateLoader
}

// MakeStateLoader creates a StateLoader.
func MakeStateLoader(rangeID roachpb.RangeID) StateLoader {
	return StateLoader{
		StateLoader: logstore.NewStateLoader(rangeID),
	}
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (s StateLoader) Load(
	ctx context.Context, stateRO StateRO, desc *roachpb.RangeDescriptor,
) (kvserverpb.ReplicaState, error) {
	var r kvserverpb.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	r.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	lease, err := s.LoadLease(ctx, stateRO)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	r.Lease = &lease

	if r.GCThreshold, err = s.LoadGCThreshold(ctx, stateRO); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	if r.GCHint, err = s.LoadGCHint(ctx, stateRO); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	if r.ForceFlushIndex, err = s.LoadRangeForceFlushIndex(ctx, stateRO); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	as, err := s.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	r.RaftAppliedIndex = as.RaftAppliedIndex
	r.RaftAppliedIndexTerm = as.RaftAppliedIndexTerm
	r.LeaseAppliedIndex = as.LeaseAppliedIndex
	ms := as.RangeStats.ToStats()
	r.Stats = &ms
	r.RaftClosedTimestamp = as.RaftClosedTimestamp

	// Invariant: TruncatedState == nil. The field is being phased out. The
	// RaftTruncatedState must be loaded separately.
	r.TruncatedState = nil

	version, err := s.LoadVersion(ctx, stateRO)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	if (version != roachpb.Version{}) {
		r.Version = &version
	}

	return r, nil
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
func (s StateLoader) Save(
	ctx context.Context, stateRW StateRW, state kvserverpb.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms := state.Stats
	if err := s.SetLease(ctx, stateRW, ms, *state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := s.SetGCThreshold(ctx, stateRW, ms, state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := s.SetGCHint(ctx, stateRW, ms, state.GCHint); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if state.Version != nil {
		if err := s.SetVersion(ctx, stateRW, ms, state.Version); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	state.Stats = ms // no-op, just an acknowledgement that the stats were updated

	as := state.ToRangeAppliedState()
	if err := s.SetRangeAppliedState(ctx, stateRW, &as); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return *ms, nil
}

// LoadLease loads the lease.
func (s StateLoader) LoadLease(ctx context.Context, stateRO StateRO) (roachpb.Lease, error) {
	var lease roachpb.Lease
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCGetOptions{})
	return lease, err
}

// SetLease persists a lease.
func (s StateLoader) SetLease(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, lease roachpb.Lease,
) error {
	return storage.MVCCPutProto(ctx, stateRW, s.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCWriteOptions{Stats: ms})
}

// SetLeaseBlind persists a lease using a blind write, updating the MVCC stats
// based on prevLease. This is particularly beneficial for expiration lease
// extensions, which do a write per range every 3 seconds. Seeking to the
// existing record has a significant aggregate cost with many ranges, and can
// also cause Pebble block cache thrashing.
//
// NB: prevLease is usually passed from the in-memory replica state. Since lease
// requests don't hold latches (they're evaluated on the local replica),
// prevLease may be modified concurrently. In that case the lease request will
// fail below Raft, so it doesn't matter if the stats are wrong.
func (s StateLoader) SetLeaseBlind(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, lease, prevLease roachpb.Lease,
) error {
	key := s.RangeLeaseKey()
	var value, prevValue roachpb.Value
	if err := value.SetProto(&lease); err != nil {
		return err
	}
	value.InitChecksum(key)
	// NB: We persist an empty lease record when writing the initial range state,
	// so we should always pass a non-empty prevValue.
	if err := prevValue.SetProto(&prevLease); err != nil {
		return err
	}
	prevValue.InitChecksum(key)
	return storage.MVCCBlindPutInlineWithPrev(ctx, stateRW, ms, key, value, prevValue)
}

// LoadRangeAppliedState loads the Range applied state.
func (s StateLoader) LoadRangeAppliedState(
	ctx context.Context, stateRO StateRO,
) (*kvserverpb.RangeAppliedState, error) {
	var as kvserverpb.RangeAppliedState
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
		storage.MVCCGetOptions{})
	return &as, err
}

// LoadMVCCStats loads the MVCC stats.
func (s StateLoader) LoadMVCCStats(
	ctx context.Context, stateRO StateRO,
) (enginepb.MVCCStats, error) {
	// Check the applied state key.
	as, err := s.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	return as.RangeStats.ToStats(), nil
}

// SetRangeAppliedState overwrites the range applied state. This state is a
// combination of the Raft and lease applied indices, along with the MVCC stats.
//
// The applied indices and the stats used to be stored separately in different
// keys. We now deem those keys to be "legacy" because they have been replaced
// by the range applied state key.
func (s StateLoader) SetRangeAppliedState(
	ctx context.Context, stateRW StateRW, as *kvserverpb.RangeAppliedState,
) error {
	// The RangeAppliedStateKey is not included in stats. This is also reflected
	// in ComputeStats.
	ms := (*enginepb.MVCCStats)(nil)
	return storage.MVCCPutProto(ctx, stateRW, s.RangeAppliedStateKey(),
		hlc.Timestamp{}, as, storage.MVCCWriteOptions{Stats: ms, Category: fs.ReplicationReadCategory})
}

// SetMVCCStats overwrites the MVCC stats. This needs to perform a read on the
// RangeAppliedState key before overwriting the stats. Use SetRangeAppliedState
// when performance is important.
func (s StateLoader) SetMVCCStats(
	ctx context.Context, stateRW StateRW, newMS *enginepb.MVCCStats,
) error {
	as, err := s.LoadRangeAppliedState(ctx, stateRW)
	if err != nil {
		return err
	}
	as.RangeStats = kvserverpb.MVCCPersistentStats(*newMS)
	return s.SetRangeAppliedState(ctx, stateRW, as)
}

// LoadGCThreshold loads the GC threshold.
func (s StateLoader) LoadGCThreshold(ctx context.Context, stateRO StateRO) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeGCThresholdKey(),
		hlc.Timestamp{}, &t, storage.MVCCGetOptions{ReadCategory: fs.MVCCGCReadCategory})
	return &t, err
}

// SetGCThreshold sets the GC threshold.
func (s StateLoader) SetGCThreshold(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return storage.MVCCPutProto(ctx, stateRW, s.RangeGCThresholdKey(),
		hlc.Timestamp{}, threshold, storage.MVCCWriteOptions{Stats: ms})
}

// LoadGCHint loads GC hint.
func (s StateLoader) LoadGCHint(ctx context.Context, stateRO StateRO) (*roachpb.GCHint, error) {
	var h roachpb.GCHint
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeGCHintKey(),
		hlc.Timestamp{}, &h, storage.MVCCGetOptions{ReadCategory: fs.MVCCGCReadCategory})
	if err != nil {
		return nil, err
	}
	return &h, nil
}

// SetGCHint writes the GC hint.
func (s StateLoader) SetGCHint(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, hint *roachpb.GCHint,
) error {
	if hint == nil {
		return errors.New("cannot persist nil GCHint")
	}
	return storage.MVCCPutProto(ctx, stateRW, s.RangeGCHintKey(),
		hlc.Timestamp{}, hint, storage.MVCCWriteOptions{Stats: ms})
}

// LoadVersion loads the replica version.
func (s StateLoader) LoadVersion(ctx context.Context, stateRO StateRO) (roachpb.Version, error) {
	var version roachpb.Version
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeVersionKey(),
		hlc.Timestamp{}, &version, storage.MVCCGetOptions{})
	return version, err
}

// SetVersion sets the replica version.
func (s StateLoader) SetVersion(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, version *roachpb.Version,
) error {
	return storage.MVCCPutProto(ctx, stateRW, s.RangeVersionKey(),
		hlc.Timestamp{}, version, storage.MVCCWriteOptions{Stats: ms})
}

// LoadRangeForceFlushIndex loads the force-flush index.
func (s StateLoader) LoadRangeForceFlushIndex(
	ctx context.Context, stateRO StateRO,
) (roachpb.ForceFlushIndex, error) {
	var ffIndex roachpb.ForceFlushIndex
	// If not found, ffIndex.Index will stay 0.
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeForceFlushKey(),
		hlc.Timestamp{}, &ffIndex, storage.MVCCGetOptions{})
	return ffIndex, err
}

// SetForceFlushIndex sets the force-flush index.
func (s StateLoader) SetForceFlushIndex(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, ffIndex *roachpb.ForceFlushIndex,
) error {
	return storage.MVCCPutProto(ctx, stateRW, s.RangeForceFlushKey(),
		hlc.Timestamp{}, ffIndex, storage.MVCCWriteOptions{Stats: ms})
}

// LoadRaftReplicaID loads the RaftReplicaID. Returns an empty RaftReplicaID if
// the key is not found, which can only happen if the replica does not exist.
// The caller must assert if they don't expect a missing replica.
func (s StateLoader) LoadRaftReplicaID(
	ctx context.Context, stateRO StateRO,
) (kvserverpb.RaftReplicaID, error) {
	var replicaID kvserverpb.RaftReplicaID
	if ok, err := storage.MVCCGetProto(
		ctx, stateRO, s.RaftReplicaIDKey(), hlc.Timestamp{}, &replicaID,
		storage.MVCCGetOptions{ReadCategory: fs.ReplicationReadCategory},
	); err != nil || !ok {
		// NB: when err == nil && !ok, there is no RaftReplicaID. This can happen
		// only if the replica does not exist.
		return kvserverpb.RaftReplicaID{}, err
	}
	return replicaID, nil
}

// SetRaftReplicaID overwrites the RaftReplicaID.
func (s StateLoader) SetRaftReplicaID(
	ctx context.Context, stateWO StateWO, replicaID roachpb.ReplicaID,
) error {
	rid := kvserverpb.RaftReplicaID{ReplicaID: replicaID}
	// "Blind" because opts.Stats == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		stateWO,
		s.RaftReplicaIDKey(),
		hlc.Timestamp{}, /* timestamp */
		&rid,
		storage.MVCCWriteOptions{}, /* opts */
	)
}

// ClearRaftReplicaID clears the RaftReplicaID key.
func (s StateLoader) ClearRaftReplicaID(stateWO StateWO) error {
	return stateWO.ClearUnversioned(s.RaftReplicaIDKey(), storage.ClearOptions{})
}

// LoadRangeTombstone loads the RangeTombstone of the range.
func (s StateLoader) LoadRangeTombstone(
	ctx context.Context, stateRO StateRO,
) (kvserverpb.RangeTombstone, error) {
	var ts kvserverpb.RangeTombstone
	if ok, err := storage.MVCCGetProto(
		ctx, stateRO, s.RangeTombstoneKey(), hlc.Timestamp{}, &ts, storage.MVCCGetOptions{},
	); err != nil || !ok {
		// NB: when err == nil && !ok, there is no RangeTombstone. It is valid to
		// return RangeTombstone{} with a zero NextReplicaID, signifying that there
		// hasn't been a single replica removed for the RangeID.
		return kvserverpb.RangeTombstone{}, err
	}
	return ts, nil
}

// SetRangeTombstone writes the RangeTombstone.
func (s StateLoader) SetRangeTombstone(
	ctx context.Context, stateWO StateWO, ts kvserverpb.RangeTombstone,
) error {
	// "Blind" because ms == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(ctx, stateWO, s.RangeTombstoneKey(),
		hlc.Timestamp{}, &ts, storage.MVCCWriteOptions{})
}

// UninitializedReplicaState returns the ReplicaState of an uninitialized
// Replica with the given range ID. It is equivalent to StateLoader.Load from an
// empty storage.
func UninitializedReplicaState(rangeID roachpb.RangeID) kvserverpb.ReplicaState {
	return kvserverpb.ReplicaState{
		Desc:           &roachpb.RangeDescriptor{RangeID: rangeID},
		Lease:          &roachpb.Lease{},
		TruncatedState: nil, // Invariant: always nil.
		GCThreshold:    &hlc.Timestamp{},
		Stats:          &enginepb.MVCCStats{},
		GCHint:         &roachpb.GCHint{},
	}
}

// SynthesizeRaftState returns the initial raft state of an initialized replica,
// taking into account the HardState of the uninitialized replica it replaces.
//
// An uninitialized replica can have a non-empty HardState because such replicas
// are able to update the Term, participate in raft elections (update Vote and
// other supporting fields). When initializing the replica, we must not regress
// this HardState.
func SynthesizeRaftState(
	oldHS raftpb.HardState, applied logstore.EntryID,
) (_ raftpb.HardState, _ kvserverpb.RaftTruncatedState, err error) {
	// Assert that the replica is uninitialized.
	if commit := oldHS.Commit; commit != 0 {
		err = errors.Newf("uninitialized replica with a non-zero commit index %d", commit)
		return
	}
	// A raft log always starts at a fixed index/term of the virtual entry that
	// represents the initial applied state of the range (populated upon the
	// cluster bootstrap, or inherited by the range when it is split out).
	initTS := kvserverpb.RaftTruncatedState{
		Index: RaftInitialLogIndex,
		Term:  RaftInitialLogTerm,
	}
	// Assert that the applied state is initialized correctly.
	if applied != initTS {
		err = errors.Newf("applied entry ID %+v mismatches %+v", applied, initTS)
		return
	}

	// If the existing HardState is empty, or has an outdated term, we forget it,
	// and install one with a newer term. This is a valid transition from raft's
	// perspective.
	if oldHS.Term < RaftInitialLogTerm {
		return raftpb.HardState{
			Term:   RaftInitialLogTerm,
			Commit: RaftInitialLogIndex,
		}, initTS, nil
	}
	// Otherwise, the uninitialized replica has already participated in the
	// initial term or above. It might have voted, and might know the leader.
	// Preserve this information, and only move the Commit index forward.
	return raftpb.HardState{
		Term:      oldHS.Term,
		Commit:    RaftInitialLogIndex,
		Vote:      oldHS.Vote,
		Lead:      oldHS.Lead,
		LeadEpoch: oldHS.LeadEpoch,
	}, initTS, nil
}
