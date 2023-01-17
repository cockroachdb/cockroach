// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stateloader

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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

// Make creates a StateLoader.
func Make(rangeID roachpb.RangeID) StateLoader {
	return StateLoader{
		StateLoader: logstore.NewStateLoader(rangeID),
	}
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (rsl StateLoader) Load(
	ctx context.Context, reader storage.Reader, desc *roachpb.RangeDescriptor,
) (kvserverpb.ReplicaState, error) {
	var s kvserverpb.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	lease, err := rsl.LoadLease(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	s.Lease = &lease

	if s.GCThreshold, err = rsl.LoadGCThreshold(ctx, reader); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	if s.GCHint, err = rsl.LoadGCHint(ctx, reader); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	as, err := rsl.LoadRangeAppliedState(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	s.RaftAppliedIndex = as.RaftAppliedIndex
	s.RaftAppliedIndexTerm = as.RaftAppliedIndexTerm
	s.LeaseAppliedIndex = as.LeaseAppliedIndex
	ms := as.RangeStats.ToStats()
	s.Stats = &ms
	s.RaftClosedTimestamp = as.RaftClosedTimestamp

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	version, err := rsl.LoadVersion(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	if (version != roachpb.Version{}) {
		s.Version = &version
	}

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
	ctx context.Context,
	readWriter storage.ReadWriter,
	state kvserverpb.ReplicaState,
	gcHintEnabled bool,
) (enginepb.MVCCStats, error) {
	ms := state.Stats
	if err := rsl.SetLease(ctx, readWriter, ms, *state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetGCThreshold(ctx, readWriter, ms, state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if gcHintEnabled {
		if _, err := rsl.SetGCHint(ctx, readWriter, ms, state.GCHint, gcHintEnabled); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	// TODO(sep-raft-log): SetRaftTruncatedState will be in a separate batch when
	// the Raft log engine is separated. Figure out the ordering required here.
	if err := rsl.SetRaftTruncatedState(ctx, readWriter, state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if state.Version != nil {
		if err := rsl.SetVersion(ctx, readWriter, ms, state.Version); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	if err := rsl.SetRangeAppliedState(
		ctx,
		readWriter,
		state.RaftAppliedIndex,
		state.LeaseAppliedIndex,
		state.RaftAppliedIndexTerm,
		ms,
		state.RaftClosedTimestamp,
		nil,
	); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return *ms, nil
}

// LoadLease loads the lease.
func (rsl StateLoader) LoadLease(
	ctx context.Context, reader storage.Reader,
) (roachpb.Lease, error) {
	var lease roachpb.Lease
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCGetOptions{})
	return lease, err
}

// SetLease persists a lease.
func (rsl StateLoader) SetLease(
	ctx context.Context, readWriter storage.ReadWriter, ms *enginepb.MVCCStats, lease roachpb.Lease,
) error {
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, &lease)
}

// LoadRangeAppliedState loads the Range applied state.
func (rsl StateLoader) LoadRangeAppliedState(
	ctx context.Context, reader storage.Reader,
) (*enginepb.RangeAppliedState, error) {
	var as enginepb.RangeAppliedState
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
		storage.MVCCGetOptions{})
	return &as, err
}

// LoadMVCCStats loads the MVCC stats.
func (rsl StateLoader) LoadMVCCStats(
	ctx context.Context, reader storage.Reader,
) (enginepb.MVCCStats, error) {
	// Check the applied state key.
	as, err := rsl.LoadRangeAppliedState(ctx, reader)
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
func (rsl StateLoader) SetRangeAppliedState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	appliedIndex, leaseAppliedIndex, appliedIndexTerm uint64,
	newMS *enginepb.MVCCStats,
	raftClosedTimestamp hlc.Timestamp,
	asAlloc *enginepb.RangeAppliedState, // optional
) error {
	if asAlloc == nil {
		asAlloc = new(enginepb.RangeAppliedState)
	}
	as := asAlloc
	*as = enginepb.RangeAppliedState{
		RaftAppliedIndex:     appliedIndex,
		LeaseAppliedIndex:    leaseAppliedIndex,
		RangeStats:           newMS.ToPersistentStats(),
		RaftClosedTimestamp:  raftClosedTimestamp,
		RaftAppliedIndexTerm: appliedIndexTerm,
	}
	// The RangeAppliedStateKey is not included in stats. This is also reflected
	// in ComputeStats.
	ms := (*enginepb.MVCCStats)(nil)
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeAppliedStateKey(),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, as)
}

// SetMVCCStats overwrites the MVCC stats. This needs to perform a read on the
// RangeAppliedState key before overwriting the stats. Use SetRangeAppliedState
// when performance is important.
func (rsl StateLoader) SetMVCCStats(
	ctx context.Context, readWriter storage.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	as, err := rsl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return err
	}
	alloc := as // reuse
	return rsl.SetRangeAppliedState(
		ctx, readWriter, as.RaftAppliedIndex, as.LeaseAppliedIndex, as.RaftAppliedIndexTerm, newMS,
		as.RaftClosedTimestamp, alloc)
}

// SetClosedTimestamp overwrites the closed timestamp.
func (rsl StateLoader) SetClosedTimestamp(
	ctx context.Context, readWriter storage.ReadWriter, closedTS hlc.Timestamp,
) error {
	as, err := rsl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return err
	}
	alloc := as // reuse
	return rsl.SetRangeAppliedState(
		ctx, readWriter, as.RaftAppliedIndex, as.LeaseAppliedIndex, as.RaftAppliedIndexTerm,
		as.RangeStats.ToStatsPtr(), closedTS, alloc)
}

// LoadGCThreshold loads the GC threshold.
func (rsl StateLoader) LoadGCThreshold(
	ctx context.Context, reader storage.Reader,
) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeGCThresholdKey(),
		hlc.Timestamp{}, &t, storage.MVCCGetOptions{})
	return &t, err
}

// SetGCThreshold sets the GC threshold.
func (rsl StateLoader) SetGCThreshold(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeGCThresholdKey(),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, threshold)
}

// LoadGCHint loads GC hint.
func (rsl StateLoader) LoadGCHint(
	ctx context.Context, reader storage.Reader,
) (*roachpb.GCHint, error) {
	var h roachpb.GCHint
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeGCHintKey(),
		hlc.Timestamp{}, &h, storage.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	return &h, nil
}

// SetGCHint writes the GC hint.
func (rsl StateLoader) SetGCHint(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	hint *roachpb.GCHint,
	gcHintEnabled bool,
) (updated bool, _ error) {
	if !gcHintEnabled {
		return false, nil
	}
	if hint == nil {
		return false, errors.New("cannot persist nil GCHint")
	}
	if err := storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeGCHintKey(),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, hint); err != nil {
		return false, err
	}
	return true, nil
}

// LoadVersion loads the replica version.
func (rsl StateLoader) LoadVersion(
	ctx context.Context, reader storage.Reader,
) (roachpb.Version, error) {
	var version roachpb.Version
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeVersionKey(),
		hlc.Timestamp{}, &version, storage.MVCCGetOptions{})
	return version, err
}

// SetVersion sets the replica version.
func (rsl StateLoader) SetVersion(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	version *roachpb.Version,
) error {
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeVersionKey(),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, version)
}

// UninitializedReplicaState returns the ReplicaState of an uninitialized
// Replica with the given range ID. It is equivalent to StateLoader.Load from an
// empty storage.
func UninitializedReplicaState(rangeID roachpb.RangeID) kvserverpb.ReplicaState {
	return kvserverpb.ReplicaState{
		Desc:           &roachpb.RangeDescriptor{RangeID: rangeID},
		Lease:          &roachpb.Lease{},
		TruncatedState: &roachpb.RaftTruncatedState{},
		GCThreshold:    &hlc.Timestamp{},
		Stats:          &enginepb.MVCCStats{},
		GCHint:         &roachpb.GCHint{},
	}
}

// The rest is not technically part of ReplicaState.

// SynthesizeRaftState creates a Raft state which synthesizes both a HardState
// and a lastIndex from pre-seeded data in the engine (typically created via
// WriteInitialReplicaState and, on a split, perhaps the activity of an
// uninitialized Raft group)
func (rsl StateLoader) SynthesizeRaftState(
	ctx context.Context, readWriter storage.ReadWriter,
) error {
	hs, err := rsl.LoadHardState(ctx, readWriter)
	if err != nil {
		return err
	}
	truncState, err := rsl.LoadRaftTruncatedState(ctx, readWriter)
	if err != nil {
		return err
	}
	as, err := rsl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return err
	}
	return rsl.SynthesizeHardState(ctx, readWriter, hs, truncState, as.RaftAppliedIndex)
}
