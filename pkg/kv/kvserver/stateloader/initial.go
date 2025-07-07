// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stateloader

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RaftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	RaftInitialLogIndex = 10
	RaftInitialLogTerm  = 5
)

const (
	// InitialLeaseAppliedIndex is the starting LAI of a Range. All proposals are
	// assigned higher LAIs.
	//
	// The LAIs <= InitialLeaseAppliedIndex are reserved, e.g. for injected
	// out-of-order proposals in tests. A bunch of tests override LAI to 1, in
	// order to force re-proposals. We don't want "real" proposals racing with
	// injected ones, this can cause a closed timestamp regression.
	//
	// https://github.com/cockroachdb/cockroach/issues/70894#issuecomment-1881165404
	InitialLeaseAppliedIndex = 10
)

// WriteInitialReplicaState sets up a new Range, but without writing an
// associated Raft state (which must be written separately via
// SynthesizeRaftState before instantiating a Replica). The main task is to
// persist a ReplicaState which does not start from zero but presupposes a few
// entries already having applied. The supplied MVCCStats are used for the Stats
// field after adjusting for persisting the state itself, and the updated stats
// are returned.
func WriteInitialReplicaState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	gcHint roachpb.GCHint,
	replicaVersion roachpb.Version,
) (enginepb.MVCCStats, error) {
	s := kvserverpb.ReplicaState{
		RaftAppliedIndex:     RaftInitialLogIndex,
		RaftAppliedIndexTerm: RaftInitialLogTerm,
		LeaseAppliedIndex:    InitialLeaseAppliedIndex,
		Desc: &roachpb.RangeDescriptor{
			RangeID: desc.RangeID,
		},
		Lease:       &lease,
		Stats:       &ms,
		GCThreshold: &gcThreshold,
		GCHint:      &gcHint,
	}
	if (replicaVersion != roachpb.Version{}) {
		s.Version = &replicaVersion
	}

	rsl := Make(desc.RangeID)
	if existingLease, err := rsl.LoadLease(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading lease")
	} else if (existingLease != roachpb.Lease{}) {
		log.Fatalf(ctx, "expected trivial lease, but found %+v", existingLease)
	}

	if existingGCThreshold, err := rsl.LoadGCThreshold(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCThreshold")
	} else if !existingGCThreshold.IsEmpty() {
		log.Fatalf(ctx, "expected trivial GCthreshold, but found %+v", existingGCThreshold)
	}

	if existingGCHint, err := rsl.LoadGCHint(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCHint")
	} else if !existingGCHint.IsEmpty() {
		return enginepb.MVCCStats{}, errors.AssertionFailedf("expected trivial GCHint, but found %+v", existingGCHint)
	}

	if existingVersion, err := rsl.LoadVersion(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading Version")
	} else if (existingVersion != roachpb.Version{}) {
		log.Fatalf(ctx, "expected trivial version, but found %+v", existingVersion)
	}

	newMS, err := rsl.Save(ctx, readWriter, s)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}

// WriteInitialTruncState writes the initial truncated state.
//
// TODO(arul): this can be removed once no longer call this from the split
// evaluation path.
func WriteInitialTruncState(ctx context.Context, w storage.Writer, rangeID roachpb.RangeID) error {
	return logstore.NewStateLoader(rangeID).SetRaftTruncatedState(ctx, w,
		&kvserverpb.RaftTruncatedState{
			Index: RaftInitialLogIndex,
			Term:  RaftInitialLogTerm,
		})
}

// WriteInitialRangeState writes the initial range state. It's called during
// bootstrap.
func WriteInitialRangeState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	desc roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
	replicaVersion roachpb.Version,
) error {
	initialLease := roachpb.Lease{}
	initialGCThreshold := hlc.Timestamp{}
	initialGCHint := roachpb.GCHint{}
	initialMS := enginepb.MVCCStats{}

	if _, err := WriteInitialReplicaState(
		ctx, readWriter, initialMS, desc, initialLease, initialGCThreshold, initialGCHint,
		replicaVersion,
	); err != nil {
		return err
	}
	// Maintain the invariant that any replica (uninitialized or initialized),
	// with persistent state, has a RaftReplicaID.
	if err := Make(desc.RangeID).SetRaftReplicaID(ctx, readWriter, replicaID); err != nil {
		return err
	}

	// TODO(sep-raft-log): when the log storage is separated, raft state must be
	// written separately.
	return WriteInitialRaftState(ctx, readWriter, desc.RangeID)
}

// WriteInitialRaftState writes raft state for an initialized replica created
// during cluster bootstrap.
func WriteInitialRaftState(
	ctx context.Context, writer storage.Writer, rangeID roachpb.RangeID,
) error {
	sl := logstore.NewStateLoader(rangeID)
	// Initialize the HardState with the term and commit index matching the
	// initial applied state of the replica.
	if err := sl.SetHardState(ctx, writer, raftpb.HardState{
		Term:   RaftInitialLogTerm,
		Commit: RaftInitialLogIndex,
	}); err != nil {
		return err
	}
	// The raft log is initialized empty, with the truncated state matching the
	// committed / applied initial state of the replica.
	return sl.SetRaftTruncatedState(ctx, writer, &kvserverpb.RaftTruncatedState{
		Index: RaftInitialLogIndex,
		Term:  RaftInitialLogTerm,
	})
}
