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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// raftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	raftInitialLogIndex = 10
	raftInitialLogTerm  = 5
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
	stateRW, logRW storage.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	gcHint roachpb.GCHint,
	replicaVersion roachpb.Version,
) (enginepb.MVCCStats, error) {
	rsl := Make(desc.RangeID)
	var s kvserverpb.ReplicaState
	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.RaftAppliedIndexTerm = s.TruncatedState.Term
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = &ms
	s.Lease = &lease
	s.GCThreshold = &gcThreshold
	s.GCHint = &gcHint
	if (replicaVersion != roachpb.Version{}) {
		s.Version = &replicaVersion
	}

	if existingLease, err := rsl.LoadLease(ctx, stateRW); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading lease")
	} else if (existingLease != roachpb.Lease{}) {
		log.Fatalf(ctx, "expected trivial lease, but found %+v", existingLease)
	}

	if existingGCThreshold, err := rsl.LoadGCThreshold(ctx, stateRW); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCThreshold")
	} else if !existingGCThreshold.IsEmpty() {
		log.Fatalf(ctx, "expected trivial GCthreshold, but found %+v", existingGCThreshold)
	}

	if existingGCHint, err := rsl.LoadGCHint(ctx, stateRW); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCHint")
	} else if !existingGCHint.IsEmpty() {
		return enginepb.MVCCStats{}, errors.AssertionFailedf("expected trivial GCHint, but found %+v", existingGCHint)
	}

	if existingVersion, err := rsl.LoadVersion(ctx, stateRW); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading Version")
	} else if (existingVersion != roachpb.Version{}) {
		log.Fatalf(ctx, "expected trivial version, but found %+v", existingVersion)
	}

	newMS, err := rsl.Save(ctx, stateRW, s)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	if err := rsl.SetRaftTruncatedState(ctx, logRW, s.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}

// WriteInitialRangeState writes the initial range state. It's called during
// bootstrap.
func WriteInitialRangeState(
	ctx context.Context,
	stateRW, logRW storage.ReadWriter,
	desc roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
	replicaVersion roachpb.Version,
) error {
	initialLease := roachpb.Lease{}
	initialGCThreshold := hlc.Timestamp{}
	initialGCHint := roachpb.GCHint{}
	initialMS := enginepb.MVCCStats{}

	if _, err := WriteInitialReplicaState(
		ctx, stateRW, logRW, initialMS, desc, initialLease, initialGCThreshold, initialGCHint,
		replicaVersion,
	); err != nil {
		return err
	}

	sl := Make(desc.RangeID)
	if err := sl.SynthesizeRaftState(ctx, stateRW, logRW); err != nil {
		return err
	}
	// Maintain the invariant that any replica (uninitialized or initialized),
	// with persistent state, has a RaftReplicaID.
	if err := sl.SetRaftReplicaID(ctx, logRW, replicaID); err != nil {
		return err
	}
	return nil
}
