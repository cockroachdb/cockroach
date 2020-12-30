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
	readWriter storage.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	truncStateType TruncatedStateType,
	replicaVersion roachpb.Version,
) (enginepb.MVCCStats, error) {
	rsl := Make(desc.RangeID)
	var s kvserverpb.ReplicaState
	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = &ms
	s.Lease = &lease
	s.GCThreshold = &gcThreshold
	if (replicaVersion != roachpb.Version{}) {
		s.Version = &replicaVersion
	}
	if truncStateType != TruncatedStateLegacyReplicatedAndNoAppliedKey {
		s.UsingAppliedStateKey = true
	}

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

	if existingVersion, err := rsl.LoadVersion(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading Version")
	} else if (existingVersion != roachpb.Version{}) {
		log.Fatalf(ctx, "expected trivial version, but found %+v", existingVersion)
	}

	newMS, err := rsl.Save(ctx, readWriter, s, truncStateType)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}

// WriteInitialRangeState writes the initial range state. It's called during
// bootstrap.
func WriteInitialRangeState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	desc roachpb.RangeDescriptor,
	replicaVersion roachpb.Version,
) error {
	const initialTruncStateType = TruncatedStateUnreplicated
	return WriteInitialRangeStateWithTruncatedState(ctx, readWriter, desc, replicaVersion, initialTruncStateType)
}

// WriteInitialRangeStateWithTruncatedState is the same as
// WriteInitialRangeState, but allows the caller to override the truncated state
// type.
//
// TODO(irfansharif): This can be removed in the v21.2 cycle after we no longer
// need to test the truncated state migration.
func WriteInitialRangeStateWithTruncatedState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	desc roachpb.RangeDescriptor,
	replicaVersion roachpb.Version,
	truncState TruncatedStateType,
) error {
	initialLease := roachpb.Lease{}
	initialGCThreshold := hlc.Timestamp{}
	initialMS := enginepb.MVCCStats{}
	initialTruncStateType := truncState

	if _, err := WriteInitialReplicaState(
		ctx, readWriter, initialMS, desc, initialLease, initialGCThreshold,
		initialTruncStateType, replicaVersion,
	); err != nil {
		return err
	}
	if err := Make(desc.RangeID).SynthesizeRaftState(ctx, readWriter); err != nil {
		return err
	}
	return nil
}
