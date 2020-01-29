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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/raftstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// RaftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	RaftInitialLogIndex = 10
	RaftInitialLogTerm  = 5
)

// WriteInitialReplicaStateUpstreamOfRaft is like writeInitialReplicaState,
// except "raft data" is addressed to just the one provided engine.ReadWriter.
// Since we're upstream of raft, we're only evaluating replica state writes.
// Because commands are only evaluated on batches derived from the one data
// engine, we simply collect replica state writes in the one provided
// engine.Batch.
func WriteInitialReplicaStateUpstreamOfRaft(
	ctx context.Context,
	readWriter engine.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	truncStateType TruncatedStateType,
) (enginepb.MVCCStats, error) {
	// Upstream of raft, we're only ever evaluating writes to replica state, not
	// actually persisting them.
	batch, ok := readWriter.(engine.Batch)
	if !ok {
		panic("expected readWriter to be an engine.Batch")
	}

	return writeInitialReplicaState(
		ctx, readWriter, raftstorage.WrapBatch(batch), ms, desc, lease, gcThreshold, truncStateType,
	)
}

// WriteInitialStateDuringBootstrap writes the initial replica state to the
// provided engine.ReadWriters (derived from the data engine and the raft engine
// respectively), and synthesizes the Raft state required before instantiating a
// replica.
func WriteInitialStateDuringBootstrap(
	ctx context.Context,
	readWriter engine.ReadWriter,
	raftRW raftstorage.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	truncStateType TruncatedStateType,
) (enginepb.MVCCStats, error) {
	newMS, err := writeInitialReplicaState(
		ctx, readWriter, raftRW, ms, desc, lease, gcThreshold, truncStateType,
	)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := Make(desc.RangeID).SynthesizeRaftState(ctx, readWriter, raftRW); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return newMS, nil
}

// writeInitialReplicaState sets up a new Range, but without writing an
// associated Raft state (which must be written separately via
// SynthesizeRaftState). The main task is to persist a ReplicaState which does
// not start from zero but presupposes a few entries already having applied. The
// supplied MVCCStats are used for the Stats field after adjusting for
// persisting the state itself, and the updated stats are returned.
func writeInitialReplicaState(
	ctx context.Context,
	readWriter engine.ReadWriter,
	raftRW raftstorage.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	truncStateType TruncatedStateType,
) (enginepb.MVCCStats, error) {
	rsl := Make(desc.RangeID)
	var s storagepb.ReplicaState
	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  RaftInitialLogTerm,
		Index: RaftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = &ms
	s.Lease = &lease
	s.GCThreshold = &gcThreshold
	s.UsingAppliedStateKey = true

	if existingLease, err := rsl.LoadLease(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading lease")
	} else if (existingLease != roachpb.Lease{}) {
		log.Fatalf(ctx, "expected trivial lease, but found %+v", existingLease)
	}

	if existingGCThreshold, err := rsl.LoadGCThreshold(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCThreshold")
	} else if (*existingGCThreshold != hlc.Timestamp{}) {
		log.Fatalf(ctx, "expected trivial GChreshold, but found %+v", existingGCThreshold)
	}

	newMS, err := rsl.Save(ctx, readWriter, raftRW, s, truncStateType)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}
