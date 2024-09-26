// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Implementation of the replicaForTruncator interface.
type raftTruncatorReplica Replica

var _ replicaForTruncator = &raftTruncatorReplica{}

func (r *raftTruncatorReplica) getRangeID() roachpb.RangeID {
	return r.RangeID
}

func (r *raftTruncatorReplica) getTruncatedState() kvserverpb.RaftTruncatedState {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TruncatedState is guaranteed to be non-nil.
	return *r.mu.state.TruncatedState
}

func (r *raftTruncatorReplica) setTruncatedStateAndSideEffects(
	ctx context.Context,
	trunc *kvserverpb.RaftTruncatedState,
	expectedFirstIndexPreTruncation kvpb.RaftIndex,
) (expectedFirstIndexWasAccurate bool) {
	_, expectedFirstIndexAccurate := (*Replica)(r).handleTruncatedStateResult(
		ctx, trunc, expectedFirstIndexPreTruncation)
	return expectedFirstIndexAccurate
}

func (r *raftTruncatorReplica) setTruncationDeltaAndTrusted(deltaBytes int64, isDeltaTrusted bool) {
	r.raftMu.AssertHeld()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.orRaftMu.raftLogSize += deltaBytes
	r.mu.orRaftMu.raftLogLastCheckSize += deltaBytes
	// Ensure raftLog{,LastCheck}Size is not negative since it isn't persisted
	// between server restarts.
	if r.mu.orRaftMu.raftLogSize < 0 {
		r.mu.orRaftMu.raftLogSize = 0
	}
	if r.mu.orRaftMu.raftLogLastCheckSize < 0 {
		r.mu.orRaftMu.raftLogLastCheckSize = 0
	}
	if !isDeltaTrusted {
		r.mu.orRaftMu.raftLogSizeTrusted = false
	}
}

func (r *raftTruncatorReplica) getPendingTruncs() *pendingLogTruncations {
	return &r.pendingLogTruncations
}

func (r *raftTruncatorReplica) sideloadedBytesIfTruncatedFromTo(
	ctx context.Context, from, to kvpb.RaftIndex,
) (freed int64, err error) {
	freed, _, err = r.raftMu.sideloaded.BytesIfTruncatedFromTo(ctx, from, to)
	return freed, err
}

func (r *raftTruncatorReplica) getStateLoader() stateloader.StateLoader {
	// NB: the replicaForTruncator contract says that Replica.raftMu is held for
	// the duration of the existence of replicaForTruncator, so we return the
	// r.raftMu.stateloader (and not r.mu.stateLoader).
	return r.raftMu.stateLoader
}
