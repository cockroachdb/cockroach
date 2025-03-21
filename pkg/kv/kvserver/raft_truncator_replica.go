// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	r.mu.Lock() // TODO(pav-kv): not needed if raftMu is held.
	defer r.mu.Unlock()
	return r.shMu.raftTruncState
}

func (r *raftTruncatorReplica) handleTruncationResult(ctx context.Context, pt pendingTruncation) {
	(*Replica)(r).handleTruncatedStateResultRaftMuLocked(ctx, pt)
}

func (r *raftTruncatorReplica) getPendingTruncs() *pendingLogTruncations {
	return &r.pendingLogTruncations
}

func (r *raftTruncatorReplica) sideloadedStats(
	ctx context.Context, span kvpb.RaftSpan,
) (entries uint64, freed int64, err error) {
	return r.raftMu.sideloaded.Stats(ctx, span)
}

func (r *raftTruncatorReplica) getStateLoader() stateloader.StateLoader {
	// NB: the replicaForTruncator contract says that Replica.raftMu is held for
	// the duration of the existence of replicaForTruncator, so we return the
	// r.raftMu.stateloader (and not r.mu.stateLoader).
	return r.raftMu.stateLoader
}
