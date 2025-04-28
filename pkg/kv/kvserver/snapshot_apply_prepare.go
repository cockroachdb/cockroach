// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// prepareSnapshotInput contains the data needed to prepare the on-disk state for a snapshot.
type prepareSnapshotInput struct {
	ctx                   context.Context
	id                    storage.FullReplicaID
	st                    *cluster.Settings
	truncState            kvserverpb.RaftTruncatedState
	hs                    raftpb.HardState
	logSL                 *logstore.StateLoader
	writeSST              func(context.Context, []byte) error
	desc                  *roachpb.RangeDescriptor
	subsumedDescs         []*roachpb.RangeDescriptor
	todoEng               storage.Engine
	subsumedNextReplicaID roachpb.ReplicaID
}

// preparedSnapshot contains the results of preparing the snapshot on disk.
type preparedSnapshot struct {
	clearedSpan          roachpb.Span
	clearedSubsumedSpans []roachpb.Span
}

// prepareSnapshot writes the unreplicated SST for the snapshot and clears disk data for subsumed replicas.
func prepareSnapshot(input prepareSnapshotInput) (preparedSnapshot, error) {
	// Step 1: Write unreplicated SST
	unreplicatedSSTFile, clearedSpan, err := writeUnreplicatedSST(
		input.ctx, input.id, input.st, input.truncState, input.hs, input.logSL,
	)
	if err != nil {
		return preparedSnapshot{}, err
	}
	if err := input.writeSST(input.ctx, unreplicatedSSTFile.Data()); err != nil {
		return preparedSnapshot{}, err
	}

	var clearedSubsumedSpans []roachpb.Span
	if len(input.subsumedDescs) > 0 {
		// If we're subsuming a replica below, we don't have its last NextReplicaID,
		// nor can we obtain it. That's OK: we can just be conservative and use the
		// maximum possible replica ID. preDestroyRaftMuLocked will write a replica
		// tombstone using this maximum possible replica ID, which would normally be
		// problematic, as it would prevent this store from ever having a new replica
		// of the removed range. In this case, however, it's copacetic, as subsumed
		// ranges _can't_ have new replicas.
		spans, err := clearSubsumedReplicaDiskData(
			input.ctx, input.st, input.todoEng, input.writeSST,
			input.desc, input.subsumedDescs, input.subsumedNextReplicaID,
		)
		if err != nil {
			return preparedSnapshot{}, err
		}
		clearedSubsumedSpans = spans
	}

	return preparedSnapshot{
		clearedSpan:          clearedSpan,
		clearedSubsumedSpans: clearedSubsumedSpans,
	}, nil
}
