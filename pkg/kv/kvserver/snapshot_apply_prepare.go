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
	ctx        context.Context
	id         storage.FullReplicaID
	st         *cluster.Settings
	truncState kvserverpb.RaftTruncatedState
	hs         raftpb.HardState
	logSL      *logstore.StateLoader
	writeSST   func(context.Context, []byte) error
}

// preparedSnapshot contains the results of preparing the snapshot on disk.
type preparedSnapshot struct {
	clearedSpan roachpb.Span
}

// prepareSnapshot writes the unreplicated SST for the snapshot and returns the cleared span.
func prepareSnapshot(input prepareSnapshotInput) (preparedSnapshot, error) {
	unreplicatedSSTFile, clearedSpan, err := writeUnreplicatedSST(
		input.ctx, input.id, input.st, input.truncState, input.hs, input.logSL,
	)
	if err != nil {
		return preparedSnapshot{}, err
	}
	if err := input.writeSST(input.ctx, unreplicatedSSTFile.Data()); err != nil {
		return preparedSnapshot{}, err
	}
	return preparedSnapshot{
		clearedSpan: clearedSpan,
	}, nil
}
