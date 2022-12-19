// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// PrepareLogStoreSnapshotSST prepares the SST that should be ingested to apply
// the snapshot with the given metadata.
//
// The full snapshot contains additional SSTs reflecting the state machine which
// is not reflected here. It must be ingested first.
//
// If a crash occurs between ingesting the statemachine SST(s) and the log store SSTs,
// start-up reconciliation will re-ingest.
func PrepareLogStoreSnapshotSST(
	ctx context.Context,
	id storage.FullReplicaID,
	hs raftpb.HardState,
	snap raftpb.SnapshotMetadata,
	logStoreSSTWriter *storage.SSTWriter,
) error {
	if raft.IsEmptyHardState(hs) {
		// Raft will never provide an empty HardState if it is providing a
		// nonempty snapshot because we discard snapshots that do not increase
		// the commit index.
		return errors.AssertionFailedf("found empty HardState for non-empty Snapshot %+v", snap)
	}

	// Clearing the unreplicated state.
	//
	// NB: We do not expect to see range keys in the unreplicated state, so
	// we don't drop a range tombstone across the range key space.
	unreplicatedPrefixKey := keys.MakeRangeIDUnreplicatedPrefix(id.RangeID)
	unreplicatedStart := unreplicatedPrefixKey
	unreplicatedEnd := unreplicatedPrefixKey.PrefixEnd()
	if err := logStoreSSTWriter.ClearRawRange(
		unreplicatedStart, unreplicatedEnd, true /* pointKeys */, false, /* rangeKeys */
	); err != nil {
		return errors.Wrapf(err, "error clearing range of unreplicated SST writer")
	}

	sl := logstore.NewStateLoader(id.RangeID)
	// Update HardState.
	if err := sl.SetHardState(ctx, logStoreSSTWriter, hs); err != nil {
		return errors.Wrapf(err, "unable to write HardState to unreplicated SST writer")
	}
	// We've cleared all the raft state above, so we are forced to write the
	// RaftReplicaID again here.
	if err := sl.SetRaftReplicaID(
		ctx, logStoreSSTWriter, id.ReplicaID); err != nil {
		return errors.Wrapf(err, "unable to write RaftReplicaID to unreplicated SST writer")
	}
	if err := sl.SetRaftTruncatedState(
		ctx, logStoreSSTWriter,
		&roachpb.RaftTruncatedState{
			Index: snap.Index,
			Term:  snap.Term,
		},
	); err != nil {
		return errors.Wrapf(err, "unable to write TruncatedState to unreplicated SST writer")
	}

	return nil
}
