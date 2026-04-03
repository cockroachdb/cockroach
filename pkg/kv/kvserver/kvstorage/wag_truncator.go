// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
)

// truncateAppliedNode checks the first WAG node and deletes it if all of its
// events have been applied to the state engine. Returns true if a node was
// deleted.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet.
func truncateAppliedNode(ctx context.Context, raft Raft, stateRO StateRO) (bool, error) {
	var iter wag.Iterator
	for index, node := range iter.Iter(ctx, raft.RO) {
		// TODO(ibrahim): Right now, the canApplyWAGNode function returns a list of
		// raftCatchUpTargets that are not needed for the purposes of truncation,
		// consider refactoring the function to return only the needed info.
		replayAction, err := canApplyWAGNode(ctx, node, stateRO)
		if err != nil {
			return false, err
		}
		if replayAction.apply {
			// If an event needs to be applied, the WAG node cannot be deleted yet.
			return false, nil
		}
		if err := wag.Delete(raft.WO, index); err != nil {
			return false, err
		}
		// TODO(Ibrahim): Add logic to clear raft state (log entries, HardState,
		// TruncatedState) for destroyed/subsumed replicas.
		// TODO(ibrahim): Support deleting multiple WAG nodes within the same batch.
		return true, nil
	}
	if err := iter.Error(); err != nil {
		return false, err
	}
	return false, nil
}
