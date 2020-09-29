// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

var _ Migration = UnreplicatedTruncatedStateMigration
var _ Migration = GenerationComparableMigration
var _ Migration = SeparateRaftLogMigration
var _ Migration = ReplicatedLockTableMigration
var _ Migration = ReplicasKnowReplicaIDMigration

func UnreplicatedTruncatedStateMigration(ctx context.Context, h *Helper) error {
	if err := h.IterRangeDescriptors(func(descs ...roachpb.RangeDescriptor) error {
		// Really AdminMigrate but it doesn't exist yet. It takes an argument
		// determining the range version to migrate into. Unsure if that's a
		// different kind of version or just `h.Version()`. I think I lean
		// towards the latter.
		_, err := h.db.Scan(ctx, descs[0].StartKey, descs[len(descs)-1].EndKey, -1)
		return err
	}); err != nil {
		return err
	}
	if err := h.EveryNode(ctx, "forced-replicagc"); err != nil {
		return err
	}
	return nil
}

func GenerationComparableMigration(ctx context.Context, h *Helper) error {
	// Hmm now that I look at this again I think we can remove it just like that.
	// No snapshots are in flight any more that don't respect the generational
	// rules. Replicas get replicaGC'ed eagerly. (Once round of forced-replicagc
	// somewhere is enough). Need to think this through more.
	return nil
}

func SeparateRaftLogMigration(ctx context.Context, h *Helper) error {
	// Impl will loop through all repls on the node and migrate them one by one
	// (get raftMu, check if already copied, if not copy stuff, delete old stuff,
	// release raftMu).
	if err := h.EveryNode(ctx, "move-raft-logs"); err != nil {
		return err
	}
	// TODO: sanity check that all are really using separate raft log now?
	return nil
}

func ReplicatedLockTableMigration(ctx context.Context, h *Helper) error {
	// Pretty vanilla Raft migration, see
	// https://github.com/cockroachdb/cockroach/issues/41720#issuecomment-590817261
	// also run forced-replicagc.
	// Should probably bake the forced replicaGC and migrate command into a
	// helper, there's never a reason not to run forced-replicaGC after a Migrate.
	return nil
}

func ReplicasKnowReplicaIDMigration(ctx context.Context, h *Helper) error {
	// Is a migration necessary here or are we already nuking them at startup anyway?
	// Oh seems like it, so nothing to do here:
	// https://github.com/cockroachdb/cockroach/blob/f0e751f00b7ba41f39dd83ddc8238011fa5b9c19/pkg/storage/store.go#L1347-L1350
	return nil
}
