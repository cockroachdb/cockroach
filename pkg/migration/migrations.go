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
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the version gate.
var registry = make(map[clusterversion.ClusterVersion]Migration)

func init() {
	register(clusterversion.TruncatedAndRangeAppliedStateMigration, TruncatedStateMigration,
		"use unreplicated TruncatedState and RangeAppliedState for all ranges")
}

// Status represents the status of any given migration. This is captured in the
// system.migrations table.
type Status string

const (
	// StatusRunning is for migrations that are currently in progress.
	StatusRunning Status = "running"
	// StatusSucceeded is for jobs migrations that have successfully completed.
	StatusSucceeded Status = "succeeded"
	// StatusFailed is for migrations that have failed to execute.
	StatusFailed Status = "failed"
)

// Migration defines a program to be executed once every node in the cluster is
// (a) running a specific binary version, and (b) has completed all prior
// migrations.
//
// Each migration is associated with a specific internal cluster version and is
// idempotent in nature. When setting the cluster version (via `SET CLUSTER
// SETTING version`), the manager process determines the set of migrations
// needed to bridge the gap between the current active cluster version, and the
// target one. See [1] for where that happens.
//
// To introduce a migration, start by adding version key to pkg/clusterversion
// and introducing a corresponding internal cluster version for it. See [2] for
// more details. Following that, define a Migration in this package and add it
// to the registry. Be sure to key it in with the new cluster version we just
// added. During cluster upgrades, once the operator is able to set a cluster
// version setting that's past the version that was introduced (typically the
// major release version the migration was introduced in), the manager will
// execute the defined migration before letting the upgrade finalize.
//
// If the migration requires below-Raft level changes ([3] is one example),
// you'll need to add a version switch and the relevant KV-level migration in
// [4]. See IterateRangeDescriptors and the Migrate KV request for more details.
//
// [1]: `(*Manager).Migrate`
// [2]: pkg/clusterversion/cockroach_versions.go
// [3]: TruncatedStateMigration
// [4]: pkg/kv/kvserver/batch_eval/cmd_migrate.go
type Migration struct {
	cv   clusterversion.ClusterVersion
	fn   migrationFn
	desc string
}

type migrationFn func(context.Context, *Helper) error

// Run kickstarts the actual migration process. It's responsible for recording
// the ongoing status of the migration in the `system.migrations` table.
func (m *Migration) Run(ctx context.Context, h *Helper) (err error) {
	defer func() {
		if err != nil {
			// Mark ourselves as failed and record the last known error for
			// visibility.
			_ = h.updateStatus(ctx, StatusFailed)
			_ = h.UpdateProgress(ctx, fmt.Sprintf("error during migration: %s", err))
		}
	}()

	if err := h.insertMigrationRecord(ctx, m.desc); err != nil {
		return err
	}

	if err := m.fn(ctx, h); err != nil {
		return err
	}

	if err := h.updateStatus(ctx, StatusSucceeded); err != nil {
		return err
	}

	return nil
}

// defaultBlockSize controls
// We pull this block size out of thin air(-ish). Let's consider a cluster
// with 50k ranges, with each range taking ~200ms. We're being somewhat
// conservative with the duration, but in a wide-area cluster with large
// hops between the manager and the replicas, it could be true. Here's how
// long it'll take for various block sizes:
//
//   block size == 1 	~ 	2h 46m
//   block size == 50 	~ 	3m 20s
//   block size == 200	~ 	50s
const defaultBlockSize = 200

func TruncatedStateMigration(ctx context.Context, h *Helper) error {
	var batchIdx, numMigratedRanges int
	init := func() { batchIdx, numMigratedRanges = 1, 0 }
	if err := h.IterateRangeDescriptors(ctx, defaultBlockSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
		for _, desc := range descriptors {
			// This is a bit of a wart. We want to reach the first range, but we
			// can't address the (local) StartKey. However, keys.LocalMax is on
			// r1, so we'll just use that instead to target r1.
			start, end := desc.StartKey, desc.EndKey
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				start, _ = keys.Addr(keys.LocalMax)
			}
			if err := h.DB().Migrate(ctx, start, end, h.ClusterVersion().Version); err != nil {
				return err
			}
		}

		numMigratedRanges += len(descriptors)
		progress := fmt.Sprintf("[batch %d/??] migrated %d ranges", batchIdx, numMigratedRanges)
		if err := h.UpdateProgress(ctx, progress); err != nil {
			return err
		}
		batchIdx++

		return nil
	}); err != nil {
		return err
	}

	progress := fmt.Sprintf("[batch %d/%d] migrated %d ranges", batchIdx, batchIdx, numMigratedRanges)
	if err := h.UpdateProgress(ctx, progress); err != nil {
		return err
	}

	return nil
}
