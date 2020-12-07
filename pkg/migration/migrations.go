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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
)

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]Migration)

func init() {
	// TODO(irfansharif): We'll want to register individual migrations with
	// specific internal cluster versions here.
	_ = register // register(clusterversion.WhateverMigration, WhateverMigration, "whatever migration")
	register(clusterversion.TruncatedAndRangeAppliedStateMigration, TruncatedStateMigration,
		"use unreplicated TruncatedState and RangeAppliedState for all ranges")
}

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
//
// TODO(irfansharif): [3] and [4] are currently referring to what was prototyped
// in #57445. Once that makes its way into master, this TODO can be removed.
type Migration struct {
	cv   clusterversion.ClusterVersion
	fn   migrationFn
	desc string
}

type migrationFn func(context.Context, *Helper) error

// Run kickstarts the actual migration process. It's responsible for recording
// the ongoing status of the migration into a system table.
//
// TODO(irfansharif): Introduce a `system.migrations` table, and populate it here.
func (m *Migration) Run(ctx context.Context, h *Helper) (err error) {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("migration=%s", h.ClusterVersion()), nil)

	if err := m.fn(ctx, h); err != nil {
		return err
	}

	return nil
}

// defaultPageSize controls how many ranges are paged in by default when
// iterating through all ranges in a cluster during any given migration. We
// pulled this number out of thin air(-ish). Let's consider a cluster with 50k
// ranges, with each range taking ~200ms. We're being somewhat conservative with
// the duration, but in a wide-area cluster with large hops between the manager
// and the replicas, it could be true. Here's how long it'll take for various
// block sizes:
//
//   block size == 1    ~   2h 46m
//   block size == 50   ~   3m 20s
//   block size == 200  ~   50s
const defaultPageSize = 200

func TruncatedStateMigration(ctx context.Context, h *Helper) error {
	var batchIdx, numMigratedRanges int
	init := func() { batchIdx, numMigratedRanges = 1, 0 }
	if err := h.IterateRangeDescriptors(ctx, defaultPageSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
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

		// TODO(irfansharif): Instead of logging this to the debug log, we
		// should insert these into a `system.migrations` table for external
		// observability.
		numMigratedRanges += len(descriptors)
		log.Infof(ctx, "[batch %d/??] migrated %d ranges", batchIdx, numMigratedRanges)
		batchIdx++

		return nil
	}); err != nil {
		return err
	}

	log.Infof(ctx, "[batch %d/%d] migrated %d ranges", batchIdx, batchIdx, numMigratedRanges)
	return nil
}
