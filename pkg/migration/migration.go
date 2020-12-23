// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migration captures the facilities needed to define and execute
// migrations for a crdb cluster. These migrations can be arbitrarily long
// running, are free to send out arbitrary requests cluster wide, change
// internal DB state, and much more. They're typically reserved for crdb
// internal operations and state. Each migration is idempotent in nature, is
// associated with a specific cluster version, and executed when the cluster
// version is made activate on every node in the cluster.
//
// Examples of migrations that apply would be migrations to move all raft state
// from one storage engine to another, or purging all usage of the replicated
// truncated state in KV. A "sister" package of interest is pkg/sqlmigrations.
package migration

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/logtags"
)

// Manager coordinates long-running migrations.
type Manager interface {
	Migrate(ctx context.Context, user security.SQLUsername, from, to clusterversion.ClusterVersion) error
}

// Cluster abstracts a physical KV cluster and can be utilized by a long-runnng
// migration.
type Cluster interface {

	// DB returns access to the kv.
	DB() *kv.DB

	// ForEveryNode is a short hand to execute the given closure (named by the
	// informational parameter op) against every node in the cluster at a given
	// point in time. Given it's possible for nodes to join or leave the cluster
	// during (we don't make any guarantees for the ordering of cluster membership
	// events), we only expect this to be used in conjunction with
	// UntilClusterStable (see the comment there for how these two primitives can be
	// put together).
	ForEveryNode(
		ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
	) error

	// UntilClusterStable invokes the given closure until the cluster membership is
	// stable, i.e once the set of nodes in the cluster before and after the closure
	// are identical, and no nodes have restarted in the interim, we can return to
	// the caller[*].
	//
	// The mechanism for doing so, while accounting for the possibility of new nodes
	// being added to the cluster in the interim, is provided by the following
	// structure:
	//   (a) We'll retrieve the list of node IDs for all nodes in the system
	//   (b) We'll invoke the closure
	//   (c) We'll retrieve the list of node IDs again to account for the
	//       possibility of a new node being added during (b), or a node
	//       restarting
	//   (d) If there any discrepancies between the list retrieved in (a)
	//       and (c), we'll invoke the closure again
	//   (e) We'll continue to loop around until the node ID list stabilizes
	//
	// [*]: We can be a bit more precise here. What UntilClusterStable gives us is a
	// strict causal happenedbefore relation between running the given closure and
	// the next node that joins the cluster. Put another way: using
	// UntilClusterStable callers will have managed to run something without a new
	// node joining halfway through (which could have allowed it to pick up some
	// state off one of the existing nodes that hadn't heard from us yet).
	//
	// To consider an example of how this primitive is used, let's consider our use
	// of it to bump the cluster version. We use in conjunction with ForEveryNode,
	// where after we return, we can rely on the guarantee that all nodes in the
	// cluster will have their cluster versions bumped. This then implies that
	// future node additions will observe the latest version (through the join RPC).
	// That in turn lets us author migrations that can assume that a certain version
	// gate has been enabled on all nodes in the cluster, and will always be enabled
	// for any new nodes in the system.
	//
	// Given that it'll always be possible for new nodes to join after an
	// UntilClusterStable round, it means that some migrations may have to be split
	// up into two version bumps: one that phases out the old version (i.e. stops
	// creation of stale data or behavior) and a cleanup version, which removes any
	// vestiges of the stale data/behavior, and which, when active, ensures that the
	// old data has vanished from the system. This is similar in spirit to how
	// schema changes are split up into multiple smaller steps that are carried out
	// sequentially.
	UntilClusterStable(ctx context.Context, fn func() error) error

	// IterateRangeDescriptors provides a handle on every range descriptor in the
	// system, which callers can then use to send out arbitrary KV requests to in
	// order to run arbitrary KV-level migrations. These requests will typically
	// just be the `Migrate` request, with code added within [1] to do the specific
	// things intended for the specified version.
	//
	// It's important to note that the closure is being executed in the context of a
	// distributed transaction that may be automatically retried. So something like
	// the following is an anti-pattern:
	//
	//     processed := 0
	//     _ = h.IterateRangeDescriptors(...,
	//         func(descriptors ...roachpb.RangeDescriptor) error {
	//             processed += len(descriptors) // we'll over count if retried
	//             log.Infof(ctx, "processed %d ranges", processed)
	//         },
	//     )
	//
	// Instead we allow callers to pass in a callback to signal on every attempt
	// (including the first). This lets us salvage the example above:
	//
	//     var processed int
	//     init := func() { processed = 0 }
	//     _ = h.IterateRangeDescriptors(..., init,
	//         func(descriptors ...roachpb.RangeDescriptor) error {
	//             processed += len(descriptors)
	//             log.Infof(ctx, "processed %d ranges", processed)
	//         },
	//     )
	//
	// [1]: pkg/kv/kvserver/batch_eval/cmd_migrate.go
	IterateRangeDescriptors(ctx context.Context, size int, init func(), f func(descriptors ...roachpb.RangeDescriptor) error) error
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
// more details. Following that, define a Migration in the migrations package
// and add it to the appropriate migrations slice to the registry. Be sure to
// key it in with the new cluster version we just added. During cluster
// upgrades, once the operator is able to set a cluster version setting that's
// past the version that was introduced (typically the major release version
// the migration was introduced in), the manager will execute the defined
// migration before letting the upgrade finalize.
//
// If the migration requires below-Raft level changes ([3] is one example),
// you'll need to add a version switch and the relevant KV-level migration in
// [4]. See IterateRangeDescriptors and the Migrate KV request for more details.
//
// [1]: `(*Manager).Migrate`
// [2]: pkg/clusterversion/cockroach_versions.go
// [3]: truncatedStateMigration
// [4]: pkg/kv/kvserver/batch_eval/cmd_migrate.go
//
type Migration interface {
	ClusterVersion() clusterversion.ClusterVersion
}

type migration struct {
	description string
	cv          clusterversion.ClusterVersion
}

// ClusterVersion makes KVMigration a Migration.
func (m *migration) ClusterVersion() clusterversion.ClusterVersion {
	return m.cv
}

// KVMigration is an implementation of Migration for KV-level migrations.
type KVMigration struct {
	migration
	fn KVMigrationFn
}

// NewKVMigration constructs a KVMigration.
func NewKVMigration(
	description string, cv clusterversion.ClusterVersion, fn KVMigrationFn,
) *KVMigration {
	return &KVMigration{
		migration: migration{
			description: description,
			cv:          cv,
		},
		fn: fn,
	}
}

// KVMigrationFn contains the logic of a KVMigration.
type KVMigrationFn func(context.Context, clusterversion.ClusterVersion, Cluster) error

// Run kickstarts the actual migration process. It's responsible for recording
// the ongoing status of the migration into a system table.
//
// TODO(irfansharif): Introduce a `system.migrations` table, and populate it here.
func (m *KVMigration) Run(
	ctx context.Context, cv clusterversion.ClusterVersion, h Cluster,
) (err error) {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("migration=%s", cv), nil)

	if err := m.fn(ctx, cv, h); err != nil {
		return err
	}

	return nil
}
