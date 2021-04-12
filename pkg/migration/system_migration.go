// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/logtags"
)

// Cluster abstracts a physical KV cluster and can be utilized by a long-running
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
		ctx context.Context,
		op string,
		fn func(context.Context, serverpb.MigrationClient) error,
	) error

	// UntilClusterStable invokes the given closure until the cluster membership
	// is stable, i.e once the set of nodes in the cluster before and after the
	// closure are identical, and no nodes have restarted in the interim, we can
	// return to the caller[*].
	//
	// The mechanism for doing so, while accounting for the possibility of new
	// nodes being added to the cluster in the interim, is provided by the
	// following structure:
	//   (a) We'll retrieve the list of node IDs for all nodes in the system
	//   (b) We'll invoke the closure
	//   (c) We'll retrieve the list of node IDs again to account for the
	//       possibility of a new node being added during (b), or a node
	//       restarting
	//   (d) If there any discrepancies between the list retrieved in (a)
	//       and (c), we'll invoke the closure again
	//   (e) We'll continue to loop around until the node ID list stabilizes
	//
	// [*]: We can be a bit more precise here. What UntilClusterStable gives us is
	// a strict causal happens-before relation between running the given closure
	// and the next node that joins the cluster. Put another way: using
	// UntilClusterStable callers will have managed to run something without a new
	// node joining halfway through (which could have allowed it to pick up some
	// state off one of the existing nodes that hadn't heard from us yet).
	//
	// To consider an example of how this primitive is used, let's consider our
	// use of it to bump the cluster version. We use in conjunction with
	// ForEveryNode, where after we return, we can rely on the guarantee that all
	// nodes in the cluster will have their cluster versions bumped. This then
	// implies that future node additions will observe the latest version (through
	// the join RPC). That in turn lets us author migrations that can assume that
	// a certain version gate has been enabled on all nodes in the cluster, and
	// will always be enabled for any new nodes in the system.
	//
	// Given that it'll always be possible for new nodes to join after an
	// UntilClusterStable round, it means that some migrations may have to be
	// split up into two version bumps: one that phases out the old version (i.e.
	// stops creation of stale data or behavior) and a cleanup version, which
	// removes any vestiges of the stale data/behavior, and which, when active,
	// ensures that the old data has vanished from the system. This is similar in
	// spirit to how schema changes are split up into multiple smaller steps that
	// are carried out sequentially.
	UntilClusterStable(ctx context.Context, fn func() error) error

	// IterateRangeDescriptors provides a handle on every range descriptor in the
	// system, which callers can then use to send out arbitrary KV requests to in
	// order to run arbitrary KV-level migrations. These requests will typically
	// just be the `Migrate` request, with code added within [1] to do the
	// specific things intended for the specified version.
	//
	// It's important to note that the closure is being executed in the context of
	// a distributed transaction that may be automatically retried. So something
	// like the following is an anti-pattern:
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
	IterateRangeDescriptors(
		ctx context.Context,
		size int,
		init func(),
		f func(descriptors ...roachpb.RangeDescriptor) error,
	) error
}

// SystemMigration is an implementation of Migration for system-level
// migrations. It is only to be run on the system tenant. These migrations
// tend to touch the kv layer.
type SystemMigration struct {
	migration
	fn SystemMigrationFunc
}

// SystemMigrationFunc is used to perform kv-level migrations. It should only be
// run from the system tenant.
type SystemMigrationFunc func(context.Context, clusterversion.ClusterVersion, Cluster) error

// NewSystemMigration constructs a SystemMigration.
func NewSystemMigration(
	description string, cv clusterversion.ClusterVersion, fn SystemMigrationFunc,
) *SystemMigration {
	return &SystemMigration{
		migration: migration{
			description: description,
			cv:          cv,
		},
		fn: fn,
	}
}

// Run kickstarts the actual migration process for system-level migrations.
func (m *SystemMigration) Run(
	ctx context.Context, cv clusterversion.ClusterVersion, h Cluster,
) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("migration=%s", cv), nil)
	return m.fn(ctx, cv, h)
}
