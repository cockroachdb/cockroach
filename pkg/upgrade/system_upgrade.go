// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrade

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/logtags"
)

// Cluster abstracts a physical KV cluster and can be utilized by a long-running
// upgrade.
type Cluster interface {
	// NumNodes returns the number of nodes in the cluster. This is merely a
	// convenience method and is not meant to be used to infer cluster stability;
	// for that, use UntilClusterStable.
	NumNodes(ctx context.Context) (int, error)

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
	// the join RPC). That in turn lets us author upgrades that can assume that
	// a certain version gate has been enabled on all nodes in the cluster, and
	// will always be enabled for any new nodes in the system.
	//
	// Given that it'll always be possible for new nodes to join after an
	// UntilClusterStable round, it means that some upgrades may have to be
	// split up into two version bumps: one that phases out the old version (i.e.
	// stops creation of stale data or behavior) and a cleanup version, which
	// removes any vestiges of the stale data/behavior, and which, when active,
	// ensures that the old data has vanished from the system. This is similar in
	// spirit to how schema changes are split up into multiple smaller steps that
	// are carried out sequentially.
	UntilClusterStable(ctx context.Context, fn func() error) error

	// IterateRangeDescriptors provides a handle on every range descriptor in the
	// system, which callers can then use to send out arbitrary KV requests to in
	// order to run arbitrary KV-level upgrades. These requests will typically
	// just be the `Migrate` request, with code added within [1] to do the
	// specific things intended for the specified version.
	//
	// [1]: pkg/kv/kvserver/batch_eval/cmd_migrate.go
	IterateRangeDescriptors(
		ctx context.Context,
		size int,
		init func(),
		f func(descriptors ...roachpb.RangeDescriptor) error,
	) error
}

// SystemDeps are the dependencies of upgrades which perform actions at the
// KV layer on behalf of the system tenant.
type SystemDeps struct {
	Cluster     Cluster
	DB          descs.DB
	Settings    *cluster.Settings
	JobRegistry *jobs.Registry
	DistSender  *kvcoord.DistSender
	Stopper     *stop.Stopper
	KeyVisKnobs *keyvisualizer.TestingKnobs
}

// SystemUpgrade is an implementation of Upgrade for system-level
// upgrades. It is only to be run on the system tenant. These upgrades
// tend to touch the kv layer.
type SystemUpgrade struct {
	upgrade
	fn SystemUpgradeFunc
}

// SystemUpgradeFunc is used to perform kv-level upgrades. It should only be
// run from the system tenant.
type SystemUpgradeFunc func(context.Context, clusterversion.ClusterVersion, SystemDeps) error

// NewSystemUpgrade constructs a SystemUpgrade.
func NewSystemUpgrade(description string, v roachpb.Version, fn SystemUpgradeFunc) *SystemUpgrade {
	return &SystemUpgrade{
		upgrade: upgrade{
			description: description,
			v:           v,
		},
		fn: fn,
	}
}

// NewPermanentSystemUpgrade constructs a SystemUpgrade that is marked as
// "permanent": an upgrade that will run regardless of the cluster's bootstrap
// version.
func NewPermanentSystemUpgrade(
	description string, v roachpb.Version, fn SystemUpgradeFunc, v22_2StartupMigrationName string,
) *SystemUpgrade {
	return &SystemUpgrade{
		upgrade: upgrade{
			description:               description,
			v:                         v,
			permanent:                 true,
			v22_2StartupMigrationName: v22_2StartupMigrationName,
		},
		fn: fn,
	}
}

// Run kickstarts the actual upgrade process for system-level upgrades.
func (m *SystemUpgrade) Run(ctx context.Context, v roachpb.Version, d SystemDeps) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("upgrade=%s", v), nil)
	return m.fn(ctx, clusterversion.ClusterVersion{Version: v}, d)
}
