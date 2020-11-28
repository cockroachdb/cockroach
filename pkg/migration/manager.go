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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// Migration defines a program to be executed once every node in the cluster is
// (a) running a specific binary version, and (b) has completed all prior
// migrations.
//
// Each migration is associated with a specific internal cluster version and is
// idempotent in nature. When setting the cluster version (via `SET CLUSTER
// SETTING version`), a manager process determines the set of migrations needed
// to bridge the gap between the current active cluster version, and the target
// one.
//
// To introduce a migration, introduce a version key in pkg/clusterversion, and
// introduce a corresponding internal cluster version for it. See [1] for
// details. Following that, define a Migration in this package and add it to the
// Registry. Be sure to key it in with the new cluster version we just added.
// During cluster upgrades, once the operator is able to set a cluster version
// setting that's past the version that was introduced (typically the major
// release version the migration was introduced in), the manager will execute
// the defined migration before letting the upgrade finalize.
//
// [1]: pkg/clusterversion/cockroach_versions.go
type Migration func(context.Context, *Helper) error

// Manager is the instance responsible for executing migrations across the
// cluster.
type Manager struct {
	dialer   *nodedialer.Dialer
	nl       nodeLiveness
	executor *sql.InternalExecutor
	db       *kv.DB
}

// Helper captures all the primitives required to fully specify a migration.
type Helper struct {
	*Manager
}

// RequiredNodeIDs returns the node IDs for all nodes that are currently part of
// the cluster (i.e. they haven't been decommissioned away). Migrations have the
// pre-requisite that all required nodes are up and running so that we're able
// to execute all relevant node-level operations on them. If any of the nodes
// are found to be unavailable, an error is returned.
func (h *Helper) RequiredNodeIDs(ctx context.Context) ([]roachpb.NodeID, error) {
	var nodeIDs []roachpb.NodeID
	ls, err := h.nl.GetLivenessesFromKV(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls {
		if l.Membership.Decommissioned() {
			continue
		}
		live, err := h.nl.IsLive(l.NodeID)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, errors.Newf("n%d required, but unavailable", l.NodeID)
		}
		nodeIDs = append(nodeIDs, l.NodeID)
	}
	return nodeIDs, nil
}

// EveryNode invokes the given closure (named by the informational parameter op)
// across every node in the cluster. The mechanism for ensuring that we've done
// so, while accounting for the possibility of new nodes being added to the
// cluster in the interim, is provided by the following structure:
//   (a) We'll retrieve the list of node IDs for all nodes in the system
//   (b) For each node, we'll invoke the closure
//   (c) We'll retrieve the list of node IDs again to account for the
//       possibility of a new node being added during (b)
//   (d) If there any discrepancies between the list retrieved in (a)
//       and (c), we'll invoke the closure each node again
//   (e) We'll continue to loop around until the node ID list stabilizes
//
// By the time EveryNode returns, we'll have thus invoked the closure against
// every node in the cluster.
//
// To consider one example of how this primitive is used, let's consider our use
// of it to bump the cluster version. After we return, given all nodes in the
// cluster will have their cluster versions bumped, and future node additions
// will observe the latest version (through the join RPC). This lets us author
// migrations that can assume that a certain version gate has been enabled on
// all nodes in the cluster, and will always be enabled for any new nodes in the
// system.
//
// It may be possible however that right after we return, a new node may join.
// This means that some migrations may have to be split up into two version
// bumps: one that phases out the old version (i.e. stops creation of stale data
// or behavior) and a clean-up version, which removes any vestiges of the stale
// data/behavior, and which, when active, ensures that the old data has vanished
// from the system. This is similar in spirit to how schema changes are split up
// into multiple smaller steps that are carried out sequentially.
func (h *Helper) EveryNode(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {
	nodeIDs, err := h.RequiredNodeIDs(ctx)
	if err != nil {
		return err
	}

	for {
		// TODO(irfansharif): We can/should send out these RPCs in parallel.
		log.Infof(ctx, "executing op=%s on nodes=%s", op, nodeIDs)
		for _, nodeID := range nodeIDs {
			conn, err := h.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
			if err != nil {
				return err
			}
			client := serverpb.NewMigrationClient(conn)
			if err := fn(ctx, client); err != nil {
				return err
			}
		}

		curNodeIDs, err := h.RequiredNodeIDs(ctx)
		if err != nil {
			return err
		}

		if !identical(nodeIDs, curNodeIDs) {
			nodeIDs = curNodeIDs
			continue
		}

		break
	}

	return nil
}

// nodeLiveness is the subset of the interface satisfied by CRDB's node liveness
// component that the migration manager relies upon.
type nodeLiveness interface {
	GetLivenessesFromKV(context.Context) ([]livenesspb.Liveness, error)
	IsLive(roachpb.NodeID) (bool, error)
}

// NewManager constructs a new Manager.
//
// TODO(irfansharif): We'll need to eventually plumb in on a lease manager here.
func NewManager(
	dialer *nodedialer.Dialer, nl nodeLiveness, executor *sql.InternalExecutor, db *kv.DB,
) *Manager {
	return &Manager{
		dialer:   dialer,
		executor: executor,
		db:       db,
		nl:       nl,
	}
}

// Migrate runs the set of migrations required to upgrade the cluster version
// from the current version to the target one.
func (m *Manager) Migrate(ctx context.Context, from, to clusterversion.ClusterVersion) error {
	// TODO(irfansharif): Should we inject every ctx here with specific labels
	// for each migration, so they log distinctly?
	ctx = logtags.AddTag(ctx, "migration-mgr", nil)
	if from == to {
		// Nothing to do here.
		log.Infof(ctx, "no need to migrate, cluster already at newest version")
		return nil
	}

	// TODO(irfansharif): We'll need to acquire a lease here and refresh it
	// throughout during the migration to ensure mutual exclusion.

	// TODO(irfansharif): We'll need to create a system table to store
	// in-progress state of long running migrations, for introspection.

	clusterVersions := clusterversion.GetVersionsBetween(from, to)
	if len(clusterVersions) == 0 {
		// We're attempt to migrate to something that's not defined in cluster
		// versions. This only happens in tests, when we're exercising version
		// upgrades over non-existent versions (like in the cluster_version
		// logictest). These tests explicitly override the
		// binary{,MinSupportedVersion} in order to work. End-user attempts to
		// do something similar would be caught at the sql layer (also tested in
		// the same logictest). We'll just explicitly append the target version
		// here instead, so that we're able to actually migrate into it.
		clusterVersions = append(clusterVersions, to)
	}
	log.Infof(ctx, "migrating cluster from %s to %s (stepping through %s)", from, to, clusterVersions)

	for _, clusterVersion := range clusterVersions {
		h := &Helper{Manager: m}

		// Push out the version gate to every node in the cluster. Each node
		// will persist the version, bump the local version gates, and then
		// return. The migration associated with the specific version can assume
		// that every node in the cluster has the corresponding version
		// activated.
		//
		// We'll need to first bump the fence version for each intermediate
		// cluster version, before bumping the "real" one. Doing so allows us to
		// provide the invariant that whenever a cluster version is active, all
		// nodes in the cluster (including ones added concurrently during
		// version upgrades) are running binaries that know about the version.

		{
			// The migrations infrastructure makes use of internal fence
			// versions when stepping through consecutive versions. It's
			// instructive to walk through how we expect a version migration
			// from v21.1 to v21.2 to take place, and how we behave in the
			// presence of new v21.1 or v21.2 nodes being added to the cluster.
			//   - All nodes are running v21.1
			//   - All nodes are rolled into v21.2 binaries, but with active
			//     cluster version still as v21.1
			//   - The first version bump will be into v21.2-1(fence), see the
			//     migration manager above for where that happens
			// Then concurrently:
			//   - A new node is added to the cluster, but running binary v21.1
			//   - We try bumping the cluster gates to v21.2-1(fence)
			//
			// If the v21.1 nodes manages to sneak in before the version bump,
			// it's fine as the version bump is a no-op one (all fence versions
			// are). Any subsequent bumps (including the "actual" one bumping to
			// v21.2) will fail during the validation step where we'll first
			// check to see that all nodes are running v21.2 binaries.
			//
			// If the v21.1 node is only added after v21.2-1(fence) is active,
			// it won't be able to actually join the cluster (it'll be prevented
			// by the join RPC).
			//
			// All of which is to say that once we've seen the node list
			// stabilize (as EveryNode enforces), any new nodes that can join
			// the cluster will run a release that support the fence version,
			// and by design also supports the actual version (which is the
			// direct successor of the fence).
			fenceVersion := fenceVersionFor(ctx, clusterVersion)
			req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &fenceVersion}
			op := fmt.Sprintf("bump-cv=%s", req.ClusterVersion.PrettyPrint())
			err := h.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.BumpClusterVersion(ctx, req)
				return err
			})
			if err != nil {
				return err
			}
		}
		{
			// Now sanity check that we'll actually be able to perform the real
			// cluster version bump, cluster-wide.
			req := &serverpb.ValidateTargetClusterVersionRequest{ClusterVersion: &clusterVersion}
			op := fmt.Sprintf("validate-cv=%s", req.ClusterVersion.PrettyPrint())
			err := h.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.ValidateTargetClusterVersion(ctx, req)
				return err
			})
			if err != nil {
				return err
			}
		}
		{
			// Finally, bump the real version cluster-wide.
			req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &clusterVersion}
			op := fmt.Sprintf("bump-cv=%s", req.ClusterVersion.PrettyPrint())
			if err := h.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.BumpClusterVersion(ctx, req)
				return err
			}); err != nil {
				return err
			}
		}

		// TODO(irfansharif): We'll want to retrieve the right migration off of
		// our registry of migrations, if any, and execute it.
		// TODO(irfansharif): We'll want to be able to override which migration
		// is retrieved here within tests. We could make the registry be a part
		// of the manager, and all tests to provide their own.
		_ = Registry[clusterVersion]
	}

	return nil
}

// fenceVersionFor constructs the appropriate "fence version" for the given
// cluster version. Fence versions allow the migrations infrastructure to safely
// step through consecutive cluster versions in the presence of nodes (running
// any binary version) being added to the cluster. See the migration manager
// above for intended usage.
//
// Fence versions (and the migrations infrastructure entirely) were introduced
// in the 21.1 release cycle. In the same release cycle, we introduced the
// invariant that new user-defined versions (users being crdb engineers) must
// always have even-numbered Internal versions, thus reserving the odd numbers
// to slot in fence versions for each cluster version. See top-level
// documentation in pkg/clusterversion for more details.
func fenceVersionFor(
	ctx context.Context, cv clusterversion.ClusterVersion,
) clusterversion.ClusterVersion {
	if (cv.Internal % 2) != 0 {
		log.Fatalf(ctx, "only even numbered internal versions allowed, found %s", cv.Version)
	}

	// We'll pick the odd internal version preceding the cluster version,
	// slotting ourselves right before it.
	fenceCV := cv
	fenceCV.Internal--
	return fenceCV
}
