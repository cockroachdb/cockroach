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
	if from == to {
		// Nothing to do here.
		return nil
	}

	// TODO(irfansharif): Should we inject every ctx here with specific labels
	// for each migration, so they log distinctly?
	ctx = logtags.AddTag(ctx, "migration-mgr", nil)
	log.Infof(ctx, "migrating cluster from %s to %s", from, to)

	// TODO(irfansharif): We'll need to acquire a lease here and refresh it
	// throughout during the migration to ensure mutual exclusion.

	// TODO(irfansharif): We'll need to create a system table to store
	// in-progress state of long running migrations, for introspection.

	// TODO(irfansharif): We'll want to either write to a KV key to record the
	// version up until which we've already migrated to, or consult the system
	// table mentioned above. Perhaps it makes sense to consult any given
	// `StoreClusterVersionKey`, since the manager here will want to push out
	// cluster version bumps for vX before attempting to migrate into vX+1.

	// TODO(irfansharif): After determining the last completed migration, if
	// any, we'll be want to assemble the list of remaining migrations to step
	// through to get to targetV.

	// TODO(irfansharif): We'll need to introduce fence/noop versions in order
	// for the infrastructure here to step through adjacent cluster versions.
	// It's instructive to walk through how we expect a version migration from
	// v21.1 to v21.2 to take place, and how we would behave in the presence of
	// new v21.1 or v21.2 nodes being added to the cluster during.
	//   - All nodes are running v21.1
	//   - All nodes are rolled into v21.2 binaries, but with active cluster
	//     version still as v21.1
	//   - The first version bump will be into v21.2.0-1noop
	//   - Validation for setting active cluster version to v21.2.0-1noop first
	//     checks to see that all nodes are running v21.2 binaries
	// Then concurrently:
	//   - A new node is added to the cluster, but running binary v21.1
	//   - We try bumping the cluster gates to v21.2.0-1noop
	//
	// If the v21.1 nodes manages to sneak in before the version bump, it's
	// fine as the version bump is a no-op one. Any subsequent bumps (including
	// the "actual" one bumping to v21.2.0) will fail during validation.
	//
	// If the v21.1 node is only added after v21.2.0-1noop is active, it won't
	// be able to actually join the cluster (it'll be prevented by the join
	// RPC).
	//
	// It would be nice to only contain this "fence" version tag within this
	// package. Perhaps by defining yet another proto version type, but for
	// pkg/migrations internal use only? I think the UX we want for engineers
	// defining migrations is that they'd only care about introducing the next
	// version key within pkg/clusterversion, and registering a corresponding
	// migration for it here.
	var clusterVersions = []clusterversion.ClusterVersion{to}

	for _, clusterVersion := range clusterVersions {
		h := &Helper{Manager: m}

		// Push out the version gate to every node in the cluster. Each node
		// will persist the version, bump the local version gates, and then
		// return. The migration associated with the specific version can assume
		// that every node in the cluster has the corresponding version
		// activated.
		{
			// First sanity check that we'll actually be able to perform the
			// cluster version bump, cluster-wide.
			req := &serverpb.ValidateTargetClusterVersionRequest{
				ClusterVersion: &clusterVersion,
			}
			err := h.EveryNode(ctx, "validate-cv", func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.ValidateTargetClusterVersion(ctx, req)
				return err
			})
			if err != nil {
				return err
			}
		}
		{
			req := &serverpb.BumpClusterVersionRequest{
				ClusterVersion: &clusterVersion,
			}
			err := h.EveryNode(ctx, "bump-cv", func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.BumpClusterVersion(ctx, req)
				return err
			})
			if err != nil {
				return err
			}
		}

		// TODO(irfansharif): We'll want to retrieve the right migration off of
		// our registry of migrations, and execute it.
		// TODO(irfansharif): We'll want to be able to override which migration
		// is retrieved here within tests. We could make the registry be a part
		// of the manager, and all tests to provide their own.
		_ = Registry[clusterVersion]
	}

	return nil
}
