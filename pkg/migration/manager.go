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
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
)

// Manager is the instance responsible for executing migrations across the
// cluster.
type Manager struct {
	dialer   *nodedialer.Dialer
	nl       nodeLiveness
	executor *sql.InternalExecutor
	db       *kv.DB
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

	clusterVersions := clusterversion.ListBetween(from, to)
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
		cluster := newCluster(m.nl, m.dialer, m.executor, m.db)
		h := newHelper(cluster, clusterVersion)

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
			op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
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
			op := fmt.Sprintf("validate-cluster-version=%s", req.ClusterVersion.PrettyPrint())
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
			op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			if err := h.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.BumpClusterVersion(ctx, req)
				return err
			}); err != nil {
				return err
			}
		}

		// TODO(irfansharif): We'll want to be able to override which migration
		// is retrieved here within tests. We could make the registry be a part
		// of the manager, and all tests to provide their own.

		// Finally, run the actual migration.
		migration, ok := registry[clusterVersion]
		if !ok {
			log.Infof(ctx, "no migration registered for %s, skipping", clusterVersion)
			continue
		}
		if err := migration.Run(ctx, h); err != nil {
			return err
		}
	}

	return nil
}
