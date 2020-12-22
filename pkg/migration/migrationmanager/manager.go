// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrationmanager provides an implementation of migration.Manager
// for use on kv nodes.
package migrationmanager

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/migration/migrationcluster"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
)

// Manager is the instance responsible for executing migrations across the
// cluster.
type Manager struct {
	c migration.Cluster
}

// NewManager constructs a new Manager.
//
// TODO(irfansharif): We'll need to eventually plumb in on a lease manager here.
func NewManager(
	dialer migrationcluster.NodeDialer, nl migrationcluster.NodeLiveness, db *kv.DB,
) *Manager {
	return &Manager{
		c: migrationcluster.New(migrationcluster.ClusterConfig{
			NodeLiveness: nl,
			Dialer:       dialer,
			DB:           db,
		}),
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
		// First, run the actual migration if any.
		if mig, ok := migrations.GetMigration(clusterVersion); ok {
			if err := mig.(*migration.KVMigration).Run(ctx, clusterVersion, m.c); err != nil {
				return err
			}
		}

		// Next we'll push out the version gate to every node in the cluster.
		// Each node will persist the version, bump the local version gates, and
		// then return. The migration associated with the specific version is
		// executed before every node in the cluster has the corresponding
		// version activated. Migrations that depend on a certain version
		// already being activated will need to registered using a cluster
		// version greater than it.
		//
		// For each intermediate version, we'll need to first bump the fence
		// version before bumping the "real" one. Doing so allows us to provide
		// the invariant that whenever a cluster version is active, all Nodes in
		// the cluster (including ones added concurrently during version
		// upgrades) are running binaries that know about the version.

		// Below-raft migrations mutate replica state, making use of the
		// Migrate(version=V) primitive which they issue against the entire
		// keyspace. These migrations typically want to rely on the invariant
		// that there are no extant replicas in the system that haven't seen the
		// specific Migrate command.
		//
		// This is partly achieved through the implementation of the Migrate
		// command itself, which waits until it's applied on all followers[2]
		// before returning. This also addresses the concern of extant snapshots
		// with pre-migrated state possibly instantiating older version
		// replicas. The intended learner replicas are listed as part of the
		// range descriptor, and is also waited on for during command
		// application. As for stale snapshots, if they specify a replicaID
		// that's no longer part of the raft group, they're discarded by the
		// recipient. Snapshots are also discarded unless they move the LAI
		// forward.
		//
		// That still leaves rooms for replicas in the replica GC queue to evade
		// detection. To address this, below-raft migrations typically take a
		// two-phrase approach (the TruncatedAndRangeAppliedStateMigration being
		// one example of this), where after having migrated the entire keyspace
		// to version V, and after having prevented subsequent snapshots
		// originating from replicas with versions < V, the migration sets out
		// to purge outdated replicas in the system[3]. Specifically it
		// processes all replicas in the GC queue with a version < V (which are
		// not accessible during the application of the Migrate command).
		//
		// [1]: See ReplicaState.Version.
		// [2]: See Replica.executeWriteBatch, specifically how proposals with the
		//      Migrate request are handled downstream of raft.
		// [3]: See PurgeOutdatedReplicas from the KVMigration service.

		{
			// The migrations infrastructure makes use of internal fence
			// versions when stepping through consecutive versions. It's
			// instructive to walk through how we expect a version migration
			// from v21.1 to v21.2 to take place, and how we behave in the
			// presence of new v21.1 or v21.2 Nodes being added to the cluster.
			//
			//   - All Nodes are running v21.1
			//   - All Nodes are rolled into v21.2 binaries, but with active
			//     cluster version still as v21.1
			//   - The first version bump will be into v21.2-1(fence), see the
			//     migration manager above for where that happens
			//
			// Then concurrently:
			//
			//   - A new node is added to the cluster, but running binary v21.1
			//   - We try bumping the cluster gates to v21.2-1(fence)
			//
			// If the v21.1 Nodes manages to sneak in before the version bump,
			// it's fine as the version bump is a no-op one (all fence versions
			// are). Any subsequent bumps (including the "actual" one bumping to
			// v21.2) will fail during the validation step where we'll first
			// check to see that all Nodes are running v21.2 binaries.
			//
			// If the v21.1 node is only added after v21.2-1(fence) is active,
			// it won't be able to actually join the cluster (it'll be prevented
			// by the join RPC).
			//
			// All of which is to say that once we've seen the node list
			// stabilize (as UntilClusterStable enforces), any new nodes that
			// can join the cluster will run a release that support the fence
			// version, and by design also supports the actual version (which is
			// the direct successor of the fence).
			fenceVersion := migration.FenceVersionFor(ctx, clusterVersion)
			req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &fenceVersion}
			op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			if err := m.c.UntilClusterStable(ctx, func() error {
				return m.c.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
					_, err := client.BumpClusterVersion(ctx, req)
					return err
				})
			}); err != nil {
				return err
			}
		}
		{
			// Now sanity check that we'll actually be able to perform the real
			// cluster version bump, cluster-wide.
			req := &serverpb.ValidateTargetClusterVersionRequest{ClusterVersion: &clusterVersion}
			op := fmt.Sprintf("validate-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			if err := m.c.UntilClusterStable(ctx, func() error {
				return m.c.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
					_, err := client.ValidateTargetClusterVersion(ctx, req)
					return err
				})
			}); err != nil {
				return err
			}
		}
		{
			// Finally, bump the real version cluster-wide.
			req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &clusterVersion}
			op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			if err := m.c.UntilClusterStable(ctx, func() error {
				return m.c.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
					_, err := client.BumpClusterVersion(ctx, req)
					return err
				})
			}); err != nil {
				return err
			}
		}
	}

	return nil
}
