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

	cv "github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// Migration defines a program to be executed once every node in the cluster is
// (a) running a specific binary version, and (b) has completed all prior
// migrations.
//
// Each migration is associated with a specific (unstable) cluster version and
// is idempotent in nature. When setting the cluster version (via `SET CLUSTER
// SETTING version`), a manager process determines the set of migrations needed
// to bridge the gap between the current active cluster version, and the target
// one.
//
// To introduce a migration, introduce a version key in pkg/clusterversion, and
// introduce a corresponding unstable cluster version for it. See
// `pkg/clusterversion/cockroach_versions.go` for details. Following that,
// define a Migration in this package and register it. Be sure to key it in with
// the new cluster version we just added. During cluster upgrades, once the
// operator is able to set a cluster version setting that's past the version you
// just introduced (typically the major release version the migration code was
// added during), the manager will execute the defined migration before letting
// the upgrade finalize.
//
// TODO(irfansharif): Should we instead make this a struct that embeds the right
// function, and directly associates it with the right cluster version? Or just
// leave it as is, with the registry being the unit responsible for the mapping?
// We do typically want each migration to be attached with a specific version,
// so perhaps we should make that more explicit.
type Migration func(context.Context, *Helper) error

// Manager is the instance responsible for executing migrations across the
// cluster.
type Manager struct {
	dialer *nodedialer.Dialer
}

// Helper captures all the primitives required to fully specify a migration.
type Helper struct {
	*Manager
}

// NewManager constructs a new Manager.
//
// TODO(irfansharif): We'll need to eventually plumb in a few things here. We'll
// need a handle on node liveness, a lease manager, an internal executor, and a
// kv.DB.
func NewManager(dialer *nodedialer.Dialer) *Manager {
	return &Manager{
		dialer: dialer,
	}
}

// IterateRangeDescriptors is a primitive for constructing migrations. It
// provides a handle on every range in descriptor in the system, which callers
// can then use to send out arbitrary KV requests to in order to run arbitrary
// KV-level migrations. These requests will typically just be the Migrate
// request, with code added within pkg/kv to do the specific things intended for
// the specified version.
//
// TODO(irfansharif): We want to eventually be able to send out a Migrate
// request spanning the entire keyspace. We'll need to make sure all stores have
// synced once to persist any raft command applications. Implement this.
func (h *Helper) IterateRangeDescriptors(f func(...roachpb.RangeDescriptor) error) error {
	return nil
}

// RequiredNodes returns the node IDs for all nodes that are currently part of
// the cluster (i.e. they haven't been decommissioned away). Migrations have the
// pre-requisite that all required nodes are up and running so that we're able
// to execute all relevant node-level operations on them.
//
// TODO(irfansharif): Implement this.
func (h *Helper) RequiredNodes(ctx context.Context) ([]roachpb.NodeID, error) {
	return []roachpb.NodeID{}, nil
}

// Log lets migrations insert arbitrary log events. These events are recorded in
// a system table, and are available for introspection into the progress of the
// specific ongoing migration.
//
// TODO(irfansharif): Implement this.
func (h *Helper) Log(event string) error {
	return nil
}

// EveryNode lets migrations execute the given EveryNodeRequest across every
// node in the cluster.
func (h *Helper) EveryNode(ctx context.Context, req EveryNodeRequest) error {
	nodeIDs, err := h.RequiredNodes(ctx)
	if err != nil {
		return err
	}

	// TODO(irfansharif): We can/should send out these RPCs in parallel.
	log.Infof(ctx, "executing req=%s on nodes=%s", req, nodeIDs)
	for _, nodeID := range nodeIDs {
		conn, err := h.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
		if err != nil {
			return err
		}

		// TODO(irfansharif): This typecasting below is busted. We'll need
		// wrapping/unwrapping code around the EveryNodeRequest internal union
		// type.
		client := serverpb.NewAdminClient(conn)
		if _, err := client.EveryNode(ctx, req.(*serverpb.EveryNodeRequest)); err != nil {
			return err
		}
	}

	// TODO(irfansharif): We'll need to check RequiredNodes again to make sure
	// that no new nodes were added to the cluster in the interim, and loop back
	// around if so.
	return nil
}

// EveryNodeRequest is an interface only satisfied by valid request types to the
// EveryNode RPC.
//
// TODO(irfansharif): Make this so. Should probably be defined in
// pkg/server/serverpb.
type EveryNodeRequest interface{}

// MigrateTo runs the set of migrations required to upgrade the cluster version
// to the provided target version.
func (m *Manager) MigrateTo(ctx context.Context, targetV roachpb.Version) error {
	// TODO(irfansharif): Should we inject every ctx here with specific labels
	// for each migration, so they log distinctly? Do we need an AmbientContext?
	ctx = logtags.AddTag(ctx, "migration-mgr", nil)

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
	// through to get to targetV. For now we've hard-coded this list.
	_ = targetV
	vs := []roachpb.Version{
		cv.VersionByKey(cv.VersionNoopMigration),
	}

	for _, version := range vs {
		h := &Helper{Manager: m}
		// Push out the version gate to every node in the cluster. Each node
		// will persist the version, bump the local version gates, and then
		// return. The migration associated with the specific version can assume
		// that every node in the cluster has the corresponding version
		// activated.
		req := serverpb.AckClusterVersionRequest{Version: &version}
		if err := h.EveryNode(ctx, req); err != nil {
			return err
		}

		// TODO(irfansharif): We'll want a testing override here to be able to
		// stub out migrations as needed.

		migration, ok := Registry[version]
		if !ok {
			return errors.Newf("migration for %s not found", version)
		}
		if err := migration(ctx, h); err != nil {
			return err
		}
	}

	return nil
}
