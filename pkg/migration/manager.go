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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

// MigrateTo runs the set of migrations required to upgrade the cluster version
// to the provided target version.
//
// TODO(irfansharif): Do something real here.
func (m *Manager) MigrateTo(ctx context.Context, targetV roachpb.Version) error {
	// TODO(irfansharif): Should we inject every ctx here with specific labels
	// for each migration, so they log distinctly?
	_ = logtags.AddTag(ctx, "migration-mgr", nil)

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
	_ = targetV
	var vs []roachpb.Version

	for _, version := range vs {
		_ = &Helper{Manager: m}
		// TODO(irfansharif): We'll want to out the version gate to every node
		// in the cluster. Each node will want to persist the version, bump the
		// local version gates, and then return. The migration associated with
		// the specific version can then assume that every node in the cluster
		// has the corresponding version activated.

		// TODO(irfansharif): We'll want to retrieve the right migration off of
		// our registry of migrations, and execute it.
		// TODO(irfansharif): We'll want to be able to override which migration
		// is retrieved here within tests. We could make the registry be a part
		// of the manager, and all tests to provide their own.
		_ = Registry[version]
	}

	return nil
}
