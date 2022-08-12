// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrade captures the facilities needed to define and execute
// upgrades for a crdb cluster. These upgrades can be arbitrarily long
// running, are free to send out arbitrary requests cluster wide, change
// internal DB state, and much more. They're typically reserved for crdb
// internal operations and state. Each upgrade is idempotent in nature, is
// associated with a specific cluster version, and executed when the cluster
// version is made active on every node in the cluster.
//
// Examples of upgrades that apply would be upgrades to move all raft state
// from one storage engine to another, or purging all usage of the replicated
// truncated state in KV.
package upgrade

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
)

// Upgrade defines a program to be executed once every node in the cluster is
// (a) running a specific binary version, and (b) has completed all prior
// upgrades. Note that there are two types of upgrades, a SystemUpgrade
// and a TenantUpgrade. A SystemUpgrade only runs on the system tenant and
// is used to migrate state at the KV layer. A TenantUpgrade runs on all
// tenants (including the system tenant) and should be used whenever state at
// the SQL layer is being migrated.
//
// Each upgrade is associated with a specific internal cluster version and is
// idempotent in nature. When setting the cluster version (via `SET CLUSTER
// SETTING version`), the manager process determines the set of upgrades
// needed to bridge the gap between the current active cluster version, and the
// target one. See [1] for where that happens.
//
// To introduce an upgrade, start by adding version key to pkg/clusterversion
// and introducing a corresponding internal cluster version for it. See [2] for
// more details. Following that, define an Upgrade in the upgrades package
// and add it to the appropriate upgrades slice to the registry. Be sure to
// key it in with the new cluster version we just added. During cluster
// upgrades, once the operator is able to set a cluster version setting that's
// past the version that was introduced (typically the major release version
// the upgrade was introduced in), the manager will execute the defined
// upgrade before letting the upgrade finalize.
//
// If the upgrade requires below-Raft level changes ([3] is one example),
// you'll need to add a version switch and the relevant system-level upgrade
// in [4]. See IterateRangeDescriptors and the Migrate KV request for more
// details.
//
// [1]: `(*Manager).Migrate`
// [2]: pkg/clusterversion/cockroach_versions.go
// [3]: truncatedStateMigration
// [4]: pkg/kv/kvserver/batch_eval/cmd_migrate.go
type Upgrade interface {
	ClusterVersion() clusterversion.ClusterVersion
	Name() string
	internal() // restrict implementations to this package
}

// JobDeps are upgrade-specific dependencies used by the upgrade job to run
// upgrades.
type JobDeps interface {

	// GetUpgrade returns the upgrade associated with the cluster version
	// if one exists.
	GetUpgrade(key clusterversion.ClusterVersion) (Upgrade, bool)

	// SystemDeps returns a handle to upgrade dependencies on a system tenant.
	SystemDeps() SystemDeps
}

type upgrade struct {
	description string
	cv          clusterversion.ClusterVersion
}

// ClusterVersion makes SystemUpgrade an Upgrade.
func (m *upgrade) ClusterVersion() clusterversion.ClusterVersion {
	return m.cv
}

// Name returns a human-readable name for this upgrade.
func (m *upgrade) Name() string {
	return fmt.Sprintf("Upgrade to %s: %q", m.cv.String(), m.description)
}

func (m *upgrade) internal() {}
