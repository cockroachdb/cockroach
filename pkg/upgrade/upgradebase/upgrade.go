// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package upgradebase

import "github.com/cockroachdb/cockroach/pkg/roachpb"

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
	Version() roachpb.Version
	// Name returns a human-readable name for this upgrade.
	Name() string
	// Permanent returns true for "permanent" upgrades - i.e. upgrades that are
	// not baked into the bootstrap image and need to be run on new cluster
	// regardless of the cluster's bootstrap version.
	Permanent() bool

	RestoreBehavior() string
}
