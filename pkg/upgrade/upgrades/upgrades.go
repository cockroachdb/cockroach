// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrades contains the implementation of upgrades. It is imported
// by the server library.
//
// This package registers the upgrades with the upgrade package.
package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
)

// SettingsDefaultOverrides documents the effect of several migrations that add
// an explicit value for a setting, effectively changing the "default value"
// from what was defined in code.
var SettingsDefaultOverrides = map[settings.InternalKey]string{
	"diagnostics.reporting.enabled": "true",
	"cluster.secret":                "<random>",
}

// GetUpgrade returns the upgrade corresponding to this version if
// one exists.
func GetUpgrade(key roachpb.Version) (upgradebase.Upgrade, bool) {
	m, ok := registry[key]
	if !ok {
		return nil, false
	}
	return m, ok
}

// NoTenantUpgradeFunc is a TenantUpgradeFunc that doesn't do anything.
func NoTenantUpgradeFunc(context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated upgrade. The upgrade is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[roachpb.Version]upgradebase.Upgrade)

var upgrades = []upgradebase.Upgrade{
	upgrade.NewSystemUpgrade("intitialize the system cluster",
		clusterversion.VBootstrapSystem.Version(),
		bootstrapSystem,
		upgrade.RestoreActionNotRequired("initialization runs before restore")),

	upgrade.NewTenantUpgrade("intitialize the cluster",
		clusterversion.VBootstrapTenant.Version(),
		upgrade.NoPrecondition,
		bootstrapCluster,
		upgrade.RestoreActionNotRequired("initialization runs before restore")),

	newFirstUpgrade(clusterversion.V24_1Start.Version()),

	upgrade.NewTenantUpgrade(
		"hide unused payload and progress columns from system.jobs table",
		clusterversion.V24_1_DropPayloadAndProgressFromSystemJobsTable.Version(),
		upgrade.NoPrecondition,
		hidePayloadProgressFromSystemJobs,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the system.jobs table"),
	),

	upgrade.NewTenantUpgrade(
		"migrate old-style PTS records to the new style",
		clusterversion.V24_1_MigrateOldStylePTSRecords.Version(),
		upgrade.NoPrecondition,
		migrateOldStylePTSRecords,
		upgrade.RestoreActionNotRequired("restore does not restore the PTS table"),
	),

	upgrade.NewTenantUpgrade(
		"stop writing expiration based leases to system.lease table (equivalent to experimental_use_session_based_leasing=drain)",
		clusterversion.V24_1_SessionBasedLeasingDrain.Version(),
		upgrade.NoPrecondition,
		disableWritesForExpiryBasedLeases,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the system.lease table"),
	),

	upgrade.NewTenantUpgrade(
		"only use session based leases  (equivalent to experimental_use_session_based_leasing=session)",
		clusterversion.V24_1_SessionBasedLeasingOnly.Version(),
		upgrade.NoPrecondition,
		adoptUsingOnlySessionBasedLeases,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the system.lease table"),
	),

	upgrade.NewTenantUpgrade(
		"update system.lease descriptor to be session base",
		clusterversion.V24_1_SessionBasedLeasingUpgradeDescriptor.Version(),
		upgrade.NoPrecondition,
		upgradeSystemLeasesDescriptor,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the system.lease table"),
	),

	upgrade.NewTenantUpgrade(
		"set survivability goal on MR system database; fix-up gc.ttl and exclude_data_from_backup",
		clusterversion.V24_1_SystemDatabaseSurvivability.Version(),
		upgrade.NoPrecondition,
		alterSystemDatabaseSurvivalGoal,
		upgrade.RestoreActionNotRequired("cluster restore does not preserve the multiregion configuration of the system database"),
	),

	upgrade.NewTenantUpgrade(
		"add the span_counts table to the system tenant",
		clusterversion.V24_1_AddSpanCounts.Version(),
		upgrade.NoPrecondition,
		addSpanCountTable,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	newFirstUpgrade(clusterversion.V24_2Start.Version()),

	upgrade.NewTenantUpgrade(
		"add the redacted column to system.statement_diagnostics_requests table",
		clusterversion.V24_2_StmtDiagRedacted.Version(),
		upgrade.NoPrecondition,
		stmtDiagRedactedMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}
