// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	newFirstUpgrade(clusterversion.V26_1_Start.Version()),

	upgrade.NewTenantUpgrade(
		"create table_statistics_locks table",
		clusterversion.V26_1_AddTableStatisticsLocksTable.Version(),
		upgrade.NoPrecondition,
		createTableStatisticsLocksTable,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	newFirstUpgrade(clusterversion.V26_2_Start.Version()),

	upgrade.NewTenantUpgrade(
		"add delayDelete column to system.table_statistics",
		clusterversion.V26_2_AddTableStatisticsDelayDeleteColumn.Version(),
		upgrade.NoPrecondition,
		tableStatisticsDelayDeleteColumnMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new column"),
	),

	upgrade.NewTenantUpgrade(
		"create cluster_metrics table",
		clusterversion.V26_2_AddSystemClusterMetricsTable.Version(),
		upgrade.NoPrecondition,
		createClusterMetricsTable,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	upgrade.NewTenantUpgrade(
		"repair trigger backrefs to include trigger ID",
		clusterversion.V26_2_TriggerBackrefRepair.Version(),
		upgrade.NoPrecondition,
		repairTriggerBackrefs,
		upgrade.RestoreActionImplemented("handled in RunRestoreChanges"),
	),

	upgrade.NewTenantUpgrade(
		"add hint_type, hint_name, and enabled columns to statement_hints table",
		clusterversion.V26_2_StatementHintsTypeNameEnabledColumnsAdded.Version(),
		upgrade.NoPrecondition,
		statementHintsAddColumnsTableMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new column"),
	),

	upgrade.NewTenantUpgrade(
		"remove default value from hint_type column in the statement_hints table",
		clusterversion.V26_2_StatementHintsTypeColumnBackfilled.Version(),
		upgrade.NoPrecondition,
		statementHintsRemoveTypeDefaultTableMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new column"),
	),

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}
