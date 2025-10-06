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

	newFirstUpgrade(clusterversion.V24_2Start.Version()),

	upgrade.NewTenantUpgrade(
		"add the redacted column to system.statement_diagnostics_requests table",
		clusterversion.V24_2_StmtDiagRedacted.Version(),
		upgrade.NoPrecondition,
		stmtDiagRedactedMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	upgrade.NewTenantUpgrade(
		"create all missing system tables in app tenants",
		clusterversion.V24_2_TenantSystemTables.Version(),
		upgrade.NoPrecondition,
		createTenantSystemTables,
		upgrade.RestoreActionNotRequired("cluster restore does not restore these tables"),
	),

	upgrade.NewTenantUpgrade(
		"add new columns to the system.tenant_usage table to store tenant consumption rates",
		clusterversion.V24_2_TenantRates.Version(),
		upgrade.NoPrecondition,
		tenantRatesMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new field"),
	),

	upgrade.NewTenantUpgrade(
		"delete version row in system.tenant_settings",
		clusterversion.V24_2_DeleteTenantSettingsVersion.Version(),
		upgrade.NoPrecondition,
		deleteVersionTenantSettings,
		upgrade.RestoreActionImplemented("bad row skipped when restoring system.tenant_settings"),
	),

	newFirstUpgrade(clusterversion.V24_3_Start.Version()),

	upgrade.NewSystemUpgrade(
		"create a zone config for the timeseries range if one does not exist already",
		clusterversion.V24_3_AddTimeseriesZoneConfig.Version(),
		addTimeseriesZoneConfig,
		upgrade.RestoreActionNotRequired("this zone config isn't necessary for restore"),
	),

	upgrade.NewTenantUpgrade(
		"add new table_metadata table and job to the system tenant",
		clusterversion.V24_3_TableMetadata.Version(),
		upgrade.NoPrecondition,
		addTableMetadataTableAndJob,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	upgrade.NewTenantUpgrade(
		"add exclude_data_from_backup to certain system tables on tenants",
		clusterversion.V24_3_TenantExcludeDataFromBackup.Version(),
		upgrade.NoPrecondition,
		tenantExcludeDataFromBackup,
		upgrade.RestoreActionNotRequired("cluster restore does not restore affected tables"),
	),

	upgrade.NewTenantUpgrade(
		"add new column to the system.sql_instances table to store whether a node is draining",
		clusterversion.V24_3_SQLInstancesAddDraining.Version(),
		upgrade.NoPrecondition,
		sqlInstancesAddDrainingMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new field"),
	),

	upgrade.NewTenantUpgrade(
		"check that we are not in violation of the new license policies",
		clusterversion.V24_3_MaybePreventUpgradeForCoreLicenseDeprecation.Version(),
		checkForPostUpgradeThrottlePreCond,
		checkForPostUpgradeThrottleProcessing,
		upgrade.RestoreActionNotRequired("this check does not persist anything"),
	),

	upgrade.NewTenantUpgrade(
		"adds new columns to table_metadata table",
		clusterversion.V24_3_AddTableMetadataCols.Version(),
		upgrade.NoPrecondition,
		addTableMetadataCols,
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
