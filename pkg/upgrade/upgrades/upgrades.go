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
		"add request_id column/index to system.statement_diagnostics",
		clusterversion.V26_2_StmtDiagnosticsRequestID.Version(),
		upgrade.NoPrecondition,
		stmtDiagnosticsAddRequestIDColumnAndIndex,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
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

	upgrade.NewTenantUpgrade(
		"cleanup lease manager descriptor txn keys",
		clusterversion.V26_2_DescriptorTxnKeyCleanup.Version(),
		upgrade.NoPrecondition,
		descriptorTxnKeyCleanup,
		upgrade.RestoreActionImplemented("handled in RunRestoreChanges"),
	),

	upgrade.NewTenantUpgrade(
		"add computed columns and covering index to system.statement_statistics",
		clusterversion.V26_2_AddStatementStatisticsComputedColumns.Version(),
		upgrade.NoPrecondition,
		statementStatisticsComputedColumnsMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore computed columns"),
	),

	upgrade.NewTenantUpgrade(
		"add computed columns and covering index to system.transaction_statistics",
		clusterversion.V26_2_AddTransactionStatisticsComputedColumns.Version(),
		upgrade.NoPrecondition,
		transactionStatisticsComputedColumnsMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore computed columns"),
	),

	upgrade.NewTenantUpgrade(
		"backfill changefeed persisted frontiers from span-level checkpoints",
		clusterversion.V26_2_ChangefeedsStopReadingSpanLevelCheckpoints.Version(),
		upgrade.NoPrecondition,
		backfillChangefeedPersistedFrontiers,
		upgrade.RestoreActionNotRequired("changefeed jobs are not restored"),
	),

	upgrade.NewTenantUpgrade(
		"stop writing changefeed span-level checkpoints",
		clusterversion.V26_2_ChangefeedsStopWritingSpanLevelCheckpoints.Version(),
		upgrade.NoPrecondition,
		// NB: We need a migration to bump the system database schema version.
		NoTenantUpgradeFunc,
		upgrade.RestoreActionNotRequired("changefeed jobs are not restored"),
	),

	upgrade.NewTenantUpgrade(
		"purge changefeed span-level checkpoints",
		clusterversion.V26_2_ChangefeedsNoLongerHaveSpanLevelCheckpoints.Version(),
		upgrade.NoPrecondition,
		purgeChangefeedSpanLevelCheckpoints,
		upgrade.RestoreActionNotRequired("changefeed jobs are not restored"),
	),

	upgrade.NewTenantUpgrade(
		"grant TEMPORARY privilege to public on all databases",
		clusterversion.V26_2_GrantTemporaryToPublic.Version(),
		upgrade.NoPrecondition,
		grantTemporaryToPublic,
		upgrade.RestoreActionNotRequired("privilege is granted on restore via NewBaseDatabasePrivilegeDescriptor"),
	),

	upgrade.NewTenantUpgrade(
		"create statements table",
		clusterversion.V26_2_AddSystemStatementsTable.Version(),
		upgrade.NoPrecondition,
		createStatementsTable,
		upgrade.RestoreActionNotRequired(
			"restore for a cluster predating this table can leave it empty",
		),
	),

	newFirstUpgrade(clusterversion.V26_3_Start.Version()),

	upgrade.NewTenantUpgrade(
		"add max_execution_latency column to diagnostics requests tables",
		clusterversion.V26_3_StmtDiagnosticsMaxLatency.Version(),
		upgrade.NoPrecondition,
		diagnosticsAddMaxLatencyColumn,
		upgrade.RestoreActionNotRequired("cluster restore does not restore these tables"),
	),

	upgrade.NewTenantUpgrade(
		"create advisory_locks table",
		clusterversion.V26_3_AddAdvisoryLocksTable.Version(),
		upgrade.NoPrecondition,
		createAdvisoryLocksTable,
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
