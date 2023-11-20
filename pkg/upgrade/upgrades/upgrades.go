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
	upgrade.NewPermanentTenantUpgrade(
		"add users and roles",
		clusterversion.VPrimordial1.Version(),
		addRootUser,
		// v22_2StartupMigrationName - this upgrade corresponds to 3 old
		// startupmigrations, out of which "make root a member..." is the last one.
		"make root a member of the admin role",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"enable diagnostics reporting",
		clusterversion.VPrimordial2.Version(),
		optInToDiagnosticsStatReporting,
		"enable diagnostics reporting", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentSystemUpgrade(
		"populate initial version cluster setting table entry",
		clusterversion.VPrimordial3.Version(),
		populateVersionSetting,
		"populate initial version cluster setting table entry", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"initialize the cluster.secret setting",
		clusterversion.VPrimordial4.Version(),
		initializeClusterSecret,
		"initialize cluster.secret", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"update system.locations with default location data",
		clusterversion.VPrimordial5.Version(),
		updateSystemLocationData,
		"update system.locations with default location data", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"create default databases",
		clusterversion.VPrimordial6.Version(),
		createDefaultDbs,
		"create default databases", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"add default SQL schema telemetry schedule",
		clusterversion.Permanent_V22_2SQLSchemaTelemetryScheduledJobs.Version(),
		ensureSQLSchemaTelemetrySchedule,
		"add default SQL schema telemetry schedule",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentSystemUpgrade("add tables and jobs to support persisting key visualizer samples",
		clusterversion.Permanent_V23_1KeyVisualizerTablesAndJobs.Version(),
		keyVisualizerTablesMigration,
		"initialize key visualizer tables and jobs",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade("create jobs metrics polling job",
		clusterversion.Permanent_V23_1_CreateJobsMetricsPollingJob.Version(),
		createJobsMetricsPollingJob,
		"create jobs metrics polling job",
		upgrade.RestoreActionNotRequired("jobs are not restored"),
	),
	upgrade.NewPermanentTenantUpgrade("create auto config runner job",
		clusterversion.Permanent_V23_1_CreateAutoConfigRunnerJob.Version(),
		createAutoConfigRunnerJob,
		"create auto config runner job",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentSystemUpgrade(
		"change TTL for SQL Stats system tables",
		clusterversion.Permanent_V23_1ChangeSQLStatsTTL.Version(),
		sqlStatsTTLChange,
		"change TTL for SQL Stats system tables",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"create sql activity updater job",
		clusterversion.Permanent_V23_1CreateSystemActivityUpdateJob.Version(),
		createActivityUpdateJobMigration,
		"create statement_activity and transaction_activity job",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),

	newFirstUpgrade(clusterversion.V23_2Start.Version()),

	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support plan gist matching",
		clusterversion.V23_2_StmtDiagForPlanGist.Version(),
		upgrade.NoPrecondition,
		stmtDiagForPlanGistMigration,
		upgrade.RestoreActionNotRequired("diagnostics requests are unique to the cluster on which they were requested"),
	),
	upgrade.NewTenantUpgrade(
		"create system.region_liveness table",
		clusterversion.V23_2_RegionaLivenessTable.Version(),
		upgrade.NoPrecondition,
		createRegionLivenessTables,
		upgrade.RestoreActionNotRequired("ephemeral table that is not backed up or restored"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"create system.mvcc_statistics table and job",
		clusterversion.Permanent_V23_2_MVCCStatisticsTable.Version(),
		createMVCCStatisticsTableAndJobMigration,
		"create system.mvcc_statistics table and job",
		upgrade.RestoreActionNotRequired("table is relevant to the storage cluster and is not restored"),
	),
	upgrade.NewTenantUpgrade(
		"create transaction_execution_insights and statement_execution_insights tables",
		clusterversion.V23_2_AddSystemExecInsightsTable.Version(),
		upgrade.NoPrecondition,
		systemExecInsightsTableMigration,
		upgrade.RestoreActionNotRequired("execution insights are specific to the cluster that executed some query and are not restored"),
	),

	newFirstUpgrade(clusterversion.V24_1Start.Version()),

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}
