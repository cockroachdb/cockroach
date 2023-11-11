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
		toCV(clusterversion.VPrimordial1),
		addRootUser,
		// v22_2StartupMigrationName - this upgrade corresponds to 3 old
		// startupmigrations, out of which "make root a member..." is the last one.
		"make root a member of the admin role",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"enable diagnostics reporting",
		toCV(clusterversion.VPrimordial2),
		optInToDiagnosticsStatReporting,
		"enable diagnostics reporting", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentSystemUpgrade(
		"populate initial version cluster setting table entry",
		toCV(clusterversion.VPrimordial3),
		populateVersionSetting,
		"populate initial version cluster setting table entry", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"initialize the cluster.secret setting",
		toCV(clusterversion.VPrimordial4),
		initializeClusterSecret,
		"initialize cluster.secret", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"update system.locations with default location data",
		toCV(clusterversion.VPrimordial5),
		updateSystemLocationData,
		"update system.locations with default location data", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"create default databases",
		toCV(clusterversion.VPrimordial6),
		createDefaultDbs,
		"create default databases", // v22_2StartupMigrationName
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"add default SQL schema telemetry schedule",
		toCV(clusterversion.Permanent_V22_2SQLSchemaTelemetryScheduledJobs),
		ensureSQLSchemaTelemetrySchedule,
		"add default SQL schema telemetry schedule",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentSystemUpgrade("add tables and jobs to support persisting key visualizer samples",
		toCV(clusterversion.Permanent_V23_1KeyVisualizerTablesAndJobs),
		keyVisualizerTablesMigration,
		"initialize key visualizer tables and jobs",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade("create jobs metrics polling job",
		toCV(clusterversion.Permanent_V23_1_CreateJobsMetricsPollingJob),
		createJobsMetricsPollingJob,
		"create jobs metrics polling job",
		upgrade.RestoreActionNotRequired("jobs are not restored"),
	),
	upgrade.NewPermanentTenantUpgrade("create auto config runner job",
		toCV(clusterversion.Permanent_V23_1_CreateAutoConfigRunnerJob),
		createAutoConfigRunnerJob,
		"create auto config runner job",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentSystemUpgrade(
		"change TTL for SQL Stats system tables",
		toCV(clusterversion.Permanent_V23_1ChangeSQLStatsTTL),
		sqlStatsTTLChange,
		"change TTL for SQL Stats system tables",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"create sql activity updater job",
		toCV(clusterversion.Permanent_V23_1CreateSystemActivityUpdateJob),
		createActivityUpdateJobMigration,
		"create statement_activity and transaction_activity job",
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),

	newFirstUpgrade(toCV(clusterversion.V23_2Start)),

	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support plan gist matching",
		toCV(clusterversion.V23_2_StmtDiagForPlanGist),
		upgrade.NoPrecondition,
		stmtDiagForPlanGistMigration,
		upgrade.RestoreActionNotRequired("diagnostics requests are unique to the cluster on which they were requested"),
	),
	upgrade.NewTenantUpgrade(
		"create system.region_liveness table",
		toCV(clusterversion.V23_2_RegionaLivenessTable),
		upgrade.NoPrecondition,
		createRegionLivenessTables,
		upgrade.RestoreActionNotRequired("ephemeral table that is not backed up or restored"),
	),
	upgrade.NewTenantUpgrade(
		"grant EXECUTE on all functions to the public role",
		toCV(clusterversion.V23_2_GrantExecuteToPublic),
		upgrade.NoPrecondition,
		grantExecuteToPublicOnAllFunctions,
		upgrade.RestoreActionNotRequired("TODO explain why this migration does not need to consider restore"),
	),
	upgrade.NewPermanentTenantUpgrade(
		"create system.mvcc_statistics table and job",
		toCV(clusterversion.Permanent_V23_2_MVCCStatisticsTable),
		createMVCCStatisticsTableAndJobMigration,
		"create system.mvcc_statistics table and job",
		upgrade.RestoreActionNotRequired("table is relevant to the storage cluster and is not restored"),
	),
	upgrade.NewTenantUpgrade(
		"create transaction_execution_insights and statement_execution_insights tables",
		toCV(clusterversion.V23_2_AddSystemExecInsightsTable),
		upgrade.NoPrecondition,
		systemExecInsightsTableMigration,
		upgrade.RestoreActionNotRequired("execution insights are specific to the cluster that executed some query and are not restored"),
	),

	newFirstUpgrade(toCV(clusterversion.V24_1Start)),

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}

func toCV(key clusterversion.Key) roachpb.Version {
	return clusterversion.ByKey(key)
}
