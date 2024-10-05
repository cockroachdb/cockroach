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
	upgrade.NewPermanentTenantUpgrade(
		"add users and roles",
		toCV(clusterversion.VPrimordial1),
		addRootUser,
		// v22_2StartupMigrationName - this upgrade corresponds to 3 old
		// startupmigrations, out of which "make root a member..." is the last one.
		"make root a member of the admin role",
	),
	upgrade.NewPermanentTenantUpgrade(
		"enable diagnostics reporting",
		toCV(clusterversion.VPrimordial2),
		optInToDiagnosticsStatReporting,
		"enable diagnostics reporting", // v22_2StartupMigrationName
	),
	upgrade.NewPermanentSystemUpgrade(
		"populate initial version cluster setting table entry",
		toCV(clusterversion.VPrimordial3),
		populateVersionSetting,
		"populate initial version cluster setting table entry", // v22_2StartupMigrationName
	),
	upgrade.NewPermanentTenantUpgrade(
		"initialize the cluster.secret setting",
		toCV(clusterversion.VPrimordial4),
		initializeClusterSecret,
		"initialize cluster.secret", // v22_2StartupMigrationName
	),
	upgrade.NewPermanentTenantUpgrade(
		"update system.locations with default location data",
		toCV(clusterversion.VPrimordial5),
		updateSystemLocationData,
		"update system.locations with default location data", // v22_2StartupMigrationName
	),
	upgrade.NewPermanentTenantUpgrade(
		"create default databases",
		toCV(clusterversion.VPrimordial6),
		createDefaultDbs,
		"create default databases", // v22_2StartupMigrationName
	),
	upgrade.NewPermanentTenantUpgrade(
		"add default SQL schema telemetry schedule",
		toCV(clusterversion.Permanent_V22_2SQLSchemaTelemetryScheduledJobs),
		ensureSQLSchemaTelemetrySchedule,
		"add default SQL schema telemetry schedule",
	),
	upgrade.NewTenantUpgrade("add columns to system.tenants and populate a system tenant entry",
		toCV(clusterversion.V23_1TenantNamesStateAndServiceMode),
		upgrade.NoPrecondition,
		extendTenantsTable,
	),
	upgrade.NewTenantUpgrade("set the value or system.descriptor_id_seq for the system tenant",
		toCV(clusterversion.V23_1DescIDSequenceForSystemTenant),
		upgrade.NoPrecondition,
		descIDSequenceForSystemTenant,
	),
	upgrade.NewTenantUpgrade("add a partial predicate and full statistics ID columns to system.table_statistics",
		toCV(clusterversion.V23_1AddPartialStatisticsColumns),
		upgrade.NoPrecondition,
		alterSystemTableStatisticsAddPartialPredicateAndID,
	),
	upgrade.NewTenantUpgrade(
		"create system.job_info table",
		toCV(clusterversion.V23_1CreateSystemJobInfoTable),
		upgrade.NoPrecondition,
		systemJobInfoTableMigration,
	),
	upgrade.NewTenantUpgrade("add role_id and member_id columns to system.role_members",
		toCV(clusterversion.V23_1RoleMembersTableHasIDColumns),
		upgrade.NoPrecondition,
		alterSystemRoleMembersAddIDColumns,
	),
	upgrade.NewTenantUpgrade("backfill role_id and member_id columns in system.role_members",
		toCV(clusterversion.V23_1RoleMembersIDColumnsBackfilled),
		upgrade.NoPrecondition,
		backfillSystemRoleMembersIDColumns,
	),
	upgrade.NewTenantUpgrade(
		"add job_type column to system.jobs table",
		toCV(clusterversion.V23_1AddTypeColumnToJobsTable),
		upgrade.NoPrecondition,
		alterSystemJobsAddJobType,
	),
	upgrade.NewTenantUpgrade(
		"backfill job_type column in system.jobs table",
		toCV(clusterversion.V23_1BackfillTypeColumnInJobsTable),
		upgrade.NoPrecondition,
		backfillJobTypeColumn,
	),
	upgrade.NewTenantUpgrade(
		"create virtual column indexes_usage based on (statistics->'statistics'->'indexes') with index "+
			"on table system.statement_statistics",
		toCV(clusterversion.V23_1_AlterSystemStatementStatisticsAddIndexesUsage),
		upgrade.NoPrecondition,
		createIndexOnIndexUsageOnSystemStatementStatistics,
	),
	upgrade.NewTenantUpgrade(
		"add column sql_addr to table system.sql_instances",
		toCV(clusterversion.V23_1AlterSystemSQLInstancesAddSQLAddr),
		upgrade.NoPrecondition,
		alterSystemSQLInstancesAddSqlAddr,
	),
	upgrade.NewPermanentSystemUpgrade("add tables and jobs to support persisting key visualizer samples",
		toCV(clusterversion.Permanent_V23_1KeyVisualizerTablesAndJobs),
		keyVisualizerTablesMigration,
		"initialize key visualizer tables and jobs",
	),
	upgrade.NewTenantUpgrade("delete descriptors of dropped functions",
		toCV(clusterversion.V23_1_DeleteDroppedFunctionDescriptors),
		upgrade.NoPrecondition,
		deleteDescriptorsOfDroppedFunctions,
	),
	upgrade.NewPermanentTenantUpgrade("create jobs metrics polling job",
		toCV(clusterversion.Permanent_V23_1_CreateJobsMetricsPollingJob),
		createJobsMetricsPollingJob,
		"create jobs metrics polling job",
	),
	upgrade.NewTenantUpgrade(
		"add user_id column to system.privileges table",
		toCV(clusterversion.V23_1SystemPrivilegesTableHasUserIDColumn),
		upgrade.NoPrecondition,
		alterSystemPrivilegesAddUserIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"backfill user_id column in system.privileges table",
		toCV(clusterversion.V23_1SystemPrivilegesTableUserIDColumnBackfilled),
		upgrade.NoPrecondition,
		backfillSystemPrivilegesUserIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"add user_id column to system.web_sessions table",
		toCV(clusterversion.V23_1WebSessionsTableHasUserIDColumn),
		upgrade.NoPrecondition,
		alterWebSessionsTableAddUserIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"backfill user_id column in system.web_sessions table",
		toCV(clusterversion.V23_1WebSessionsTableUserIDColumnBackfilled),
		upgrade.NoPrecondition,
		backfillWebSessionsTableUserIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"add column sql_addr to table system.sql_instances",
		toCV(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates),
		upgrade.NoPrecondition,
		waitForSchemaChangerElementMigration,
	),
	upgrade.NewTenantUpgrade(
		"add secondary index on (path,username) to table system.privileges",
		toCV(clusterversion.V23_1AlterSystemPrivilegesAddIndexOnPathAndUsername),
		upgrade.NoPrecondition,
		alterSystemPrivilegesAddSecondaryIndex,
	),
	upgrade.NewTenantUpgrade(
		"add role_id column to system.database_role_settings table",
		toCV(clusterversion.V23_1DatabaseRoleSettingsHasRoleIDColumn),
		upgrade.NoPrecondition,
		alterDatabaseRoleSettingsTableAddRoleIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"backfill role_id column in system.database_role_settings table",
		toCV(clusterversion.V23_1DatabaseRoleSettingsRoleIDColumnBackfilled),
		upgrade.NoPrecondition,
		backfillDatabaseRoleSettingsTableRoleIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"backfill system tables with regional by row compatible indexes",
		toCV(clusterversion.V23_1_SystemRbrReadNew),
		upgrade.NoPrecondition,
		backfillRegionalByRowIndex,
	),
	upgrade.NewTenantUpgrade(
		"clean up old indexes for regional by row compatible system tables",
		toCV(clusterversion.V23_1_SystemRbrCleanup),
		upgrade.NoPrecondition,
		cleanUpRegionalByTableIndex,
	),
	upgrade.NewTenantUpgrade(
		"add owner_id column to system.external_connections table",
		toCV(clusterversion.V23_1ExternalConnectionsTableHasOwnerIDColumn),
		upgrade.NoPrecondition,
		alterExternalConnectionsTableAddOwnerIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"backfill owner_id column in system.external_connections table",
		toCV(clusterversion.V23_1ExternalConnectionsTableOwnerIDColumnBackfilled),
		upgrade.NoPrecondition,
		backfillExternalConnectionsTableOwnerIDColumn,
	),
	upgrade.NewTenantUpgrade(
		"backfill the system.job_info table with the payload and progress of each job in the system.jobs table",
		toCV(clusterversion.V23_1JobInfoTableIsBackfilled),
		upgrade.NoPrecondition,
		backfillJobInfoTable,
	),
	upgrade.NewTenantUpgrade("ensure all GC jobs send DeleteRange requests",
		toCV(clusterversion.V23_1_UseDelRangeInGCJob),
		checkForPausedGCJobs,
		waitForDelRangeInGCJob,
	),
	upgrade.NewSystemUpgrade(
		"create system.tenant_tasks and system.task_payloads",
		toCV(clusterversion.V23_1_TaskSystemTables),
		createTaskSystemTables,
	),
	upgrade.NewPermanentTenantUpgrade("create auto config runner job",
		toCV(clusterversion.Permanent_V23_1_CreateAutoConfigRunnerJob),
		createAutoConfigRunnerJob,
		"create auto config runner job",
	),
	upgrade.NewTenantUpgrade(
		"create and index new computed columns on system sql stats tables",
		toCV(clusterversion.V23_1AddSQLStatsComputedIndexes),
		upgrade.NoPrecondition,
		createComputedIndexesOnSystemSQLStatistics,
	),
	upgrade.NewTenantUpgrade(
		"create statement_activity and transaction_activity tables",
		toCV(clusterversion.V23_1AddSystemActivityTables),
		upgrade.NoPrecondition,
		systemStatisticsActivityTableMigration,
	),
	upgrade.NewPermanentSystemUpgrade(
		"change TTL for SQL Stats system tables",
		toCV(clusterversion.Permanent_V23_1ChangeSQLStatsTTL),
		sqlStatsTTLChange,
		"change TTL for SQL Stats system tables",
	),
	upgrade.NewTenantUpgrade(
		"stop writing payload and progress to system.jobs",
		toCV(clusterversion.V23_1StopWritingPayloadAndProgressToSystemJobs),
		upgrade.NoPrecondition,
		alterPayloadColumnToNullable,
	),
	upgrade.NewSystemUpgrade(
		"create system.tenant_id_seq",
		toCV(clusterversion.V23_1_TenantIDSequence),
		tenantIDSequenceForSystemTenant,
	),
	upgrade.NewPermanentTenantUpgrade(
		"create sql activity updater job",
		toCV(clusterversion.Permanent_V23_1CreateSystemActivityUpdateJob),
		createActivityUpdateJobMigration,
		"create statement_activity and transaction_activity job",
	),
	firstUpgradeTowardsV23_2,
	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support plan gist matching",
		toCV(clusterversion.V23_2_StmtDiagForPlanGist),
		upgrade.NoPrecondition,
		stmtDiagForPlanGistMigration,
	),
	upgrade.NewTenantUpgrade(
		"create system.region_liveness table",
		toCV(clusterversion.V23_2_RegionaLivenessTable),
		upgrade.NoPrecondition,
		createRegionLivenessTables,
	),
	upgrade.NewPermanentTenantUpgrade(
		"create system.mvcc_statistics table and job",
		toCV(clusterversion.Permanent_V23_2_MVCCStatisticsTable),
		createMVCCStatisticsTableAndJobMigration,
		"create system.mvcc_statistics table and job",
	),
	upgrade.NewTenantUpgrade(
		"create transaction_execution_insights and statement_execution_insights tables",
		toCV(clusterversion.V23_2_AddSystemExecInsightsTable),
		upgrade.NoPrecondition,
		systemExecInsightsTableMigration,
	),
}

var (
	firstUpgradeTowardsV23_2 = upgrade.NewTenantUpgrade(
		"prepare upgrade to v23.2 release",
		toCV(clusterversion.V23_2Start),
		FirstUpgradeFromReleasePrecondition,
		FirstUpgradeFromRelease,
	)

	// This slice must contain all upgrades bound to V??_?Start cluster
	// version keys. These should have FirstUpgradeFromReleasePrecondition as a
	// precondition and FirstUpgradeFromRelease as the upgrade function.
	firstUpgradesAfterPreExistingReleases = []upgradebase.Upgrade{
		firstUpgradeTowardsV23_2,
	}
)

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}

func toCV(key clusterversion.Key) roachpb.Version {
	return clusterversion.ByKey(key)
}
