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
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/errors"
)

// SettingsDefaultOverrides documents the effect of several migrations that add
// an explicit value for a setting, effectively changing the "default value"
// from what was defined in code.
var SettingsDefaultOverrides = map[string]string{
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
	// Introduced in v2.1.
	// TODO(knz): bake this migration into v19.1.
	upgrade.NewPermanentTenantUpgrade(
		"create default databases",
		toCV(clusterversion.VPrimordial6),
		createDefaultDbs,
		"create default databases", // v22_2StartupMigrationName
	),
	upgrade.NewTenantUpgrade(
		"ensure preconditions are met before starting upgrading to v22.2",
		toCV(clusterversion.TODODelete_V22_2Start),
		preconditionBeforeStartingAnUpgrade,
		NoTenantUpgradeFunc,
	),
	upgrade.NewTenantUpgrade(
		"upgrade sequences to be referenced by ID",
		toCV(clusterversion.TODODelete_V22_2UpgradeSequenceToBeReferencedByID),
		upgrade.NoPrecondition,
		upgradeSequenceToBeReferencedByID,
	),
	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support sampling probabilities",
		toCV(clusterversion.TODODelete_V22_2SampledStmtDiagReqs),
		upgrade.NoPrecondition,
		sampledStmtDiagReqsMigration,
	),
	upgrade.NewTenantUpgrade(
		"add the system.external_connections table",
		toCV(clusterversion.TODODelete_V22_2SystemExternalConnectionsTable),
		upgrade.NoPrecondition,
		systemExternalConnectionsTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add default SQL schema telemetry schedule",
		toCV(clusterversion.TODODelete_V22_2SQLSchemaTelemetryScheduledJobs),
		upgrade.NoPrecondition,
		ensureSQLSchemaTelemetrySchedule,
	),
	upgrade.NewTenantUpgrade("ensure all GC jobs send DeleteRange requests",
		toCV(clusterversion.TODODelete_V22_2WaitedForDelRangeInGCJob),
		checkForPausedGCJobs,
		waitForDelRangeInGCJob,
	),
	upgrade.NewTenantUpgrade(
		"wait for all in-flight schema changes",
		toCV(clusterversion.TODODelete_V22_2NoNonMVCCAddSSTable),
		upgrade.NoPrecondition,
		waitForAllSchemaChanges,
	),
	upgrade.NewTenantUpgrade("update invalid column IDs in sequence back references",
		toCV(clusterversion.TODODelete_V22_2UpdateInvalidColumnIDsInSequenceBackReferences),
		upgrade.NoPrecondition,
		updateInvalidColumnIDsInSequenceBackReferences,
	),
	upgrade.NewTenantUpgrade("fix corrupt user-file related table descriptors",
		toCV(clusterversion.TODODelete_V22_2FixUserfileRelatedDescriptorCorruption),
		upgrade.NoPrecondition,
		fixInvalidObjectsThatLookLikeBadUserfileConstraint,
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
		toCV(clusterversion.V23_1KeyVisualizerTablesAndJobs),
		keyVisualizerTablesMigration,
		"initialize key visualizer tables and jobs",
	),
	upgrade.NewTenantUpgrade("delete descriptors of dropped functions",
		toCV(clusterversion.V23_1_DeleteDroppedFunctionDescriptors),
		upgrade.NoPrecondition,
		deleteDescriptorsOfDroppedFunctions,
	),
	upgrade.NewPermanentTenantUpgrade("create jobs metrics polling job",
		toCV(clusterversion.V23_1_CreateJobsMetricsPollingJob),
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
}

func init() {
	for _, m := range upgrades {
		if _, exists := registry[m.Version()]; exists {
			panic(errors.AssertionFailedf("duplicate upgrade registration for %v", m.Version()))
		}
		registry[m.Version()] = m
	}
}

func toCV(key clusterversion.Key) roachpb.Version {
	return clusterversion.ByKey(key)
}
