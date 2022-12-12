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
		toCV(clusterversion.V22_2Start),
		preconditionBeforeStartingAnUpgrade,
		NoTenantUpgradeFunc,
	),
	upgrade.NewTenantUpgrade(
		"upgrade sequences to be referenced by ID",
		toCV(clusterversion.V22_2UpgradeSequenceToBeReferencedByID),
		upgrade.NoPrecondition,
		upgradeSequenceToBeReferencedByID,
	),
	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support sampling probabilities",
		toCV(clusterversion.V22_2SampledStmtDiagReqs),
		upgrade.NoPrecondition,
		sampledStmtDiagReqsMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column locality to table system.sql_instances",
		toCV(clusterversion.V22_2AlterSystemSQLInstancesAddLocality),
		upgrade.NoPrecondition,
		alterSystemSQLInstancesAddLocality,
	),
	upgrade.NewTenantUpgrade(
		"add the system.external_connections table",
		toCV(clusterversion.V22_2SystemExternalConnectionsTable),
		upgrade.NoPrecondition,
		systemExternalConnectionsTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column index_recommendations to table system.statement_statistics",
		toCV(clusterversion.V22_2AlterSystemStatementStatisticsAddIndexRecommendations),
		upgrade.NoPrecondition,
		alterSystemStatementStatisticsAddIndexRecommendations,
	),
	upgrade.NewTenantUpgrade("add system.role_id_sequence",
		toCV(clusterversion.V22_2RoleIDSequence),
		upgrade.NoPrecondition,
		roleIDSequenceMigration,
	),
	// Add user_id column, the column will not be backfilled.
	// However, new users created from this point forward will be created
	// with an ID. We cannot start using the IDs in this version
	// as old users are not backfilled. The key here is that we have a cut
	// off point where we know which users need to be backfilled and no
	// more users can be created without ids.
	upgrade.NewTenantUpgrade("alter system.users to include user_id column",
		toCV(clusterversion.V22_2AddSystemUserIDColumn),
		upgrade.NoPrecondition,
		alterSystemUsersAddUserIDColumnWithIndex,
	),
	upgrade.NewTenantUpgrade("backfill users with ids and add an index on the id column",
		toCV(clusterversion.V22_2SystemUsersIDColumnIsBackfilled),
		upgrade.NoPrecondition,
		backfillSystemUsersIDColumn,
	),
	upgrade.NewTenantUpgrade("set user_id column to not null",
		toCV(clusterversion.V22_2SetSystemUsersUserIDColumnNotNull),
		upgrade.NoPrecondition,
		setUserIDNotNull,
	),
	upgrade.NewTenantUpgrade(
		"add default SQL schema telemetry schedule",
		toCV(clusterversion.V22_2SQLSchemaTelemetryScheduledJobs),
		upgrade.NoPrecondition,
		ensureSQLSchemaTelemetrySchedule,
	),
	upgrade.NewTenantUpgrade("alter system.role_options to include user_id column",
		toCV(clusterversion.V22_2RoleOptionsTableHasIDColumn),
		upgrade.NoPrecondition,
		alterSystemRoleOptionsAddUserIDColumnWithIndex,
	),
	upgrade.NewTenantUpgrade("backfill entries in system.role_options to include IDs",
		toCV(clusterversion.V22_2RoleOptionsIDColumnIsBackfilled),
		upgrade.NoPrecondition,
		backfillSystemRoleOptionsIDColumn,
	),
	upgrade.NewTenantUpgrade("set system.role_options user_id column to not null",
		toCV(clusterversion.V22_2SetRoleOptionsUserIDColumnNotNull),
		upgrade.NoPrecondition,
		setSystemRoleOptionsUserIDColumnNotNull,
	),
	upgrade.NewTenantUpgrade("ensure all GC jobs send DeleteRange requests",
		toCV(clusterversion.V22_2WaitedForDelRangeInGCJob),
		checkForPausedGCJobs,
		waitForDelRangeInGCJob,
	),
	upgrade.NewTenantUpgrade(
		"wait for all in-flight schema changes",
		toCV(clusterversion.V22_2NoNonMVCCAddSSTable),
		upgrade.NoPrecondition,
		waitForAllSchemaChanges,
	),
	upgrade.NewTenantUpgrade("update invalid column IDs in sequence back references",
		toCV(clusterversion.V22_2UpdateInvalidColumnIDsInSequenceBackReferences),
		upgrade.NoPrecondition,
		updateInvalidColumnIDsInSequenceBackReferences,
	),
	upgrade.NewTenantUpgrade("fix corrupt user-file related table descriptors",
		toCV(clusterversion.V22_2FixUserfileRelatedDescriptorCorruption),
		upgrade.NoPrecondition,
		fixInvalidObjectsThatLookLikeBadUserfileConstraint,
	),
	upgrade.NewTenantUpgrade("add a name column to system.tenants and populate a system tenant entry",
		toCV(clusterversion.V23_1TenantNames),
		upgrade.NoPrecondition,
		addTenantNameColumnAndSystemTenantEntry,
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
