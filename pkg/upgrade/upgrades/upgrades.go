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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// GetUpgrade returns the upgrade corresponding to this version if
// one exists.
func GetUpgrade(key clusterversion.ClusterVersion) (upgrade.Upgrade, bool) {
	m, ok := registry[key]
	return m, ok
}

// NoPrecondition is a PreconditionFunc that doesn't check anything.
func NoPrecondition(context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps) error {
	return nil
}

// NoTenantUpgradeFunc is a TenantUpgradeFunc that doesn't do anything.
func NoTenantUpgradeFunc(
	context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps, *jobs.Job,
) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated upgrade. The upgrade is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]upgrade.Upgrade)

var upgrades = []upgrade.Upgrade{
	upgrade.NewTenantUpgrade(
		"ensure preconditions are met before starting upgrading to v22.2",
		toCV(clusterversion.Start22_2),
		preconditionBeforeStartingAnUpgrade,
		NoTenantUpgradeFunc,
	),
	upgrade.NewTenantUpgrade(
		"remove grant privilege from users",
		toCV(clusterversion.RemoveGrantPrivilege),
		NoPrecondition,
		removeGrantMigration,
	),
	upgrade.NewTenantUpgrade(
		"upgrade sequences to be referenced by ID",
		toCV(clusterversion.UpgradeSequenceToBeReferencedByID),
		NoPrecondition,
		upgradeSequenceToBeReferencedByID,
	),
	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support sampling probabilities",
		toCV(clusterversion.SampledStmtDiagReqs),
		NoPrecondition,
		sampledStmtDiagReqsMigration,
	),
	upgrade.NewTenantUpgrade(
		"add the system.privileges table",
		toCV(clusterversion.SystemPrivilegesTable),
		NoPrecondition,
		systemPrivilegesTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column locality to table system.sql_instances",
		toCV(clusterversion.AlterSystemSQLInstancesAddLocality),
		NoPrecondition,
		alterSystemSQLInstancesAddLocality,
	),
	upgrade.NewTenantUpgrade(
		"add the system.external_connections table",
		toCV(clusterversion.SystemExternalConnectionsTable),
		NoPrecondition,
		systemExternalConnectionsTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column index_recommendations to table system.statement_statistics",
		toCV(clusterversion.AlterSystemStatementStatisticsAddIndexRecommendations),
		NoPrecondition,
		alterSystemStatementStatisticsAddIndexRecommendations,
	),
	upgrade.NewTenantUpgrade("add system.role_id_sequence",
		toCV(clusterversion.RoleIDSequence),
		NoPrecondition,
		roleIDSequenceMigration,
	),
	// Add user_id column, the column will not be backfilled.
	// However, new users created from this point forward will be created
	// with an ID. We cannot start using the IDs in this version
	// as old users are not backfilled. The key here is that we have a cut
	// off point where we know which users need to be backfilled and no
	// more users can be created without ids.
	upgrade.NewTenantUpgrade("alter system.users to include user_id column",
		toCV(clusterversion.AddSystemUserIDColumn),
		NoPrecondition,
		alterSystemUsersAddUserIDColumnWithIndex,
	),
	upgrade.NewTenantUpgrade("backfill users with ids and add an index on the id column",
		toCV(clusterversion.SystemUsersIDColumnIsBackfilled),
		NoPrecondition,
		backfillSystemUsersIDColumn,
	),
	upgrade.NewTenantUpgrade("set user_id column to not null",
		toCV(clusterversion.SetSystemUsersUserIDColumnNotNull),
		NoPrecondition,
		setUserIDNotNull,
	),
	upgrade.NewTenantUpgrade(
		"add default SQL schema telemetry schedule",
		toCV(clusterversion.SQLSchemaTelemetryScheduledJobs),
		NoPrecondition,
		ensureSQLSchemaTelemetrySchedule,
	),
	upgrade.NewTenantUpgrade("alter system.role_options to include user_id column",
		toCV(clusterversion.RoleOptionsTableHasIDColumn),
		NoPrecondition,
		alterSystemRoleOptionsAddUserIDColumnWithIndex,
	),
	upgrade.NewTenantUpgrade("backfill entries in system.role_options to include IDs",
		toCV(clusterversion.RoleOptionsIDColumnIsBackfilled),
		NoPrecondition,
		backfillSystemRoleOptionsIDColumn,
	),
	upgrade.NewTenantUpgrade("set system.role_options user_id column to not null",
		toCV(clusterversion.SetRoleOptionsUserIDColumnNotNull),
		NoPrecondition,
		setSystemRoleOptionsUserIDColumnNotNull,
	),
	upgrade.NewTenantUpgrade("ensure all GC jobs send DeleteRange requests",
		toCV(clusterversion.WaitedForDelRangeInGCJob),
		checkForPausedGCJobs,
		waitForDelRangeInGCJob,
	),
	upgrade.NewTenantUpgrade(
		"wait for all in-flight schema changes",
		toCV(clusterversion.NoNonMVCCAddSSTable),
		NoPrecondition,
		waitForAllSchemaChanges,
	),
	upgrade.NewTenantUpgrade("update invalid column IDs in sequence back references",
		toCV(clusterversion.UpdateInvalidColumnIDsInSequenceBackReferences),
		NoPrecondition,
		updateInvalidColumnIDsInSequenceBackReferences,
	),
}

func init() {
	for _, m := range upgrades {
		if _, exists := registry[m.ClusterVersion()]; exists {
			panic(errors.AssertionFailedf("duplicate upgrade registration for %v", m.ClusterVersion()))
		}
		registry[m.ClusterVersion()] = m
	}
}

func toCV(key clusterversion.Key) clusterversion.ClusterVersion {
	return clusterversion.ClusterVersion{
		Version: clusterversion.ByKey(key),
	}
}
