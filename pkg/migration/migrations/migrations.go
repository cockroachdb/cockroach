// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrations contains the implementation of migrations. It is imported
// by the server library.
//
// This package registers the migrations with the migration package.
package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/errors"
)

// GetMigration returns the migration corresponding to this version if
// one exists.
func GetMigration(key clusterversion.ClusterVersion) (migration.Migration, bool) {
	m, ok := registry[key]
	return m, ok
}

// NoPrecondition is a PreconditionFunc that doesn't check anything.
func NoPrecondition(context.Context, clusterversion.ClusterVersion, migration.TenantDeps) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]migration.Migration)

var migrations = []migration.Migration{
	migration.NewTenantMigration(
		"ensure that draining names are no longer in use",
		toCV(clusterversion.DrainingNamesMigration),
		NoPrecondition,
		ensureNoDrainingNames,
	),
	migration.NewTenantMigration(
		"add column avgSize to table system.table_statistics",
		toCV(clusterversion.AlterSystemTableStatisticsAddAvgSizeCol),
		NoPrecondition,
		alterSystemTableStatisticsAddAvgSize,
	),
	migration.NewTenantMigration(
		"update system.statement_diagnostics_requests table to support conditional stmt diagnostics",
		toCV(clusterversion.AlterSystemStmtDiagReqs),
		NoPrecondition,
		alterSystemStmtDiagReqs,
	),
	migration.NewTenantMigration(
		"seed system.span_configurations with configs for existing tenants",
		toCV(clusterversion.SeedTenantSpanConfigs),
		NoPrecondition,
		seedTenantSpanConfigsMigration,
	),
	migration.NewTenantMigration("insert missing system.namespace entries for public schemas",
		toCV(clusterversion.InsertPublicSchemaNamespaceEntryOnRestore),
		NoPrecondition,
		insertMissingPublicSchemaNamespaceEntry,
	),
	migration.NewTenantMigration(
		"add column target to system.protected_ts_records",
		toCV(clusterversion.AlterSystemProtectedTimestampAddColumn),
		NoPrecondition,
		alterTableProtectedTimestampRecords,
	),
	migration.NewTenantMigration("update synthetic public schemas to be backed by a descriptor",
		toCV(clusterversion.PublicSchemasWithDescriptors),
		NoPrecondition,
		publicSchemaMigration,
	),
	migration.NewTenantMigration(
		"enable span configs infrastructure",
		toCV(clusterversion.EnsureSpanConfigReconciliation),
		NoPrecondition,
		ensureSpanConfigReconciliation,
	),
	migration.NewSystemMigration(
		"enable span configs infrastructure",
		toCV(clusterversion.EnsureSpanConfigSubscription),
		ensureSpanConfigSubscription,
	),
	migration.NewTenantMigration(
		"track grant options on users and enable granting/revoking with them",
		toCV(clusterversion.ValidateGrantOption),
		NoPrecondition,
		grantOptionMigration,
	),
	migration.NewTenantMigration(
		"delete comments that belong to dropped indexes",
		toCV(clusterversion.DeleteCommentsWithDroppedIndexes),
		NoPrecondition,
		ensureCommentsHaveNonDroppedIndexes,
	),
	migration.NewTenantMigration(
		"convert incompatible database privileges to default privileges",
		toCV(clusterversion.RemoveIncompatibleDatabasePrivileges),
		NoPrecondition,
		runRemoveInvalidDatabasePrivileges,
	),
	migration.NewSystemMigration(
		"populate RangeAppliedState.RaftAppliedIndexTerm for all ranges",
		toCV(clusterversion.AddRaftAppliedIndexTermMigration),
		raftAppliedIndexTermMigration,
	),
	migration.NewSystemMigration(
		"purge all replicas not populating RangeAppliedState.RaftAppliedIndexTerm",
		toCV(clusterversion.PostAddRaftAppliedIndexTermMigration),
		postRaftAppliedIndexTermMigration,
	),
	migration.NewTenantMigration(
		"add the system.tenant_settings table",
		toCV(clusterversion.TenantSettingsTable),
		NoPrecondition,
		tenantSettingsTableMigration,
	),
}

func init() {
	for _, m := range migrations {
		if _, exists := registry[m.ClusterVersion()]; exists {
			panic(errors.AssertionFailedf("duplicate migration registration for %v", m.ClusterVersion()))
		}
		registry[m.ClusterVersion()] = m
	}
}

func toCV(key clusterversion.Key) clusterversion.ClusterVersion {
	return clusterversion.ClusterVersion{
		Version: clusterversion.ByKey(key),
	}
}
