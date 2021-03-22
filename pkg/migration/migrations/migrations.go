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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
)

// GetMigration returns the migration corresponding to this version if
// one exists.
func GetMigration(key clusterversion.ClusterVersion) (migration.Migration, bool) {
	m, ok := registry[key]
	return m, ok
}

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]migration.Migration)

var migrations = []migration.Migration{
	migration.NewSQLMigration(
		"add the system.migrations table",
		toCV(clusterversion.LongRunningMigrations),
		migrationsTableMigration,
	),
	migration.NewKVMigration(
		"use unreplicated TruncatedState and RangeAppliedState for all ranges",
		toCV(clusterversion.TruncatedAndRangeAppliedStateMigration),
		truncatedStateMigration,
	),
	migration.NewKVMigration(
		"purge all replicas using the replicated TruncatedState",
		toCV(clusterversion.PostTruncatedAndRangeAppliedStateMigration),
		postTruncatedStateMigration,
	),
	migration.NewSQLMigration(
		"copy all namespace entries to new namespace table",
		toCV(clusterversion.NamespaceTableWithSchemasMigration),
		namespaceMigration,
	),
	migration.NewSQLMigration(
		"upgrade old foreign key representation",
		toCV(clusterversion.ForeignKeyRepresentationMigration),
		foreignKeyRepresentationUpgrade,
	),
	migration.NewSQLMigration(
		"fix system.protected_ts_meta privileges",
		toCV(clusterversion.ProtectedTsMetaPrivilegesMigration),
		protectedTsMetaPrivilegesMigration,
	),
	migration.NewSQLMigration(
		"add the systems.join_tokens table",
		toCV(clusterversion.JoinTokensTable),
		joinTokensTableMigration,
	),
}

func init() {
	for _, m := range migrations {
		registry[m.ClusterVersion()] = m
	}
}

func toCV(key clusterversion.Key) clusterversion.ClusterVersion {
	return clusterversion.ClusterVersion{
		Version: clusterversion.ByKey(key),
	}
}
