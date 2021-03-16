// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// clusterBackupInclusion is an enum that specifies whether a system table
// should be included in a cluster backup.
type clusterBackupInclusion int

const (
	// invalidBackupInclusion indicates that no preference for cluster backup inclusion has been
	// set. This is to ensure that newly added system tables have to explicitly
	// opt-in or out of cluster backups.
	invalidBackupInclusion = iota
	// optInToClusterBackup indicates that the system table
	// should be included in the cluster backup.
	optInToClusterBackup
	// optOutOfClusterBackup indicates that the system table
	// should not be included in the cluster backup.
	optOutOfClusterBackup
)

// systemBackupConfiguration holds any configuration related to backing up
// system tables. System tables differ from normal tables with respect to backup
// for 2 reasons:
// 1) For some tables, their contents are read during the restore, so it is non-
//    trivial to restore this data without affecting the restore job itself.
//
// 2) It may reference system data which could be rewritten. This is particularly
//    problematic for data that references tables. At time of writing, cluster
//    restore creates descriptors with the same ID as they had in the backing up
//    cluster so there is no need to rewrite system table data.
type systemBackupConfiguration struct {
	shouldIncludeInClusterBackup clusterBackupInclusion
	// restoreBeforeData indicates that this system table should be fully restored
	// before restoring the user data. If a system table is restored before the
	// user data, the restore will see this system table during the restore.
	// The default is `false` because most of the system tables should be set up
	// to support the restore (e.g. users that can run the restore, cluster settings
	// that control how the restore runs, etc...).
	restoreBeforeData bool
	// customRestoreFunc is responsible for restoring the data from a table that
	// holds the restore system table data into the given system table. If none
	// is provided then `defaultRestoreFunc` is used.
	customRestoreFunc func(ctx context.Context, execCtx *sql.ExecutorConfig, txn *kv.Txn, systemTableName, tempTableName string) error
}

// defaultSystemTableRestoreFunc is how system table data is restored. This can
// be overwritten with the system table's
// systemBackupConfiguration.customRestoreFunc.
func defaultSystemTableRestoreFunc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	systemTableName, tempTableName string,
) error {
	executor := execCfg.InternalExecutor

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	opName := systemTableName + "-data-deletion"
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := executor.Exec(ctx, opName, txn, deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s);",
		systemTableName, tempTableName)
	opName = systemTableName + "-data-insert"
	if _, err := executor.Exec(ctx, opName, txn, restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

// Custom restore functions for different system tables.

// When restoring the jobs table we don't want to remove existing jobs, since
// that includes the restore that we're running.
func jobsRestoreFunc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	systemTableName, tempTableName string,
) error {
	executor := execCfg.InternalExecutor

	// When restoring jobs, don't clear the existing table.

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s);",
		systemTableName, tempTableName)
	opName := systemTableName + "-data-insert"
	if _, err := executor.Exec(ctx, opName, txn, restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

// When restoring the settings table, we want to make sure to not override the
// version.
func settingsRestoreFunc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	systemTableName, tempTableName string,
) error {
	executor := execCfg.InternalExecutor

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE name <> 'version'", systemTableName)
	opName := systemTableName + "-data-deletion"
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := executor.Exec(ctx, opName, txn, deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s WHERE name <> 'version');",
		systemTableName, tempTableName)
	opName = systemTableName + "-data-insert"
	if _, err := executor.Exec(ctx, opName, txn, restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

// systemTableBackupConfiguration is a map from every systemTable present in the
// cluster to a configuration struct which specifies how it should be treated by
// backup. Every system table should have a specification defined here, enforced
// by TestAllSystemTablesHaveBackupConfig.
var systemTableBackupConfiguration = map[string]systemBackupConfiguration{
	systemschema.UsersTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.ZonesTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
		// The zones table should be restored before the user data so that the range
		// allocator properly distributes ranges during the restore.
		restoreBeforeData: true,
	},
	systemschema.SettingsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
		customRestoreFunc:            settingsRestoreFunc,
	},
	systemschema.LocationsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.RoleMembersTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.RoleOptionsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.UITable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.CommentsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.JobsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
		customRestoreFunc:            jobsRestoreFunc,
	},
	systemschema.ScheduledJobsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
	},
	systemschema.TableStatisticsTable.GetName(): {
		// Table statistics are backed up in the backup descriptor for now.
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DescriptorTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.EventLogTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.LeaseTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.NamespaceTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DeprecatedNamespaceTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsMetaTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsRecordsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.RangeEventTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationConstraintStatsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationCriticalLocalitiesTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReportsMetaTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationStatsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SqllivenessTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementBundleChunksTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsRequestsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TenantsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.WebSessionsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.MigrationsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
}

// GetSystemTablesToIncludeInClusterBackup returns a set of system table names that
// should be included in a cluster backup.
func GetSystemTablesToIncludeInClusterBackup() map[string]struct{} {
	systemTablesToInclude := make(map[string]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.shouldIncludeInClusterBackup == optInToClusterBackup {
			systemTablesToInclude[systemTableName] = struct{}{}
		}
	}

	return systemTablesToInclude
}

// getSystemTablesToRestoreBeforeData returns the set of system tables that
// should be restored before the user data.
func getSystemTablesToRestoreBeforeData() map[string]struct{} {
	systemTablesToRestoreBeforeData := make(map[string]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.shouldIncludeInClusterBackup == optInToClusterBackup && backupConfig.restoreBeforeData {
			systemTablesToRestoreBeforeData[systemTableName] = struct{}{}
		}
	}

	return systemTablesToRestoreBeforeData
}
