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
	// invalid indicates that no preference for cluster backup inclusion has been
	// set. This is to ensure that newly added system tables have to explicitly
	// opt-in or out of cluster backups.
	invalid = iota
	optInToClusterBackup
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
	includeInClusterBackup clusterBackupInclusion
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
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.ZonesTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.SettingsTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
		customRestoreFunc:      settingsRestoreFunc,
	},
	systemschema.LocationsTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.RoleMembersTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.RoleOptionsTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.UITable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.CommentsTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.JobsTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
		customRestoreFunc:      jobsRestoreFunc,
	},
	systemschema.ScheduledJobsTable.GetName(): {
		includeInClusterBackup: optInToClusterBackup,
	},
	systemschema.TableStatisticsTable.GetName(): {
		// Table statistics are backed up in the backup descriptor for now.
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DescriptorTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.EventLogTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.LeaseTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.NamespaceTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DeprecatedNamespaceTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsMetaTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsRecordsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.RangeEventTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationConstraintStatsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationCriticalLocalitiesTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReportsMetaTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationStatsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SqllivenessTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementBundleChunksTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsRequestsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TenantsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.WebSessionsTable.GetName(): {
		includeInClusterBackup: optOutOfClusterBackup,
	},
}

// getSystemTablesToBackup returns a set of system table names that should be
// included in a cluster backup.
func getSystemTablesToIncludeInClusterBackup() map[string]struct{} {
	systemTablesToInclude := make(map[string]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.includeInClusterBackup == optInToClusterBackup {
			systemTablesToInclude[systemTableName] = struct{}{}
		}
	}

	return systemTablesToInclude
}
