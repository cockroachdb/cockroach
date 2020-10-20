// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"

// clusterBackupInclusion is an enum that specifies whether a system table
// should be included in a cluster backup.
type clusterBackupInclusion int

const (
	// invalid indicates that no preference for cluster backup inclusion has been
	// set. This is to ensure that newly added system tables have to explicitly
	// opt-in or out of cluster backups.
	invalid = iota
	optIntoClusterBackup
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
}

// systemTableBackupConfiguration is a map from every systemTable present in the
// cluster to a configuration struct which specifies how it should be treated by
// backup. Every system table should have a specification defined here, enforced
// by TestAllSystemTablesHaveBackupConfig.
var systemTableBackupConfiguration = map[string]systemBackupConfiguration{
	systemschema.UsersTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.ZonesTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.SettingsTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.LocationsTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.RoleMembersTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.RoleOptionsTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.UITable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.CommentsTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.JobsTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.ScheduledJobsTable.Name: {
		includeInClusterBackup: optIntoClusterBackup,
	},
	systemschema.TableStatisticsTable.Name: {
		// Table statistics are backed up in the backup descriptor for now.
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DescriptorTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.EventLogTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.LeaseTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.NamespaceTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DeprecatedNamespaceTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsMetaTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsRecordsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.RangeEventTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationConstraintStatsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationCriticalLocalitiesTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReportsMetaTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationStatsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SqllivenessTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementBundleChunksTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsRequestsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TenantsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.WebSessionsTable.Name: {
		includeInClusterBackup: optOutOfClusterBackup,
	},
}

// getSystemTablesToBackup returns a set of system table names that should be
// included in a cluster backup.
func getSystemTablesToIncludeInClusterBackup() map[string]struct{} {
	systemTablesToInclude := make(map[string]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.includeInClusterBackup == optIntoClusterBackup {
			systemTablesToInclude[systemTableName] = struct{}{}
		}
	}

	return systemTablesToInclude
}
