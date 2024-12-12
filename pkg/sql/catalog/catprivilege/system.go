// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catprivilege

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

var (
	readSystemTables = []catconstants.SystemTableName{
		catconstants.NamespaceTableName,
		catconstants.DescriptorTableName,
		catconstants.DescIDSequenceTableName,
		catconstants.TenantIDSequenceTableName,
		catconstants.TenantsTableName,
		catconstants.ProtectedTimestampsMetaTableName,
		catconstants.ProtectedTimestampsRecordsTableName,
		catconstants.StatementStatisticsTableName,
		catconstants.TransactionStatisticsTableName,
		catconstants.StatementActivityTableName,
		catconstants.TransactionActivityTableName,
		catconstants.PreparedTransactionsTableName,
	}

	readWriteSystemTables = []catconstants.SystemTableName{
		catconstants.UsersTableName,
		catconstants.ZonesTableName,
		catconstants.SettingsTableName,
		catconstants.LeaseTableName,
		catconstants.EventLogTableName,
		catconstants.RangeEventTableName,
		catconstants.UITableName,
		catconstants.JobsTableName,
		catconstants.WebSessionsTableName,
		catconstants.TableStatisticsTableName,
		catconstants.LocationsTableName,
		catconstants.RoleMembersTableName,
		catconstants.CommentsTableName,
		catconstants.ReportsMetaTableName,
		catconstants.ReplicationConstraintStatsTableName,
		catconstants.ReplicationCriticalLocalitiesTableName,
		catconstants.ReplicationStatsTableName,
		catconstants.RoleOptionsTableName,
		catconstants.StatementBundleChunksTableName,
		catconstants.StatementDiagnosticsRequestsTableName,
		catconstants.StatementDiagnosticsTableName,
		catconstants.ScheduledJobsTableName,
		catconstants.SqllivenessTableName,
		catconstants.MigrationsTableName,
		catconstants.JoinTokensTableName,
		catconstants.DatabaseRoleSettingsTableName,
		catconstants.TenantUsageTableName,
		catconstants.SQLInstancesTableName,
		catconstants.SpanConfigurationsTableName,
		catconstants.TaskPayloadsTableName,
		catconstants.TenantSettingsTableName,
		catconstants.TenantTasksTableName,
		catconstants.SpanCountTableName,
		catconstants.SystemPrivilegeTableName,
		catconstants.SystemExternalConnectionsTableName,
		catconstants.SystemJobInfoTableName,
		catconstants.SpanStatsUniqueKeys,
		catconstants.SpanStatsBuckets,
		catconstants.SpanStatsSamples,
		catconstants.SpanStatsTenantBoundaries,
		catconstants.RegionalLiveness,
		catconstants.MVCCStatistics,
		catconstants.TxnExecInsightsTableName,
		catconstants.StmtExecInsightsTableName,
		catconstants.TableMetadata,
		catconstants.JobsProgressTableName,
		catconstants.JobsProgressHistoryTableName,
		catconstants.JobsStatusTableName,
		catconstants.JobsMessageTableName,
	}

	readWriteSystemSequences = []catconstants.SystemTableName{
		catconstants.RoleIDSequenceName,
	}

	systemSuperuserPrivileges = func() map[descpb.NameInfo]privilege.List {
		m := make(map[descpb.NameInfo]privilege.List)
		tableKey := descpb.NameInfo{
			ParentID:       keys.SystemDatabaseID,
			ParentSchemaID: keys.SystemPublicSchemaID,
		}
		for _, rw := range readWriteSystemTables {
			tableKey.Name = string(rw)
			m[tableKey] = privilege.ReadWriteData
		}
		for _, r := range readSystemTables {
			tableKey.Name = string(r)
			m[tableKey] = privilege.ReadData
		}
		for _, r := range readWriteSystemSequences {
			tableKey.Name = string(r)
			m[tableKey] = privilege.ReadWriteSequenceData
		}
		m[descpb.NameInfo{Name: catconstants.SystemDatabaseName}] = privilege.List{privilege.CONNECT}
		return m
	}()
)

// SystemSuperuserPrivileges returns the privilege list for super-users found
// for the given system descriptor name key. Returns nil if none was found.
func SystemSuperuserPrivileges(nameKey catalog.NameKey) privilege.List {
	key := descpb.NameInfo{
		ParentID:       nameKey.GetParentID(),
		ParentSchemaID: nameKey.GetParentSchemaID(),
		Name:           nameKey.GetName(),
	}
	return systemSuperuserPrivileges[key]
}
