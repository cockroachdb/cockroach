// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vtable"
	"github.com/cockroachdb/errors"
)

var informationSchemaMap = map[string]*tree.CreateTable{}
var pgCatalogMap = map[string]*tree.CreateTable{}
var systemMap = map[string]*tree.CreateTable{}

var informationSchemaTables = []string{
	vtable.InformationSchemaColumns,
	vtable.InformationSchemaAdministrableRoleAuthorizations,
	vtable.InformationSchemaApplicableRoles,
	vtable.InformationSchemaColumnPrivileges,
	vtable.InformationSchemaSchemata,
	vtable.InformationSchemaTables,
}

var pgCatalogTables = []string{
	vtable.PGCatalogAm,
	vtable.PGCatalogAttrDef,
	vtable.PGCatalogAttribute,
	vtable.PGCatalogCast,
	vtable.PGCatalogAuthID,
	vtable.PGCatalogAuthMembers,
	vtable.PGCatalogAvailableExtensions,
	vtable.PGCatalogClass,
	vtable.PGCatalogCollation,
	vtable.PGCatalogConstraint,
	vtable.PGCatalogConversion,
	vtable.PGCatalogDatabase,
	vtable.PGCatalogDefaultACL,
	vtable.PGCatalogDepend,
	vtable.PGCatalogDescription,
	vtable.PGCatalogSharedDescription,
	vtable.PGCatalogEnum,
	vtable.PGCatalogEventTrigger,
	vtable.PGCatalogExtension,
	vtable.PGCatalogForeignDataWrapper,
	vtable.PGCatalogForeignServer,
	vtable.PGCatalogForeignTable,
	vtable.PGCatalogIndex,
	vtable.PGCatalogIndexes,
	vtable.PGCatalogInherits,
	vtable.PGCatalogLanguage,
	vtable.PGCatalogLocks,
	vtable.PGCatalogMatViews,
	vtable.PGCatalogNamespace,
	vtable.PGCatalogOperator,
	vtable.PGCatalogPreparedXacts,
	vtable.PGCatalogPreparedStatements,
	vtable.PGCatalogProc,
	vtable.PGCatalogRange,
	vtable.PGCatalogRewrite,
	vtable.PGCatalogRoles,
	vtable.PGCatalogSecLabels,
	vtable.PGCatalogSequence,
	vtable.PGCatalogSettings,
	vtable.PGCatalogShdepend,
	vtable.PGCatalogTables,
	vtable.PGCatalogTablespace,
	vtable.PGCatalogTrigger,
	vtable.PGCatalogType,
	vtable.PGCatalogUser,
	vtable.PGCatalogUserMapping,
	vtable.PGCatalogStatActivity,
	vtable.PGCatalogSecurityLabel,
	vtable.PGCatalogSharedSecurityLabel,
	vtable.PGCatalogViews,
	vtable.PGCatalogAggregate,
}

var systemTables = []string{
	systemschema.NamespaceTableSchema,
	systemschema.DescriptorTableSchema,
	systemschema.UsersTableSchema,
	systemschema.RoleOptionsTableSchema,
	systemschema.ZonesTableSchema,
	systemschema.SettingsTableSchema,
	systemschema.TenantsTableSchema,
	systemschema.LeaseTableSchema,
	systemschema.EventLogTableSchema,
	systemschema.RangeEventTableSchema,
	systemschema.UITableSchema,
	systemschema.JobsTableSchema,
	systemschema.WebSessionsTableSchema,
	systemschema.TableStatisticsTableSchema,
	systemschema.LocationsTableSchema,
	systemschema.RoleMembersTableSchema,
	systemschema.CommentsTableSchema,
	systemschema.ReportsMetaTableSchema,
	systemschema.ReplicationConstraintStatsTableSchema,
	systemschema.ReplicationCriticalLocalitiesTableSchema,
	systemschema.ReplicationStatsTableSchema,
	systemschema.ProtectedTimestampsMetaTableSchema,
	systemschema.ProtectedTimestampsRecordsTableSchema,
	systemschema.StatementBundleChunksTableSchema,
	systemschema.StatementDiagnosticsRequestsTableSchema,
	systemschema.StatementDiagnosticsTableSchema,
	systemschema.ScheduledJobsTableSchema,
	systemschema.SqllivenessTableSchema,
	systemschema.MigrationsTableSchema,
	systemschema.JoinTokensTableSchema,
	systemschema.StatementStatisticsTableSchema,
	systemschema.TransactionStatisticsTableSchema,
	systemschema.DatabaseRoleSettingsTableSchema,
	systemschema.TenantUsageTableSchema,
	systemschema.SQLInstancesTableSchema,
	systemschema.SpanConfigurationsTableSchema,
	systemschema.TenantSettingsTableSchema,
	systemschema.SpanCountTableSchema,
	systemschema.SystemPrivilegeTableSchema,
}

func init() {
	// Build a map that maps the names of the various virtual tables
	// to their CREATE TABLE AST.
	buildMap := func(catalogName, schemaName string, tableList []string, tableMap map[string]*tree.CreateTable) {
		for _, table := range tableList {
			parsed, err := parser.ParseOne(table)
			if err != nil {
				panic(errors.Wrap(err, "error initializing virtual table map"))
			}

			ct, ok := parsed.AST.(*tree.CreateTable)
			if !ok {
				panic(errors.New("virtual table schemas must be CREATE TABLE statements"))
			}

			ct.Table.SchemaName = tree.Name(schemaName)
			ct.Table.ExplicitSchema = true

			ct.Table.CatalogName = tree.Name(catalogName)
			ct.Table.ExplicitCatalog = true

			name := ct.Table
			tableMap[name.ObjectName.String()] = ct
		}
	}

	buildMap(testDB, "information_schema", informationSchemaTables, informationSchemaMap)
	buildMap(testDB, "pg_catalog", pgCatalogTables, pgCatalogMap)
	buildMap("system", "public", systemTables, systemMap)
}

// Resolve returns true and the AST node describing the virtual table referenced.
// TODO(justin): make this complete for all virtual tables.
func resolveVTable(name *tree.TableName) (*tree.CreateTable, bool) {
	switch name.SchemaName {
	case "information_schema":
		schema, ok := informationSchemaMap[name.ObjectName.String()]
		return schema, ok

	case "pg_catalog":
		schema, ok := pgCatalogMap[name.ObjectName.String()]
		return schema, ok

	case "system":
		schema, ok := systemMap[name.ObjectName.String()]
		return schema, ok
	}

	return nil, false
}
