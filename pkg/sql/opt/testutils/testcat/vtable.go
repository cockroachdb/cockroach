// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
var pgCatalogViewsMap = map[string]*tree.CreateView{}
var crdbInternalMap = map[string]*tree.CreateTable{}
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

var pgCatalogViews = []string{
	vtable.PGCatalogDescription,
	vtable.PGCatalogSharedDescription,
}

var crdbInternalTables = []string{
	vtable.CrdbInternalCatalogComments,
	vtable.CrdbInternalBuiltinFunctionComments,
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
	systemschema.SystemMVCCStatisticsSchema,
	systemschema.TxnExecutionStatsTableSchema,
	systemschema.StatementExecutionStatsTableSchema,
	systemschema.TableMetadataTableSchema,
	systemschema.PreparedTransactionsTableSchema,
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

	// Build a map that maps the names of the various virtual tables
	// to their CREATE VIEW AST.
	buildViewMap := func(catalogName, schemaName string, tableList []string, viewMap map[string]*tree.CreateView) {
		for _, table := range tableList {
			parsed, err := parser.ParseOne(table)
			if err != nil {
				panic(errors.Wrap(err, "error initializing virtual table map"))
			}

			cv, ok := parsed.AST.(*tree.CreateView)
			if !ok {
				panic(errors.New("virtual view schemas must be CREATE VIEW statements"))
			}

			cv.Name.SchemaName = tree.Name(schemaName)
			cv.Name.ExplicitSchema = true

			cv.Name.CatalogName = tree.Name(catalogName)
			cv.Name.ExplicitCatalog = true

			name := cv.Name
			viewMap[name.ObjectName.String()] = cv
		}
	}

	buildMap("system", "public", systemTables, systemMap)
	buildMap(testDB, "crdb_internal", crdbInternalTables, crdbInternalMap)
	buildMap(testDB, "information_schema", informationSchemaTables, informationSchemaMap)
	buildMap(testDB, "pg_catalog", pgCatalogTables, pgCatalogMap)
	buildViewMap(testDB, "pg_catalog", pgCatalogViews, pgCatalogViewsMap)
}

// Resolve returns true and the AST node describing the virtual table referenced.
// TODO(justin): make this complete for all virtual tables.
func resolveVTable(name *tree.TableName) (*tree.CreateTable, bool) {
	switch {
	case name.CatalogName == "system" || (name.CatalogName == "" && name.SchemaName == "system"):
		schema, ok := systemMap[name.ObjectName.String()]
		return schema, ok

	case name.SchemaName == "information_schema":
		schema, ok := informationSchemaMap[name.ObjectName.String()]
		return schema, ok

	case name.SchemaName == "pg_catalog":
		schema, ok := pgCatalogMap[name.ObjectName.String()]
		return schema, ok

	case name.SchemaName == "crdb_internal":
		schema, ok := crdbInternalMap[name.ObjectName.String()]
		return schema, ok
	}

	return nil, false
}

// resolveVirtualView returns true and the AST node describing the virtual view
// referenced.
func resolveVirtualView(name *tree.TableName) (*tree.CreateView, bool) {
	switch {
	case name.SchemaName == "pg_catalog":
		schema, ok := pgCatalogViewsMap[name.ObjectName.String()]
		return schema, ok
	}
	return nil, false
}
