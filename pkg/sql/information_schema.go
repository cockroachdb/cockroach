// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vtable"
	"github.com/cockroachdb/cockroach/pkg/util/collatedstring"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

const (
	pgCatalogName = catconstants.PgCatalogName
)

var pgCatalogNameDString = tree.NewDString(pgCatalogName)

// informationSchema lists all the table definitions for
// information_schema.
var informationSchema = virtualSchema{
	name: catconstants.InformationSchemaName,
	undefinedTables: buildStringSet(
		// Generated with:
		// select distinct '"'||table_name||'",' from information_schema.tables
		//    where table_schema='information_schema' order by table_name;
		"_pg_foreign_data_wrappers",
		"_pg_foreign_servers",
		"_pg_foreign_table_columns",
		"_pg_foreign_tables",
		"_pg_user_mappings",
		"sql_languages",
		"sql_packages",
		"sql_sizing_profiles",
	),
	tableDefs: map[descpb.ID]virtualSchemaDef{
		catconstants.InformationSchemaAdministrableRoleAuthorizationsID:   informationSchemaAdministrableRoleAuthorizations,
		catconstants.InformationSchemaApplicableRolesID:                   informationSchemaApplicableRoles,
		catconstants.InformationSchemaAttributesTableID:                   informationSchemaAttributesTable,
		catconstants.InformationSchemaCharacterSets:                       informationSchemaCharacterSets,
		catconstants.InformationSchemaCheckConstraintRoutineUsageTableID:  informationSchemaCheckConstraintRoutineUsageTable,
		catconstants.InformationSchemaCheckConstraints:                    informationSchemaCheckConstraints,
		catconstants.InformationSchemaCollationCharacterSetApplicability:  informationSchemaCollationCharacterSetApplicability,
		catconstants.InformationSchemaCollations:                          informationSchemaCollations,
		catconstants.InformationSchemaColumnColumnUsageTableID:            informationSchemaColumnColumnUsageTable,
		catconstants.InformationSchemaColumnDomainUsageTableID:            informationSchemaColumnDomainUsageTable,
		catconstants.InformationSchemaColumnOptionsTableID:                informationSchemaColumnOptionsTable,
		catconstants.InformationSchemaColumnPrivilegesID:                  informationSchemaColumnPrivileges,
		catconstants.InformationSchemaColumnStatisticsTableID:             informationSchemaColumnStatisticsTable,
		catconstants.InformationSchemaColumnUDTUsageID:                    informationSchemaColumnUDTUsage,
		catconstants.InformationSchemaColumnsExtensionsTableID:            informationSchemaColumnsExtensionsTable,
		catconstants.InformationSchemaColumnsTableID:                      informationSchemaColumnsTable,
		catconstants.InformationSchemaConstraintColumnUsageTableID:        informationSchemaConstraintColumnUsageTable,
		catconstants.InformationSchemaConstraintTableUsageTableID:         informationSchemaConstraintTableUsageTable,
		catconstants.InformationSchemaDataTypePrivilegesTableID:           informationSchemaDataTypePrivilegesTable,
		catconstants.InformationSchemaDomainConstraintsTableID:            informationSchemaDomainConstraintsTable,
		catconstants.InformationSchemaDomainUdtUsageTableID:               informationSchemaDomainUdtUsageTable,
		catconstants.InformationSchemaDomainsTableID:                      informationSchemaDomainsTable,
		catconstants.InformationSchemaElementTypesTableID:                 informationSchemaElementTypesTable,
		catconstants.InformationSchemaEnabledRolesID:                      informationSchemaEnabledRoles,
		catconstants.InformationSchemaEnginesTableID:                      informationSchemaEnginesTable,
		catconstants.InformationSchemaEventsTableID:                       informationSchemaEventsTable,
		catconstants.InformationSchemaFilesTableID:                        informationSchemaFilesTable,
		catconstants.InformationSchemaForeignDataWrapperOptionsTableID:    informationSchemaForeignDataWrapperOptionsTable,
		catconstants.InformationSchemaForeignDataWrappersTableID:          informationSchemaForeignDataWrappersTable,
		catconstants.InformationSchemaForeignServerOptionsTableID:         informationSchemaForeignServerOptionsTable,
		catconstants.InformationSchemaForeignServersTableID:               informationSchemaForeignServersTable,
		catconstants.InformationSchemaForeignTableOptionsTableID:          informationSchemaForeignTableOptionsTable,
		catconstants.InformationSchemaForeignTablesTableID:                informationSchemaForeignTablesTable,
		catconstants.InformationSchemaInformationSchemaCatalogNameTableID: informationSchemaInformationSchemaCatalogNameTable,
		catconstants.InformationSchemaKeyColumnUsageTableID:               informationSchemaKeyColumnUsageTable,
		catconstants.InformationSchemaKeywordsTableID:                     informationSchemaKeywordsTable,
		catconstants.InformationSchemaOptimizerTraceTableID:               informationSchemaOptimizerTraceTable,
		catconstants.InformationSchemaParametersTableID:                   informationSchemaParametersTable,
		catconstants.InformationSchemaPartitionsTableID:                   informationSchemaPartitionsTable,
		catconstants.InformationSchemaPluginsTableID:                      informationSchemaPluginsTable,
		catconstants.InformationSchemaProcesslistTableID:                  informationSchemaProcesslistTable,
		catconstants.InformationSchemaProfilingTableID:                    informationSchemaProfilingTable,
		catconstants.InformationSchemaReferentialConstraintsTableID:       informationSchemaReferentialConstraintsTable,
		catconstants.InformationSchemaResourceGroupsTableID:               informationSchemaResourceGroupsTable,
		catconstants.InformationSchemaRoleColumnGrantsTableID:             informationSchemaRoleColumnGrantsTable,
		catconstants.InformationSchemaRoleRoutineGrantsTableID:            informationSchemaRoleRoutineGrantsTable,
		catconstants.InformationSchemaRoleTableGrantsID:                   informationSchemaRoleTableGrants,
		catconstants.InformationSchemaRoleUdtGrantsTableID:                informationSchemaRoleUdtGrantsTable,
		catconstants.InformationSchemaRoleUsageGrantsTableID:              informationSchemaRoleUsageGrantsTable,
		catconstants.InformationSchemaRoutinePrivilegesTableID:            informationSchemaRoutinePrivilegesTable,
		catconstants.InformationSchemaRoutineTableID:                      informationSchemaRoutineTable,
		catconstants.InformationSchemaSQLFeaturesTableID:                  informationSchemaSQLFeaturesTable,
		catconstants.InformationSchemaSQLImplementationInfoTableID:        informationSchemaSQLImplementationInfoTable,
		catconstants.InformationSchemaSQLPartsTableID:                     informationSchemaSQLPartsTable,
		catconstants.InformationSchemaSQLSizingTableID:                    informationSchemaSQLSizingTable,
		catconstants.InformationSchemaSchemataExtensionsTableID:           informationSchemaSchemataExtensionsTable,
		catconstants.InformationSchemaSchemataTableID:                     informationSchemaSchemataTable,
		catconstants.InformationSchemaSchemataTablePrivilegesID:           informationSchemaSchemataTablePrivileges,
		catconstants.InformationSchemaSequencesID:                         informationSchemaSequences,
		catconstants.InformationSchemaSessionVariables:                    informationSchemaSessionVariables,
		catconstants.InformationSchemaStGeometryColumnsTableID:            informationSchemaStGeometryColumnsTable,
		catconstants.InformationSchemaStSpatialReferenceSystemsTableID:    informationSchemaStSpatialReferenceSystemsTable,
		catconstants.InformationSchemaStUnitsOfMeasureTableID:             informationSchemaStUnitsOfMeasureTable,
		catconstants.InformationSchemaStatisticsTableID:                   informationSchemaStatisticsTable,
		catconstants.InformationSchemaTableConstraintTableID:              informationSchemaTableConstraintTable,
		catconstants.InformationSchemaTableConstraintsExtensionsTableID:   informationSchemaTableConstraintsExtensionsTable,
		catconstants.InformationSchemaTablePrivilegesID:                   informationSchemaTablePrivileges,
		catconstants.InformationSchemaTablesExtensionsTableID:             informationSchemaTablesExtensionsTable,
		catconstants.InformationSchemaTablesTableID:                       informationSchemaTablesTable,
		catconstants.InformationSchemaTablespacesExtensionsTableID:        informationSchemaTablespacesExtensionsTable,
		catconstants.InformationSchemaTablespacesTableID:                  informationSchemaTablespacesTable,
		catconstants.InformationSchemaTransformsTableID:                   informationSchemaTransformsTable,
		catconstants.InformationSchemaTriggeredUpdateColumnsTableID:       informationSchemaTriggeredUpdateColumnsTable,
		catconstants.InformationSchemaTriggersTableID:                     informationSchemaTriggersTable,
		catconstants.InformationSchemaTypePrivilegesID:                    informationSchemaTypePrivilegesTable,
		catconstants.InformationSchemaUdtPrivilegesTableID:                informationSchemaUdtPrivilegesTable,
		catconstants.InformationSchemaUsagePrivilegesTableID:              informationSchemaUsagePrivilegesTable,
		catconstants.InformationSchemaUserAttributesTableID:               informationSchemaUserAttributesTable,
		catconstants.InformationSchemaUserDefinedTypesTableID:             informationSchemaUserDefinedTypesTable,
		catconstants.InformationSchemaUserMappingOptionsTableID:           informationSchemaUserMappingOptionsTable,
		catconstants.InformationSchemaUserMappingsTableID:                 informationSchemaUserMappingsTable,
		catconstants.InformationSchemaUserPrivilegesID:                    informationSchemaUserPrivileges,
		catconstants.InformationSchemaViewColumnUsageTableID:              informationSchemaViewColumnUsageTable,
		catconstants.InformationSchemaViewRoutineUsageTableID:             informationSchemaViewRoutineUsageTable,
		catconstants.InformationSchemaViewTableUsageTableID:               informationSchemaViewTableUsageTable,
		catconstants.InformationSchemaViewsTableID:                        informationSchemaViewsTable,
	},
	tableValidator:             validateInformationSchemaTable,
	validWithNoDatabaseContext: true,
}

func buildStringSet(ss ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

var (
	emptyString = tree.NewDString("")
	// information_schema was defined before the BOOLEAN data type was added to
	// the SQL specification. Because of this, boolean values are represented as
	// STRINGs. The BOOLEAN data type should NEVER be used in information_schema
	// tables. Instead, define columns as STRINGs and map bools to STRINGs using
	// yesOrNoDatum.
	yesString    = tree.NewDString("YES")
	noString     = tree.NewDString("NO")
	alwaysString = tree.NewDString("ALWAYS")
	neverString  = tree.NewDString("NEVER")
)

func yesOrNoDatum(b bool) tree.Datum {
	if b {
		return yesString
	}
	return noString
}

func alwaysOrNeverDatum(b bool) tree.Datum {
	if b {
		return alwaysString
	}
	return neverString
}

func dNameOrNull(s string) tree.Datum {
	if s == "" {
		return tree.DNull
	}
	return tree.NewDName(s)
}

func dIntFnOrNull(fn func() (int32, bool)) tree.Datum {
	if n, ok := fn(); ok {
		return tree.NewDInt(tree.DInt(n))
	}
	return tree.DNull
}

func validateInformationSchemaTable(table *descpb.TableDescriptor) error {
	// Make sure no tables have boolean columns.
	for i := range table.Columns {
		if table.Columns[i].Type.Family() == types.BoolFamily {
			return errors.Errorf("information_schema tables should never use BOOL columns. "+
				"See the comment about yesOrNoDatum. Found BOOL column in %s.", table.Name)
		}
	}
	return nil
}

var informationSchemaAdministrableRoleAuthorizations = virtualSchemaTable{
	comment: `roles for which the current user has admin option
` + docs.URL("information-schema.html#administrable_role_authorizations") + `
https://www.postgresql.org/docs/9.5/infoschema-administrable-role-authorizations.html`,
	schema: vtable.InformationSchemaAdministrableRoleAuthorizations,
	populate: func(
		ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		return populateRoleHierarchy(ctx, p, addRow, true /* onlyIsAdmin */)
	},
}

var informationSchemaApplicableRoles = virtualSchemaTable{
	comment: `roles available to the current user
` + docs.URL("information-schema.html#applicable_roles") + `
https://www.postgresql.org/docs/9.5/infoschema-applicable-roles.html`,
	schema: vtable.InformationSchemaApplicableRoles,
	populate: func(
		ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		return populateRoleHierarchy(ctx, p, addRow, false /* onlyIsAdmin */)
	},
}

func populateRoleHierarchy(
	ctx context.Context, p *planner, addRow func(...tree.Datum) error, onlyIsAdmin bool,
) error {
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return err
	}
	return forEachRoleMembershipAtCacheReadTS(ctx, p, func(
		ctx context.Context, role, member username.SQLUsername, isAdmin bool,
	) error {
		// The ADMIN OPTION is inherited through the role hierarchy, and grantee
		// is supposed to be the role that has the ADMIN OPTION. The current user
		// inherits all the ADMIN OPTIONs of its ancestors.
		isRole := member == p.User()
		_, hasRole := allRoles[member]
		if (hasRole || isRole) && (!onlyIsAdmin || isAdmin) {
			if err := addRow(
				tree.NewDString(member.Normalized()), // grantee
				tree.NewDString(role.Normalized()),   // role_name
				yesOrNoDatum(isAdmin),                // is_grantable
			); err != nil {
				return err
			}
		}
		return nil
	},
	)
}

var informationSchemaAttributesTable = virtualSchemaView{
	comment: `attributes of composite data types in the current database` +
		docs.URL("information-schema.html#attributes") + `
    https://www.postgresql.org/docs/16/infoschema-attributes.html`,
	schema: vtable.InformationSchemaAttributes,
	resultColumns: colinfo.ResultColumns{
		{Name: "udt_catalog", Typ: types.String},
		{Name: "udt_schema", Typ: types.String},
		{Name: "udt_name", Typ: types.String},
		{Name: "attribute_name", Typ: types.String},
		{Name: "ordinal_position", Typ: types.Int},
		{Name: "attribute_default", Typ: types.String},
		{Name: "is_nullable", Typ: types.String},
		{Name: "data_type", Typ: types.String},
		{Name: "character_maximum_length", Typ: types.Int},
		{Name: "character_octet_length", Typ: types.Int},
		{Name: "character_set_catalog", Typ: types.String},
		{Name: "character_set_schema", Typ: types.String},
		{Name: "character_set_name", Typ: types.String},
		{Name: "collation_catalog", Typ: types.String},
		{Name: "collation_schema", Typ: types.String},
		{Name: "collation_name", Typ: types.String},
		{Name: "numeric_precision", Typ: types.Int},
		{Name: "numeric_precision_radix", Typ: types.Int},
		{Name: "numeric_scale", Typ: types.Int},
		{Name: "datetime_precision", Typ: types.Int},
		{Name: "interval_type", Typ: types.String},
		{Name: "interval_precision", Typ: types.Int},
		{Name: "attribute_udt_catalog", Typ: types.String},
		{Name: "attribute_udt_schema", Typ: types.String},
		{Name: "attribute_udt_name", Typ: types.String},
		{Name: "scope_catalog", Typ: types.String},
		{Name: "scope_schema", Typ: types.String},
		{Name: "scope_name", Typ: types.String},
		{Name: "maximum_cardinality", Typ: types.Int},
		{Name: "dtd_identifier", Typ: types.String},
		{Name: "is_derived_reference_attribute", Typ: types.String},
	},
}

var informationSchemaCharacterSets = virtualSchemaTable{
	comment: `character sets available in the current database
` + docs.URL("information-schema.html#character_sets") + `
https://www.postgresql.org/docs/9.5/infoschema-character-sets.html`,
	schema: vtable.InformationSchemaCharacterSets,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /* all databases */, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				return addRow(
					tree.DNull,                    // character_set_catalog
					tree.DNull,                    // character_set_schema
					tree.NewDString("UTF8"),       // character_set_name: UTF8 is the only available encoding
					tree.NewDString("UCS"),        // character_repertoire: UCS for UTF8 encoding
					tree.NewDString("UTF8"),       // form_of_use: same as the database encoding
					tree.NewDString(db.GetName()), // default_collate_catalog
					tree.DNull,                    // default_collate_schema
					tree.DNull,                    // default_collate_name
				)
			})
	},
}

var informationSchemaCheckConstraints = virtualSchemaTable{
	comment: `check constraints
` + docs.URL("information-schema.html#check_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-check-constraints.html`,
	schema: vtable.InformationSchemaCheckConstraints,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* no constraints in virtual tables */
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table := descCtx.database, descCtx.schema, descCtx.table
			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			for _, ck := range table.EnforcedCheckConstraints() {
				// Like with pg_catalog.pg_constraint, Postgres wraps the check
				// constraint expression in two pairs of parentheses.
				chkExprStr := tree.NewDString(fmt.Sprintf("((%s))", ck.GetExpr()))
				if err := addRow(
					dbNameStr,                     // constraint_catalog
					scNameStr,                     // constraint_schema
					tree.NewDString(ck.GetName()), // constraint_name
					chkExprStr,                    // check_clause
				); err != nil {
					return err
				}
			}

			// Unlike with pg_catalog.pg_constraint, Postgres also includes NOT
			// NULL column constraints in information_schema.check_constraints.
			// Cockroach doesn't track these constraints as check constraints,
			// but we can pull them off of the table's column descriptors.
			for _, column := range table.PublicColumns() {
				// Only visible, non-nullable columns are included.
				if column.IsHidden() || column.IsNullable() {
					continue
				}
				// Generate a unique name for each NOT NULL constraint. Postgres
				// uses the format <namespace_oid>_<table_oid>_<col_idx>_not_null.
				// We might as well do the same.
				conNameStr := tree.NewDString(fmt.Sprintf(
					"%s_%s_%d_not_null",
					schemaOid(sc.GetID()),
					tableOid(table.GetID()), column.Ordinal()+1,
				))
				chkExprStr := tree.NewDString(fmt.Sprintf(
					"%s IS NOT NULL", column.GetName(),
				))
				if err := addRow(
					dbNameStr,  // constraint_catalog
					scNameStr,  // constraint_schema
					conNameStr, // constraint_name
					chkExprStr, // check_clause
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

var informationSchemaColumnPrivileges = virtualSchemaTable{
	comment: `column privilege grants (incomplete)
` + docs.URL("information-schema.html#column_privileges") + `
https://www.postgresql.org/docs/9.5/infoschema-column-privileges.html`,
	schema: vtable.InformationSchemaColumnPrivileges,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: virtualMany}
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table := descCtx.database, descCtx.schema, descCtx.table
			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			columndata := privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE} // privileges for column level granularity
			privDesc, err := p.getPrivilegeDescriptor(ctx, table)
			if err != nil {
				return err
			}
			for _, u := range privDesc.Users {
				for _, priv := range columndata {
					if priv.Mask()&u.Privileges != 0 {
						for _, cd := range table.PublicColumns() {
							if err := addRow(
								tree.DNull,                                  // grantor
								tree.NewDString(u.User().Normalized()),      // grantee
								dbNameStr,                                   // table_catalog
								scNameStr,                                   // table_schema
								tree.NewDString(table.GetName()),            // table_name
								tree.NewDString(cd.GetName()),               // column_name
								tree.NewDString(string(priv.DisplayName())), // privilege_type
								tree.DNull,                                  // is_grantable
							); err != nil {
								return err
							}
						}
					}
				}
			}
			return nil
		})
	},
}

var informationSchemaColumnsTable = virtualSchemaTable{
	comment: `table and view columns (incomplete)
` + docs.URL("information-schema.html#columns") + `
https://www.postgresql.org/docs/9.5/infoschema-columns.html`,
	schema: vtable.InformationSchemaColumns,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		allComments, err := p.Descriptors().GetAllComments(ctx, p.Txn(), dbContext)
		if err != nil {
			return err
		}
		opts := forEachTableDescOptions{virtualOpts: virtualMany}
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table := descCtx.database, descCtx.schema, descCtx.table
			// Get the collations for all comments of current database.
			if table.IsSequence() {
				return nil
			}
			// Push all comments of columns into map.
			commentMap := make(map[descpb.PGAttributeNum]string)
			if err := allComments.ForEachCommentOnDescriptor(
				table.GetID(),
				func(key catalogkeys.CommentKey, cmt string) error {
					if key.CommentType != catalogkeys.ColumnCommentType {
						return nil
					}
					commentMap[descpb.PGAttributeNum(key.SubID)] = cmt
					return nil
				},
			); err != nil {
				return err
			}

			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			for _, column := range table.AccessibleColumns() {
				collationCatalog := tree.DNull
				collationSchema := tree.DNull
				collationName := tree.DNull
				if locale := column.GetType().Locale(); locale != "" {
					collationCatalog = dbNameStr
					collationSchema = pgCatalogNameDString
					collationName = tree.NewDString(locale)
				}
				colDefault := tree.DNull
				if column.HasDefault() {
					colExpr, err := schemaexpr.FormatExprForDisplay(
						ctx, table, column.GetDefaultExpr(), p.EvalContext(), &p.semaCtx, p.SessionData(), tree.FmtParsableNumerics,
					)
					if err != nil {
						return err
					}
					colDefault = tree.NewDString(colExpr)
				}
				colComputed := emptyString
				if column.IsComputed() {
					colExpr, err := schemaexpr.FormatExprForDisplay(
						ctx, table, column.GetComputeExpr(), p.EvalContext(), &p.semaCtx, p.SessionData(), tree.FmtSimple,
					)
					if err != nil {
						return err
					}
					colComputed = tree.NewDString(colExpr)
				}
				colGeneratedAsIdentity := emptyString
				if column.IsGeneratedAsIdentity() {
					if column.IsGeneratedAlwaysAsIdentity() {
						colGeneratedAsIdentity = tree.NewDString("ALWAYS")
					} else if column.IsGeneratedByDefaultAsIdentity() {
						colGeneratedAsIdentity = tree.NewDString("BY DEFAULT")
					} else {
						return errors.AssertionFailedf(
							"column %s is of wrong generated as identity type (neither ALWAYS nor BY DEFAULT)",
							column.GetName(),
						)
					}
				}

				description := tree.DNull
				if cmt, ok := commentMap[column.GetPGAttributeNum()]; ok {
					description = tree.NewDString(cmt)
				}

				// udt_schema is set to pg_catalog for builtin types. If, however, the
				// type is a user defined type, then we should fill this value based on
				// the schema it is under.
				udtSchema := pgCatalogNameDString
				typeMetaName := column.GetType().TypeMeta.Name
				if typeMetaName != nil {
					udtSchema = tree.NewDString(typeMetaName.Schema)
				}

				// Get the sequence option if it's an identity column.
				identityStart := tree.DNull
				identityIncrement := tree.DNull
				identityMax := tree.DNull
				identityMin := tree.DNull
				generatedAsIdentitySeqOpt, err := column.GetGeneratedAsIdentitySequenceOption(column.GetType().Width())
				if err != nil {
					return err
				}
				if generatedAsIdentitySeqOpt != nil {
					identityStart = tree.NewDString(strconv.FormatInt(generatedAsIdentitySeqOpt.Start, 10))
					identityIncrement = tree.NewDString(strconv.FormatInt(generatedAsIdentitySeqOpt.Increment, 10))
					identityMax = tree.NewDString(strconv.FormatInt(generatedAsIdentitySeqOpt.MaxValue, 10))
					identityMin = tree.NewDString(strconv.FormatInt(generatedAsIdentitySeqOpt.MinValue, 10))
				}

				err = addRow(
					dbNameStr,                         // table_catalog
					scNameStr,                         // table_schema
					tree.NewDString(table.GetName()),  // table_name
					tree.NewDString(column.GetName()), // column_name
					description,                       // column_comment
					tree.NewDInt(tree.DInt(column.GetPGAttributeNum())), // ordinal_position
					colDefault,                        // column_default
					yesOrNoDatum(column.IsNullable()), // is_nullable
					tree.NewDString(column.GetType().InformationSchemaName()), // data_type
					characterMaximumLength(column.GetType()),                  // character_maximum_length
					characterOctetLength(column.GetType()),                    // character_octet_length
					numericPrecision(column.GetType()),                        // numeric_precision
					numericPrecisionRadix(column.GetType()),                   // numeric_precision_radix
					numericScale(column.GetType()),                            // numeric_scale
					datetimePrecision(column.GetType()),                       // datetime_precision
					tree.DNull,                                                // interval_type
					tree.DNull,                                                // interval_precision
					tree.DNull,                                                // character_set_catalog
					tree.DNull,                                                // character_set_schema
					tree.DNull,                                                // character_set_name
					collationCatalog,                                          // collation_catalog
					collationSchema,                                           // collation_schema
					collationName,                                             // collation_name
					tree.DNull,                                                // domain_catalog
					tree.DNull,                                                // domain_schema
					tree.DNull,                                                // domain_name
					dbNameStr,                                                 // udt_catalog
					udtSchema,                                                 // udt_schema
					tree.NewDString(column.GetType().PGName()), // udt_name
					tree.DNull, // scope_catalog
					tree.DNull, // scope_schema
					tree.DNull, // scope_name
					tree.DNull, // maximum_cardinality
					tree.DNull, // dtd_identifier
					tree.DNull, // is_self_referencing
					yesOrNoDatum(column.IsGeneratedAsIdentity()), // is_identity
					colGeneratedAsIdentity,                       // identity_generation
					identityStart,                                // identity_start
					identityIncrement,                            // identity_increment
					identityMax,                                  // identity_maximum
					identityMin,                                  // identity_minimum
					// TODO(janexing): we don't support CYCLE syntax for sequences yet.
					// https://github.com/cockroachdb/cockroach/issues/20961
					tree.DNull,                              // identity_cycle
					alwaysOrNeverDatum(column.IsComputed()), // is_generated
					colComputed,                             // generation_expression
					yesOrNoDatum(table.IsTable() &&
						!table.IsVirtualTable() &&
						!column.IsComputed(),
					), // is_updatable
					yesOrNoDatum(column.IsHidden()),               // is_hidden
					tree.NewDString(column.GetType().SQLString()), // crdb_sql_type
				)
				if err != nil {
					return err
				}
			}
			return nil
		})
	},
}

var informationSchemaColumnUDTUsage = virtualSchemaTable{
	comment: `columns with user defined types
` + docs.URL("information-schema.html#column_udt_usage") + `
https://www.postgresql.org/docs/current/infoschema-column-udt-usage.html`,
	schema: vtable.InformationSchemaColumnUDTUsage,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(sc.GetName())
				tbNameStr := tree.NewDString(table.GetName())
				for _, col := range table.PublicColumns() {
					if !col.GetType().UserDefined() {
						continue
					}
					if err := addRow(
						tree.NewDString(col.GetType().TypeMeta.Name.Catalog), // UDT_CATALOG
						tree.NewDString(col.GetType().TypeMeta.Name.Schema),  // UDT_SCHEMA
						tree.NewDString(col.GetType().TypeMeta.Name.Name),    // UDT_NAME
						dbNameStr,                      // TABLE_CATALOG
						scNameStr,                      // TABLE_SCHEMA
						tbNameStr,                      // TABLE_NAME
						tree.NewDString(col.GetName()), // COLUMN_NAME
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
	},
}

var informationSchemaEnabledRoles = virtualSchemaTable{
	comment: `roles for the current user
` + docs.URL("information-schema.html#enabled_roles") + `
https://www.postgresql.org/docs/9.5/infoschema-enabled-roles.html`,
	schema: vtable.InformationSchemaEnabledRoles,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		currentUser := p.SessionData().User()
		memberMap, err := p.MemberOfWithAdminOption(ctx, currentUser)
		if err != nil {
			return err
		}

		// The current user is always listed.
		if err := addRow(
			tree.NewDString(currentUser.Normalized()), // role_name: the current user
		); err != nil {
			return err
		}

		for roleName := range memberMap {
			if err := addRow(
				tree.NewDString(roleName.Normalized()), // role_name
			); err != nil {
				return err
			}
		}

		return nil
	},
}

// characterMaximumLength returns the declared maximum length of
// characters if the type is a character or bit string data
// type. Returns false if the data type is not a character or bit
// string, or if the string's length is not bounded.
func characterMaximumLength(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		// "char" columns have a width of 1, but should report a NULL maximum
		// character length.
		if colType.Oid() == oid.T_char {
			return 0, false
		}
		switch colType.Family() {
		case types.StringFamily, types.CollatedStringFamily, types.BitFamily:
			if colType.Width() > 0 {
				return colType.Width(), true
			}
		}
		return 0, false
	})
}

// characterOctetLength returns the maximum possible length in
// octets of a datum if the T is a character string. Returns
// false if the data type is not a character string, or if the
// string's length is not bounded.
func characterOctetLength(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		// "char" columns have a width of 1, but should report a NULL octet
		// length.
		if colType.Oid() == oid.T_char {
			return 0, false
		}
		switch colType.Family() {
		case types.StringFamily, types.CollatedStringFamily:
			if colType.Width() > 0 {
				return colType.Width() * utf8.UTFMax, true
			}
		}
		return 0, false
	})
}

// numericPrecision returns the declared or implicit precision of numeric
// data types. Returns false if the data type is not numeric, or if the precision
// of the numeric type is not bounded.
func numericPrecision(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.IntFamily:
			return colType.Width(), true
		case types.FloatFamily:
			if colType.Width() == 32 {
				return 24, true
			}
			return 53, true
		case types.DecimalFamily:
			if colType.Precision() > 0 {
				return colType.Precision(), true
			}
		}
		return 0, false
	})
}

// numericPrecisionRadix returns the implicit precision radix of
// numeric data types. Returns false if the data type is not numeric.
func numericPrecisionRadix(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.IntFamily:
			return 2, true
		case types.FloatFamily:
			return 2, true
		case types.DecimalFamily:
			return 10, true
		}
		return 0, false
	})
}

// NumericScale returns the declared or implicit precision of exact numeric
// data types. Returns false if the data type is not an exact numeric, or if the
// scale of the exact numeric type is not bounded.
func numericScale(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.IntFamily:
			return 0, true
		case types.DecimalFamily:
			if colType.Precision() > 0 {
				return colType.Width(), true
			}
		}
		return 0, false
	})
}

// datetimePrecision returns the declared or implicit precision of Time,
// Timestamp or Interval data types. Returns false if the data type is not
// a Time, Timestamp or Interval.
func datetimePrecision(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.TimeFamily, types.TimeTZFamily, types.TimestampFamily, types.TimestampTZFamily, types.IntervalFamily:
			return colType.Precision(), true
		}
		return 0, false
	})
}

var informationSchemaConstraintColumnUsageTable = virtualSchemaTable{
	comment: `columns usage by constraints
https://www.postgresql.org/docs/9.5/infoschema-constraint-column-usage.html`,
	schema: vtable.InformationSchemaConstraintColumnUsage,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* no constraints in virtual tables */
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table, tableLookup := descCtx.database, descCtx.schema, descCtx.table, descCtx.tableLookup
			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			for _, c := range table.AllConstraints() {
				conNameStr := tree.NewDString(c.GetName())
				refSchema := sc
				refTable := table
				var cols []catalog.Column
				if ck := c.AsCheck(); ck != nil {
					cols = table.CheckConstraintColumns(ck)
				} else if fk := c.AsForeignKey(); fk != nil {
					var err error
					refTable, err = tableLookup.getTableByID(fk.GetReferencedTableID())
					if err != nil {
						return errors.NewAssertionErrorWithWrappedErrf(err,
							"error resolving table %d referenced in foreign key %q in table %q",
							fk.GetReferencedTableID(), fk.GetName(), table.GetName())
					}
					refSchema, err = tableLookup.getSchemaByID(refTable.GetParentSchemaID())
					if err != nil {
						return errors.NewAssertionErrorWithWrappedErrf(err,
							"error resolving schema %d referenced in foreign key %q in table %q",
							refTable.GetParentSchemaID(), fk.GetName(), table.GetName())
					}
					cols = refTable.ForeignKeyReferencedColumns(fk)
				} else if uwi := c.AsUniqueWithIndex(); uwi != nil {
					cols = table.IndexKeyColumns(uwi)
				} else if uwoi := c.AsUniqueWithoutIndex(); uwoi != nil {
					cols = table.UniqueWithoutIndexColumns(uwoi)
				}
				for _, col := range cols {
					if err := addRow(
						dbNameStr,                            // table_catalog
						tree.NewDString(refSchema.GetName()), // table_schema
						tree.NewDString(refTable.GetName()),  // table_name
						tree.NewDString(col.GetName()),       // column_name
						dbNameStr,                            // constraint_catalog
						scNameStr,                            // constraint_schema
						conNameStr,                           // constraint_name
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/key-column-usage-table.html
var informationSchemaKeyColumnUsageTable = virtualSchemaTable{
	comment: `column usage by indexes and key constraints
` + docs.URL("information-schema.html#key_column_usage") + `
https://www.postgresql.org/docs/9.5/infoschema-key-column-usage.html`,
	schema: vtable.InformationSchemaKeyColumnUsage,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* no constraints in virtual tables */
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table := descCtx.database, descCtx.schema, descCtx.table
			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			tbNameStr := tree.NewDString(table.GetName())
			for _, c := range table.AllConstraints() {
				cstNameStr := tree.NewDString(c.GetName())
				var cols []catalog.Column
				// Only Primary Key, Foreign Key, and Unique constraints are included.
				if fk := c.AsForeignKey(); fk != nil {
					cols = table.ForeignKeyOriginColumns(fk)
				} else if uwi := c.AsUniqueWithIndex(); uwi != nil {
					cols = table.IndexKeyColumns(uwi)
				} else if uwoi := c.AsUniqueWithoutIndex(); uwoi != nil {
					cols = table.UniqueWithoutIndexColumns(uwoi)
				}
				for pos, col := range cols {
					ordinalPos := tree.NewDInt(tree.DInt(pos + 1))
					uniquePos := tree.DNull
					if c.AsForeignKey() != nil {
						uniquePos = ordinalPos
					}
					if err := addRow(
						dbNameStr,                      // constraint_catalog
						scNameStr,                      // constraint_schema
						cstNameStr,                     // constraint_name
						dbNameStr,                      // table_catalog
						scNameStr,                      // table_schema
						tbNameStr,                      // table_name
						tree.NewDString(col.GetName()), // column_name
						ordinalPos,                     // ordinal_position, 1-indexed
						uniquePos,                      // position_in_unique_constraint
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-parameters.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/parameters-table.html
var informationSchemaParametersTable = virtualSchemaView{
	comment: `function parameters
https://www.postgresql.org/docs/9.5/infoschema-parameters.html`,
	schema: vtable.InformationSchemaParameters,
	resultColumns: colinfo.ResultColumns{
		{Name: "specific_catalog", Typ: types.String},
		{Name: "specific_schema", Typ: types.String},
		{Name: "specific_name", Typ: types.String},
		{Name: "ordinal_position", Typ: types.Int},
		{Name: "parameter_mode", Typ: types.String},
		{Name: "is_result", Typ: types.String},
		{Name: "as_locator", Typ: types.String},
		{Name: "parameter_name", Typ: types.String},
		{Name: "data_type", Typ: types.String},
		{Name: "character_maximum_length", Typ: types.Int},
		{Name: "character_octet_length", Typ: types.Int},
		{Name: "character_set_catalog", Typ: types.String},
		{Name: "character_set_schema", Typ: types.String},
		{Name: "character_set_name", Typ: types.String},
		{Name: "collation_catalog", Typ: types.String},
		{Name: "collation_schema", Typ: types.String},
		{Name: "collation_name", Typ: types.String},
		{Name: "numeric_precision", Typ: types.Int},
		{Name: "numeric_precision_radix", Typ: types.Int},
		{Name: "numeric_scale", Typ: types.Int},
		{Name: "datetime_precision", Typ: types.Int},
		{Name: "interval_type", Typ: types.String},
		{Name: "interval_precision", Typ: types.Int},
		{Name: "udt_catalog", Typ: types.String},
		{Name: "udt_schema", Typ: types.String},
		{Name: "udt_name", Typ: types.String},
		{Name: "scope_catalog", Typ: types.String},
		{Name: "scope_schema", Typ: types.String},
		{Name: "scope_name", Typ: types.String},
		{Name: "maximum_cardinality", Typ: types.Int},
		{Name: "dtd_identifier", Typ: types.String},
		{Name: "parameter_default", Typ: types.String},
	},
}

var (
	matchOptionFull    = tree.NewDString("FULL")
	matchOptionPartial = tree.NewDString("PARTIAL")
	matchOptionNone    = tree.NewDString("NONE")

	matchOptionMap = map[semenumpb.Match]tree.Datum{
		semenumpb.Match_SIMPLE:  matchOptionNone,
		semenumpb.Match_FULL:    matchOptionFull,
		semenumpb.Match_PARTIAL: matchOptionPartial,
	}

	refConstraintRuleNoAction   = tree.NewDString("NO ACTION")
	refConstraintRuleRestrict   = tree.NewDString("RESTRICT")
	refConstraintRuleSetNull    = tree.NewDString("SET NULL")
	refConstraintRuleSetDefault = tree.NewDString("SET DEFAULT")
	refConstraintRuleCascade    = tree.NewDString("CASCADE")
)

func dStringForFKAction(action semenumpb.ForeignKeyAction) tree.Datum {
	switch action {
	case semenumpb.ForeignKeyAction_NO_ACTION:
		return refConstraintRuleNoAction
	case semenumpb.ForeignKeyAction_RESTRICT:
		return refConstraintRuleRestrict
	case semenumpb.ForeignKeyAction_SET_NULL:
		return refConstraintRuleSetNull
	case semenumpb.ForeignKeyAction_SET_DEFAULT:
		return refConstraintRuleSetDefault
	case semenumpb.ForeignKeyAction_CASCADE:
		return refConstraintRuleCascade
	}
	panic(errors.Errorf("unexpected ForeignKeyReference_Action: %v", action))
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/referential-constraints-table.html
var informationSchemaReferentialConstraintsTable = virtualSchemaTable{
	comment: `foreign key constraints
` + docs.URL("information-schema.html#referential_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-referential-constraints.html`,
	schema: vtable.InformationSchemaReferentialConstraints,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* no constraints in virtual tables */
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table := descCtx.database, descCtx.schema, descCtx.table
			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			tbNameStr := tree.NewDString(table.GetName())
			for _, fk := range table.OutboundForeignKeys() {
				refTable, err := descCtx.tableLookup.getTableByID(fk.GetReferencedTableID())
				if err != nil {
					return err
				}
				var matchType = tree.DNull
				if r, ok := matchOptionMap[fk.Match()]; ok {
					matchType = r
				}
				refConstraint, err := catalog.FindFKReferencedUniqueConstraint(refTable, fk)
				if err != nil {
					return err
				}
				// Note: Cross DB references are deprecated, but this should be
				// a cached look up when they don't exist.
				refDB, err := p.Descriptors().ByIDWithoutLeased(p.Txn()).Get().Database(ctx, refTable.GetParentID())
				if err != nil {
					return err
				}
				refSchema, err := p.Descriptors().ByIDWithoutLeased(p.Txn()).Get().Schema(ctx, refTable.GetParentSchemaID())
				if err != nil {
					return err
				}
				if err := addRow(
					dbNameStr,                                // constraint_catalog
					scNameStr,                                // constraint_schema
					tree.NewDString(fk.GetName()),            // constraint_name
					tree.NewDString(refDB.GetName()),         // unique_constraint_catalog
					tree.NewDString(refSchema.GetName()),     // unique_constraint_schema
					tree.NewDString(refConstraint.GetName()), // unique_constraint_name
					matchType,                                // match_option
					dStringForFKAction(fk.OnUpdate()),        // update_rule
					dStringForFKAction(fk.OnDelete()),        // delete_rule
					tbNameStr,                                // table_name
					tree.NewDString(refTable.GetName()),      // referenced_table_name
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-role-table-grants.html
// MySQL:    missing
var informationSchemaRoleTableGrants = virtualSchemaTable{
	comment: `privileges granted on table or views (incomplete; see also information_schema.table_privileges; may contain excess users or roles)
` + docs.URL("information-schema.html#role_table_grants") + `
https://www.postgresql.org/docs/9.5/infoschema-role-table-grants.html`,
	schema: vtable.InformationSchemaRoleTableGrants,
	// This is the same as information_schema.table_privileges. In postgres, this virtual table does
	// not show tables with grants provided through PUBLIC, but table_privileges does.
	// Since we don't have the PUBLIC concept, the two virtual tables are identical.
	populate: populateTablePrivileges,
}

// MySQL:    https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/routines-table.html
var informationSchemaRoutineTable = virtualSchemaView{
	comment: `built-in functions and user-defined functions
https://www.postgresql.org/docs/15/infoschema-routines.html`,
	schema: vtable.InformationSchemaRoutines,
	resultColumns: colinfo.ResultColumns{
		{Name: "specific_catalog", Typ: types.String},
		{Name: "specific_schema", Typ: types.String},
		{Name: "specific_name", Typ: types.String},
		{Name: "routine_catalog", Typ: types.String},
		{Name: "routine_schema", Typ: types.String},
		{Name: "routine_name", Typ: types.String},
		{Name: "routine_type", Typ: types.String},
		{Name: "module_catalog", Typ: types.String},
		{Name: "module_schema", Typ: types.String},
		{Name: "module_name", Typ: types.String},
		{Name: "udt_catalog", Typ: types.String},
		{Name: "udt_schema", Typ: types.String},
		{Name: "udt_name", Typ: types.String},
		{Name: "data_type", Typ: types.String},
		{Name: "character_maximum_length", Typ: types.Int},
		{Name: "character_octet_length", Typ: types.Int},
		{Name: "character_set_catalog", Typ: types.String},
		{Name: "character_set_schema", Typ: types.String},
		{Name: "character_set_name", Typ: types.String},
		{Name: "collation_catalog", Typ: types.String},
		{Name: "collation_schema", Typ: types.String},
		{Name: "collation_name", Typ: types.String},
		{Name: "numeric_precision", Typ: types.Int},
		{Name: "numeric_precision_radix", Typ: types.Int},
		{Name: "numeric_scale", Typ: types.Int},
		{Name: "datetime_precision", Typ: types.Int},
		{Name: "interval_type", Typ: types.String},
		{Name: "interval_precision", Typ: types.Int},
		{Name: "type_udt_catalog", Typ: types.String},
		{Name: "type_udt_schema", Typ: types.String},
		{Name: "type_udt_name", Typ: types.String},
		{Name: "scope_catalog", Typ: types.String},
		{Name: "scope_schema", Typ: types.String},
		{Name: "scope_name", Typ: types.String},
		{Name: "maximum_cardinality", Typ: types.Int},
		{Name: "dtd_identifier", Typ: types.String},
		{Name: "routine_body", Typ: types.String},
		{Name: "routine_definition", Typ: types.String},
		{Name: "external_name", Typ: types.String},
		{Name: "external_language", Typ: types.String},
		{Name: "parameter_style", Typ: types.String},
		{Name: "is_deterministic", Typ: types.String},
		{Name: "sql_data_access", Typ: types.String},
		{Name: "is_null_call", Typ: types.String},
		{Name: "sql_path", Typ: types.String},
		{Name: "schema_level_routine", Typ: types.String},
		{Name: "max_dynamic_result_sets", Typ: types.Int},
		{Name: "is_user_defined_cast", Typ: types.String},
		{Name: "is_implicitly_invocable", Typ: types.String},
		{Name: "security_type", Typ: types.String},
		{Name: "to_sql_specific_catalog", Typ: types.String},
		{Name: "to_sql_specific_schema", Typ: types.String},
		{Name: "to_sql_specific_name", Typ: types.String},
		{Name: "as_locator", Typ: types.String},
		{Name: "created", Typ: types.TimestampTZ},
		{Name: "last_altered", Typ: types.TimestampTZ},
		{Name: "new_savepoint_level", Typ: types.String},
		{Name: "is_udt_dependent", Typ: types.String},
		{Name: "result_cast_from_data_type", Typ: types.String},
		{Name: "result_cast_as_locator", Typ: types.String},
		{Name: "result_cast_char_max_length", Typ: types.Int},
		{Name: "result_cast_char_octet_length", Typ: types.Int},
		{Name: "result_cast_char_set_catalog", Typ: types.String},
		{Name: "result_cast_char_set_schema", Typ: types.String},
		{Name: "result_cast_char_set_name", Typ: types.String},
		{Name: "result_cast_collation_catalog", Typ: types.String},
		{Name: "result_cast_collation_schema", Typ: types.String},
		{Name: "result_cast_collation_name", Typ: types.String},
		{Name: "result_cast_numeric_precision", Typ: types.Int},
		{Name: "result_cast_numeric_precision_radix", Typ: types.Int},
		{Name: "result_cast_numeric_scale", Typ: types.Int},
		{Name: "result_cast_datetime_precision", Typ: types.Int},
		{Name: "result_cast_interval_type", Typ: types.String},
		{Name: "result_cast_interval_precision", Typ: types.Int},
		{Name: "result_cast_type_udt_catalog", Typ: types.String},
		{Name: "result_cast_type_udt_schema", Typ: types.String},
		{Name: "result_cast_type_udt_name", Typ: types.String},
		{Name: "result_cast_scope_catalog", Typ: types.String},
		{Name: "result_cast_scope_schema", Typ: types.String},
		{Name: "result_cast_scope_name", Typ: types.String},
		{Name: "result_cast_maximum_cardinality", Typ: types.Int},
		{Name: "result_cast_dtd_identifier", Typ: types.String},
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/schemata-table.html
var informationSchemaSchemataTable = virtualSchemaTable{
	comment: `database schemas (may contain schemata without permission)
` + docs.URL("information-schema.html#schemata") + `
https://www.postgresql.org/docs/9.5/infoschema-schemata.html`,
	schema: vtable.InformationSchemaSchemata,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				return forEachSchema(ctx, p, db, true /* requiresPrivileges */, func(ctx context.Context, sc catalog.SchemaDescriptor) error {
					return addRow(
						tree.NewDString(db.GetName()), // catalog_name
						tree.NewDString(sc.GetName()), // schema_name
						tree.DNull,                    // default_character_set_name
						tree.DNull,                    // sql_path
						yesOrNoDatum(sc.SchemaKind() == catalog.SchemaUserDefined), // crdb_is_user_defined
					)
				})
			})
	},
}

var builtinTypePrivileges = []struct {
	grantee *tree.DString
	kind    *tree.DString
}{
	{tree.NewDString(username.RootUser), tree.NewDString(string(privilege.ALL.DisplayName()))},
	{tree.NewDString(username.AdminRole), tree.NewDString(string(privilege.ALL.DisplayName()))},
	{tree.NewDString(username.PublicRole), tree.NewDString(string(privilege.USAGE.DisplayName()))},
}

// Custom; PostgreSQL has data_type_privileges, which only shows one row per type,
// which may result in confusing semantics for the user compared to this table
// which has one row for each grantee.
var informationSchemaTypePrivilegesTable = virtualSchemaTable{
	comment: `type privileges (incomplete; may contain excess users or roles)
` + docs.URL("information-schema.html#type_privileges"),
	schema: vtable.InformationSchemaTypePrivileges,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				dbNameStr := tree.NewDString(db.GetName())
				pgCatalogStr := tree.NewDString("pg_catalog")
				// Generate one for each existing type.
				for _, typ := range types.OidToType {
					typeNameStr := tree.NewDString(typ.Name())
					for _, it := range builtinTypePrivileges {
						if err := addRow(
							it.grantee,   // grantee
							dbNameStr,    // type_catalog
							pgCatalogStr, // type_schema
							typeNameStr,  // type_name
							it.kind,      // privilege_type
							noString,     // is_grantable
						); err != nil {
							return err
						}
					}
				}

				// And for all user defined types.
				return forEachTypeDesc(ctx, p, db, func(ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, typeDesc catalog.TypeDescriptor) error {
					scNameStr := tree.NewDString(sc.GetName())
					typeNameStr := tree.NewDString(typeDesc.GetName())
					// TODO(knz): This should filter for the current user, see
					// https://github.com/cockroachdb/cockroach/issues/35572
					privs, err := typeDesc.GetPrivileges().Show(privilege.Type, true /* showImplicitOwnerPrivs */)
					if err != nil {
						return err
					}
					for _, u := range privs {
						userNameStr := tree.NewDString(u.User.Normalized())
						for _, priv := range u.Privileges {
							// We use this function to check for the grant option so that the
							// object owner also gets is_grantable=true.
							isGrantable, err := p.CheckGrantOptionsForUser(
								ctx, typeDesc.GetPrivileges(), typeDesc, []privilege.Kind{priv.Kind}, u.User,
							)
							if err != nil {
								return err
							}
							if err := addRow(
								userNameStr, // grantee
								dbNameStr,   // type_catalog
								scNameStr,   // type_schema
								typeNameStr, // type_name
								tree.NewDString(string(priv.Kind.DisplayName())), // privilege_type
								yesOrNoDatum(isGrantable),                        // is_grantable
							); err != nil {
								return err
							}
						}
					}
					return nil
				})
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/schema-privileges-table.html
var informationSchemaSchemataTablePrivileges = virtualSchemaTable{
	comment: `schema privileges (incomplete; may contain excess users or roles)
` + docs.URL("information-schema.html#schema_privileges"),
	schema: vtable.InformationSchemaSchemaPrivileges,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				return forEachSchema(ctx, p, db, true /* requiresPrivileges */, func(ctx context.Context, sc catalog.SchemaDescriptor) error {
					privs, err := sc.GetPrivileges().Show(privilege.Schema, true /* showImplicitOwnerPrivs */)
					if err != nil {
						return err
					}
					dbNameStr := tree.NewDString(db.GetName())
					scNameStr := tree.NewDString(sc.GetName())
					// TODO(knz): This should filter for the current user, see
					// https://github.com/cockroachdb/cockroach/issues/35572
					for _, u := range privs {
						userNameStr := tree.NewDString(u.User.Normalized())
						for _, priv := range u.Privileges {
							// We use this function to check for the grant option so that the
							// object owner also gets is_grantable=true.
							isGrantable, err := p.CheckGrantOptionsForUser(
								ctx, sc.GetPrivileges(), sc, []privilege.Kind{priv.Kind}, u.User,
							)
							if err != nil {
								return err
							}
							if err := addRow(
								userNameStr, // grantee
								dbNameStr,   // table_catalog
								scNameStr,   // table_schema
								tree.NewDString(string(priv.Kind.DisplayName())), // privilege_type
								yesOrNoDatum(isGrantable),                        // is_grantable
							); err != nil {
								return err
							}
						}
					}
					return nil
				})
			})
	},
}

var (
	indexDirectionNA   = tree.NewDString("N/A")
	indexDirectionAsc  = tree.NewDString(catenumpb.IndexColumn_ASC.String())
	indexDirectionDesc = tree.NewDString(catenumpb.IndexColumn_DESC.String())
)

func dStringForIndexDirection(dir catenumpb.IndexColumn_Direction) tree.Datum {
	switch dir {
	case catenumpb.IndexColumn_ASC:
		return indexDirectionAsc
	case catenumpb.IndexColumn_DESC:
		return indexDirectionDesc
	}
	panic("unreachable")
}

var informationSchemaSequences = virtualSchemaTable{
	comment: `sequences
` + docs.URL("information-schema.html#sequences") + `
https://www.postgresql.org/docs/9.5/infoschema-sequences.html`,
	schema: vtable.InformationSchemaSequences,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext,
			forEachTableDescOptions{virtualOpts: hideVirtual}, /* no sequences in virtual schemas */
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				if !table.IsSequence() {
					return nil
				}
				typ := "INT8"
				precision := 64
				switch table.GetSequenceOpts().AsIntegerType {
				case "INT2":
					precision = 16
					typ = "INT2"
				case "INT4":
					precision = 32
					typ = "INT4"
				}
				return addRow(
					tree.NewDString(db.GetName()),      // catalog
					tree.NewDString(sc.GetName()),      // schema
					tree.NewDString(table.GetName()),   // name
					tree.NewDString(typ),               // integer type, one of ["INT2", "INT4", "INT8"]
					tree.NewDInt(tree.DInt(precision)), // numeric precision
					tree.NewDInt(2),                    // numeric precision radix
					tree.NewDInt(0),                    // numeric scale
					tree.NewDString(strconv.FormatInt(table.GetSequenceOpts().Start, 10)),     // start value
					tree.NewDString(strconv.FormatInt(table.GetSequenceOpts().MinValue, 10)),  // min value
					tree.NewDString(strconv.FormatInt(table.GetSequenceOpts().MaxValue, 10)),  // max value
					tree.NewDString(strconv.FormatInt(table.GetSequenceOpts().Increment, 10)), // increment
					noString, // cycle
				)
			})
	},
}

// Postgres: missing
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/statistics-table.html
var informationSchemaStatisticsTable = virtualSchemaTable{
	comment: `index metadata and statistics (incomplete)
` + docs.URL("information-schema.html#statistics"),
	schema: vtable.InformationSchemaStatistics,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext,
			forEachTableDescOptions{virtualOpts: hideVirtual}, /* virtual tables have no indexes */
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(sc.GetName())
				tbNameStr := tree.NewDString(table.GetName())

				appendRow := func(index catalog.Index, colName string, sequence int,
					direction tree.Datum, isStored, isImplicit bool,
				) error {
					idxInvisibility := index.GetInvisibility()
					return addRow(
						dbNameStr,                            // table_catalog
						scNameStr,                            // table_schema
						tbNameStr,                            // table_name
						yesOrNoDatum(!index.IsUnique()),      // non_unique
						scNameStr,                            // index_schema
						tree.NewDString(index.GetName()),     // index_name
						tree.NewDInt(tree.DInt(sequence)),    // seq_in_index
						tree.NewDString(colName),             // column_name
						tree.DNull,                           // collation
						tree.DNull,                           // cardinality
						direction,                            // direction
						yesOrNoDatum(isStored),               // storing
						yesOrNoDatum(isImplicit),             // implicit
						yesOrNoDatum(idxInvisibility == 0.0), // is_visible
						tree.NewDFloat(tree.DFloat(1-idxInvisibility)), // visibility
					)
				}

				return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
					// Columns in the primary key that aren't in index.KeyColumnNames or
					// index.StoreColumnNames are implicit columns in the index.
					var implicitCols map[string]struct{}
					var hasImplicitCols bool
					if index.HasOldStoredColumns() {
						// Old STORING format: implicit columns are extra columns minus stored
						// columns.
						hasImplicitCols = index.NumKeySuffixColumns() > index.NumSecondaryStoredColumns()
					} else {
						// New STORING format: implicit columns are extra columns.
						hasImplicitCols = index.NumKeySuffixColumns() > 0
					}
					if hasImplicitCols {
						implicitCols = make(map[string]struct{})
						for i := 0; i < table.GetPrimaryIndex().NumKeyColumns(); i++ {
							col := table.GetPrimaryIndex().GetKeyColumnName(i)
							implicitCols[col] = struct{}{}
						}
					}

					sequence := 1
					for i := 0; i < index.NumKeyColumns(); i++ {
						col := index.GetKeyColumnName(i)
						// We add a row for each column of index.
						dir := dStringForIndexDirection(index.GetKeyColumnDirection(i))
						if err := appendRow(
							index,
							col,
							sequence,
							dir,
							false,
							i < index.ExplicitColumnStartIdx(),
						); err != nil {
							return err
						}
						sequence++
						delete(implicitCols, col)
					}
					for i := 0; i < index.NumPrimaryStoredColumns()+index.NumSecondaryStoredColumns(); i++ {
						col := index.GetStoredColumnName(i)
						// We add a row for each stored column of index.
						if err := appendRow(index, col, sequence,
							indexDirectionNA, true, false); err != nil {
							return err
						}
						sequence++
						delete(implicitCols, col)
					}
					if len(implicitCols) > 0 {
						// In order to have the implicit columns reported in a
						// deterministic order, we will add all of them in the
						// same order as they are mentioned in the primary key.
						//
						// Note that simply iterating over implicitCols map
						// produces non-deterministic output.
						for i := 0; i < table.GetPrimaryIndex().NumKeyColumns(); i++ {
							col := table.GetPrimaryIndex().GetKeyColumnName(i)
							if _, isImplicit := implicitCols[col]; isImplicit {
								// We add a row for each implicit column of index.
								if err := appendRow(index, col, sequence,
									indexDirectionAsc, index.IsUnique(), true); err != nil {
									return err
								}
								sequence++
							}
						}
					}
					return nil
				})
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
var informationSchemaTableConstraintTable = virtualSchemaTable{
	comment: `table constraints
` + docs.URL("information-schema.html#table_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-table-constraints.html`,
	schema: vtable.InformationSchemaTableConstraint,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* virtual tables have no constraints */
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(sc.GetName())
				tbNameStr := tree.NewDString(table.GetName())

				for _, c := range table.AllConstraints() {
					kind := catconstants.ConstraintTypeUnique
					if c.AsCheck() != nil {
						kind = catconstants.ConstraintTypeCheck
					} else if c.AsForeignKey() != nil {
						kind = catconstants.ConstraintTypeFK
					} else if u := c.AsUniqueWithIndex(); u != nil && u.Primary() {
						kind = catconstants.ConstraintTypePK
					}
					if err := addRow(
						dbNameStr,                     // constraint_catalog
						scNameStr,                     // constraint_schema
						tree.NewDString(c.GetName()),  // constraint_name
						dbNameStr,                     // table_catalog
						scNameStr,                     // table_schema
						tbNameStr,                     // table_name
						tree.NewDString(string(kind)), // constraint_type
						yesOrNoDatum(false),           // is_deferrable
						yesOrNoDatum(false),           // initially_deferred
					); err != nil {
						return err
					}
				}
				// Unlike with pg_catalog.pg_constraint, Postgres also includes NOT
				// NULL column constraints in information_schema.check_constraints.
				// Cockroach doesn't track these constraints as check constraints,
				// but we can pull them off of the table's column descriptors.
				for _, col := range table.PublicColumns() {
					if col.IsNullable() {
						continue
					}
					// NOT NULL column constraints are implemented as a CHECK in postgres.
					conNameStr := tree.NewDString(fmt.Sprintf(
						"%s_%s_%d_not_null",
						schemaOid(sc.GetID()),
						tableOid(table.GetID()), col.Ordinal()+1,
					))
					if err := addRow(
						dbNameStr,                // constraint_catalog
						scNameStr,                // constraint_schema
						conNameStr,               // constraint_name
						dbNameStr,                // table_catalog
						scNameStr,                // table_schema
						tbNameStr,                // table_name
						tree.NewDString("CHECK"), // constraint_type
						yesOrNoDatum(false),      // is_deferrable
						yesOrNoDatum(false),      // initially_deferred
					); err != nil {
						return err
					}
				}
				return nil
			})
	},
}

var informationSchemaUserDefinedTypesTable = virtualSchemaView{
	comment: `user-defined types` +
		docs.URL("information-schema.html#user-defined-types") + `
    https://www.postgresql.org/docs/16/infoschema-user-defined-types.html`,
	schema: vtable.InformationSchemaUserDefinedTypes,
	resultColumns: colinfo.ResultColumns{
		{Name: "user_defined_type_catalog", Typ: types.String},
		{Name: "user_defined_type_schema", Typ: types.String},
		{Name: "user_defined_type_name", Typ: types.String},
		{Name: "user_defined_type_category", Typ: types.String},
		{Name: "is_instantiable", Typ: types.String},
		{Name: "is_final", Typ: types.String},
		{Name: "ordering_form", Typ: types.String},
		{Name: "ordering_category", Typ: types.String},
		{Name: "ordering_routine_catalog", Typ: types.String},
		{Name: "ordering_routine_schema", Typ: types.String},
		{Name: "ordering_routine_name", Typ: types.String},
		{Name: "reference_type", Typ: types.String},
		{Name: "data_type", Typ: types.String},
		{Name: "character_maximum_length", Typ: types.Int},
		{Name: "character_octet_length", Typ: types.Int},
		{Name: "character_set_catalog", Typ: types.String},
		{Name: "character_set_schema", Typ: types.String},
		{Name: "character_set_name", Typ: types.String},
		{Name: "collation_catalog", Typ: types.String},
		{Name: "collation_schema", Typ: types.String},
		{Name: "collation_name", Typ: types.String},
		{Name: "numeric_precision", Typ: types.Int},
		{Name: "numeric_precision_radix", Typ: types.Int},
		{Name: "numeric_scale", Typ: types.Int},
		{Name: "datetime_precision", Typ: types.Int},
		{Name: "interval_type", Typ: types.String},
		{Name: "interval_precision", Typ: types.Int},
		{Name: "source_dtd_identifier", Typ: types.String},
		{Name: "ref_dtd_identifier", Typ: types.String},
	},
}

// Postgres: not provided
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/user-privileges-table.html
// TODO(knz): this introspection facility is of dubious utility.
var informationSchemaUserPrivileges = virtualSchemaTable{
	comment: `grantable privileges (incomplete)`,
	schema:  vtable.InformationSchemaUserPrivileges,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(ctx context.Context, dbDesc catalog.DatabaseDescriptor) error {
				dbNameStr := tree.NewDString(dbDesc.GetName())
				for _, u := range []string{username.RootUser, username.AdminRole} {
					grantee := tree.NewDString(u)
					validPrivs, err := privilege.GetValidPrivilegesForObject(privilege.Table)
					if err != nil {
						return err
					}
					for _, p := range validPrivs.SortedDisplayNames() {
						if err := addRow(
							grantee,            // grantee
							dbNameStr,          // table_catalog
							tree.NewDString(p), // privilege_type
							tree.DNull,         // is_grantable
						); err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/table-privileges-table.html
var informationSchemaTablePrivileges = virtualSchemaTable{
	comment: `privileges granted on table or views (incomplete; may contain excess users or roles)
` + docs.URL("information-schema.html#table_privileges") + `
https://www.postgresql.org/docs/9.5/infoschema-table-privileges.html`,
	schema:   vtable.InformationSchemaTablePrivileges,
	populate: populateTablePrivileges,
}

// populateTablePrivileges is used to populate both table_privileges and role_table_grants.
func populateTablePrivileges(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	addRow func(...tree.Datum) error,
) error {
	opts := forEachTableDescOptions{virtualOpts: virtualMany}
	return forEachTableDesc(ctx, p, dbContext, opts,
		func(ctx context.Context, descCtx tableDescContext) error {
			db, sc, table := descCtx.database, descCtx.schema, descCtx.table
			dbNameStr := tree.NewDString(db.GetName())
			scNameStr := tree.NewDString(sc.GetName())
			tbNameStr := tree.NewDString(table.GetName())
			// TODO(knz): This should filter for the current user, see
			// https://github.com/cockroachdb/cockroach/issues/35572
			tableType := table.GetObjectType()
			desc, err := p.getPrivilegeDescriptor(ctx, table)
			if err != nil {
				return err
			}
			showPrivs, err := desc.Show(tableType, true /* showImplicitOwnerPrivs */)
			if err != nil {
				return err
			}
			for _, u := range showPrivs {
				granteeNameStr := tree.NewDString(u.User.Normalized())
				for _, priv := range u.Privileges {
					// We use this function to check for the grant option so that the
					// object owner also gets is_grantable=true.
					privs, err := p.getPrivilegeDescriptor(ctx, table)
					if err != nil {
						return err
					}
					isGrantable, err := p.CheckGrantOptionsForUser(
						ctx, privs, table, []privilege.Kind{priv.Kind}, u.User,
					)
					if err != nil {
						return err
					}
					if err := addRow(
						tree.DNull,     // grantor
						granteeNameStr, // grantee
						dbNameStr,      // table_catalog
						scNameStr,      // table_schema
						tbNameStr,      // table_name
						tree.NewDString(string(priv.Kind.DisplayName())), // privilege_type
						yesOrNoDatum(isGrantable),                        // is_grantable
						yesOrNoDatum(priv.Kind == privilege.SELECT),      // with_hierarchy
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
}

var (
	tableTypeSystemView = tree.NewDString("SYSTEM VIEW")
	tableTypeBaseTable  = tree.NewDString("BASE TABLE")
	tableTypeView       = tree.NewDString("VIEW")
	tableTypeTemporary  = tree.NewDString("LOCAL TEMPORARY")
)

var informationSchemaTablesTable = virtualSchemaTable{
	comment: `tables and views
` + docs.URL("information-schema.html#tables") + `
https://www.postgresql.org/docs/9.5/infoschema-tables.html`,
	schema: vtable.InformationSchemaTables,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: virtualMany}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(
				ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				if table.IsSequence() {
					return nil
				}
				tableType := tableTypeBaseTable
				insertable := yesString
				if table.IsVirtualTable() {
					tableType = tableTypeSystemView
					insertable = noString
				} else if table.IsView() {
					tableType = tableTypeView
					insertable = noString
				} else if table.IsTemporary() {
					tableType = tableTypeTemporary
				}
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(sc.GetName())
				tbNameStr := tree.NewDString(table.GetName())
				return addRow(
					dbNameStr,  // table_catalog
					scNameStr,  // table_schema
					tbNameStr,  // table_name
					tableType,  // table_type
					insertable, // is_insertable_into
					tree.NewDInt(tree.DInt(table.GetVersion())), // version
				)
			})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-views.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/views-table.html
var informationSchemaViewsTable = virtualSchemaTable{
	comment: `views (incomplete)
` + docs.URL("information-schema.html#views") + `
https://www.postgresql.org/docs/9.5/infoschema-views.html`,
	schema: vtable.InformationSchemaViews,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* virtual schemas have no views */
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				if !table.IsView() {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition returned,
				// unlike Postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while Postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(a-robinson): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					tree.NewDString(db.GetName()),         // table_catalog
					tree.NewDString(sc.GetName()),         // table_schema
					tree.NewDString(table.GetName()),      // table_name
					tree.NewDString(table.GetViewQuery()), // view_definition
					tree.DNull,                            // check_option
					noString,                              // is_updatable
					noString,                              // is_insertable_into
					noString,                              // is_trigger_updatable
					noString,                              // is_trigger_deletable
					noString,                              // is_trigger_insertable_into
				)
			})
	},
}

// Postgres: https://www.postgresql.org/docs/current/infoschema-collations.html
// MySQL:    https://dev.mysql.com/doc/refman/8.0/en/information-schema-collations-table.html
var informationSchemaCollations = virtualSchemaTable{
	comment: `shows the collations available in the current database
https://www.postgresql.org/docs/current/infoschema-collations.html`,
	schema: vtable.InformationSchemaCollations,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		dbNameStr := tree.NewDString(p.CurrentDatabase())
		add := func(collName string) error {
			return addRow(
				dbNameStr,
				pgCatalogNameDString,
				tree.NewDString(collName),
				// Always NO PAD (The alternative PAD SPACE is not supported.)
				tree.NewDString("NO PAD"),
			)
		}
		for _, tag := range collatedstring.Supported() {
			if err := add(tag); err != nil {
				return err
			}
		}
		return nil
	},
}

// Postgres: https://www.postgresql.org/docs/current/infoschema-collation-character-set-applicab.html
// MySQL:    https://dev.mysql.com/doc/refman/8.0/en/information-schema-collation-character-set-applicability-table.html
var informationSchemaCollationCharacterSetApplicability = virtualSchemaTable{
	comment: `identifies which character set the available collations are
applicable to. As UTF-8 is the only available encoding this table does not
provide much useful information.
https://www.postgresql.org/docs/current/infoschema-collation-character-set-applicab.html`,
	schema: vtable.InformationSchemaCollationCharacterSetApplicability,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		dbNameStr := tree.NewDString(p.CurrentDatabase())
		add := func(collName string) error {
			return addRow(
				dbNameStr,                 // collation_catalog
				pgCatalogNameDString,      // collation_schema
				tree.NewDString(collName), // collation_name
				tree.DNull,                // character_set_catalog
				tree.DNull,                // character_set_schema
				tree.NewDString("UTF8"),   // character_set_name: UTF8 is the only available encoding
			)
		}
		for _, tag := range collatedstring.Supported() {
			if err := add(tag); err != nil {
				return err
			}
		}
		return nil
	},
}

var informationSchemaSessionVariables = virtualSchemaTable{
	comment: `exposes the session variables.`,
	schema:  vtable.InformationSchemaSessionVariables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			if gen.Hidden {
				continue
			}
			value, err := gen.Get(&p.extendedEvalCtx, p.Txn())
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDString(vName),
				tree.NewDString(value),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var informationSchemaRoutinePrivilegesTable = virtualSchemaTable{
	comment: "routine_privileges was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaRoutinePrivileges,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaRoleRoutineGrantsTable = virtualSchemaTable{
	comment: "privileges granted on functions (incomplete; only contains privileges of user-defined functions)",
	schema:  vtable.InformationSchemaRoleRoutineGrants,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		var dbDescs []catalog.DatabaseDescriptor
		if db == nil {
			var err error
			dbDescs, err = p.Descriptors().GetAllDatabaseDescriptors(ctx, p.Txn())
			if err != nil {
				return err
			}
		} else {
			dbDescs = append(dbDescs, db)
		}
		for _, db := range dbDescs {
			dbNameStr := tree.NewDString(db.GetName())
			exPriv := tree.NewDString(string(privilege.EXECUTE.DisplayName()))
			roleNameForBuiltins := []*tree.DString{
				tree.NewDString(username.AdminRole),
				tree.NewDString(username.RootUser),
				tree.NewDString(username.PublicRole),
			}
			for _, name := range builtins.AllBuiltinNames() {
				parts := strings.Split(name, ".")
				if len(parts) > 2 || len(parts) == 0 {
					// This shouldn't happen in theory.
					return errors.AssertionFailedf("invalid builtin function name: %s", name)
				}

				var fnNameStr string
				var fnName *tree.DString
				var scNameStr *tree.DString
				if len(parts) == 2 {
					scNameStr = tree.NewDString(parts[0])
					fnNameStr = parts[1]
					fnName = tree.NewDString(fnNameStr)
				} else {
					scNameStr = tree.NewDString(catconstants.PgCatalogName)
					fnNameStr = name
					fnName = tree.NewDString(fnNameStr)
				}

				_, overloads := builtinsregistry.GetBuiltinProperties(name)
				for _, o := range overloads {
					fnSpecificName := tree.NewDString(nameConcatOid(fnNameStr, o.Oid))
					for _, grantee := range roleNameForBuiltins {
						if err := addRow(
							tree.DNull, // grantor
							grantee,
							dbNameStr,      // specific_catalog
							scNameStr,      // specific_schema
							fnSpecificName, // specific_name
							dbNameStr,      // routine_catalog
							scNameStr,      // routine_schema
							fnName,         // routine_name
							exPriv,         // privilege_type
							noString,       // is_grantable
						); err != nil {
							return err
						}
					}
				}
			}

			err := db.ForEachSchema(func(id descpb.ID, name string) error {
				sc, err := p.Descriptors().ByIDWithLeased(p.txn).Get().Schema(ctx, id)
				if err != nil {
					return err
				}
				return sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
					fn, err := p.Descriptors().MutableByID(p.txn).Function(ctx, sig.ID)
					if err != nil {
						return err
					}
					canSeeDescriptor, err := userCanSeeDescriptor(
						ctx, p, fn, db, false /* allowAdding */, false /* includeDropped */)
					if err != nil {
						return err
					}
					if !canSeeDescriptor {
						return nil
					}
					privs, err := fn.GetPrivileges().Show(privilege.Routine, true /* showImplicitOwnerPrivs */)
					if err != nil {
						return err
					}
					scNameStr := tree.NewDString(sc.GetName())

					fnSpecificName := tree.NewDString(nameConcatOid(fn.GetName(), catid.FuncIDToOID(fn.GetID())))
					fnName := tree.NewDString(fn.GetName())
					for _, u := range privs {
						userNameStr := tree.NewDString(u.User.Normalized())
						for _, priv := range u.Privileges {
							// We use this function to check for the grant option so that the
							// object owner also gets is_grantable=true.
							isGrantable, err := p.CheckGrantOptionsForUser(
								ctx, fn.GetPrivileges(), sc, []privilege.Kind{priv.Kind}, u.User,
							)
							if err != nil {
								return err
							}
							if err := addRow(
								tree.DNull,     // grantor
								userNameStr,    // grantee
								dbNameStr,      // specific_catalog
								scNameStr,      // specific_schema
								fnSpecificName, // specific_name
								dbNameStr,      // routine_catalog
								scNameStr,      // routine_schema
								fnName,         // routine_name
								tree.NewDString(string(priv.Kind.DisplayName())), // privilege_type
								yesOrNoDatum(isGrantable),                        // is_grantable
							); err != nil {
								return err
							}
						}
					}
					return nil
				})
			})
			if err != nil {
				return err
			}
		}
		return nil
	},
}

var informationSchemaElementTypesTable = virtualSchemaTable{
	comment: "element_types was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaElementTypes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaRoleUdtGrantsTable = virtualSchemaTable{
	comment: "role_udt_grants was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaRoleUdtGrants,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaColumnOptionsTable = virtualSchemaTable{
	comment: "column_options was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaColumnOptions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaForeignDataWrapperOptionsTable = virtualSchemaTable{
	comment: "foreign_data_wrapper_options was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaForeignDataWrapperOptions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTransformsTable = virtualSchemaTable{
	comment: "transforms was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTransforms,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaViewColumnUsageTable = virtualSchemaTable{
	comment: "view_column_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaViewColumnUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaInformationSchemaCatalogNameTable = virtualSchemaTable{
	comment: "information_schema_catalog_name was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaInformationSchemaCatalogName,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaForeignTablesTable = virtualSchemaTable{
	comment: "foreign_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaForeignTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaViewRoutineUsageTable = virtualSchemaTable{
	comment: "view_routine_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaViewRoutineUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaRoleColumnGrantsTable = virtualSchemaTable{
	comment: "role_column_grants was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaRoleColumnGrants,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaDomainConstraintsTable = virtualSchemaTable{
	comment: "domain_constraints was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaDomainConstraints,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaUserMappingsTable = virtualSchemaTable{
	comment: "user_mappings was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaUserMappings,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaCheckConstraintRoutineUsageTable = virtualSchemaTable{
	comment: "check_constraint_routine_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaCheckConstraintRoutineUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaColumnDomainUsageTable = virtualSchemaTable{
	comment: "column_domain_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaColumnDomainUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaForeignDataWrappersTable = virtualSchemaTable{
	comment: "foreign_data_wrappers was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaForeignDataWrappers,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaColumnColumnUsageTable = virtualSchemaTable{
	comment: "column_column_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaColumnColumnUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaSQLSizingTable = virtualSchemaTable{
	comment: "sql_sizing was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaSQLSizing,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaUsagePrivilegesTable = virtualSchemaTable{
	comment: "usage_privileges was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaUsagePrivileges,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaDomainsTable = virtualSchemaTable{
	comment: "domains was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaDomains,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaSQLImplementationInfoTable = virtualSchemaTable{
	comment: "sql_implementation_info was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaSQLImplementationInfo,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaUdtPrivilegesTable = virtualSchemaTable{
	comment: "udt_privileges was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaUdtPrivileges,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaPartitionsTable = virtualSchemaTable{
	comment: "partitions was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaPartitions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTablespacesExtensionsTable = virtualSchemaTable{
	comment: "tablespaces_extensions was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTablespacesExtensions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaResourceGroupsTable = virtualSchemaTable{
	comment: "resource_groups was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaResourceGroups,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaForeignServerOptionsTable = virtualSchemaTable{
	comment: "foreign_server_options was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaForeignServerOptions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaStUnitsOfMeasureTable = virtualSchemaTable{
	comment: "st_units_of_measure was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaStUnitsOfMeasure,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaSchemataExtensionsTable = virtualSchemaTable{
	comment: "schemata_extensions was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaSchemataExtensions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaColumnStatisticsTable = virtualSchemaTable{
	comment: "column_statistics was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaColumnStatistics,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaConstraintTableUsageTable = virtualSchemaTable{
	comment: "constraint_table_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaConstraintTableUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaDataTypePrivilegesTable = virtualSchemaTable{
	comment: "data_type_privileges was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaDataTypePrivileges,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaRoleUsageGrantsTable = virtualSchemaTable{
	comment: "role_usage_grants was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaRoleUsageGrants,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaFilesTable = virtualSchemaTable{
	comment: "files was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaFiles,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaEnginesTable = virtualSchemaTable{
	comment: "engines was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaEngines,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaForeignTableOptionsTable = virtualSchemaTable{
	comment: "foreign_table_options was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaForeignTableOptions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaEventsTable = virtualSchemaTable{
	comment: "events was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaEvents,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaDomainUdtUsageTable = virtualSchemaTable{
	comment: "domain_udt_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaDomainUdtUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaUserAttributesTable = virtualSchemaTable{
	comment: "user_attributes was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaUserAttributes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaKeywordsTable = virtualSchemaTable{
	comment: "keywords was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaKeywords,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaUserMappingOptionsTable = virtualSchemaTable{
	comment: "user_mapping_options was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaUserMappingOptions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaOptimizerTraceTable = virtualSchemaTable{
	comment: "optimizer_trace was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaOptimizerTrace,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTableConstraintsExtensionsTable = virtualSchemaTable{
	comment: "table_constraints_extensions was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTableConstraintsExtensions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaColumnsExtensionsTable = virtualSchemaTable{
	comment: "columns_extensions was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaColumnsExtensions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaSQLFeaturesTable = virtualSchemaTable{
	comment: "sql_features was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaSQLFeatures,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaStGeometryColumnsTable = virtualSchemaTable{
	comment: "st_geometry_columns was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaStGeometryColumns,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaSQLPartsTable = virtualSchemaTable{
	comment: "sql_parts was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaSQLParts,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaPluginsTable = virtualSchemaTable{
	comment: "plugins was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaPlugins,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaStSpatialReferenceSystemsTable = virtualSchemaTable{
	comment: "st_spatial_reference_systems was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaStSpatialReferenceSystems,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaProcesslistTable = virtualSchemaTable{
	comment: "processlist was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaProcesslist,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaForeignServersTable = virtualSchemaTable{
	comment: "foreign_servers was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaForeignServers,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTriggeredUpdateColumnsTable = virtualSchemaTable{
	comment: "triggered_update_columns was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTriggeredUpdateColumns,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTriggersTable = virtualSchemaTable{
	comment: "triggers was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTriggers,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTablesExtensionsTable = virtualSchemaTable{
	comment: "tables_extensions was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTablesExtensions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaProfilingTable = virtualSchemaTable{
	comment: "profiling was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaProfiling,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaTablespacesTable = virtualSchemaTable{
	comment: "tablespaces was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaTablespaces,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var informationSchemaViewTableUsageTable = virtualSchemaTable{
	comment: "view_table_usage was created for compatibility and is currently unimplemented",
	schema:  vtable.InformationSchemaViewTableUsage,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

// forEachSchema iterates over the physical and virtual schemas.
func forEachSchema(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	requiresPrivileges bool,
	fn func(ctx context.Context, sc catalog.SchemaDescriptor) error,
) error {
	forEachDatabase := func(db catalog.DatabaseDescriptor) error {
		c, err := p.Descriptors().GetAllSchemasInDatabase(ctx, p.txn, db)
		if err != nil {
			return err
		}
		var schemas []catalog.SchemaDescriptor
		if err := c.ForEachDescriptor(func(desc catalog.Descriptor) error {
			if requiresPrivileges {
				canSeeDescriptor, err := userCanSeeDescriptor(
					ctx, p, desc, db, false /* allowAdding */, false /* includeDropped */)
				if err != nil {
					return err
				}
				if !canSeeDescriptor {
					return nil
				}
			}
			sc, err := catalog.AsSchemaDescriptor(desc)
			schemas = append(schemas, sc)
			return err
		}); err != nil {
			return err
		}
		sort.Slice(schemas, func(i int, j int) bool {
			return schemas[i].GetName() < schemas[j].GetName()
		})
		for _, sc := range schemas {
			if err := fn(ctx, sc); err != nil {
				return err
			}
		}
		return nil
	}

	if dbContext != nil {
		return iterutil.Map(forEachDatabase(dbContext))
	}
	c, err := p.Descriptors().GetAllDatabases(ctx, p.txn)
	if err != nil {
		return err
	}
	return c.ForEachDescriptor(func(desc catalog.Descriptor) error {
		db, err := catalog.AsDatabaseDescriptor(desc)
		if err != nil {
			return err
		}
		return forEachDatabase(db)
	})
}

// forEachDatabaseDesc calls a function for the given DatabaseDescriptor, or if
// it is nil, retrieves all database descriptors and iterates through them in
// lexicographical order with respect to their name. If privileges are required,
// the function is only called if the user has privileges on the database.
func forEachDatabaseDesc(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	requiresPrivileges bool,
	fn func(ctx context.Context, descriptor catalog.DatabaseDescriptor) error,
) error {
	var dbDescs []catalog.DatabaseDescriptor
	if dbContext == nil {
		allDbDescs, err := p.Descriptors().GetAllDatabaseDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		dbDescs = allDbDescs
	} else {
		dbDescs = append(dbDescs, dbContext)
	}

	// Ignore databases that the user cannot see. We add a special case for the
	// current database. This is because we currently allow a user to connect
	// to a database even without the CONNECT privilege, but it would be poor
	// UX to not show the current database in pg_catalog/information_schema
	// tables.
	// See https://github.com/cockroachdb/cockroach/issues/59875.
	for _, dbDesc := range dbDescs {
		canSeeDescriptor := !requiresPrivileges
		if requiresPrivileges {
			hasPriv, err := userCanSeeDescriptor(ctx, p, dbDesc, nil /* parentDBDesc */, false /* allowAdding */, false /* includeDropped */)
			if err != nil {
				return err
			}
			canSeeDescriptor = hasPriv || p.CurrentDatabase() == dbDesc.GetName()
		}
		if canSeeDescriptor {
			if err := fn(ctx, dbDesc); err != nil {
				return err
			}
		}
	}

	return nil
}

// forEachTypeDesc calls a function for each TypeDescriptor. If dbContext is
// not nil, then the function is called for only TypeDescriptors within the
// given database.
func forEachTypeDesc(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	fn func(ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, typ catalog.TypeDescriptor) error,
) (err error) {
	var all nstree.Catalog
	if dbContext != nil &&
		useIndexLookupForDescriptorsInDatabase.Get(&p.EvalContext().Settings.SV) {
		all, err = p.Descriptors().GetAllDescriptorsForDatabase(ctx, p.txn, dbContext)
	} else {
		all, err = p.Descriptors().GetAllDescriptors(ctx, p.txn)
	}
	if err != nil {
		return err
	}
	lCtx := newInternalLookupCtx(all.OrderedDescriptors(), dbContext)
	for _, id := range lCtx.typIDs {
		typ := lCtx.typDescs[id]
		dbDesc, err := lCtx.getDatabaseByID(typ.GetParentID())
		if err != nil {
			continue
		}
		sc, err := lCtx.getSchemaByID(typ.GetParentSchemaID())
		if err != nil {
			return err
		}
		canSeeDescriptor, err := userCanSeeDescriptor(
			ctx, p, typ, dbDesc, false /* allowAdding */, false /* includeDropped */)
		if err != nil {
			return err
		}
		if !canSeeDescriptor {
			continue
		}
		if err := fn(ctx, dbDesc, sc, typ); err != nil {
			return err
		}
	}
	return nil
}

type virtualOpts int

const (
	// virtualMany iterates over virtual schemas in every catalog/database.
	virtualMany virtualOpts = iota
	// virtualCurrentDB iterates over virtual schemas in the current database.
	virtualCurrentDB
	// hideVirtual completely hides virtual schemas during iteration.
	hideVirtual
)

type forEachTableDescOptions struct {
	virtualOpts virtualOpts
	allowAdding bool
	// Include dropped tables (but not garbage collected) when run at the cluster
	// level (i.e., when database = ""). This only works when the scope is at the
	// cluster level because `GetAllDescriptorsForDatabase` doesn't include such
	// tables.
	includeDropped bool
}

type tableDescContext struct {
	database    catalog.DatabaseDescriptor
	schema      catalog.SchemaDescriptor
	table       catalog.TableDescriptor
	tableLookup tableLookupFn
}

// forEachTableDesc retrieves all table descriptors from the current
// database and all system databases and iterates through them. For
// each table, the function will call fn with its respective database
// and table descriptor.
//
// The dbContext argument specifies in which database context we are
// requesting the descriptors. In context nil all descriptors are
// visible, in non-empty contexts only the descriptors of that
// database are visible.
//
// The virtualOpts argument specifies how virtual tables are made
// visible.
func forEachTableDesc(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	opts forEachTableDescOptions,
	fn func(context.Context, tableDescContext) error,
) (err error) {
	var all nstree.Catalog
	if dbContext != nil && useIndexLookupForDescriptorsInDatabase.Get(&p.EvalContext().Settings.SV) {
		all, err = p.Descriptors().GetAllDescriptorsForDatabase(ctx, p.txn, dbContext)
	} else {
		all, err = p.Descriptors().GetAllDescriptors(ctx, p.txn)
	}
	if err != nil {
		return err
	}
	return forEachTableDescFromDescriptors(
		ctx, p, dbContext, all, opts, fn)
}

func forEachTableDescFromDescriptors(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	c nstree.Catalog,
	opts forEachTableDescOptions,
	fn func(context.Context, tableDescContext) error,
) error {
	lCtx := newInternalLookupCtx(c.OrderedDescriptors(), dbContext)

	vOpts := opts.virtualOpts
	if vOpts == virtualMany || vOpts == virtualCurrentDB {
		// Virtual descriptors first.
		vt := p.getVirtualTabler()
		vEntries := vt.getSchemas()
		vSchemaOrderedNames := vt.getSchemaNames()
		iterate := func(dbDesc catalog.DatabaseDescriptor) error {
			for _, virtSchemaName := range vSchemaOrderedNames {
				virtSchemaEntry := vEntries[virtSchemaName]
				for _, tName := range virtSchemaEntry.orderedDefNames {
					te := virtSchemaEntry.defs[tName]
					if err := fn(ctx, tableDescContext{
						dbDesc, virtSchemaEntry.desc, te.desc, lCtx}); err != nil {
						return err
					}
				}
			}
			return nil
		}

		switch vOpts {
		case virtualCurrentDB:
			if err := iterate(dbContext); err != nil {
				return err
			}
		case virtualMany:
			for _, dbID := range lCtx.dbIDs {
				dbDesc := lCtx.dbDescs[dbID]
				if err := iterate(dbDesc); err != nil {
					return err
				}
			}
		}
	}

	// Physical descriptors next.
	for _, tbID := range lCtx.tbIDs {
		table := lCtx.tbDescs[tbID]
		dbDesc, parentExists := lCtx.dbDescs[table.GetParentID()]
		canSeeDescriptor, err := userCanSeeDescriptor(
			ctx, p, table, dbDesc, opts.allowAdding, opts.includeDropped)
		if err != nil {
			return err
		}
		if !canSeeDescriptor {
			continue
		}
		var sc catalog.SchemaDescriptor
		if parentExists {
			sc, err = lCtx.getSchemaByID(table.GetParentSchemaID())
			if err != nil && !table.IsTemporary() {
				return err
			} else if table.IsTemporary() {
				// Look up the schemas for this database if we discover that there is a
				// missing temporary schema name. Temporary schemas have namespace
				// entries. The below code will go and lookup schema names from the
				// namespace table if needed to qualify the name of a temporary table.
				if err := forEachSchema(ctx, p, dbDesc, false /* requiresPrivileges*/, func(ctx context.Context, schema catalog.SchemaDescriptor) error {
					if schema.GetID() != table.GetParentSchemaID() {
						return nil
					}
					_, exists, err := lCtx.GetSchemaName(ctx, schema.GetID(), dbDesc.GetID(), p.ExecCfg().Settings.Version)
					if err != nil || exists {
						return err
					}
					sc = schema
					lCtx.schemaNames[sc.GetID()] = sc.GetName()
					lCtx.schemaDescs[sc.GetID()] = sc
					lCtx.schemaIDs = append(lCtx.schemaIDs, sc.GetID())
					return nil
				}); err != nil {
					return errors.Wrapf(err, "failed to look up schema id %d", table.GetParentSchemaID())
				}
				if sc == nil {
					sc = schemadesc.NewTemporarySchema(catconstants.PgTempSchemaName, table.GetParentSchemaID(), dbDesc.GetID())
				}
			}
		}
		if err := fn(ctx, tableDescContext{dbDesc, sc, table, lCtx}); err != nil {
			return err
		}
	}
	return nil
}

func forEachTypeDescWithTableLookupInternalFromDescriptors(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	allowAdding bool,
	c nstree.Catalog,
	fn func(context.Context, catalog.DatabaseDescriptor, catalog.SchemaDescriptor, catalog.TypeDescriptor, tableLookupFn) error,
) error {
	lCtx := newInternalLookupCtx(c.OrderedDescriptors(), dbContext)

	for _, typID := range lCtx.typIDs {
		typDesc := lCtx.typDescs[typID]
		if typDesc.Dropped() {
			continue
		}
		dbDesc, err := lCtx.getDatabaseByID(typDesc.GetParentID())
		if err != nil {
			return err
		}
		canSeeDescriptor, err := userCanSeeDescriptor(
			ctx, p, typDesc, dbDesc, allowAdding, false /* includeDropped */)
		if err != nil {
			return err
		}
		if !canSeeDescriptor {
			continue
		}
		sc, err := lCtx.getSchemaByID(typDesc.GetParentSchemaID())
		if err != nil {
			return err
		}
		if err := fn(ctx, dbDesc, sc, typDesc, lCtx); err != nil {
			return err
		}
	}
	return nil
}

type roleOptions struct {
	*tree.DJSON
}

func (r roleOptions) noLogin() (tree.DBool, error) {
	nologin, err := r.Exists("NOLOGIN")
	return tree.DBool(nologin), err
}

func (r roleOptions) validUntil(p *planner) (tree.Datum, error) {
	const validUntilKey = "VALID UNTIL"
	jsonValue, err := r.FetchValKey(validUntilKey)
	if err != nil {
		return nil, err
	}
	if jsonValue == nil {
		return tree.DNull, nil
	}
	validUntilText, err := jsonValue.AsText()
	if err != nil {
		return nil, err
	}
	if validUntilText == nil {
		return tree.DNull, nil
	}
	validUntil, _, err := pgdate.ParseTimestamp(
		p.EvalContext().GetRelativeParseTime(),
		pgdate.DefaultDateStyle(),
		*validUntilText,
		nil, /* h */
	)
	if err != nil {
		return nil, errors.Errorf("rolValidUntil string %s could not be parsed with datestyle %s", *validUntilText, p.EvalContext().GetDateStyle())
	}
	return tree.MakeDTimestampTZ(validUntil, time.Second)
}

func (r roleOptions) createDB() (tree.DBool, error) {
	createDB, err := r.Exists("CREATEDB")
	return tree.DBool(createDB), err
}

func (r roleOptions) createRole() (tree.DBool, error) {
	createRole, err := r.Exists("CREATEROLE")
	return tree.DBool(createRole), err
}

// forEachRoleAtCacheReadTS reads from system.users and related tables using a
// timestamp based on when the role membership cache was refreshed.
func forEachRoleAtCacheReadTS(
	ctx context.Context,
	p *planner,
	fn func(ctx context.Context, userName username.SQLUsername, isRole bool, options roleOptions, settings tree.Datum) error,
) error {
	const query = `
SELECT
	u.username,
	"isRole",
  drs.settings,
	json_object_agg(COALESCE(ro.option, 'null'), ro.value)
FROM
	system.users AS u
	LEFT JOIN system.role_options AS ro ON
			ro.username = u.username
  LEFT JOIN system.database_role_settings AS drs ON
			drs.role_name = u.username AND drs.database_id = 0
GROUP BY
	u.username, "isRole", drs.settings;`

	var rows []tree.Datums
	if err := p.ExecCfg().RoleMemberCache.RunAtCacheReadTS(
		ctx, p.ExecCfg().InternalDB, p.InternalSQLTxn(),
		func(ctx context.Context, txn descs.Txn) error {
			// For some reason, using the iterator API here causes privilege_builtins
			// logic test fail in 3node-tenant config with 'txn already encountered an
			// error' (because of the context cancellation), so we buffer all roles
			// first.
			var err error
			rows, err = txn.QueryBufferedEx(
				ctx, "read-roles", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				query,
			)
			if err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}

	for _, row := range rows {
		usernameS := tree.MustBeDString(row[0])
		isRole, ok := row[1].(*tree.DBool)
		if !ok {
			return errors.Errorf("isRole should be a boolean value, found %s instead", row[1].ResolvedType())
		}

		defaultSettings := row[2]
		roleOptionsJSON, ok := row[3].(*tree.DJSON)
		if !ok {
			return errors.Errorf("roleOptionJson should be a JSON value, found %s instead", row[3].ResolvedType())
		}
		options := roleOptions{roleOptionsJSON}

		// system tables already contain normalized usernames.
		userName := username.MakeSQLUsernameFromPreNormalizedString(string(usernameS))
		if err := fn(ctx, userName, bool(*isRole), options, defaultSettings); err != nil {
			return err
		}
	}

	// Add a row for the internal `node` user. It does not exist in system.users
	// since you can't log in as it, but it does own objects like the system
	// database, so it should be viewable in introspection.
	nodeOptionsJSON, err := json.MakeJSON(map[string]interface{}{roleoption.NOLOGIN.String(): true})
	if err != nil {
		return err
	}
	nodeOptions := roleOptions{tree.NewDJSON(nodeOptionsJSON)}
	if err := fn(ctx, username.NodeUserName(), false /* isRole */, nodeOptions, tree.DNull /* settings */); err != nil {
		return err
	}

	return nil
}

// forEachRoleMembershipAtCacheReadTS reads from system.role_members using a
// timestamp based on when the role membership cache was refreshed.
func forEachRoleMembershipAtCacheReadTS(
	ctx context.Context,
	p *planner,
	fn func(ctx context.Context, role, member username.SQLUsername, isAdmin bool) error,
) error {
	const query = `SELECT "role", "member", "isAdmin" FROM system.role_members`

	if err := p.ExecCfg().RoleMemberCache.RunAtCacheReadTS(
		ctx, p.ExecCfg().InternalDB, p.InternalSQLTxn(),
		func(ctx context.Context, txn descs.Txn) (retErr error) {
			it, err := txn.QueryIteratorEx(ctx, "read-members", txn.KV(),
				sessiondata.NodeUserSessionDataOverride, query)
			if err != nil {
				return err
			}
			// We have to make sure to close the iterator since we might return from the
			// for loop early (before Next() returns false).
			defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

			var ok bool
			var loopErr error
			for ok, loopErr = it.Next(ctx); ok; ok, loopErr = it.Next(ctx) {
				row := it.Cur()
				roleName := tree.MustBeDString(row[0])
				memberName := tree.MustBeDString(row[1])
				isAdmin := row[2].(*tree.DBool)

				// The names in the system tables are already normalized.
				if err := fn(
					ctx,
					username.MakeSQLUsernameFromPreNormalizedString(string(roleName)),
					username.MakeSQLUsernameFromPreNormalizedString(string(memberName)),
					bool(*isAdmin)); err != nil {
					return err
				}
			}
			if loopErr != nil {
				return loopErr
			}
			return nil
		},
	); err != nil {
		return err
	}
	return nil
}

func userCanSeeDescriptor(
	ctx context.Context,
	p *planner,
	desc, parentDBDesc catalog.Descriptor,
	allowAdding bool,
	includeDropped bool,
) (bool, error) {
	if !descriptorIsVisible(desc, allowAdding, includeDropped) {
		return false, nil
	}

	// Short-circuit for virtual tables, so that we avoid fetching the synthetic
	// privileges for virtual tables in a thundering herd while populating a table
	// like pg_class, which has a row for every table, including virtual tables.
	if tab, ok := desc.(catalog.TableDescriptor); ok && tab.IsVirtualTable() {
		return true, nil
	}

	// TODO(richardjcai): We may possibly want to remove the ability to view
	// the descriptor if they have any privilege on the descriptor and only
	// allow the descriptor to be viewed if they have CONNECT on the DB. #59827.
	if ok, err := p.HasAnyPrivilege(ctx, desc); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}

	// Users can see objects in the database if they have connect privilege.
	if parentDBDesc != nil {
		if ok, err := p.HasPrivilege(ctx, parentDBDesc, privilege.CONNECT, p.User()); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	return false, nil
}

func descriptorIsVisible(desc catalog.Descriptor, allowAdding bool, includeDropped bool) bool {
	return desc.Public() || (allowAdding && desc.Adding()) || (includeDropped && desc.Dropped())
}

// nameConcatOid is a Go version of the nameconcatoid builtin function. The
// result is the same as fmt.Sprintf("%s_%d", s, o) except that, if it would not
// fit in 63 characters, we make it do so by truncating the name input (not the
// oid).
func nameConcatOid(s string, o oid.Oid) string {
	const maxLen = 63
	oidStr := strconv.Itoa(int(o))
	if len(s)+1+len(oidStr) <= maxLen {
		return s + "_" + oidStr
	}
	return s[:maxLen-1-len(oidStr)] + "_" + oidStr
}
