// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"golang.org/x/text/collate"
)

var (
	oidZero   = tree.NewDOid(0)
	zeroVal   = tree.DZero
	negOneVal = tree.NewDInt(-1)

	passwdStarString = tree.NewDString("********")
)

const (
	indexTypeForwardIndex  = "prefix"
	indexTypeInvertedIndex = "inverted"
	defaultCollationTag    = "en-US"
)

// Bitmasks for pg_index.indoption. Each column in the index has a bitfield
// indicating how the columns are indexed. The constants below are the same as
// the ones in Postgres:
// https://github.com/postgres/postgres/blob/b6423e92abfadaa1ed9642319872aa1654403cd6/src/include/catalog/pg_index.h#L70-L76
const (
	// indoptionDesc indicates that the values in the index are in reverse order.
	indoptionDesc = 0x01
	// indoptionNullsFirst indicates that NULLs appear first in the index.
	indoptionNullsFirst = 0x02
)

var forwardIndexOid = stringOid(indexTypeForwardIndex)
var invertedIndexOid = stringOid(indexTypeInvertedIndex)

// pgCatalog contains a set of system tables mirroring PostgreSQL's pg_catalog schema.
// This code attempts to comply as closely as possible to the system catalogs documented
// in https://www.postgresql.org/docs/9.6/static/catalogs.html.
var pgCatalog = virtualSchema{
	name: pgCatalogName,
	allTableNames: buildStringSet(
		// Generated with:
		// select distinct '"'||table_name||'",' from information_schema.tables
		//    where table_schema='pg_catalog' order by table_name;
		"pg_aggregate",
		"pg_am",
		"pg_amop",
		"pg_amproc",
		"pg_attrdef",
		"pg_attribute",
		"pg_auth_members",
		"pg_authid",
		"pg_available_extension_versions",
		"pg_available_extensions",
		"pg_cast",
		"pg_class",
		"pg_collation",
		"pg_config",
		"pg_constraint",
		"pg_conversion",
		"pg_cursors",
		"pg_database",
		"pg_db_role_setting",
		"pg_default_acl",
		"pg_depend",
		"pg_description",
		"pg_enum",
		"pg_event_trigger",
		"pg_extension",
		"pg_file_settings",
		"pg_foreign_data_wrapper",
		"pg_foreign_server",
		"pg_foreign_table",
		"pg_group",
		"pg_hba_file_rules",
		"pg_index",
		"pg_indexes",
		"pg_inherits",
		"pg_init_privs",
		"pg_language",
		"pg_largeobject",
		"pg_largeobject_metadata",
		"pg_locks",
		"pg_matviews",
		"pg_namespace",
		"pg_opclass",
		"pg_operator",
		"pg_opfamily",
		"pg_partitioned_table",
		"pg_pltemplate",
		"pg_policies",
		"pg_policy",
		"pg_prepared_statements",
		"pg_prepared_xacts",
		"pg_proc",
		"pg_publication",
		"pg_publication_rel",
		"pg_publication_tables",
		"pg_range",
		"pg_replication_origin",
		"pg_replication_origin_status",
		"pg_replication_slots",
		"pg_rewrite",
		"pg_roles",
		"pg_rules",
		"pg_seclabel",
		"pg_seclabels",
		"pg_sequence",
		"pg_sequences",
		"pg_settings",
		"pg_shadow",
		"pg_shdepend",
		"pg_shdescription",
		"pg_shseclabel",
		"pg_stat_activity",
		"pg_stat_all_indexes",
		"pg_stat_all_tables",
		"pg_stat_archiver",
		"pg_stat_bgwriter",
		"pg_stat_database",
		"pg_stat_database_conflicts",
		"pg_stat_progress_vacuum",
		"pg_stat_replication",
		"pg_stat_ssl",
		"pg_stat_subscription",
		"pg_stat_sys_indexes",
		"pg_stat_sys_tables",
		"pg_stat_user_functions",
		"pg_stat_user_indexes",
		"pg_stat_user_tables",
		"pg_stat_wal_receiver",
		"pg_stat_xact_all_tables",
		"pg_stat_xact_sys_tables",
		"pg_stat_xact_user_functions",
		"pg_stat_xact_user_tables",
		"pg_statio_all_indexes",
		"pg_statio_all_sequences",
		"pg_statio_all_tables",
		"pg_statio_sys_indexes",
		"pg_statio_sys_sequences",
		"pg_statio_sys_tables",
		"pg_statio_user_indexes",
		"pg_statio_user_sequences",
		"pg_statio_user_tables",
		"pg_statistic",
		"pg_statistic_ext",
		"pg_stats",
		"pg_subscription",
		"pg_subscription_rel",
		"pg_tables",
		"pg_tablespace",
		"pg_timezone_abbrevs",
		"pg_timezone_names",
		"pg_transform",
		"pg_trigger",
		"pg_ts_config",
		"pg_ts_config_map",
		"pg_ts_dict",
		"pg_ts_parser",
		"pg_ts_template",
		"pg_type",
		"pg_user",
		"pg_user_mapping",
		"pg_user_mappings",
		"pg_views",
	),
	tableDefs: map[sqlbase.ID]virtualSchemaDef{
		sqlbase.PgCatalogAmTableID:                  pgCatalogAmTable,
		sqlbase.PgCatalogAttrDefTableID:             pgCatalogAttrDefTable,
		sqlbase.PgCatalogAttributeTableID:           pgCatalogAttributeTable,
		sqlbase.PgCatalogAuthIDTableID:              pgCatalogAuthIDTable,
		sqlbase.PgCatalogAuthMembersTableID:         pgCatalogAuthMembersTable,
		sqlbase.PgCatalogAvailableExtensionsTableID: pgCatalogAvailableExtensionsTable,
		sqlbase.PgCatalogCastTableID:                pgCatalogCastTable,
		sqlbase.PgCatalogClassTableID:               pgCatalogClassTable,
		sqlbase.PgCatalogCollationTableID:           pgCatalogCollationTable,
		sqlbase.PgCatalogConstraintTableID:          pgCatalogConstraintTable,
		sqlbase.PgCatalogConversionTableID:          pgCatalogConversionTable,
		sqlbase.PgCatalogDatabaseTableID:            pgCatalogDatabaseTable,
		sqlbase.PgCatalogDefaultACLTableID:          pgCatalogDefaultACLTable,
		sqlbase.PgCatalogDependTableID:              pgCatalogDependTable,
		sqlbase.PgCatalogDescriptionTableID:         pgCatalogDescriptionTable,
		sqlbase.PgCatalogSharedDescriptionTableID:   pgCatalogSharedDescriptionTable,
		sqlbase.PgCatalogEnumTableID:                pgCatalogEnumTable,
		sqlbase.PgCatalogEventTriggerTableID:        pgCatalogEventTriggerTable,
		sqlbase.PgCatalogExtensionTableID:           pgCatalogExtensionTable,
		sqlbase.PgCatalogForeignDataWrapperTableID:  pgCatalogForeignDataWrapperTable,
		sqlbase.PgCatalogForeignServerTableID:       pgCatalogForeignServerTable,
		sqlbase.PgCatalogForeignTableTableID:        pgCatalogForeignTableTable,
		sqlbase.PgCatalogIndexTableID:               pgCatalogIndexTable,
		sqlbase.PgCatalogIndexesTableID:             pgCatalogIndexesTable,
		sqlbase.PgCatalogInheritsTableID:            pgCatalogInheritsTable,
		sqlbase.PgCatalogLanguageTableID:            pgCatalogLanguageTable,
		sqlbase.PgCatalogLocksTableID:               pgCatalogLocksTable,
		sqlbase.PgCatalogMatViewsTableID:            pgCatalogMatViewsTable,
		sqlbase.PgCatalogNamespaceTableID:           pgCatalogNamespaceTable,
		sqlbase.PgCatalogOperatorTableID:            pgCatalogOperatorTable,
		sqlbase.PgCatalogPreparedStatementsTableID:  pgCatalogPreparedStatementsTable,
		sqlbase.PgCatalogPreparedXactsTableID:       pgCatalogPreparedXactsTable,
		sqlbase.PgCatalogProcTableID:                pgCatalogProcTable,
		sqlbase.PgCatalogAggregateTableID:           pgCatalogAggregateTable,
		sqlbase.PgCatalogRangeTableID:               pgCatalogRangeTable,
		sqlbase.PgCatalogRewriteTableID:             pgCatalogRewriteTable,
		sqlbase.PgCatalogRolesTableID:               pgCatalogRolesTable,
		sqlbase.PgCatalogSecLabelsTableID:           pgCatalogSecLabelsTable,
		sqlbase.PgCatalogSequencesTableID:           pgCatalogSequencesTable,
		sqlbase.PgCatalogSettingsTableID:            pgCatalogSettingsTable,
		sqlbase.PgCatalogShdependTableID:            pgCatalogShdependTable,
		sqlbase.PgCatalogUserTableID:                pgCatalogUserTable,
		sqlbase.PgCatalogUserMappingTableID:         pgCatalogUserMappingTable,
		sqlbase.PgCatalogTablesTableID:              pgCatalogTablesTable,
		sqlbase.PgCatalogTablespaceTableID:          pgCatalogTablespaceTable,
		sqlbase.PgCatalogTriggerTableID:             pgCatalogTriggerTable,
		sqlbase.PgCatalogTypeTableID:                pgCatalogTypeTable,
		sqlbase.PgCatalogViewsTableID:               pgCatalogViewsTable,
		sqlbase.PgCatalogStatActivityTableID:        pgCatalogStatActivityTable,
		sqlbase.PgCatalogSecurityLabelTableID:       pgCatalogSecurityLabelTable,
		sqlbase.PgCatalogSharedSecurityLabelTableID: pgCatalogSharedSecurityLabelTable,
	},
	// Postgres's catalogs are ill-defined when there is no current
	// database set. Simply reject any attempts to use them in that
	// case.
	validWithNoDatabaseContext: false,
	containsTypes:              true,
}

// The catalog pg_am stores information about relation access methods.
// It's important to note that this table changed drastically between Postgres
// versions 9.5 and 9.6. We currently support both versions of this table.
// See: https://www.postgresql.org/docs/9.5/static/catalog-pg-am.html and
// https://www.postgresql.org/docs/9.6/static/catalog-pg-am.html.
var pgCatalogAmTable = virtualSchemaTable{
	comment: `index access methods (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-am.html`,
	schema: `
CREATE TABLE pg_catalog.pg_am (
	oid OID,
	amname NAME,
	amstrategies INT2,
	amsupport INT2,
	amcanorder BOOL,
	amcanorderbyop BOOL,
	amcanbackward BOOL,
	amcanunique BOOL,
	amcanmulticol BOOL,
	amoptionalkey BOOL,
	amsearcharray BOOL,
	amsearchnulls BOOL,
	amstorage BOOL,
	amclusterable BOOL,
	ampredlocks BOOL,
	amkeytype OID,
	aminsert OID,
	ambeginscan OID,
	amgettuple OID,
	amgetbitmap OID,
	amrescan OID,
	amendscan OID,
	ammarkpos OID,
	amrestrpos OID,
	ambuild OID,
	ambuildempty OID,
	ambulkdelete OID,
	amvacuumcleanup OID,
	amcanreturn OID,
	amcostestimate OID,
	amoptions OID,
	amhandler OID,
	amtype CHAR
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// add row for forward indexes
		if err := addRow(
			forwardIndexOid,                      // oid - all versions
			tree.NewDName(indexTypeForwardIndex), // amname - all versions
			zeroVal,                              // amstrategies - < v9.6
			zeroVal,                              // amsupport - < v9.6
			tree.DBoolTrue,                       // amcanorder - < v9.6
			tree.DBoolFalse,                      // amcanorderbyop - < v9.6
			tree.DBoolTrue,                       // amcanbackward - < v9.6
			tree.DBoolTrue,                       // amcanunique - < v9.6
			tree.DBoolTrue,                       // amcanmulticol - < v9.6
			tree.DBoolTrue,                       // amoptionalkey - < v9.6
			tree.DBoolTrue,                       // amsearcharray - < v9.6
			tree.DBoolTrue,                       // amsearchnulls - < v9.6
			tree.DBoolFalse,                      // amstorage - < v9.6
			tree.DBoolFalse,                      // amclusterable - < v9.6
			tree.DBoolFalse,                      // ampredlocks - < v9.6
			oidZero,                              // amkeytype - < v9.6
			tree.DNull,                           // aminsert - < v9.6
			tree.DNull,                           // ambeginscan - < v9.6
			oidZero,                              // amgettuple - < v9.6
			oidZero,                              // amgetbitmap - < v9.6
			tree.DNull,                           // amrescan - < v9.6
			tree.DNull,                           // amendscan - < v9.6
			tree.DNull,                           // ammarkpos - < v9.6
			tree.DNull,                           // amrestrpos - < v9.6
			tree.DNull,                           // ambuild - < v9.6
			tree.DNull,                           // ambuildempty - < v9.6
			tree.DNull,                           // ambulkdelete - < v9.6
			tree.DNull,                           // amvacuumcleanup - < v9.6
			tree.DNull,                           // amcanreturn - < v9.6
			tree.DNull,                           // amcostestimate - < v9.6
			tree.DNull,                           // amoptions - < v9.6
			tree.DNull,                           // amhandler - > v9.6
			tree.NewDString("i"),                 // amtype - > v9.6
		); err != nil {
			return err
		}

		// add row for inverted indexes
		if err := addRow(
			invertedIndexOid,                      // oid - all versions
			tree.NewDName(indexTypeInvertedIndex), // amname - all versions
			zeroVal,                               // amstrategies - < v9.6
			zeroVal,                               // amsupport - < v9.6
			tree.DBoolFalse,                       // amcanorder - < v9.6
			tree.DBoolFalse,                       // amcanorderbyop - < v9.6
			tree.DBoolFalse,                       // amcanbackward - < v9.6
			tree.DBoolFalse,                       // amcanunique - < v9.6
			tree.DBoolFalse,                       // amcanmulticol - < v9.6
			tree.DBoolFalse,                       // amoptionalkey - < v9.6
			tree.DBoolFalse,                       // amsearcharray - < v9.6
			tree.DBoolTrue,                        // amsearchnulls - < v9.6
			tree.DBoolFalse,                       // amstorage - < v9.6
			tree.DBoolFalse,                       // amclusterable - < v9.6
			tree.DBoolFalse,                       // ampredlocks - < v9.6
			oidZero,                               // amkeytype - < v9.6
			tree.DNull,                            // aminsert - < v9.6
			tree.DNull,                            // ambeginscan - < v9.6
			oidZero,                               // amgettuple - < v9.6
			oidZero,                               // amgetbitmap - < v9.6
			tree.DNull,                            // amrescan - < v9.6
			tree.DNull,                            // amendscan - < v9.6
			tree.DNull,                            // ammarkpos - < v9.6
			tree.DNull,                            // amrestrpos - < v9.6
			tree.DNull,                            // ambuild - < v9.6
			tree.DNull,                            // ambuildempty - < v9.6
			tree.DNull,                            // ambulkdelete - < v9.6
			tree.DNull,                            // amvacuumcleanup - < v9.6
			tree.DNull,                            // amcanreturn - < v9.6
			tree.DNull,                            // amcostestimate - < v9.6
			tree.DNull,                            // amoptions - < v9.6
			tree.DNull,                            // amhandler - > v9.6
			tree.NewDString("i"),                  // amtype - > v9.6
		); err != nil {
			return err
		}
		return nil
	},
}

var pgCatalogAttrDefTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`column default values
https://www.postgresql.org/docs/9.5/catalog-pg-attrdef.html`,
	`
CREATE TABLE pg_catalog.pg_attrdef (
	oid OID,
	adrelid OID NOT NULL,
	adnum INT2,
	adbin STRING,
	adsrc STRING,
  INDEX(adrelid)
)`,
	virtualMany, false, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *sqlbase.ImmutableDatabaseDescriptor, scName string,
		table *sqlbase.ImmutableTableDescriptor,
		lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error) error {
		colNum := 0
		return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
			colNum++
			if column.DefaultExpr == nil {
				// pg_attrdef only expects rows for columns with default values.
				return nil
			}
			var defSrc *tree.DString
			expr, err := parser.ParseExpr(*column.DefaultExpr)
			if err != nil {
				defSrc = tree.NewDString(*column.DefaultExpr)
			} else {
				// Use type check to resolve types in expr. We don't use
				// DeserializeTableDescExpr here because that returns a typed
				// expression under the hood. Since typed expressions don't contain
				// type annotations, we wouldn't format some defaults as intended.
				if _, err := expr.TypeCheck(ctx, &p.semaCtx, types.Any); err != nil {
					return err
				}
				ctx := tree.NewFmtCtx(tree.FmtPGAttrdefAdbin)
				ctx.FormatNode(expr)
				defSrc = tree.NewDString(ctx.String())
			}
			return addRow(
				h.ColumnOid(table.ID, column.ID),                     // oid
				tableOid(table.ID),                                   // adrelid
				tree.NewDInt(tree.DInt(column.GetLogicalColumnID())), // adnum
				defSrc, // adbin
				defSrc, // adsrc
			)
		})
	})

var pgCatalogAttributeTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`table columns (incomplete - see also information_schema.columns)
https://www.postgresql.org/docs/9.5/catalog-pg-attribute.html`,
	`
CREATE TABLE pg_catalog.pg_attribute (
	attrelid OID NOT NULL,
	attname NAME,
	atttypid OID,
	attstattarget INT4,
	attlen INT2,
	attnum INT2,
	attndims INT4,
	attcacheoff INT4,
	atttypmod INT4,
	attbyval BOOL,
	attstorage CHAR,
	attalign CHAR,
	attnotnull BOOL,
	atthasdef BOOL,
	attisdropped BOOL,
	attislocal BOOL,
	attinhcount INT4,
	attcollation OID,
	attacl STRING[],
	attoptions STRING[],
	attfdwoptions STRING[],
  INDEX(attrelid)
)`,
	virtualMany, true, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *sqlbase.ImmutableDatabaseDescriptor, scName string,
		table *sqlbase.ImmutableTableDescriptor,
		lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error) error {
		// addColumn adds adds either a table or a index column to the pg_attribute table.
		addColumn := func(column *sqlbase.ColumnDescriptor, attRelID tree.Datum, colID sqlbase.ColumnID) error {
			colTyp := column.Type
			return addRow(
				attRelID,                       // attrelid
				tree.NewDName(column.Name),     // attname
				typOid(colTyp),                 // atttypid
				zeroVal,                        // attstattarget
				typLen(colTyp),                 // attlen
				tree.NewDInt(tree.DInt(colID)), // attnum
				zeroVal,                        // attndims
				negOneVal,                      // attcacheoff
				tree.NewDInt(tree.DInt(colTyp.TypeModifier())), // atttypmod
				tree.DNull, // attbyval (see pg_type.typbyval)
				tree.DNull, // attstorage
				tree.DNull, // attalign
				tree.MakeDBool(tree.DBool(!column.Nullable)),          // attnotnull
				tree.MakeDBool(tree.DBool(column.DefaultExpr != nil)), // atthasdef
				tree.DBoolFalse,    // attisdropped
				tree.DBoolTrue,     // attislocal
				zeroVal,            // attinhcount
				typColl(colTyp, h), // attcollation
				tree.DNull,         // attacl
				tree.DNull,         // attoptions
				tree.DNull,         // attfdwoptions
			)
		}

		// Columns for table.
		if err := forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
			tableID := tableOid(table.ID)
			return addColumn(column, tableID, column.GetLogicalColumnID())
		}); err != nil {
			return err
		}

		// Columns for each index.
		return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
			return forEachColumnInIndex(table, index,
				func(column *sqlbase.ColumnDescriptor) error {
					idxID := h.IndexOid(table.ID, index.ID)
					return addColumn(column, idxID, column.GetLogicalColumnID())
				},
			)
		})
	})

var pgCatalogCastTable = virtualSchemaTable{
	comment: `casts (empty - needs filling out)
https://www.postgresql.org/docs/9.6/catalog-pg-cast.html`,
	schema: `
CREATE TABLE pg_catalog.pg_cast (
	oid OID,
	castsource OID,
	casttarget OID,
	castfunc OID,
	castcontext CHAR,
	castmethod CHAR
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// TODO(someone): to populate this, we should split up the big PerformCast
		// method in tree/eval.go into entries in a list. Then, this virtual table
		// can simply range over the list. This would probably be better for
		// maintainability anyway.
		return nil
	},
}

var pgCatalogAuthIDTable = virtualSchemaTable{
	comment: `authorization identifiers - differs from postgres as we do not display passwords, 
and thus do not require admin privileges for access. 
https://www.postgresql.org/docs/9.5/catalog-pg-authid.html`,
	schema: `
CREATE TABLE pg_catalog.pg_authid (
  oid OID,
  rolname NAME,
  rolsuper BOOL,
  rolinherit BOOL,
  rolcreaterole BOOL,
  rolcreatedb BOOL,
  rolcanlogin BOOL,
  rolreplication BOOL,
  rolbypassrls BOOL,
  rolconnlimit INT4,
  rolpassword TEXT, 
  rolvaliduntil TIMESTAMPTZ
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRole(ctx, p, func(username string, isRole bool, noLogin bool, rolValidUntil *time.Time) error {
			isRoot := tree.DBool(username == security.RootUser || username == sqlbase.AdminRole)
			isRoleDBool := tree.DBool(isRole)
			roleCanLogin := tree.DBool(!noLogin)
			roleValidUntilValue := tree.DNull
			if rolValidUntil != nil {
				var err error
				roleValidUntilValue, err = tree.MakeDTimestampTZ(*rolValidUntil, time.Second)
				if err != nil {
					return err
				}
			}

			return addRow(
				h.UserOid(username),          // oid
				tree.NewDName(username),      // rolname
				tree.MakeDBool(isRoot),       // rolsuper
				tree.MakeDBool(isRoleDBool),  // rolinherit. Roles inherit by default.
				tree.MakeDBool(isRoot),       // rolcreaterole
				tree.MakeDBool(isRoot),       // rolcreatedb
				tree.MakeDBool(roleCanLogin), // rolcanlogin.
				tree.DBoolFalse,              // rolreplication
				tree.DBoolFalse,              // rolbypassrls
				negOneVal,                    // rolconnlimit
				passwdStarString,             // rolpassword
				roleValidUntilValue,          // rolvaliduntil
			)
		})
	},
}

var pgCatalogAuthMembersTable = virtualSchemaTable{
	comment: `role membership
https://www.postgresql.org/docs/9.5/catalog-pg-auth-members.html`,
	schema: `
CREATE TABLE pg_catalog.pg_auth_members (
	roleid OID,
	member OID,
	grantor OID,
	admin_option BOOL
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRoleMembership(ctx, p,
			func(roleName, memberName string, isAdmin bool) error {
				return addRow(
					h.UserOid(roleName),                 // roleid
					h.UserOid(memberName),               // member
					tree.DNull,                          // grantor
					tree.MakeDBool(tree.DBool(isAdmin)), // admin_option
				)
			})
	},
}

var pgCatalogAvailableExtensionsTable = virtualSchemaTable{
	comment: `available extensions
https://www.postgresql.org/docs/9.6/view-pg-available-extensions.html`,
	schema: `
CREATE TABLE pg_catalog.pg_available_extensions (
	name NAME,
	default_version TEXT,
	installed_version TEXT,
	comment TEXT
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// We support no extensions.
		return nil
	},
}

var (
	relKindTable    = tree.NewDString("r")
	relKindIndex    = tree.NewDString("i")
	relKindView     = tree.NewDString("v")
	relKindSequence = tree.NewDString("S")

	relPersistencePermanent = tree.NewDString("p")
)

var pgCatalogClassTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`tables and relation-like objects (incomplete - see also information_schema.tables/sequences/views)
https://www.postgresql.org/docs/9.5/catalog-pg-class.html`,
	`
CREATE TABLE pg_catalog.pg_class (
	oid OID NOT NULL,
	relname NAME NOT NULL,
	relnamespace OID,
	reltype OID,
	reloftype OID,
	relowner OID,
	relam OID,
	relfilenode OID,
	reltablespace OID,
	relpages INT4,
	reltuples FLOAT4,
	relallvisible INT4,
	reltoastrelid OID,
	relhasindex BOOL,
	relisshared BOOL,
	relpersistence CHAR,
	relistemp BOOL,
	relkind CHAR,
	relnatts INT2,
	relchecks INT2,
	relhasoids BOOL,
	relhaspkey BOOL,
	relhasrules BOOL,
	relhastriggers BOOL,
	relhassubclass BOOL,
	relfrozenxid INT,
	relacl STRING[],
	reloptions STRING[],
  INDEX (oid)
)`,
	virtualMany, true, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *sqlbase.ImmutableDatabaseDescriptor, scName string,
		table *sqlbase.ImmutableTableDescriptor, _ simpleSchemaResolver, addRow func(...tree.Datum) error) error {
		// The only difference between tables, views and sequences are the relkind and relam columns.
		relKind := relKindTable
		relAm := forwardIndexOid
		if table.IsView() {
			relKind = relKindView
			relAm = oidZero
		} else if table.IsSequence() {
			relKind = relKindSequence
			relAm = oidZero
		}
		namespaceOid := h.NamespaceOid(db, scName)
		if err := addRow(
			tableOid(table.ID),        // oid
			tree.NewDName(table.Name), // relname
			namespaceOid,              // relnamespace
			oidZero,                   // reltype (PG creates a composite type in pg_type for each table)
			oidZero,                   // reloftype (PG creates a composite type in pg_type for each table)
			tree.DNull,                // relowner
			relAm,                     // relam
			oidZero,                   // relfilenode
			oidZero,                   // reltablespace
			tree.DNull,                // relpages
			tree.DNull,                // reltuples
			zeroVal,                   // relallvisible
			oidZero,                   // reltoastrelid
			tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhasindex
			tree.DBoolFalse,         // relisshared
			relPersistencePermanent, // relPersistence
			tree.DBoolFalse,         // relistemp
			relKind,                 // relkind
			tree.NewDInt(tree.DInt(len(table.Columns))), // relnatts
			tree.NewDInt(tree.DInt(len(table.Checks))),  // relchecks
			tree.DBoolFalse, // relhasoids
			tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhaspkey
			tree.DBoolFalse, // relhasrules
			tree.DBoolFalse, // relhastriggers
			tree.DBoolFalse, // relhassubclass
			zeroVal,         // relfrozenxid
			tree.DNull,      // relacl
			tree.DNull,      // reloptions
		); err != nil {
			return err
		}

		// Skip adding indexes for sequences (their table descriptors hav a primary
		// index to make them comprehensible to backup/restore, but PG doesn't include
		// an index in pg_class).
		if table.IsSequence() {
			return nil
		}

		// Indexes.
		return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
			indexType := forwardIndexOid
			if index.Type == sqlbase.IndexDescriptor_INVERTED {
				indexType = invertedIndexOid
			}
			return addRow(
				h.IndexOid(table.ID, index.ID), // oid
				tree.NewDName(index.Name),      // relname
				namespaceOid,                   // relnamespace
				oidZero,                        // reltype
				oidZero,                        // reloftype
				tree.DNull,                     // relowner
				indexType,                      // relam
				oidZero,                        // relfilenode
				oidZero,                        // reltablespace
				tree.DNull,                     // relpages
				tree.DNull,                     // reltuples
				zeroVal,                        // relallvisible
				oidZero,                        // reltoastrelid
				tree.DBoolFalse,                // relhasindex
				tree.DBoolFalse,                // relisshared
				relPersistencePermanent,        // relPersistence
				tree.DBoolFalse,                // relistemp
				relKindIndex,                   // relkind
				tree.NewDInt(tree.DInt(len(index.ColumnNames))), // relnatts
				zeroVal,         // relchecks
				tree.DBoolFalse, // relhasoids
				tree.DBoolFalse, // relhaspkey
				tree.DBoolFalse, // relhasrules
				tree.DBoolFalse, // relhastriggers
				tree.DBoolFalse, // relhassubclass
				zeroVal,         // relfrozenxid
				tree.DNull,      // relacl
				tree.DNull,      // reloptions
			)
		})
	})

var pgCatalogCollationTable = virtualSchemaTable{
	comment: `available collations (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-collation.html`,
	schema: `
CREATE TABLE pg_catalog.pg_collation (
  oid OID,
  collname STRING,
  collnamespace OID,
  collowner OID,
  collencoding INT4,
  collcollate STRING,
  collctype STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false /* requiresPrivileges */, func(db *sqlbase.ImmutableDatabaseDescriptor) error {
			namespaceOid := h.NamespaceOid(db, pgCatalogName)
			for _, tag := range collate.Supported() {
				collName := tag.String()
				if err := addRow(
					h.CollationOid(collName),  // oid
					tree.NewDString(collName), // collname
					namespaceOid,              // collnamespace
					tree.DNull,                // collowner
					builtins.DatEncodingUTFId, // collencoding
					// It's not clear how to translate a Go collation tag into the format
					// required by LC_COLLATE and LC_CTYPE.
					tree.DNull, // collcollate
					tree.DNull, // collctype
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

var (
	conTypeCheck     = tree.NewDString("c")
	conTypeFK        = tree.NewDString("f")
	conTypePKey      = tree.NewDString("p")
	conTypeUnique    = tree.NewDString("u")
	conTypeTrigger   = tree.NewDString("t")
	conTypeExclusion = tree.NewDString("x")

	// Avoid unused warning for constants.
	_ = conTypeTrigger
	_ = conTypeExclusion

	fkActionNone       = tree.NewDString("a")
	fkActionRestrict   = tree.NewDString("r")
	fkActionCascade    = tree.NewDString("c")
	fkActionSetNull    = tree.NewDString("n")
	fkActionSetDefault = tree.NewDString("d")

	fkActionMap = map[sqlbase.ForeignKeyReference_Action]tree.Datum{
		sqlbase.ForeignKeyReference_NO_ACTION:   fkActionNone,
		sqlbase.ForeignKeyReference_RESTRICT:    fkActionRestrict,
		sqlbase.ForeignKeyReference_CASCADE:     fkActionCascade,
		sqlbase.ForeignKeyReference_SET_NULL:    fkActionSetNull,
		sqlbase.ForeignKeyReference_SET_DEFAULT: fkActionSetDefault,
	}

	fkMatchTypeFull    = tree.NewDString("f")
	fkMatchTypePartial = tree.NewDString("p")
	fkMatchTypeSimple  = tree.NewDString("s")

	fkMatchMap = map[sqlbase.ForeignKeyReference_Match]tree.Datum{
		sqlbase.ForeignKeyReference_SIMPLE:  fkMatchTypeSimple,
		sqlbase.ForeignKeyReference_FULL:    fkMatchTypeFull,
		sqlbase.ForeignKeyReference_PARTIAL: fkMatchTypePartial,
	}
)

func populateTableConstraints(
	ctx context.Context,
	p *planner,
	h oidHasher,
	db *sqlbase.ImmutableDatabaseDescriptor,
	scName string,
	table *sqlbase.ImmutableTableDescriptor,
	tableLookup simpleSchemaResolver,
	addRow func(...tree.Datum) error,
) error {
	conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
	if err != nil {
		return err
	}
	namespaceOid := h.NamespaceOid(db, scName)
	tblOid := tableOid(table.ID)
	for conName, con := range conInfo {
		oid := tree.DNull
		contype := tree.DNull
		conindid := oidZero
		confrelid := oidZero
		confupdtype := tree.DNull
		confdeltype := tree.DNull
		confmatchtype := tree.DNull
		conkey := tree.DNull
		confkey := tree.DNull
		consrc := tree.DNull
		conbin := tree.DNull
		condef := tree.DNull

		// Determine constraint kind-specific fields.
		var err error
		switch con.Kind {
		case sqlbase.ConstraintTypePK:
			oid = h.PrimaryKeyConstraintOid(db, scName, table.TableDesc(), con.Index)
			contype = conTypePKey
			conindid = h.IndexOid(table.ID, con.Index.ID)

			var err error
			if conkey, err = colIDArrayToDatum(con.Index.ColumnIDs); err != nil {
				return err
			}
			condef = tree.NewDString(table.PrimaryKeyString())

		case sqlbase.ConstraintTypeFK:
			oid = h.ForeignKeyConstraintOid(db, scName, table.TableDesc(), con.FK)
			contype = conTypeFK
			// Foreign keys don't have a single linked index. Pick the first one
			// that matches on the referenced table.
			referencedTable, err := tableLookup.getTableByID(con.FK.ReferencedTableID)
			if err != nil {
				return err
			}
			if idx, err := sqlbase.FindFKReferencedIndex(referencedTable, con.FK.ReferencedColumnIDs); err != nil {
				// We couldn't find an index that matched. This shouldn't happen.
				log.Warningf(ctx, "broken fk reference: %v", err)
			} else {
				conindid = h.IndexOid(con.ReferencedTable.ID, idx.ID)
			}
			confrelid = tableOid(con.ReferencedTable.ID)
			if r, ok := fkActionMap[con.FK.OnUpdate]; ok {
				confupdtype = r
			}
			if r, ok := fkActionMap[con.FK.OnDelete]; ok {
				confdeltype = r
			}
			if r, ok := fkMatchMap[con.FK.Match]; ok {
				confmatchtype = r
			}
			if conkey, err = colIDArrayToDatum(con.FK.OriginColumnIDs); err != nil {
				return err
			}
			if confkey, err = colIDArrayToDatum(con.FK.ReferencedColumnIDs); err != nil {
				return err
			}
			var buf bytes.Buffer
			if err := showForeignKeyConstraint(&buf, db.GetName(), table, con.FK, tableLookup); err != nil {
				return err
			}
			condef = tree.NewDString(buf.String())

		case sqlbase.ConstraintTypeUnique:
			oid = h.UniqueConstraintOid(db, scName, table.TableDesc(), con.Index)
			contype = conTypeUnique
			conindid = h.IndexOid(table.ID, con.Index.ID)
			var err error
			if conkey, err = colIDArrayToDatum(con.Index.ColumnIDs); err != nil {
				return err
			}
			f := tree.NewFmtCtx(tree.FmtSimple)
			f.WriteString("UNIQUE (")
			con.Index.ColNamesFormat(f)
			f.WriteByte(')')
			condef = tree.NewDString(f.CloseAndGetString())

		case sqlbase.ConstraintTypeCheck:
			oid = h.CheckConstraintOid(db, scName, table.TableDesc(), con.CheckConstraint)
			contype = conTypeCheck
			if conkey, err = colIDArrayToDatum(con.CheckConstraint.ColumnIDs); err != nil {
				return err
			}
			consrc = tree.NewDString(fmt.Sprintf("(%s)", con.Details))
			conbin = consrc
			condef = tree.NewDString(fmt.Sprintf("CHECK ((%s))", con.Details))
		}

		if err := addRow(
			oid,                  // oid
			dNameOrNull(conName), // conname
			namespaceOid,         // connamespace
			contype,              // contype
			tree.DBoolFalse,      // condeferrable
			tree.DBoolFalse,      // condeferred
			tree.MakeDBool(tree.DBool(!con.Unvalidated)), // convalidated
			tblOid,         // conrelid
			oidZero,        // contypid
			conindid,       // conindid
			confrelid,      // confrelid
			confupdtype,    // confupdtype
			confdeltype,    // confdeltype
			confmatchtype,  // confmatchtype
			tree.DBoolTrue, // conislocal
			zeroVal,        // coninhcount
			tree.DBoolTrue, // connoinherit
			conkey,         // conkey
			confkey,        // confkey
			tree.DNull,     // conpfeqop
			tree.DNull,     // conppeqop
			tree.DNull,     // conffeqop
			tree.DNull,     // conexclop
			conbin,         // conbin
			consrc,         // consrc
			condef,         // condef
		); err != nil {
			return err
		}
	}
	return nil
}

type oneAtATimeSchemaResolver struct {
	ctx context.Context
	p   *planner
}

func (r oneAtATimeSchemaResolver) getDatabaseByID(
	id sqlbase.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	return r.p.Tables().DatabaseCache().GetDatabaseDescByID(r.ctx, r.p.txn, id)
}

func (r oneAtATimeSchemaResolver) getTableByID(id sqlbase.ID) (*TableDescriptor, error) {
	table, err := r.p.LookupTableByID(r.ctx, id)
	if err != nil {
		return nil, err
	}
	return table.Desc.TableDesc(), nil
}

// makeAllRelationsVirtualTableWithDescriptorIDIndex creates a virtual table that searches through
// all table descriptors in the system. It automatically adds a virtual index implementation to the
// table id column as well. The input schema must have a single INDEX definition
// with a single column, which must be the column that contains the table id.
// includesIndexEntries should be set to true if the indexed column produces
// index ids as well as just ordinary table descriptor ids. In this case, the
// caller must pass true for this variable to prevent failed lookups.
func makeAllRelationsVirtualTableWithDescriptorIDIndex(
	comment string,
	schemaDef string,
	virtualOpts virtualOpts,
	includesIndexEntries bool,
	populateFromTable func(ctx context.Context, p *planner, h oidHasher, db *sqlbase.ImmutableDatabaseDescriptor,
		scName string, table *sqlbase.ImmutableTableDescriptor, lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error,
	) error,
) virtualSchemaTable {
	populateAll := func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, virtualOpts,
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, table *sqlbase.ImmutableTableDescriptor, lookup tableLookupFn) error {
				return populateFromTable(ctx, p, h, db, scName, table, lookup, addRow)
			})
	}
	return virtualSchemaTable{
		comment: comment,
		schema:  schemaDef,
		indexes: []virtualIndex{
			{
				partial: includesIndexEntries,
				populate: func(ctx context.Context, constraint tree.Datum, p *planner, db *sqlbase.ImmutableDatabaseDescriptor,
					addRow func(...tree.Datum) error) (bool, error) {
					var id sqlbase.ID
					d := tree.UnwrapDatum(p.EvalContext(), constraint)
					if d == tree.DNull {
						return false, nil
					}
					switch t := d.(type) {
					case *tree.DOid:
						id = sqlbase.ID(t.DInt)
					case *tree.DInt:
						id = sqlbase.ID(*t)
					default:
						return false, errors.AssertionFailedf("unexpected type %T for table id column in virtual table %s",
							d, schemaDef)
					}
					table, err := p.LookupTableByID(ctx, id)
					if err != nil {
						if sqlbase.IsUndefinedRelationError(err) {
							// No table found, so no rows. In this case, we'll fall back to the
							// full table scan if the index isn't complete - see the
							// indexContainsNonTableDescriptorIDs parameter.
							//nolint:returnerrcheck
							return false, nil
						}
						return false, err
					}
					// Don't include tables that aren't in the current database unless
					// they're virtual, dropped tables, or ones that the user can't see.
					if (!table.Desc.IsVirtualTable() && table.Desc.ParentID != db.GetID()) ||
						table.Desc.Dropped() ||
						!userCanSeeTable(ctx, p, table.Desc, true /*allowAdding*/) {
						return false, nil
					}
					h := makeOidHasher()
					scResolver := oneAtATimeSchemaResolver{p: p, ctx: ctx}
					scName, err := resolver.ResolveSchemaNameByID(ctx, p.txn, p.ExecCfg().Codec, db.GetID(), table.Desc.GetParentSchemaID())
					if err != nil {
						return false, err
					}
					if err := populateFromTable(ctx, p, h, db, scName, table.Desc, scResolver,
						addRow); err != nil {
						return false, err
					}
					return true, nil
				},
			},
		},
		populate: populateAll,
	}
}

var pgCatalogConstraintTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`table constraints (incomplete - see also information_schema.table_constraints)
https://www.postgresql.org/docs/9.5/catalog-pg-constraint.html`,
	`
CREATE TABLE pg_catalog.pg_constraint (
	oid OID,
	conname NAME,
	connamespace OID,
	contype STRING,
	condeferrable BOOL,
	condeferred BOOL,
	convalidated BOOL,
	conrelid OID NOT NULL,
	contypid OID,
	conindid OID,
	confrelid OID,
	confupdtype STRING,
	confdeltype STRING,
	confmatchtype STRING,
	conislocal BOOL,
	coninhcount INT4,
	connoinherit BOOL,
	conkey INT2[],
	confkey INT2[],
	conpfeqop OID[],
	conppeqop OID[],
	conffeqop OID[],
	conexclop OID[],
	conbin STRING,
	consrc STRING,
	-- condef is a CockroachDB extension that provides a SHOW CREATE CONSTRAINT
	-- style string, for use by pg_get_constraintdef().
	condef STRING,
  INDEX (conrelid)
)`,
	hideVirtual, /* Virtual tables have no constraints */
	false,       /* includesIndexEntries */
	populateTableConstraints)

// colIDArrayToDatum returns an int[] containing the ColumnIDs, or NULL if there
// are no ColumnIDs.
func colIDArrayToDatum(arr []sqlbase.ColumnID) (tree.Datum, error) {
	if len(arr) == 0 {
		return tree.DNull, nil
	}
	d := tree.NewDArray(types.Int)
	for _, val := range arr {
		if err := d.Append(tree.NewDInt(tree.DInt(val))); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// colIDArrayToVector returns an INT2VECTOR containing the ColumnIDs, or NULL if
// there are no ColumnIDs.
func colIDArrayToVector(arr []sqlbase.ColumnID) (tree.Datum, error) {
	dArr, err := colIDArrayToDatum(arr)
	if err != nil {
		return nil, err
	}
	if dArr == tree.DNull {
		return dArr, nil
	}
	return tree.NewDIntVectorFromDArray(tree.MustBeDArray(dArr)), nil
}

var pgCatalogConversionTable = virtualSchemaTable{
	comment: `encoding conversions (empty - unimplemented)
https://www.postgresql.org/docs/9.6/catalog-pg-conversion.html`,
	schema: `
CREATE TABLE pg_catalog.pg_conversion (
	oid OID,
	conname NAME,
	connamespace OID,
	conowner OID,
	conforencoding INT4,
	contoencoding INT4,
	conproc OID,
  condefault BOOL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogDatabaseTable = virtualSchemaTable{
	comment: `available databases (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-database.html`,
	schema: `
CREATE TABLE pg_catalog.pg_database (
	oid OID,
	datname Name,
	datdba OID,
	encoding INT4,
	datcollate STRING,
	datctype STRING,
	datistemplate BOOL,
	datallowconn BOOL,
	datconnlimit INT4,
	datlastsysoid OID,
	datfrozenxid INT,
	datminmxid INT,
	dattablespace OID,
	datacl STRING[]
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /*all databases*/, false, /* requiresPrivileges */
			func(db *sqlbase.ImmutableDatabaseDescriptor) error {
				return addRow(
					dbOid(db.GetID()),           // oid
					tree.NewDName(db.GetName()), // datname
					tree.DNull,                  // datdba
					// If there is a change in encoding value for the database we must update
					// the definitions of getdatabaseencoding within pg_builtin.
					builtins.DatEncodingUTFId,  // encoding
					builtins.DatEncodingEnUTF8, // datcollate
					builtins.DatEncodingEnUTF8, // datctype
					tree.DBoolFalse,            // datistemplate
					tree.DBoolTrue,             // datallowconn
					negOneVal,                  // datconnlimit
					oidZero,                    // datlastsysoid
					tree.DNull,                 // datfrozenxid
					tree.DNull,                 // datminmxid
					oidZero,                    // dattablespace
					tree.DNull,                 // datacl
				)
			})
	},
}

var pgCatalogDefaultACLTable = virtualSchemaTable{
	comment: `default ACLs (empty - unimplemented)
https://www.postgresql.org/docs/9.6/catalog-pg-default-acl.html`,
	schema: `
CREATE TABLE pg_catalog.pg_default_acl (
	oid OID,
	defaclrole OID,
	defaclnamespace OID,
	defaclobjtype CHAR,
	defaclacl STRING[]
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var (
	depTypeNormal        = tree.NewDString("n")
	depTypeAuto          = tree.NewDString("a")
	depTypeInternal      = tree.NewDString("i")
	depTypeExtension     = tree.NewDString("e")
	depTypeAutoExtension = tree.NewDString("x")
	depTypePin           = tree.NewDString("p")

	// Avoid unused warning for constants.
	_ = depTypeAuto
	_ = depTypeInternal
	_ = depTypeExtension
	_ = depTypeAutoExtension
	_ = depTypePin

	pgConstraintsTableName = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_constraint"))
	pgClassTableName       = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_class"))
)

// pg_depend is a fairly complex table that details many different kinds of
// relationships between database objects. We do not implement the vast
// majority of this table, as it is mainly used by pgjdbc to address a
// deficiency in pg_constraint that was removed in postgres v9.0 with the
// addition of the conindid column. To provide backward compatibility with
// pgjdbc drivers before https://github.com/pgjdbc/pgjdbc/pull/689, we
// provide those rows in pg_depend that track the dependency of foreign key
// constraints on their supporting index entries in pg_class.
var pgCatalogDependTable = virtualSchemaTable{
	comment: `dependency relationships (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-depend.html`,
	schema: `
CREATE TABLE pg_catalog.pg_depend (
  classid OID,
  objid OID,
  objsubid INT4,
  refclassid OID,
  refobjid OID,
  refobjsubid INT4,
  deptype CHAR
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		vt := p.getVirtualTabler()
		pgConstraintsDesc, err := vt.getVirtualTableDesc(&pgConstraintsTableName)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_constraint")
		}
		pgClassDesc, err := vt.getVirtualTableDesc(&pgClassTableName)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_class")
		}
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /*virtual tables have no constraints*/, func(
			db *sqlbase.ImmutableDatabaseDescriptor,
			scName string,
			table *sqlbase.ImmutableTableDescriptor,
			tableLookup tableLookupFn,
		) error {
			pgConstraintTableOid := tableOid(pgConstraintsDesc.ID)
			pgClassTableOid := tableOid(pgClassDesc.ID)
			if table.IsSequence() &&
				!table.SequenceOpts.SequenceOwner.Equal(sqlbase.TableDescriptor_SequenceOpts_SequenceOwner{}) {
				refObjID := tableOid(table.SequenceOpts.SequenceOwner.OwnerTableID)
				refObjSubID := tree.NewDInt(tree.DInt(table.SequenceOpts.SequenceOwner.OwnerColumnID))
				objID := tableOid(table.GetID())
				return addRow(
					pgConstraintTableOid, // classid
					objID,                // objid
					zeroVal,              // objsubid
					pgClassTableOid,      // refclassid
					refObjID,             // refobjid
					refObjSubID,          // refobjsubid
					depTypeAuto,          // deptype
				)
			}
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			for _, con := range conInfo {
				if con.Kind != sqlbase.ConstraintTypeFK {
					continue
				}

				// Foreign keys don't have a single linked index. Pick the first one
				// that matches on the referenced table.
				referencedTable, err := tableLookup.getTableByID(con.FK.ReferencedTableID)
				if err != nil {
					return err
				}
				refObjID := oidZero
				if idx, err := sqlbase.FindFKReferencedIndex(referencedTable, con.FK.ReferencedColumnIDs); err != nil {
					// We couldn't find an index that matched. This shouldn't happen.
					log.Warningf(ctx, "broken fk reference: %v", err)
				} else {
					refObjID = h.IndexOid(con.ReferencedTable.ID, idx.ID)
				}
				constraintOid := h.ForeignKeyConstraintOid(db, scName, table.TableDesc(), con.FK)

				if err := addRow(
					pgConstraintTableOid, // classid
					constraintOid,        // objid
					zeroVal,              // objsubid
					pgClassTableOid,      // refclassid
					refObjID,             // refobjid
					zeroVal,              // refobjsubid
					depTypeNormal,        // deptype
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

// getComments returns all comments in the database. A comment is represented
// as a datum row, containing object id, sub id (column id in the case of
// columns), comment text, and comment type (keys.FooCommentType).
func getComments(ctx context.Context, p *planner) ([]tree.Datums, error) {
	return p.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
		ctx,
		"select-comments",
		p.EvalContext().Txn,
		`SELECT COALESCE(pc.object_id, sc.object_id) AS object_id,
              COALESCE(pc.sub_id, sc.sub_id) AS sub_id,
              COALESCE(pc.comment, sc.comment) AS comment,
              COALESCE(pc.type, sc.type) AS type
         FROM (SELECT * FROM system.comments) AS sc
    FULL JOIN (SELECT * FROM crdb_internal.predefined_comments) AS pc
           ON (pc.object_id = sc.object_id AND pc.sub_id = sc.sub_id AND pc.type = sc.type)`)
}

var pgCatalogDescriptionTable = virtualSchemaTable{
	comment: `object comments
https://www.postgresql.org/docs/9.5/catalog-pg-description.html`,
	schema: `
CREATE TABLE pg_catalog.pg_description (
	objoid OID,
	classoid OID,
	objsubid INT4,
	description STRING
)`,
	populate: func(
		ctx context.Context,
		p *planner,
		dbContext *sqlbase.ImmutableDatabaseDescriptor,
		addRow func(...tree.Datum) error) error {

		// This is less efficient than it has to be - if we see performance problems
		// here, we can push the filter into the query that getComments runs,
		// instead of filtering client-side below.
		comments, err := getComments(ctx, p)
		if err != nil {
			return err
		}
		for _, comment := range comments {
			objID := comment[0]
			objSubID := comment[1]
			description := comment[2]
			commentType := tree.MustBeDInt(comment[3])

			classOid := oidZero

			switch commentType {
			case keys.DatabaseCommentType:
				// Database comments are exported in pg_shdescription.
				continue
			case keys.ColumnCommentType, keys.TableCommentType:
				objID = tree.NewDOid(tree.MustBeDInt(objID))
				classOid = tree.NewDOid(sqlbase.PgCatalogClassTableID)
			case keys.IndexCommentType:
				objID = makeOidHasher().IndexOid(
					sqlbase.ID(tree.MustBeDInt(objID)),
					sqlbase.IndexID(tree.MustBeDInt(objSubID)))
				objSubID = tree.DZero
				classOid = tree.NewDOid(sqlbase.PgCatalogClassTableID)
			}
			if err := addRow(
				objID,
				classOid,
				objSubID,
				description); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogSharedDescriptionTable = virtualSchemaTable{
	comment: `shared object comments
https://www.postgresql.org/docs/9.5/catalog-pg-shdescription.html`,
	schema: `
CREATE TABLE pg_catalog.pg_shdescription (
	objoid OID,
	classoid OID,
	description STRING
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// See comment above - could make this more efficient if necessary.
		comments, err := getComments(ctx, p)
		if err != nil {
			return err
		}
		for _, comment := range comments {
			commentType := tree.MustBeDInt(comment[3])
			if commentType != keys.DatabaseCommentType {
				// Only database comments are exported in this table.
				continue
			}
			classOid := tree.NewDOid(sqlbase.PgCatalogDatabaseTableID)
			objID := sqlbase.ID(tree.MustBeDInt(comment[0]))
			if err := addRow(
				tableOid(objID),
				classOid,
				comment[2]); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogEnumTable = virtualSchemaTable{
	comment: `enum types and labels (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-enum.html`,
	schema: `
CREATE TABLE pg_catalog.pg_enum (
  oid OID,
  enumtypid OID,
  enumsortorder FLOAT4,
  enumlabel STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()

		return forEachTypeDesc(ctx, p, dbContext, func(_ *sqlbase.ImmutableDatabaseDescriptor, _ string, typDesc *sqlbase.ImmutableTypeDescriptor) error {
			// We only want to iterate over ENUM types.
			if typDesc.Kind != sqlbase.TypeDescriptor_ENUM {
				return nil
			}
			// Generate a row for each member of the enum. We don't represent enums
			// internally using floats for ordering like Postgres, so just pick a
			// float entry for the rows.
			typOID := tree.NewDOid(tree.DInt(types.StableTypeIDToOID(uint32(typDesc.GetID()))))
			for i, member := range typDesc.EnumMembers {
				if err := addRow(
					h.EnumEntryOid(typOID, member.PhysicalRepresentation),
					typOID,
					tree.NewDFloat(tree.DFloat(float64(i))),
					tree.NewDString(member.LogicalRepresentation),
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

var pgCatalogEventTriggerTable = virtualSchemaTable{
	comment: `event triggers (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/catalog-pg-event-trigger.html`,
	schema: `
CREATE TABLE pg_catalog.pg_event_trigger (
	evtname NAME,
	evtevent NAME,
	evtowner OID,
	evtfoid OID,
	evtenabled CHAR,
	evttags TEXT[]
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Event triggers are not currently supported.
		return nil
	},
}

var pgCatalogExtensionTable = virtualSchemaTable{
	comment: `installed extensions (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-extension.html`,
	schema: `
CREATE TABLE pg_catalog.pg_extension (
  oid OID,
  extname NAME,
  extowner OID,
  extnamespace OID,
  extrelocatable BOOL,
  extversion STRING,
  extconfig STRING,
  extcondition STRING
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Extensions are not supported.
		return nil
	},
}

var pgCatalogForeignDataWrapperTable = virtualSchemaTable{
	comment: `foreign data wrappers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-data-wrapper.html`,
	schema: `
CREATE TABLE pg_catalog.pg_foreign_data_wrapper (
  oid OID,
  fdwname NAME,
  fdwowner OID,
  fdwhandler OID,
  fdwvalidator OID,
  fdwacl STRING[],
  fdwoptions STRING[]
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Foreign data wrappers are not supported.
		return nil
	},
}

var pgCatalogForeignServerTable = virtualSchemaTable{
	comment: `foreign servers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-server.html`,
	schema: `
CREATE TABLE pg_catalog.pg_foreign_server (
  oid OID,
  srvname NAME,
  srvowner OID,
  srvfdw OID,
  srvtype STRING,
  srvversion STRING,
  srvacl STRING[],
  srvoptions STRING[]
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Foreign servers are not supported.
		return nil
	},
}

var pgCatalogForeignTableTable = virtualSchemaTable{
	comment: `foreign tables (empty  - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-table.html`,
	schema: `
CREATE TABLE pg_catalog.pg_foreign_table (
  ftrelid OID,
  ftserver OID,
  ftoptions STRING[]
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Foreign tables are not supported.
		return nil
	},
}

func makeZeroedOidVector(size int) (tree.Datum, error) {
	oidArray := tree.NewDArray(types.Oid)
	for i := 0; i < size; i++ {
		if err := oidArray.Append(oidZero); err != nil {
			return nil, err
		}
	}
	return tree.NewDOidVectorFromDArray(oidArray), nil
}

var pgCatalogIndexTable = virtualSchemaTable{
	comment: `indexes (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-index.html`,
	schema: `
CREATE TABLE pg_catalog.pg_index (
    indexrelid OID,
    indrelid OID,
    indnatts INT2,
    indisunique BOOL,
    indisprimary BOOL,
    indisexclusion BOOL,
    indimmediate BOOL,
    indisclustered BOOL,
    indisvalid BOOL,
    indcheckxmin BOOL,
    indisready BOOL,
    indislive BOOL,
    indisreplident BOOL,
    indkey INT2VECTOR,
    indcollation OIDVECTOR,
    indclass OIDVECTOR,
    indoption INT2VECTOR,
    indexprs STRING,
    indpred STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual tables do not have indexes */
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, table *sqlbase.ImmutableTableDescriptor) error {
				tableOid := tableOid(table.ID)
				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					isMutation, isWriteOnly :=
						table.GetIndexMutationCapabilities(index.ID)
					isReady := isMutation && isWriteOnly
					indkey, err := colIDArrayToVector(index.ColumnIDs)
					if err != nil {
						return err
					}
					// Get the collations for all of the columns. To do this we require
					// the type of the column.
					// Also fill in indoption for each column to indicate if the index
					// is ASC/DESC and if nulls appear first/last.
					collationOids := tree.NewDArray(types.Oid)
					indoption := tree.NewDArray(types.Int)
					for i, columnID := range index.ColumnIDs {
						col, err := table.FindColumnByID(columnID)
						if err != nil {
							return err
						}
						if err := collationOids.Append(typColl(col.Type, h)); err != nil {
							return err
						}
						// Currently, nulls always appear first if the order is ascending,
						// and always appear last if the order is descending.
						var thisIndOption tree.DInt
						if index.ColumnDirections[i] == sqlbase.IndexDescriptor_ASC {
							thisIndOption = indoptionNullsFirst
						} else {
							thisIndOption = indoptionDesc
						}
						if err := indoption.Append(tree.NewDInt(thisIndOption)); err != nil {
							return err
						}
					}
					collationOidVector := tree.NewDOidVectorFromDArray(collationOids)
					indoptionIntVector := tree.NewDIntVectorFromDArray(indoption)
					// TODO(bram): #27763 indclass still needs to be populated but it
					// requires pg_catalog.pg_opclass first.
					indclass, err := makeZeroedOidVector(len(index.ColumnIDs))
					isPrimary := table.PrimaryIndex.ID == index.ID && table.IsPhysicalTable()
					if err != nil {
						return err
					}
					return addRow(
						h.IndexOid(table.ID, index.ID), // indexrelid
						tableOid,                       // indrelid
						tree.NewDInt(tree.DInt(len(index.ColumnNames))), // indnatts
						tree.MakeDBool(tree.DBool(index.Unique)),        // indisunique
						tree.MakeDBool(tree.DBool(isPrimary)),           // indisprimary
						tree.DBoolFalse,                                 // indisexclusion
						tree.MakeDBool(tree.DBool(index.Unique)),        // indimmediate
						tree.DBoolFalse,                                 // indisclustered
						tree.MakeDBool(tree.DBool(!isMutation)),         // indisvalid
						tree.DBoolFalse,                                 // indcheckxmin
						tree.MakeDBool(tree.DBool(isReady)),             // indisready
						tree.DBoolTrue,                                  // indislive
						tree.DBoolFalse,                                 // indisreplident
						indkey,                                          // indkey
						collationOidVector,                              // indcollation
						indclass,                                        // indclass
						indoptionIntVector,                              // indoption
						tree.DNull,                                      // indexprs
						tree.DNull,                                      // indpred
					)
				})
			})
	},
}

var pgCatalogIndexesTable = virtualSchemaTable{
	comment: `index creation statements
https://www.postgresql.org/docs/9.5/view-pg-indexes.html`,
	// Note: crdb_oid is an extension of the schema to much more easily map
	// index OIDs to the corresponding index definition.
	schema: `
CREATE TABLE pg_catalog.pg_indexes (
	crdb_oid OID,
	schemaname NAME,
	tablename NAME,
	indexname NAME,
	tablespace NAME,
	indexdef STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual, /* virtual tables do not have indexes */
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, table *sqlbase.ImmutableTableDescriptor, tableLookup tableLookupFn) error {
				scNameName := tree.NewDName(scName)
				tblName := tree.NewDName(table.Name)
				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					def, err := indexDefFromDescriptor(ctx, p, db, table, index, tableLookup)
					if err != nil {
						return err
					}
					return addRow(
						h.IndexOid(table.ID, index.ID), // oid
						scNameName,                     // schemaname
						tblName,                        // tablename
						tree.NewDName(index.Name),      // indexname
						tree.DNull,                     // tablespace
						tree.NewDString(def),           // indexdef
					)
				})
			})
	},
}

// indexDefFromDescriptor creates an index definition (`CREATE INDEX ... ON (...)`) from
// and index descriptor by reconstructing a CreateIndex parser node and calling its
// String method.
func indexDefFromDescriptor(
	ctx context.Context,
	p *planner,
	db *sqlbase.ImmutableDatabaseDescriptor,
	table *sqlbase.ImmutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	tableLookup tableLookupFn,
) (string, error) {
	indexDef := tree.CreateIndex{
		Name:     tree.Name(index.Name),
		Table:    tree.MakeTableName(tree.Name(db.GetName()), tree.Name(table.Name)),
		Unique:   index.Unique,
		Columns:  make(tree.IndexElemList, len(index.ColumnNames)),
		Storing:  make(tree.NameList, len(index.StoreColumnNames)),
		Inverted: index.Type == sqlbase.IndexDescriptor_INVERTED,
	}
	for i, name := range index.ColumnNames {
		elem := tree.IndexElem{
			Column:    tree.Name(name),
			Direction: tree.Ascending,
		}
		if index.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC {
			elem.Direction = tree.Descending
		}
		indexDef.Columns[i] = elem
	}
	for i, name := range index.StoreColumnNames {
		indexDef.Storing[i] = tree.Name(name)
	}
	if len(index.Interleave.Ancestors) > 0 {
		intl := index.Interleave
		parentTable, err := tableLookup.getTableByID(intl.Ancestors[len(intl.Ancestors)-1].TableID)
		if err != nil {
			return "", err
		}
		parentDb, err := tableLookup.getDatabaseByID(parentTable.ParentID)
		if err != nil {
			return "", err
		}
		var sharedPrefixLen int
		for _, ancestor := range intl.Ancestors {
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
		}
		fields := index.ColumnNames[:sharedPrefixLen]
		intlDef := &tree.InterleaveDef{
			Parent: tree.MakeTableName(tree.Name(parentDb.GetName()), tree.Name(parentTable.Name)),
			Fields: make(tree.NameList, len(fields)),
		}
		for i, field := range fields {
			intlDef.Fields[i] = tree.Name(field)
		}
		indexDef.Interleave = intlDef
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtPGIndexDef)
	fmtCtx.FormatNode(&indexDef)
	return fmtCtx.String(), nil
}

var pgCatalogInheritsTable = virtualSchemaTable{
	comment: `table inheritance hierarchy (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-inherits.html`,
	schema: `
CREATE TABLE pg_catalog.pg_inherits (
	inhrelid OID,
	inhparent OID,
	inhseqno INT4
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Table inheritance is not supported.
		return nil
	},
}

var pgCatalogLanguageTable = virtualSchemaTable{
	comment: `available languages (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-language.html`,
	schema: `
CREATE TABLE pg_catalog.pg_language (
	oid OID,
	lanname NAME,
	lanowner OID,
	lanispl BOOL,
	lanpltrusted BOOL,
	lanplcallfoid OID,
	laninline OID,
	lanvalidator OID,
	lanacl STRING[]
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Languages to write functions and stored procedures are not supported.
		return nil
	},
}

var pgCatalogLocksTable = virtualSchemaTable{
	comment: `locks held by active processes (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/view-pg-locks.html`,
	schema: `
CREATE TABLE pg_catalog.pg_locks (
  locktype TEXT,
  database OID,
  relation OID,
  page INT4,
  tuple SMALLINT,
  virtualxid TEXT,
  transactionid INT,
  classid OID,
  objid OID,
  objsubid SMALLINT,
  virtualtransaction TEXT,
  pid INT4,
  mode TEXT,
  granted BOOLEAN,
  fastpath BOOLEAN
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogMatViewsTable = virtualSchemaTable{
	comment: `available materialized views (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/view-pg-matviews.html`,
	schema: `
CREATE TABLE pg_catalog.pg_matviews (
  schemaname NAME,
  matviewname NAME,
  matviewowner NAME,
  tablespace NAME,
  hasindexes BOOL,
  ispopulated BOOL,
  definition TEXT
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogNamespaceTable = virtualSchemaTable{
	comment: `available namespaces (incomplete; namespaces and databases are congruent in CockroachDB)
https://www.postgresql.org/docs/9.5/catalog-pg-namespace.html`,
	schema: `
CREATE TABLE pg_catalog.pg_namespace (
	oid OID,
	nspname NAME NOT NULL,
	nspowner OID,
	nspacl STRING[]
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(db *sqlbase.ImmutableDatabaseDescriptor) error {
				return forEachSchemaName(ctx, p, db, func(s string) error {
					return addRow(
						h.NamespaceOid(db, s), // oid
						tree.NewDString(s),    // nspname
						tree.DNull,            // nspowner
						tree.DNull,            // nspacl
					)
				})
			})
	},
}

var (
	infixKind   = tree.NewDString("b")
	prefixKind  = tree.NewDString("l")
	postfixKind = tree.NewDString("r")

	// Avoid unused warning for constants.
	_ = postfixKind
)

var pgCatalogOperatorTable = virtualSchemaTable{
	comment: `operators (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-operator.html`,
	schema: `
CREATE TABLE pg_catalog.pg_operator (
	oid OID,
	oprname NAME,
	oprnamespace OID,
	oprowner OID,
	oprkind TEXT,
	oprcanmerge BOOL,
	oprcanhash BOOL,
	oprleft OID,
	oprright OID,
	oprresult OID,
	oprcom OID,
	oprnegate OID,
	oprcode OID,
	oprrest OID,
	oprjoin OID
)`,
	populate: func(ctx context.Context, p *planner, db *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		nspOid := h.NamespaceOid(db, pgCatalogName)
		addOp := func(opName string, kind tree.Datum, params tree.TypeList, returnTyper tree.ReturnTyper) error {
			var leftType, rightType *tree.DOid
			switch params.Length() {
			case 1:
				leftType = oidZero
				rightType = tree.NewDOid(tree.DInt(params.Types()[0].Oid()))
			case 2:
				leftType = tree.NewDOid(tree.DInt(params.Types()[0].Oid()))
				rightType = tree.NewDOid(tree.DInt(params.Types()[1].Oid()))
			default:
				panic(fmt.Sprintf("Unexpected operator %s with %d params",
					opName, params.Length()))
			}
			returnType := tree.NewDOid(tree.DInt(returnTyper(nil).Oid()))
			err := addRow(
				h.OperatorOid(opName, leftType, rightType, returnType), // oid

				tree.NewDString(opName), // oprname
				nspOid,                  // oprnamespace
				tree.DNull,              // oprowner
				kind,                    // oprkind
				tree.DBoolFalse,         // oprcanmerge
				tree.DBoolFalse,         // oprcanhash
				leftType,                // oprleft
				rightType,               // oprright
				returnType,              // oprresult
				tree.DNull,              // oprcom
				tree.DNull,              // oprnegate
				tree.DNull,              // oprcode
				tree.DNull,              // oprrest
				tree.DNull,              // oprjoin
			)
			return err
		}
		for cmpOp, overloads := range tree.CmpOps {
			// n.b. the In operator cannot be included in this list because it isn't
			// a generalized operator. It is a special syntax form, because it only
			// permits parenthesized subqueries or row expressions on the RHS.
			if cmpOp == tree.In {
				continue
			}
			for _, overload := range overloads {
				params, returnType := tree.GetParamsAndReturnType(overload)
				if err := addOp(cmpOp.String(), infixKind, params, returnType); err != nil {
					return err
				}
				if inverse, ok := cmpOp.Inverse(); ok {
					if err := addOp(inverse.String(), infixKind, params, returnType); err != nil {
						return err
					}
				}
			}
		}
		for binOp, overloads := range tree.BinOps {
			for _, overload := range overloads {
				params, returnType := tree.GetParamsAndReturnType(overload)
				if err := addOp(binOp.String(), infixKind, params, returnType); err != nil {
					return err
				}
			}
		}
		for unaryOp, overloads := range tree.UnaryOps {
			for _, overload := range overloads {
				params, returnType := tree.GetParamsAndReturnType(overload)
				if err := addOp(unaryOp.String(), prefixKind, params, returnType); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

func newSingletonStringArray(s string) tree.Datum {
	return &tree.DArray{ParamTyp: types.String, Array: tree.Datums{tree.NewDString(s)}}
}

var (
	proArgModeInOut    = newSingletonStringArray("b")
	proArgModeIn       = newSingletonStringArray("i")
	proArgModeOut      = newSingletonStringArray("o")
	proArgModeTable    = newSingletonStringArray("t")
	proArgModeVariadic = newSingletonStringArray("v")

	// Avoid unused warning for constants.
	_ = proArgModeInOut
	_ = proArgModeIn
	_ = proArgModeOut
	_ = proArgModeTable
)

var pgCatalogPreparedXactsTable = virtualSchemaTable{
	comment: `prepared transactions (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/view-pg-prepared-xacts.html`,
	schema: `
CREATE TABLE pg_catalog.pg_prepared_xacts (
  transaction INTEGER,
  gid TEXT,
  prepared TIMESTAMP WITH TIME ZONE,
  owner NAME,
  database NAME
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

// pgCatalogPreparedStatementsTable implements the pg_prepared_statements table.
// The statement field differs in that it uses the parsed version
// of the PREPARE statement.
// The parameter_types field differs from postgres as the type names in
// cockroach are slightly different.
var pgCatalogPreparedStatementsTable = virtualSchemaTable{
	comment: `prepared statements
https://www.postgresql.org/docs/9.6/view-pg-prepared-statements.html`,
	schema: `
CREATE TABLE pg_catalog.pg_prepared_statements (
	name TEXT,
	statement TEXT,
	prepare_time TIMESTAMPTZ,
	parameter_types REGTYPE[],
	from_sql boolean
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for name, stmt := range p.preparedStatements.List() {
			placeholderTypes := stmt.PrepareMetadata.PlaceholderTypesInfo.Types
			paramTypes := tree.NewDArray(types.RegType)
			paramTypes.Array = make(tree.Datums, len(placeholderTypes))
			paramNames := make([]string, len(placeholderTypes))

			for i, placeholderType := range placeholderTypes {
				paramTypes.Array[i] = tree.NewDOidWithName(
					tree.DInt(placeholderType.Oid()),
					placeholderType,
					placeholderType.SQLStandardName(),
				)
				paramNames[i] = placeholderType.Name()
			}

			// Only append arguments to string if required.
			argumentsStr := ""
			if len(paramNames) > 0 {
				argumentsStr = fmt.Sprintf(" (%s)", strings.Join(paramNames, ", "))
			}

			fromSQL := tree.DBoolFalse
			if stmt.origin == PreparedStatementOriginSQL {
				fromSQL = tree.DBoolTrue
			}

			ts, err := tree.MakeDTimestampTZ(stmt.createdAt, time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDString(name),
				tree.NewDString(fmt.Sprintf("PREPARE %s%s AS %s", name, argumentsStr, stmt.SQL)),
				ts,
				paramTypes,
				fromSQL,
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogProcTable = virtualSchemaTable{
	comment: `built-in functions (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-proc.html`,
	schema: `
CREATE TABLE pg_catalog.pg_proc (
	oid OID,
	proname NAME,
	pronamespace OID,
	proowner OID,
	prolang OID,
	procost FLOAT4,
	prorows FLOAT4,
	provariadic OID,
	protransform STRING,
	proisagg BOOL,
	proiswindow BOOL,
	prosecdef BOOL,
	proleakproof BOOL,
	proisstrict BOOL,
	proretset BOOL,
	provolatile CHAR,
	proparallel CHAR,
	pronargs INT2,
	pronargdefaults INT2,
	prorettype OID,
	proargtypes OIDVECTOR,
	proallargtypes OID[],
	proargmodes STRING[],
	proargnames STRING[],
	proargdefaults STRING,
	protrftypes OID[],
	prosrc STRING,
	probin STRING,
	proconfig STRING[],
	proacl STRING[]
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *sqlbase.ImmutableDatabaseDescriptor) error {
				nspOid := h.NamespaceOid(db, pgCatalogName)
				for _, name := range builtins.AllBuiltinNames {
					// parser.Builtins contains duplicate uppercase and lowercase keys.
					// Only return the lowercase ones for compatibility with postgres.
					var first rune
					for _, c := range name {
						first = c
						break
					}
					if unicode.IsUpper(first) {
						continue
					}
					props, overloads := builtins.GetBuiltinProperties(name)
					isAggregate := props.Class == tree.AggregateClass
					isWindow := props.Class == tree.WindowClass
					for _, builtin := range overloads {
						dName := tree.NewDName(name)
						dSrc := tree.NewDString(name)

						var retType tree.Datum
						isRetSet := false
						if fixedRetType := builtin.FixedReturnType(); fixedRetType != nil {
							var retOid oid.Oid
							if fixedRetType.Family() == types.TupleFamily && builtin.Generator != nil {
								isRetSet = true
								// Functions returning tables with zero, or more than one
								// columns are marked to return "anyelement"
								// (e.g. `unnest`)
								retOid = oid.T_anyelement
								if len(fixedRetType.TupleContents()) == 1 {
									// Functions returning tables with exactly one column
									// are marked to return the type of that column
									// (e.g. `generate_series`).
									retOid = fixedRetType.TupleContents()[0].Oid()
								}
							} else {
								retOid = fixedRetType.Oid()
							}
							retType = tree.NewDOid(tree.DInt(retOid))
						}

						argTypes := builtin.Types
						dArgTypes := tree.NewDArray(types.Oid)
						for _, argType := range argTypes.Types() {
							if err := dArgTypes.Append(tree.NewDOid(tree.DInt(argType.Oid()))); err != nil {
								return err
							}
						}

						var argmodes tree.Datum
						var variadicType tree.Datum
						switch v := argTypes.(type) {
						case tree.VariadicType:
							if len(v.FixedTypes) == 0 {
								argmodes = proArgModeVariadic
							} else {
								ary := tree.NewDArray(types.String)
								for range v.FixedTypes {
									if err := ary.Append(tree.NewDString("i")); err != nil {
										return err
									}
								}
								if err := ary.Append(tree.NewDString("v")); err != nil {
									return err
								}
								argmodes = ary
							}
							variadicType = tree.NewDOid(tree.DInt(v.VarType.Oid()))
						case tree.HomogeneousType:
							argmodes = proArgModeVariadic
							argType := types.Any
							oid := argType.Oid()
							variadicType = tree.NewDOid(tree.DInt(oid))
						default:
							argmodes = tree.DNull
							variadicType = oidZero
						}
						provolatile, proleakproof := builtin.Volatility.ToPostgres()

						err := addRow(
							h.BuiltinOid(name, &builtin),             // oid
							dName,                                    // proname
							nspOid,                                   // pronamespace
							tree.DNull,                               // proowner
							oidZero,                                  // prolang
							tree.DNull,                               // procost
							tree.DNull,                               // prorows
							variadicType,                             // provariadic
							tree.DNull,                               // protransform
							tree.MakeDBool(tree.DBool(isAggregate)),  // proisagg
							tree.MakeDBool(tree.DBool(isWindow)),     // proiswindow
							tree.DBoolFalse,                          // prosecdef
							tree.MakeDBool(tree.DBool(proleakproof)), // proleakproof
							tree.DBoolFalse,                          // proisstrict
							tree.MakeDBool(tree.DBool(isRetSet)),     // proretset
							tree.NewDString(provolatile),             // provolatile
							tree.DNull,                               // proparallel
							tree.NewDInt(tree.DInt(builtin.Types.Length())), // pronargs
							tree.NewDInt(tree.DInt(0)),                      // pronargdefaults
							retType,                                         // prorettype
							tree.NewDOidVectorFromDArray(dArgTypes),         // proargtypes
							tree.DNull,                                      // proallargtypes
							argmodes,                                        // proargmodes
							tree.DNull,                                      // proargnames
							tree.DNull,                                      // proargdefaults
							tree.DNull,                                      // protrftypes
							dSrc,                                            // prosrc
							tree.DNull,                                      // probin
							tree.DNull,                                      // proconfig
							tree.DNull,                                      // proacl
						)
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

var pgCatalogRangeTable = virtualSchemaTable{
	comment: `range types (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-range.html`,
	schema: `
CREATE TABLE pg_catalog.pg_range (
	rngtypid OID,
	rngsubtype OID,
	rngcollation OID,
	rngsubopc OID,
	rngcanonical OID,
	rngsubdiff OID
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// We currently do not support any range types, so this table is empty.
		// This table should be populated when any range types are added to
		// oidToDatum (and therefore pg_type).
		return nil
	},
}

var pgCatalogRewriteTable = virtualSchemaTable{
	comment: `rewrite rules (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-rewrite.html`,
	schema: `
CREATE TABLE pg_catalog.pg_rewrite (
	oid OID,
	rulename NAME,
	ev_class OID,
	ev_type TEXT,
	ev_enabled TEXT,
	is_instead BOOL,
	ev_qual TEXT,
	ev_action TEXT
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Rewrite rules are not supported.
		return nil
	},
}

var pgCatalogRolesTable = virtualSchemaTable{
	comment: `database roles
https://www.postgresql.org/docs/9.5/view-pg-roles.html`,
	schema: `
CREATE TABLE pg_catalog.pg_roles (
	oid OID,
	rolname NAME,
	rolsuper BOOL,
	rolinherit BOOL,
	rolcreaterole BOOL,
	rolcreatedb BOOL,
	rolcatupdate BOOL,
	rolcanlogin BOOL,
	rolreplication BOOL,
	rolconnlimit INT4,
	rolpassword STRING,
	rolvaliduntil TIMESTAMPTZ,
	rolbypassrls BOOL,
	rolconfig STRING[]
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// We intentionally do not check if the user has access to system.user.
		// Because Postgres allows access to pg_roles by non-privileged users, we
		// need to do the same. This shouldn't be an issue, because pg_roles doesn't
		// include sensitive information such as password hashes.
		h := makeOidHasher()
		return forEachRole(ctx, p,
			func(username string, isRole bool, noLogin bool, rolValidUntil *time.Time) error {
				isRoot := tree.DBool(username == security.RootUser || username == sqlbase.AdminRole)
				isRoleDBool := tree.DBool(isRole)
				roleCanLogin := tree.DBool(!noLogin)
				roleValidUntilValue := tree.DNull
				if rolValidUntil != nil {
					var err error
					roleValidUntilValue, err = tree.MakeDTimestampTZ(*rolValidUntil, time.Second)
					if err != nil {
						return err
					}
				}

				return addRow(
					h.UserOid(username),          // oid
					tree.NewDName(username),      // rolname
					tree.MakeDBool(isRoot),       // rolsuper
					tree.MakeDBool(isRoleDBool),  // rolinherit. Roles inherit by default.
					tree.MakeDBool(isRoot),       // rolcreaterole
					tree.MakeDBool(isRoot),       // rolcreatedb
					tree.DBoolFalse,              // rolcatupdate
					tree.MakeDBool(roleCanLogin), // rolcanlogin.
					tree.DBoolFalse,              // rolreplication
					negOneVal,                    // rolconnlimit
					passwdStarString,             // rolpassword
					roleValidUntilValue,          // rolvaliduntil
					tree.DBoolFalse,              // rolbypassrls
					tree.DNull,                   // rolconfig
				)
			})
	},
}

var pgCatalogSecLabelsTable = virtualSchemaTable{
	comment: `security labels (empty)
https://www.postgresql.org/docs/9.6/view-pg-seclabels.html`,
	schema: `
CREATE TABLE pg_catalog.pg_seclabels (
	objoid OID,
  classoid OID,
  objsubid INT4,
  objtype TEXT,
	objnamespace OID,
	objname TEXT,
	provider TEXT,
	label TEXT
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogSequencesTable = virtualSchemaTable{
	comment: `sequences (see also information_schema.sequences)
https://www.postgresql.org/docs/9.5/catalog-pg-sequence.html`,
	schema: `
CREATE TABLE pg_catalog.pg_sequence (
	seqrelid OID,
	seqtypid OID,
	seqstart INT8,
	seqincrement INT8,
	seqmax INT8,
	seqmin INT8,
	seqcache INT8,
	seqcycle BOOL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual schemas do not have indexes */
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, table *sqlbase.ImmutableTableDescriptor) error {
				if !table.IsSequence() {
					return nil
				}
				opts := table.SequenceOpts
				return addRow(
					tableOid(table.ID),                      // seqrelid
					tree.NewDOid(tree.DInt(oid.T_int8)),     // seqtypid
					tree.NewDInt(tree.DInt(opts.Start)),     // seqstart
					tree.NewDInt(tree.DInt(opts.Increment)), // seqincrement
					tree.NewDInt(tree.DInt(opts.MaxValue)),  // seqmax
					tree.NewDInt(tree.DInt(opts.MinValue)),  // seqmin
					tree.NewDInt(1),                         // seqcache
					tree.DBoolFalse,                         // seqcycle
				)
			})
	},
}

var (
	varTypeString   = tree.NewDString("string")
	settingsCtxUser = tree.NewDString("user")
)

var pgCatalogSettingsTable = virtualSchemaTable{
	comment: `session variables (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-settings.html`,
	schema: `
CREATE TABLE pg_catalog.pg_settings (
    name STRING,
    setting STRING,
    unit STRING,
    category STRING,
    short_desc STRING,
    extra_desc STRING,
    context STRING,
    vartype STRING,
    source STRING,
    min_val STRING,
    max_val STRING,
    enumvals STRING,
    boot_val STRING,
    reset_val STRING,
    sourcefile STRING,
    sourceline INT4,
    pending_restart BOOL
)`,
	populate: func(_ context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			if gen.Hidden {
				continue
			}
			value := gen.Get(&p.extendedEvalCtx)
			valueDatum := tree.NewDString(value)
			var bootDatum tree.Datum = tree.DNull
			var resetDatum tree.Datum = tree.DNull
			if gen.Set == nil && gen.RuntimeSet == nil {
				// RESET/SET will leave the variable unchanged. Announce the
				// current value as boot/reset value.
				bootDatum = valueDatum
				resetDatum = bootDatum
			} else {
				if gen.GlobalDefault != nil {
					globalDefVal := gen.GlobalDefault(&p.EvalContext().Settings.SV)
					bootDatum = tree.NewDString(globalDefVal)
				}
				if hasDefault, defVal := getSessionVarDefaultString(vName, gen, p.sessionDataMutator); hasDefault {
					resetDatum = tree.NewDString(defVal)
				}
			}
			if err := addRow(
				tree.NewDString(strings.ToLower(vName)), // name
				valueDatum,                              // setting
				tree.DNull,                              // unit
				tree.DNull,                              // category
				tree.DNull,                              // short_desc
				tree.DNull,                              // extra_desc
				settingsCtxUser,                         // context
				varTypeString,                           // vartype
				tree.DNull,                              // source
				tree.DNull,                              // min_val
				tree.DNull,                              // max_val
				tree.DNull,                              // enumvals
				bootDatum,                               // boot_val
				resetDatum,                              // reset_val
				tree.DNull,                              // sourcefile
				tree.DNull,                              // sourceline
				tree.DBoolFalse,                         // pending_restart
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogShdependTable = virtualSchemaTable{
	comment: `shared dependencies (empty - not implemented)
https://www.postgresql.org/docs/9.6/catalog-pg-shdepend.html`,
	schema: `
CREATE TABLE pg_catalog.pg_shdepend (
	dbid OID,
	classid OID,
	objid OID,
  objsubid INT4,
	refclassid OID,
	refobjid OID,
	deptype CHAR
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogTablesTable = virtualSchemaTable{
	comment: `tables summary (see also information_schema.tables, pg_catalog.pg_class)
https://www.postgresql.org/docs/9.5/view-pg-tables.html`,
	schema: `
CREATE TABLE pg_catalog.pg_tables (
	schemaname NAME,
	tablename NAME,
	tableowner NAME,
	tablespace NAME,
	hasindexes BOOL,
	hasrules BOOL,
	hastriggers BOOL,
	rowsecurity BOOL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Note: pg_catalog.pg_tables is not well-defined if the dbContext is
		// empty -- listing tables across databases can yield duplicate
		// schema/table names.
		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, table *sqlbase.ImmutableTableDescriptor) error {
				if !table.IsTable() {
					return nil
				}
				return addRow(
					tree.NewDName(scName),     // schemaname
					tree.NewDName(table.Name), // tablename
					tree.DNull,                // tableowner
					tree.DNull,                // tablespace
					tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // hasindexes
					tree.DBoolFalse, // hasrules
					tree.DBoolFalse, // hastriggers
					tree.DBoolFalse, // rowsecurity
				)
			})
	},
}

var pgCatalogTablespaceTable = virtualSchemaTable{
	comment: `available tablespaces (incomplete; concept inapplicable to CockroachDB)
https://www.postgresql.org/docs/9.5/catalog-pg-tablespace.html`,
	schema: `
CREATE TABLE pg_catalog.pg_tablespace (
	oid OID,
	spcname NAME,
	spcowner OID,
	spclocation TEXT,
	spcacl TEXT[],
	spcoptions TEXT[]
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return addRow(
			oidZero,                       // oid
			tree.NewDString("pg_default"), // spcname
			tree.DNull,                    // spcowner
			tree.DNull,                    // spclocation
			tree.DNull,                    // spcacl
			tree.DNull,                    // spcoptions
		)
	},
}

var pgCatalogTriggerTable = virtualSchemaTable{
	comment: `triggers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-trigger.html`,
	schema: `
CREATE TABLE pg_catalog.pg_trigger (
	oid OID,
	tgrelid OID,
	tgname NAME,
	tgfoid OID,
	tgtype INT2,
	tgenabled TEXT,
	tgisinternal BOOL,
	tgconstrrelid OID,
	tgconstrindid OID,
	tgconstraint OID,
	tgdeferrable BOOL,
	tginitdeferred BOOL,
	tgnargs INT2,
	tgattr INT2VECTOR,
	tgargs BYTEA,
	tgqual TEXT,
	tgoldtable NAME,
	tgnewtable NAME
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Triggers are unsupported.
		return nil
	},
}

var (
	typTypeBase      = tree.NewDString("b")
	typTypeComposite = tree.NewDString("c")
	typTypeDomain    = tree.NewDString("d")
	typTypeEnum      = tree.NewDString("e")
	typTypePseudo    = tree.NewDString("p")
	typTypeRange     = tree.NewDString("r")

	// Avoid unused warning for constants.
	_ = typTypeComposite
	_ = typTypeDomain
	_ = typTypePseudo
	_ = typTypeRange

	// See https://www.postgresql.org/docs/9.6/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE.
	typCategoryArray       = tree.NewDString("A")
	typCategoryBoolean     = tree.NewDString("B")
	typCategoryComposite   = tree.NewDString("C")
	typCategoryDateTime    = tree.NewDString("D")
	typCategoryEnum        = tree.NewDString("E")
	typCategoryGeometric   = tree.NewDString("G")
	typCategoryNetworkAddr = tree.NewDString("I")
	typCategoryNumeric     = tree.NewDString("N")
	typCategoryPseudo      = tree.NewDString("P")
	typCategoryRange       = tree.NewDString("R")
	typCategoryString      = tree.NewDString("S")
	typCategoryTimespan    = tree.NewDString("T")
	typCategoryUserDefined = tree.NewDString("U")
	typCategoryBitString   = tree.NewDString("V")
	typCategoryUnknown     = tree.NewDString("X")

	// Avoid unused warning for constants.
	_ = typCategoryComposite
	_ = typCategoryEnum
	_ = typCategoryGeometric
	_ = typCategoryRange
	_ = typCategoryBitString

	typDelim = tree.NewDString(",")
)

func addPGTypeRow(
	h oidHasher, nspOid tree.Datum, typ *types.T, addRow func(...tree.Datum) error,
) error {
	cat := typCategory(typ)
	typType := typTypeBase
	typElem := oidZero
	typArray := oidZero
	builtinPrefix := builtins.PGIOBuiltinPrefix(typ)
	switch typ.Family() {
	case types.ArrayFamily:
		switch typ.Oid() {
		case oid.T_int2vector:
			// IntVector needs a special case because it's a special snowflake
			// type that behaves in some ways like a scalar type and in others
			// like an array type.
			typElem = tree.NewDOid(tree.DInt(oid.T_int2))
			typArray = tree.NewDOid(tree.DInt(oid.T__int2vector))
		case oid.T_oidvector:
			// Same story as above for OidVector.
			typElem = tree.NewDOid(tree.DInt(oid.T_oid))
			typArray = tree.NewDOid(tree.DInt(oid.T__oidvector))
		case oid.T_anyarray:
			// AnyArray does not use a prefix or element type.
		default:
			builtinPrefix = "array_"
			typElem = tree.NewDOid(tree.DInt(typ.ArrayContents().Oid()))
		}
	default:
		typArray = tree.NewDOid(tree.DInt(types.MakeArray(typ).Oid()))
	}
	if typ.Family() == types.EnumFamily {
		builtinPrefix = "enum_"
		typType = typTypeEnum
	}
	if cat == typCategoryPseudo {
		typType = typTypePseudo
	}
	typname := typ.PGName()

	return addRow(
		tree.NewDOid(tree.DInt(typ.Oid())), // oid
		tree.NewDName(typname),             // typname
		nspOid,                             // typnamespace
		tree.DNull,                         // typowner
		typLen(typ),                        // typlen
		typByVal(typ),                      // typbyval (is it fixedlen or not)
		typType,                            // typtype
		cat,                                // typcategory
		tree.DBoolFalse,                    // typispreferred
		tree.DBoolTrue,                     // typisdefined
		typDelim,                           // typdelim
		oidZero,                            // typrelid
		typElem,                            // typelem
		typArray,                           // typarray

		// regproc references
		h.RegProc(builtinPrefix+"in"),   // typinput
		h.RegProc(builtinPrefix+"out"),  // typoutput
		h.RegProc(builtinPrefix+"recv"), // typreceive
		h.RegProc(builtinPrefix+"send"), // typsend
		oidZero,                         // typmodin
		oidZero,                         // typmodout
		oidZero,                         // typanalyze

		tree.DNull,      // typalign
		tree.DNull,      // typstorage
		tree.DBoolFalse, // typnotnull
		oidZero,         // typbasetype
		negOneVal,       // typtypmod
		zeroVal,         // typndims
		typColl(typ, h), // typcollation
		tree.DNull,      // typdefaultbin
		tree.DNull,      // typdefault
		tree.DNull,      // typacl
	)
}

// TODO (rohany): We should add a virtual index on OID here. See #49208.
var pgCatalogTypeTable = virtualSchemaTable{
	comment: `scalar types (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-type.html`,
	schema: `
CREATE TABLE pg_catalog.pg_type (
	oid OID,
	typname NAME NOT NULL,
	typnamespace OID,
	typowner OID,
	typlen INT2,
	typbyval BOOL,
	typtype CHAR,
	typcategory CHAR,
	typispreferred BOOL,
	typisdefined BOOL,
	typdelim CHAR,
	typrelid OID,
	typelem OID,
	typarray OID,
	typinput REGPROC,
	typoutput REGPROC,
	typreceive REGPROC,
	typsend REGPROC,
	typmodin REGPROC,
	typmodout REGPROC,
	typanalyze REGPROC,
	typalign CHAR,
	typstorage CHAR,
	typnotnull BOOL,
	typbasetype OID,
	typtypmod INT4,
	typndims INT4,
	typcollation OID,
	typdefaultbin STRING,
	typdefault STRING,
	typacl STRING[]
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *sqlbase.ImmutableDatabaseDescriptor) error {
				nspOid := h.NamespaceOid(db, pgCatalogName)

				// Generate rows for all predefined types.
				for _, typ := range types.OidToType {
					if err := addPGTypeRow(h, nspOid, typ, addRow); err != nil {
						return err
					}
				}

				// Now generate rows for user defined types in this database.

				return forEachTypeDesc(ctx, p, dbContext, func(_ *sqlbase.ImmutableDatabaseDescriptor, _ string, typDesc *sqlbase.ImmutableTypeDescriptor) error {
					typ, err := typDesc.MakeTypesT(
						tree.NewUnqualifiedTypeName(tree.Name(typDesc.GetName())),
						p.makeTypeLookupFn(ctx),
					)
					if err != nil {
						return err
					}
					return addPGTypeRow(h, nspOid, typ, addRow)
				})
			})
	},
}

var pgCatalogUserTable = virtualSchemaTable{
	comment: `database users
https://www.postgresql.org/docs/9.5/view-pg-user.html`,
	schema: `
CREATE TABLE pg_catalog.pg_user (
	usename NAME,
	usesysid OID,
	usecreatedb BOOL,
	usesuper BOOL,
	userepl  BOOL,
	usebypassrls BOOL,
	passwd TEXT,
	valuntil TIMESTAMP,
	useconfig TEXT[]
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRole(ctx, p,
			func(username string, isRole bool, noLogin bool, rolValidUntil *time.Time) error {
				if isRole {
					return nil
				}
				isRoot := tree.DBool(username == security.RootUser)
				return addRow(
					tree.NewDName(username), // usename
					h.UserOid(username),     // usesysid
					tree.MakeDBool(isRoot),  // usecreatedb
					tree.MakeDBool(isRoot),  // usesuper
					tree.DBoolFalse,         // userepl
					tree.DBoolFalse,         // usebypassrls
					passwdStarString,        // passwd
					tree.DNull,              // valuntil
					tree.DNull,              // useconfig
				)
			})
	},
}

var pgCatalogUserMappingTable = virtualSchemaTable{
	comment: `local to remote user mapping (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-user-mapping.html`,
	schema: `
CREATE TABLE pg_catalog.pg_user_mapping (
	oid OID,
	umuser OID,
	umserver OID,
	umoptions TEXT[]
)`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// This table stores the mapping to foreign server users.
		// Foreign servers are not supported.
		return nil
	},
}

var pgCatalogStatActivityTable = virtualSchemaTable{
	comment: `backend access statistics (empty - monitoring works differently in CockroachDB)
https://www.postgresql.org/docs/9.6/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW`,
	schema: `
CREATE TABLE pg_catalog.pg_stat_activity (
	datid OID,
	datname NAME,
	pid INTEGER,
	usesysid OID,
	usename NAME,
	application_name TEXT,
	client_addr INET,
	client_hostname TEXT,
	client_port INTEGER,
	backend_start TIMESTAMPTZ,
	xact_start TIMESTAMPTZ,
	query_start TIMESTAMPTZ,
	state_change TIMESTAMPTZ,
	wait_event_type TEXT,
	wait_event TEXT,
	state TEXT,
	backend_xid INTEGER,
	backend_xmin INTEGER,
	query TEXT
)
`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogSecurityLabelTable = virtualSchemaTable{
	comment: `security labels (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-seclabel.html`,
	schema: `
CREATE TABLE pg_catalog.pg_seclabel (
	objoid OID,
	classoid OID,
	objsubid INTEGER,
	provider TEXT,
	label TEXT
)
`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogSharedSecurityLabelTable = virtualSchemaTable{
	comment: `shared security labels (empty - feature not supported)
https://www.postgresql.org/docs/9.5/catalog-pg-shseclabel.html`,
	schema: `
CREATE TABLE pg_catalog.pg_shseclabel (
	objoid OID,
	classoid OID,
	provider TEXT,
	label TEXT
)
`,
	populate: func(ctx context.Context, p *planner, _ *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

// typOid is the only OID generation approach that does not use oidHasher, because
// object identifiers for types are not arbitrary, but instead need to be kept in
// sync with Postgres.
func typOid(typ *types.T) tree.Datum {
	return tree.NewDOid(tree.DInt(typ.Oid()))
}

func typLen(typ *types.T) *tree.DInt {
	if sz, variable := tree.DatumTypeSize(typ); !variable {
		return tree.NewDInt(tree.DInt(sz))
	}
	return negOneVal
}

func typByVal(typ *types.T) tree.Datum {
	_, variable := tree.DatumTypeSize(typ)
	return tree.MakeDBool(tree.DBool(!variable))
}

// typColl returns the collation OID for a given type.
// The default collation is en-US, which is equivalent to but spelled
// differently than the default database collation, en_US.utf8.
func typColl(typ *types.T, h oidHasher) tree.Datum {
	switch typ.Family() {
	case types.AnyFamily:
		return oidZero
	case types.StringFamily:
		return h.CollationOid(defaultCollationTag)
	case types.CollatedStringFamily:
		return h.CollationOid(typ.Locale())
	}

	if typ.Equivalent(types.StringArray) {
		return h.CollationOid(defaultCollationTag)
	}
	return oidZero
}

// This mapping should be kept sync with PG's categorization.
var datumToTypeCategory = map[types.Family]*tree.DString{
	types.AnyFamily:         typCategoryPseudo,
	types.BitFamily:         typCategoryBitString,
	types.BoolFamily:        typCategoryBoolean,
	types.BytesFamily:       typCategoryUserDefined,
	types.DateFamily:        typCategoryDateTime,
	types.EnumFamily:        typCategoryEnum,
	types.TimeFamily:        typCategoryDateTime,
	types.TimeTZFamily:      typCategoryDateTime,
	types.FloatFamily:       typCategoryNumeric,
	types.IntFamily:         typCategoryNumeric,
	types.IntervalFamily:    typCategoryTimespan,
	types.GeographyFamily:   typCategoryUserDefined,
	types.GeometryFamily:    typCategoryUserDefined,
	types.JsonFamily:        typCategoryUserDefined,
	types.DecimalFamily:     typCategoryNumeric,
	types.StringFamily:      typCategoryString,
	types.TimestampFamily:   typCategoryDateTime,
	types.TimestampTZFamily: typCategoryDateTime,
	types.ArrayFamily:       typCategoryArray,
	types.TupleFamily:       typCategoryPseudo,
	types.OidFamily:         typCategoryNumeric,
	types.UuidFamily:        typCategoryUserDefined,
	types.INetFamily:        typCategoryNetworkAddr,
	types.UnknownFamily:     typCategoryUnknown,
}

func typCategory(typ *types.T) tree.Datum {
	// Special case ARRAY of ANY.
	if typ.Family() == types.ArrayFamily && typ.ArrayContents().Family() == types.AnyFamily {
		return typCategoryPseudo
	}
	return datumToTypeCategory[typ.Family()]
}

var pgCatalogViewsTable = virtualSchemaTable{
	comment: `view definitions (incomplete - see also information_schema.views)
https://www.postgresql.org/docs/9.5/view-pg-views.html`,
	schema: `
CREATE TABLE pg_catalog.pg_views (
	schemaname NAME,
	viewname NAME,
	viewowner STRING,
	definition STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Note: pg_views is not well defined if the dbContext is empty,
		// because it does not distinguish views in separate databases.
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /*virtual schemas do not have views*/
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, desc *sqlbase.ImmutableTableDescriptor) error {
				if !desc.IsView() {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition
				// returned, unlike postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(a-robinson): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					tree.NewDName(scName),           // schemaname
					tree.NewDName(desc.Name),        // viewname
					tree.DNull,                      // viewowner
					tree.NewDString(desc.ViewQuery), // definition
				)
			})
	},
}

var pgCatalogAggregateTable = virtualSchemaTable{
	comment: `aggregated built-in functions (incomplete)
https://www.postgresql.org/docs/9.6/catalog-pg-aggregate.html`,
	schema: `
CREATE TABLE pg_catalog.pg_aggregate (
	aggfnoid REGPROC,
	aggkind  CHAR,
	aggnumdirectargs INT2,
	aggtransfn REGPROC,
	aggfinalfn REGPROC,
	aggcombinefn REGPROC,
	aggserialfn REGPROC,
	aggdeserialfn REGPROC,
	aggmtransfn REGPROC,
	aggminvtransfn REGPROC,
	aggmfinalfn REGPROC,
	aggfinalextra BOOL,
	aggmfinalextra BOOL,
	aggsortop OID,
	aggtranstype OID,
	aggtransspace INT4,
	aggmtranstype OID,
	aggmtransspace INT4,
	agginitval TEXT,
	aggminitval TEXT
)
`,
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *sqlbase.ImmutableDatabaseDescriptor) error {
				for _, name := range builtins.AllAggregateBuiltinNames {
					if name == builtins.AnyNotNull {
						// any_not_null is treated as a special case.
						continue
					}
					_, overloads := builtins.GetBuiltinProperties(name)
					for _, overload := range overloads {
						params, _ := tree.GetParamsAndReturnType(overload)
						sortOperatorOid := oidZero
						aggregateKind := tree.NewDString("n")
						aggNumDirectArgs := zeroVal
						if params.Length() != 0 {
							argType := tree.NewDOid(tree.DInt(params.Types()[0].Oid()))
							returnType := tree.NewDOid(tree.DInt(oid.T_bool))
							switch name {
							// Cases to determine sort operator.
							case "max", "bool_or":
								sortOperatorOid = h.OperatorOid(">", argType, argType, returnType)
							case "min", "bool_and", "every":
								sortOperatorOid = h.OperatorOid("<", argType, argType, returnType)

							// Cases to determine aggregate kind.
							case "rank", "percent_rank", "cume_dist", "dense_rank":
								aggregateKind = tree.NewDString("h")
								aggNumDirectArgs = tree.NewDInt(1)
							case "mode":
								aggregateKind = tree.NewDString("o")
							default:
								if strings.HasPrefix(name, "percentile_") {
									aggregateKind = tree.NewDString("o")
									aggNumDirectArgs = tree.NewDInt(1)
								}
							}
						}
						regprocForZeroOid := tree.NewDOidWithName(tree.DInt(0), types.RegProc, "-")
						err := addRow(
							h.BuiltinOid(name, &overload).AsRegProc(name), // aggfnoid
							aggregateKind,     // aggkind
							aggNumDirectArgs,  // aggnumdirectargs
							regprocForZeroOid, // aggtransfn
							regprocForZeroOid, // aggfinalfn
							regprocForZeroOid, // aggcombinefn
							regprocForZeroOid, // aggserialfn
							regprocForZeroOid, // aggdeserialfn
							regprocForZeroOid, // aggmtransfn
							regprocForZeroOid, // aggminvtransfn
							regprocForZeroOid, // aggmfinalfn
							tree.DBoolFalse,   // aggfinalextra
							tree.DBoolFalse,   // aggmfinalextra
							sortOperatorOid,   // aggsortop
							tree.DNull,        // aggtranstype
							tree.DNull,        // aggtransspace
							tree.DNull,        // aggmtranstype
							tree.DNull,        // aggmtransspace
							tree.DNull,        // agginitval
							tree.DNull,        // aggminitval
						)
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

// oidHasher provides a consistent hashing mechanism for object identifiers in
// pg_catalog tables, allowing for reliable joins across tables.
//
// In Postgres, oids are physical properties of database objects which are
// sequentially generated and naturally unique across all objects. See:
// https://www.postgresql.org/docs/9.6/static/datatype-oid.html.
// Because Cockroach does not have an equivalent concept, we generate arbitrary
// fingerprints for database objects with the only requirements being that they
// are unique across all objects and that they are stable across accesses.
//
// The type has a few layers of methods:
// - write<go_type> methods write concrete types to the underlying running hash.
// - write<db_object> methods account for single database objects like TableDescriptors
//   or IndexDescriptors in the running hash. These methods aim to write information
//   that would uniquely fingerprint the object to the hash using the first layer of
//   methods.
// - <DB_Object>Oid methods use the second layer of methods to construct a unique
//   object identifier for the provided database object. This object identifier will
//   be returned as a *tree.DInt, and the running hash will be reset. These are the
//   only methods that are part of the oidHasher's external facing interface.
//
type oidHasher struct {
	h hash.Hash32
}

func makeOidHasher() oidHasher {
	return oidHasher{h: fnv.New32()}
}

func (h oidHasher) writeStr(s string) {
	if _, err := h.h.Write([]byte(s)); err != nil {
		panic(err)
	}
}

func (h oidHasher) writeBytes(b []byte) {
	if _, err := h.h.Write(b); err != nil {
		panic(err)
	}
}

func (h oidHasher) writeUInt8(i uint8) {
	if err := binary.Write(h.h, binary.BigEndian, i); err != nil {
		panic(err)
	}
}

func (h oidHasher) writeUInt32(i uint32) {
	if err := binary.Write(h.h, binary.BigEndian, i); err != nil {
		panic(err)
	}
}

func (h oidHasher) writeUInt64(i uint64) {
	if err := binary.Write(h.h, binary.BigEndian, i); err != nil {
		panic(err)
	}
}

func (h oidHasher) writeOID(oid *tree.DOid) {
	h.writeUInt64(uint64(oid.DInt))
}

type oidTypeTag uint8

const (
	_ oidTypeTag = iota
	namespaceTypeTag
	indexTypeTag
	columnTypeTag
	checkConstraintTypeTag
	fkConstraintTypeTag
	pKeyConstraintTypeTag
	uniqueConstraintTypeTag
	functionTypeTag
	userTypeTag
	collationTypeTag
	operatorTypeTag
	enumEntryTypeTag
)

func (h oidHasher) writeTypeTag(tag oidTypeTag) {
	h.writeUInt8(uint8(tag))
}

func (h oidHasher) getOid() *tree.DOid {
	i := h.h.Sum32()
	h.h.Reset()
	return tree.NewDOid(tree.DInt(i))
}

func (h oidHasher) writeDB(db *sqlbase.ImmutableDatabaseDescriptor) {
	h.writeUInt32(uint32(db.GetID()))
}

func (h oidHasher) writeSchema(scName string) {
	h.writeStr(scName)
}

func (h oidHasher) writeTable(tableID sqlbase.ID) {
	h.writeUInt32(uint32(tableID))
}

func (h oidHasher) writeIndex(indexID sqlbase.IndexID) {
	h.writeUInt32(uint32(indexID))
}

func (h oidHasher) writeCheckConstraint(check *sqlbase.TableDescriptor_CheckConstraint) {
	h.writeStr(check.Name)
	h.writeStr(check.Expr)
}

func (h oidHasher) writeForeignKeyConstraint(fk *sqlbase.ForeignKeyConstraint) {
	h.writeUInt32(uint32(fk.ReferencedTableID))
	h.writeStr(fk.Name)
}

func (h oidHasher) NamespaceOid(db *sqlbase.ImmutableDatabaseDescriptor, scName string) *tree.DOid {
	h.writeTypeTag(namespaceTypeTag)
	h.writeDB(db)
	h.writeSchema(scName)
	return h.getOid()
}

func (h oidHasher) IndexOid(tableID sqlbase.ID, indexID sqlbase.IndexID) *tree.DOid {
	h.writeTypeTag(indexTypeTag)
	h.writeTable(tableID)
	h.writeIndex(indexID)
	return h.getOid()
}

func (h oidHasher) ColumnOid(tableID sqlbase.ID, columnID sqlbase.ColumnID) *tree.DOid {
	h.writeTypeTag(columnTypeTag)
	h.writeUInt32(uint32(tableID))
	h.writeUInt32(uint32(columnID))
	return h.getOid()
}

func (h oidHasher) CheckConstraintOid(
	db *sqlbase.ImmutableDatabaseDescriptor,
	scName string,
	table *sqlbase.TableDescriptor,
	check *sqlbase.TableDescriptor_CheckConstraint,
) *tree.DOid {
	h.writeTypeTag(checkConstraintTypeTag)
	h.writeDB(db)
	h.writeSchema(scName)
	h.writeTable(table.ID)
	h.writeCheckConstraint(check)
	return h.getOid()
}

func (h oidHasher) PrimaryKeyConstraintOid(
	db *sqlbase.ImmutableDatabaseDescriptor,
	scName string,
	table *sqlbase.TableDescriptor,
	pkey *sqlbase.IndexDescriptor,
) *tree.DOid {
	h.writeTypeTag(pKeyConstraintTypeTag)
	h.writeDB(db)
	h.writeSchema(scName)
	h.writeTable(table.ID)
	h.writeIndex(pkey.ID)
	return h.getOid()
}

func (h oidHasher) ForeignKeyConstraintOid(
	db *sqlbase.ImmutableDatabaseDescriptor,
	scName string,
	table *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
) *tree.DOid {
	h.writeTypeTag(fkConstraintTypeTag)
	h.writeDB(db)
	h.writeSchema(scName)
	h.writeTable(table.ID)
	h.writeForeignKeyConstraint(fk)
	return h.getOid()
}

func (h oidHasher) UniqueConstraintOid(
	db *sqlbase.ImmutableDatabaseDescriptor,
	scName string,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
) *tree.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(db)
	h.writeSchema(scName)
	h.writeTable(table.ID)
	h.writeIndex(index.ID)
	return h.getOid()
}

func (h oidHasher) BuiltinOid(name string, builtin *tree.Overload) *tree.DOid {
	h.writeTypeTag(functionTypeTag)
	h.writeStr(name)
	h.writeStr(builtin.Types.String())
	h.writeStr(builtin.FixedReturnType().String())
	return h.getOid()
}

func (h oidHasher) RegProc(name string) tree.Datum {
	_, overloads := builtins.GetBuiltinProperties(name)
	if len(overloads) == 0 {
		return tree.DNull
	}
	return h.BuiltinOid(name, &overloads[0]).AsRegProc(name)
}

func (h oidHasher) UserOid(username string) *tree.DOid {
	h.writeTypeTag(userTypeTag)
	h.writeStr(username)
	return h.getOid()
}

func (h oidHasher) CollationOid(collation string) *tree.DOid {
	h.writeTypeTag(collationTypeTag)
	h.writeStr(collation)
	return h.getOid()
}

func (h oidHasher) OperatorOid(name string, leftType, rightType, returnType *tree.DOid) *tree.DOid {
	h.writeTypeTag(operatorTypeTag)
	h.writeStr(name)
	h.writeOID(leftType)
	h.writeOID(rightType)
	h.writeOID(returnType)
	return h.getOid()
}

func (h oidHasher) EnumEntryOid(typOID *tree.DOid, physicalRep []byte) *tree.DOid {
	h.writeTypeTag(enumEntryTypeTag)
	h.writeOID(typOID)
	h.writeBytes(physicalRep)
	return h.getOid()
}

func tableOid(id sqlbase.ID) *tree.DOid {
	return tree.NewDOid(tree.DInt(id))
}

func dbOid(id sqlbase.ID) *tree.DOid {
	return tree.NewDOid(tree.DInt(id))
}

func stringOid(s string) *tree.DOid {
	h := makeOidHasher()
	h.writeStr(s)
	return h.getOid()
}
