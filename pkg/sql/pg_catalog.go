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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vtable"
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
	tableDefs: map[descpb.ID]virtualSchemaDef{
		catconstants.PgCatalogAmTableID:                  pgCatalogAmTable,
		catconstants.PgCatalogAttrDefTableID:             pgCatalogAttrDefTable,
		catconstants.PgCatalogAttributeTableID:           pgCatalogAttributeTable,
		catconstants.PgCatalogAuthIDTableID:              pgCatalogAuthIDTable,
		catconstants.PgCatalogAuthMembersTableID:         pgCatalogAuthMembersTable,
		catconstants.PgCatalogAvailableExtensionsTableID: pgCatalogAvailableExtensionsTable,
		catconstants.PgCatalogCastTableID:                pgCatalogCastTable,
		catconstants.PgCatalogClassTableID:               pgCatalogClassTable,
		catconstants.PgCatalogCollationTableID:           pgCatalogCollationTable,
		catconstants.PgCatalogConstraintTableID:          pgCatalogConstraintTable,
		catconstants.PgCatalogConversionTableID:          pgCatalogConversionTable,
		catconstants.PgCatalogDatabaseTableID:            pgCatalogDatabaseTable,
		catconstants.PgCatalogDefaultACLTableID:          pgCatalogDefaultACLTable,
		catconstants.PgCatalogDependTableID:              pgCatalogDependTable,
		catconstants.PgCatalogDescriptionTableID:         pgCatalogDescriptionTable,
		catconstants.PgCatalogSharedDescriptionTableID:   pgCatalogSharedDescriptionTable,
		catconstants.PgCatalogEnumTableID:                pgCatalogEnumTable,
		catconstants.PgCatalogEventTriggerTableID:        pgCatalogEventTriggerTable,
		catconstants.PgCatalogExtensionTableID:           pgCatalogExtensionTable,
		catconstants.PgCatalogForeignDataWrapperTableID:  pgCatalogForeignDataWrapperTable,
		catconstants.PgCatalogForeignServerTableID:       pgCatalogForeignServerTable,
		catconstants.PgCatalogForeignTableTableID:        pgCatalogForeignTableTable,
		catconstants.PgCatalogIndexTableID:               pgCatalogIndexTable,
		catconstants.PgCatalogIndexesTableID:             pgCatalogIndexesTable,
		catconstants.PgCatalogInheritsTableID:            pgCatalogInheritsTable,
		catconstants.PgCatalogLanguageTableID:            pgCatalogLanguageTable,
		catconstants.PgCatalogLocksTableID:               pgCatalogLocksTable,
		catconstants.PgCatalogMatViewsTableID:            pgCatalogMatViewsTable,
		catconstants.PgCatalogNamespaceTableID:           pgCatalogNamespaceTable,
		catconstants.PgCatalogOpclassTableID:             pgCatalogOpclassTable,
		catconstants.PgCatalogOperatorTableID:            pgCatalogOperatorTable,
		catconstants.PgCatalogPreparedStatementsTableID:  pgCatalogPreparedStatementsTable,
		catconstants.PgCatalogPreparedXactsTableID:       pgCatalogPreparedXactsTable,
		catconstants.PgCatalogProcTableID:                pgCatalogProcTable,
		catconstants.PgCatalogAggregateTableID:           pgCatalogAggregateTable,
		catconstants.PgCatalogRangeTableID:               pgCatalogRangeTable,
		catconstants.PgCatalogRewriteTableID:             pgCatalogRewriteTable,
		catconstants.PgCatalogRolesTableID:               pgCatalogRolesTable,
		catconstants.PgCatalogSecLabelsTableID:           pgCatalogSecLabelsTable,
		catconstants.PgCatalogSequencesTableID:           pgCatalogSequencesTable,
		catconstants.PgCatalogSettingsTableID:            pgCatalogSettingsTable,
		catconstants.PgCatalogShdependTableID:            pgCatalogShdependTable,
		catconstants.PgCatalogUserTableID:                pgCatalogUserTable,
		catconstants.PgCatalogUserMappingTableID:         pgCatalogUserMappingTable,
		catconstants.PgCatalogTablesTableID:              pgCatalogTablesTable,
		catconstants.PgCatalogTablespaceTableID:          pgCatalogTablespaceTable,
		catconstants.PgCatalogTriggerTableID:             pgCatalogTriggerTable,
		catconstants.PgCatalogTypeTableID:                pgCatalogTypeTable,
		catconstants.PgCatalogViewsTableID:               pgCatalogViewsTable,
		catconstants.PgCatalogStatActivityTableID:        pgCatalogStatActivityTable,
		catconstants.PgCatalogSecurityLabelTableID:       pgCatalogSecurityLabelTable,
		catconstants.PgCatalogSharedSecurityLabelTableID: pgCatalogSharedSecurityLabelTable,
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
	schema: vtable.PGCatalogAm,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	vtable.PGCatalogAttrDef,
	virtualMany, false, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *dbdesc.Immutable, scName string,
		table catalog.TableDescriptor,
		lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error) error {
		colNum := 0
		return table.ForeachPublicColumn(func(column *descpb.ColumnDescriptor) error {
			colNum++
			if column.DefaultExpr == nil {
				// pg_attrdef only expects rows for columns with default values.
				return nil
			}
			displayExpr, err := schemaexpr.FormatExprForDisplay(ctx, table, *column.DefaultExpr, &p.semaCtx, tree.FmtPGCatalog)
			if err != nil {
				return err
			}
			defSrc := tree.NewDString(displayExpr)
			return addRow(
				h.ColumnOid(table.GetID(), column.ID),               // oid
				tableOid(table.GetID()),                             // adrelid
				tree.NewDInt(tree.DInt(column.GetPGAttributeNum())), // adnum
				defSrc, // adbin
				defSrc, // adsrc
			)
		})
	})

var pgCatalogAttributeTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`table columns (incomplete - see also information_schema.columns)
https://www.postgresql.org/docs/12/catalog-pg-attribute.html`,
	vtable.PGCatalogAttribute,
	virtualMany, true, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *dbdesc.Immutable, scName string,
		table catalog.TableDescriptor,
		lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error) error {
		// addColumn adds adds either a table or a index column to the pg_attribute table.
		addColumn := func(column *descpb.ColumnDescriptor, attRelID tree.Datum, attNum uint32) error {
			colTyp := column.Type
			// Sets the attgenerated column to 's' if the column is generated/
			// computed, zero byte otherwise.
			var isColumnComputed string
			if column.IsComputed() {
				isColumnComputed = "s"
			} else {
				isColumnComputed = ""
			}
			return addRow(
				attRelID,                        // attrelid
				tree.NewDName(column.Name),      // attname
				typOid(colTyp),                  // atttypid
				zeroVal,                         // attstattarget
				typLen(colTyp),                  // attlen
				tree.NewDInt(tree.DInt(attNum)), // attnum
				zeroVal,                         // attndims
				negOneVal,                       // attcacheoff
				tree.NewDInt(tree.DInt(colTyp.TypeModifier())), // atttypmod
				tree.DNull, // attbyval (see pg_type.typbyval)
				tree.DNull, // attstorage
				tree.DNull, // attalign
				tree.MakeDBool(tree.DBool(!column.Nullable)),          // attnotnull
				tree.MakeDBool(tree.DBool(column.DefaultExpr != nil)), // atthasdef
				tree.NewDString(""),               // attidentity
				tree.NewDString(isColumnComputed), // attgenerated
				tree.DBoolFalse,                   // attisdropped
				tree.DBoolTrue,                    // attislocal
				zeroVal,                           // attinhcount
				typColl(colTyp, h),                // attcollation
				tree.DNull,                        // attacl
				tree.DNull,                        // attoptions
				tree.DNull,                        // attfdwoptions
			)
		}

		// Columns for table.
		if err := table.ForeachPublicColumn(func(column *descpb.ColumnDescriptor) error {
			tableID := tableOid(table.GetID())
			return addColumn(column, tableID, column.GetPGAttributeNum())
		}); err != nil {
			return err
		}

		// Columns for each index.
		columnIdxMap := table.ColumnIdxMap()
		return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
			for i := 0; i < index.NumColumns(); i++ {
				colID := index.GetColumnID(i)
				idxID := h.IndexOid(table.GetID(), index.GetID())
				column := table.GetColumnAtIdx(columnIdxMap.GetDefault(colID))
				if err := addColumn(column, idxID, column.GetPGAttributeNum()); err != nil {
					return err
				}
			}
			return nil
		})
	})

var pgCatalogCastTable = virtualSchemaTable{
	comment: `casts (empty - needs filling out)
https://www.postgresql.org/docs/9.6/catalog-pg-cast.html`,
	schema: vtable.PGCatalogCast,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogAuthID,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRole(ctx, p, func(username security.SQLUsername, isRole bool, noLogin bool, rolValidUntil *time.Time) error {
			isRoot := tree.DBool(username.IsRootUser() || username.IsAdminRole())
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
				h.UserOid(username),                  // oid
				tree.NewDName(username.Normalized()), // rolname
				tree.MakeDBool(isRoot),               // rolsuper
				tree.MakeDBool(isRoleDBool),          // rolinherit. Roles inherit by default.
				tree.MakeDBool(isRoot),               // rolcreaterole
				tree.MakeDBool(isRoot),               // rolcreatedb
				tree.MakeDBool(roleCanLogin),         // rolcanlogin.
				tree.DBoolFalse,                      // rolreplication
				tree.DBoolFalse,                      // rolbypassrls
				negOneVal,                            // rolconnlimit
				passwdStarString,                     // rolpassword
				roleValidUntilValue,                  // rolvaliduntil
			)
		})
	},
}

var pgCatalogAuthMembersTable = virtualSchemaTable{
	comment: `role membership
https://www.postgresql.org/docs/9.5/catalog-pg-auth-members.html`,
	schema: vtable.PGCatalogAuthMembers,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRoleMembership(ctx, p,
			func(roleName, memberName security.SQLUsername, isAdmin bool) error {
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
	schema: vtable.PGCatalogAvailableExtensions,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// We support no extensions.
		return nil
	},
}

func getOwnerOID(desc catalog.Descriptor) tree.Datum {
	owner := getOwnerOfDesc(desc)
	h := makeOidHasher()
	return h.UserOid(owner)
}

func getOwnerName(desc catalog.Descriptor) tree.Datum {
	owner := getOwnerOfDesc(desc)
	return tree.NewDName(owner.Normalized())
}

var (
	relKindTable            = tree.NewDString("r")
	relKindIndex            = tree.NewDString("i")
	relKindView             = tree.NewDString("v")
	relKindMaterializedView = tree.NewDString("m")
	relKindSequence         = tree.NewDString("S")

	relPersistencePermanent = tree.NewDString("p")
	relPersistenceTemporary = tree.NewDString("t")
)

var pgCatalogClassTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`tables and relation-like objects (incomplete - see also information_schema.tables/sequences/views)
https://www.postgresql.org/docs/9.5/catalog-pg-class.html`,
	vtable.PGCatalogClass,
	virtualMany, true, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *dbdesc.Immutable, scName string,
		table catalog.TableDescriptor, _ simpleSchemaResolver, addRow func(...tree.Datum) error) error {
		// The only difference between tables, views and sequences are the relkind and relam columns.
		relKind := relKindTable
		relAm := forwardIndexOid
		if table.IsView() {
			relKind = relKindView
			if table.MaterializedView() {
				relKind = relKindMaterializedView
			}
			relAm = oidZero
		} else if table.IsSequence() {
			relKind = relKindSequence
			relAm = oidZero
		}
		relPersistence := relPersistencePermanent
		if table.IsTemporary() {
			relPersistence = relPersistenceTemporary
		}
		namespaceOid := h.NamespaceOid(db.GetID(), scName)
		if err := addRow(
			tableOid(table.GetID()),        // oid
			tree.NewDName(table.GetName()), // relname
			namespaceOid,                   // relnamespace
			oidZero,                        // reltype (PG creates a composite type in pg_type for each table)
			oidZero,                        // reloftype (PG creates a composite type in pg_type for each table)
			getOwnerOID(table),             // relowner
			relAm,                          // relam
			oidZero,                        // relfilenode
			oidZero,                        // reltablespace
			tree.DNull,                     // relpages
			tree.DNull,                     // reltuples
			zeroVal,                        // relallvisible
			oidZero,                        // reltoastrelid
			tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhasindex
			tree.DBoolFalse, // relisshared
			relPersistence,  // relPersistence
			tree.DBoolFalse, // relistemp
			relKind,         // relkind
			tree.NewDInt(tree.DInt(len(table.GetPublicColumns()))), // relnatts
			tree.NewDInt(tree.DInt(len(table.GetChecks()))),        // relchecks
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
		return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
			indexType := forwardIndexOid
			if index.GetType() == descpb.IndexDescriptor_INVERTED {
				indexType = invertedIndexOid
			}
			return addRow(
				h.IndexOid(table.GetID(), index.GetID()), // oid
				tree.NewDName(index.GetName()),           // relname
				namespaceOid,                             // relnamespace
				oidZero,                                  // reltype
				oidZero,                                  // reloftype
				getOwnerOID(table),                       // relowner
				indexType,                                // relam
				oidZero,                                  // relfilenode
				oidZero,                                  // reltablespace
				tree.DNull,                               // relpages
				tree.DNull,                               // reltuples
				zeroVal,                                  // relallvisible
				oidZero,                                  // reltoastrelid
				tree.DBoolFalse,                          // relhasindex
				tree.DBoolFalse,                          // relisshared
				relPersistencePermanent,                  // relPersistence
				tree.DBoolFalse,                          // relistemp
				relKindIndex,                             // relkind
				tree.NewDInt(tree.DInt(index.NumColumns())), // relnatts
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
	schema: vtable.PGCatalogCollation,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false /* requiresPrivileges */, func(db *dbdesc.Immutable) error {
			namespaceOid := h.NamespaceOid(db.GetID(), pgCatalogName)
			add := func(collName string) error {
				return addRow(
					h.CollationOid(collName),  // oid
					tree.NewDString(collName), // collname
					namespaceOid,              // collnamespace
					tree.DNull,                // collowner
					builtins.DatEncodingUTFId, // collencoding
					// It's not clear how to translate a Go collation tag into the format
					// required by LC_COLLATE and LC_CTYPE.
					tree.DNull, // collcollate
					tree.DNull, // collctype
				)
			}
			if err := add(tree.DefaultCollationTag); err != nil {
				return err
			}
			for _, tag := range collate.Supported() {
				collName := tag.String()
				if err := add(collName); err != nil {
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

	fkActionMap = map[descpb.ForeignKeyReference_Action]tree.Datum{
		descpb.ForeignKeyReference_NO_ACTION:   fkActionNone,
		descpb.ForeignKeyReference_RESTRICT:    fkActionRestrict,
		descpb.ForeignKeyReference_CASCADE:     fkActionCascade,
		descpb.ForeignKeyReference_SET_NULL:    fkActionSetNull,
		descpb.ForeignKeyReference_SET_DEFAULT: fkActionSetDefault,
	}

	fkMatchTypeFull    = tree.NewDString("f")
	fkMatchTypePartial = tree.NewDString("p")
	fkMatchTypeSimple  = tree.NewDString("s")

	fkMatchMap = map[descpb.ForeignKeyReference_Match]tree.Datum{
		descpb.ForeignKeyReference_SIMPLE:  fkMatchTypeSimple,
		descpb.ForeignKeyReference_FULL:    fkMatchTypeFull,
		descpb.ForeignKeyReference_PARTIAL: fkMatchTypePartial,
	}
)

func populateTableConstraints(
	ctx context.Context,
	p *planner,
	h oidHasher,
	db *dbdesc.Immutable,
	scName string,
	table catalog.TableDescriptor,
	tableLookup simpleSchemaResolver,
	addRow func(...tree.Datum) error,
) error {
	conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
	if err != nil {
		return err
	}
	namespaceOid := h.NamespaceOid(db.GetID(), scName)
	tblOid := tableOid(table.GetID())
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
		case descpb.ConstraintTypePK:
			oid = h.PrimaryKeyConstraintOid(db.GetID(), scName, table.GetID(), con.Index)
			contype = conTypePKey
			conindid = h.IndexOid(table.GetID(), con.Index.ID)

			var err error
			if conkey, err = colIDArrayToDatum(con.Index.ColumnIDs); err != nil {
				return err
			}
			condef = tree.NewDString(table.PrimaryKeyString())

		case descpb.ConstraintTypeFK:
			oid = h.ForeignKeyConstraintOid(db.GetID(), scName, table.GetID(), con.FK)
			contype = conTypeFK
			// Foreign keys don't have a single linked index. Pick the first one
			// that matches on the referenced table.
			referencedTable, err := tableLookup.getTableByID(con.FK.ReferencedTableID)
			if err != nil {
				return err
			}
			if refConstraint, err := tabledesc.FindFKReferencedUniqueConstraint(
				referencedTable, con.FK.ReferencedColumnIDs,
			); err != nil {
				// We couldn't find a unique constraint that matched. This shouldn't
				// happen.
				log.Warningf(ctx, "broken fk reference: %v", err)
			} else if idx, ok := refConstraint.(*descpb.IndexDescriptor); ok {
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
			if err := showForeignKeyConstraint(
				&buf, db.GetName(),
				table, con.FK,
				tableLookup,
				p.extendedEvalCtx.SessionData.SearchPath,
			); err != nil {
				return err
			}
			condef = tree.NewDString(buf.String())

		case descpb.ConstraintTypeUnique:
			contype = conTypeUnique
			f := tree.NewFmtCtx(tree.FmtSimple)
			if con.Index != nil {
				oid = h.UniqueConstraintOid(db.GetID(), scName, table.GetID(), con.Index.ID)
				conindid = h.IndexOid(table.GetID(), con.Index.ID)
				var err error
				if conkey, err = colIDArrayToDatum(con.Index.ColumnIDs); err != nil {
					return err
				}
				f.WriteString("UNIQUE (")
				con.Index.ColNamesFormat(f)
				f.WriteByte(')')
				if con.Index.IsPartial() {
					pred, err := schemaexpr.FormatExprForDisplay(ctx, table, con.Index.Predicate, p.SemaCtx(), tree.FmtPGCatalog)
					if err != nil {
						return err
					}
					f.WriteString(fmt.Sprintf(" WHERE (%s)", pred))
				}
			} else if con.UniqueWithoutIndexConstraint != nil {
				oid = h.UniqueWithoutIndexConstraintOid(
					db.GetID(), scName, table.GetID(), con.UniqueWithoutIndexConstraint,
				)
				f.WriteString("UNIQUE WITHOUT INDEX (")
				colNames, err := table.NamesForColumnIDs(con.UniqueWithoutIndexConstraint.ColumnIDs)
				if err != nil {
					return err
				}
				f.WriteString(strings.Join(colNames, ", "))
				f.WriteByte(')')
			} else {
				return errors.AssertionFailedf(
					"Index or UniqueWithoutIndexConstraint must be non-nil for a unique constraint",
				)
			}
			condef = tree.NewDString(f.CloseAndGetString())

		case descpb.ConstraintTypeCheck:
			oid = h.CheckConstraintOid(db.GetID(), scName, table.GetID(), con.CheckConstraint)
			contype = conTypeCheck
			if conkey, err = colIDArrayToDatum(con.CheckConstraint.ColumnIDs); err != nil {
				return err
			}
			displayExpr, err := schemaexpr.FormatExprForDisplay(ctx, table, con.Details, &p.semaCtx, tree.FmtPGCatalog)
			if err != nil {
				return err
			}
			consrc = tree.NewDString(fmt.Sprintf("(%s)", displayExpr))
			conbin = consrc
			validity := ""
			if con.CheckConstraint.Validity != descpb.ConstraintValidity_Validated {
				validity = " NOT VALID"
			}
			condef = tree.NewDString(fmt.Sprintf("CHECK ((%s))%s", displayExpr, validity))
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

func (r oneAtATimeSchemaResolver) getDatabaseByID(id descpb.ID) (*dbdesc.Immutable, error) {
	return r.p.Descriptors().GetImmutableDatabaseByID(r.ctx, r.p.txn, id, tree.DatabaseLookupFlags{})
}

func (r oneAtATimeSchemaResolver) getTableByID(id descpb.ID) (catalog.TableDescriptor, error) {
	table, err := r.p.LookupTableByID(r.ctx, id)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (r oneAtATimeSchemaResolver) getSchemaByID(id descpb.ID) (*schemadesc.Immutable, error) {
	// TODO (rohany): This should use the descs.Collection.
	desc, err := catalogkv.GetAnyDescriptorByID(r.ctx, r.p.txn, r.p.ExecCfg().Codec, id, catalogkv.Immutable)
	if err != nil {
		return nil, err
	}
	sc, ok := desc.(*schemadesc.Immutable)
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
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
	populateFromTable func(ctx context.Context, p *planner, h oidHasher, db *dbdesc.Immutable,
		scName string, table catalog.TableDescriptor, lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error,
	) error,
) virtualSchemaTable {
	populateAll := func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, virtualOpts,
			func(db *dbdesc.Immutable, scName string, table catalog.TableDescriptor, lookup tableLookupFn) error {
				return populateFromTable(ctx, p, h, db, scName, table, lookup, addRow)
			})
	}
	return virtualSchemaTable{
		comment: comment,
		schema:  schemaDef,
		indexes: []virtualIndex{
			{
				partial: includesIndexEntries,
				populate: func(ctx context.Context, constraint tree.Datum, p *planner, db *dbdesc.Immutable,
					addRow func(...tree.Datum) error) (bool, error) {
					var id descpb.ID
					d := tree.UnwrapDatum(p.EvalContext(), constraint)
					if d == tree.DNull {
						return false, nil
					}
					switch t := d.(type) {
					case *tree.DOid:
						id = descpb.ID(t.DInt)
					case *tree.DInt:
						id = descpb.ID(*t)
					default:
						return false, errors.AssertionFailedf("unexpected type %T for table id column in virtual table %s",
							d, schemaDef)
					}
					table, err := p.LookupTableByID(ctx, id)
					if err != nil {
						if sqlerrors.IsUndefinedRelationError(err) {
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
					if (!table.IsVirtualTable() && table.GetParentID() != db.GetID()) ||
						table.Dropped() ||
						!userCanSeeDescriptor(ctx, p, table, true /*allowAdding*/) {
						return false, nil
					}
					h := makeOidHasher()
					scResolver := oneAtATimeSchemaResolver{p: p, ctx: ctx}
					sc, err := p.Descriptors().GetImmutableSchemaByID(
						ctx, p.txn, table.GetParentSchemaID(), tree.SchemaLookupFlags{})
					if err != nil {
						return false, err
					}
					if err := populateFromTable(ctx, p, h, db, sc.Name, table, scResolver,
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
	vtable.PGCatalogConstraint,
	hideVirtual, /* Virtual tables have no constraints */
	false,       /* includesIndexEntries */
	populateTableConstraints)

// colIDArrayToDatum returns an int[] containing the ColumnIDs, or NULL if there
// are no ColumnIDs.
func colIDArrayToDatum(arr []descpb.ColumnID) (tree.Datum, error) {
	if len(arr) == 0 {
		return tree.DNull, nil
	}
	d := tree.NewDArray(types.Int2)
	for _, val := range arr {
		if err := d.Append(tree.NewDInt(tree.DInt(val))); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// colIDArrayToVector returns an INT2VECTOR containing the ColumnIDs, or NULL if
// there are no ColumnIDs.
func colIDArrayToVector(arr []descpb.ColumnID) (tree.Datum, error) {
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
	schema: vtable.PGCatalogConversion,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogDatabaseTable = virtualSchemaTable{
	comment: `available databases (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-database.html`,
	schema: vtable.PGCatalogDatabase,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /*all databases*/, false, /* requiresPrivileges */
			func(db *dbdesc.Immutable) error {
				return addRow(
					dbOid(db.GetID()),           // oid
					tree.NewDName(db.GetName()), // datname
					getOwnerOID(db),             // datdba
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
	schema: vtable.PGCatalogDefaultACL,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogDepend,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
			db *dbdesc.Immutable,
			scName string,
			table catalog.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			pgConstraintTableOid := tableOid(pgConstraintsDesc.ID)
			pgClassTableOid := tableOid(pgClassDesc.ID)
			if table.IsSequence() &&
				!table.GetSequenceOpts().SequenceOwner.Equal(descpb.TableDescriptor_SequenceOpts_SequenceOwner{}) {
				refObjID := tableOid(table.GetSequenceOpts().SequenceOwner.OwnerTableID)
				refObjSubID := tree.NewDInt(tree.DInt(table.GetSequenceOpts().SequenceOwner.OwnerColumnID))
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

			// In the case of table/view relationship, In PostgreSQL pg_depend.objid refers to
			// pg_rewrite.oid, then pg_rewrite ev_class refers to the dependent object, but
			// cockroach db does not implements pg_rewrite yet
			//
			// Issue #57417: https://github.com/cockroachdb/cockroach/issues/57417
			reportViewDependency := func(dep *descpb.TableDescriptor_Reference) error {
				for _, colID := range dep.ColumnIDs {
					if err := addRow(
						pgClassTableOid,                //classid
						tableOid(dep.ID),               //objid
						zeroVal,                        //objsubid
						pgClassTableOid,                //refclassid
						tableOid(table.GetID()),        //refobjid
						tree.NewDInt(tree.DInt(colID)), //refobjsubid
						depTypeNormal,                  //deptype
					); err != nil {
						return err
					}
				}

				return nil
			}

			if table.IsTable() || table.IsView() {
				if err := table.ForeachDependedOnBy(reportViewDependency); err != nil {
					return err
				}
			}

			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			for _, con := range conInfo {
				if con.Kind != descpb.ConstraintTypeFK {
					continue
				}

				// Foreign keys don't have a single linked index. Pick the first one
				// that matches on the referenced table.
				referencedTable, err := tableLookup.getTableByID(con.FK.ReferencedTableID)
				if err != nil {
					return err
				}
				refObjID := oidZero
				if refConstraint, err := tabledesc.FindFKReferencedUniqueConstraint(
					referencedTable, con.FK.ReferencedColumnIDs,
				); err != nil {
					// We couldn't find a unique constraint that matched. This shouldn't
					// happen.
					log.Warningf(ctx, "broken fk reference: %v", err)
				} else if idx, ok := refConstraint.(*descpb.IndexDescriptor); ok {
					refObjID = h.IndexOid(con.ReferencedTable.ID, idx.ID)
				}
				constraintOid := h.ForeignKeyConstraintOid(db.GetID(), scName, table.GetID(), con.FK)

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
	schema: vtable.PGCatalogDescription,
	populate: func(
		ctx context.Context,
		p *planner,
		dbContext *dbdesc.Immutable,
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
				classOid = tree.NewDOid(catconstants.PgCatalogClassTableID)
			case keys.IndexCommentType:
				objID = makeOidHasher().IndexOid(
					descpb.ID(tree.MustBeDInt(objID)),
					descpb.IndexID(tree.MustBeDInt(objSubID)))
				objSubID = tree.DZero
				classOid = tree.NewDOid(catconstants.PgCatalogClassTableID)
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
	schema: vtable.PGCatalogSharedDescription,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
			classOid := tree.NewDOid(catconstants.PgCatalogDatabaseTableID)
			objID := descpb.ID(tree.MustBeDInt(comment[0]))
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
	schema: vtable.PGCatalogEnum,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()

		return forEachTypeDesc(ctx, p, dbContext, func(_ *dbdesc.Immutable, _ string, typDesc *typedesc.Immutable) error {
			switch typDesc.Kind {
			case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
			// We only want to iterate over ENUM types and multi-region enums.
			default:
				return nil
			}
			// Generate a row for each member of the enum. We don't represent enums
			// internally using floats for ordering like Postgres, so just pick a
			// float entry for the rows.
			typOID := tree.NewDOid(tree.DInt(typedesc.TypeIDToOID(typDesc.GetID())))
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
	schema: vtable.PGCatalogEventTrigger,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Event triggers are not currently supported.
		return nil
	},
}

var pgCatalogExtensionTable = virtualSchemaTable{
	comment: `installed extensions (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-extension.html`,
	schema: vtable.PGCatalogExtension,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Extensions are not supported.
		return nil
	},
}

var pgCatalogForeignDataWrapperTable = virtualSchemaTable{
	comment: `foreign data wrappers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-data-wrapper.html`,
	schema: vtable.PGCatalogForeignDataWrapper,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Foreign data wrappers are not supported.
		return nil
	},
}

var pgCatalogForeignServerTable = virtualSchemaTable{
	comment: `foreign servers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-server.html`,
	schema: vtable.PGCatalogForeignServer,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Foreign servers are not supported.
		return nil
	},
}

var pgCatalogForeignTableTable = virtualSchemaTable{
	comment: `foreign tables (empty  - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-table.html`,
	schema: vtable.PGCatalogForeignTable,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogIndex,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual tables do not have indexes */
			func(db *dbdesc.Immutable, scName string, table catalog.TableDescriptor) error {
				tableOid := tableOid(table.GetID())
				return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
					isMutation, isWriteOnly :=
						table.GetIndexMutationCapabilities(index.GetID())
					isReady := isMutation && isWriteOnly

					// Get the collations for all of the columns. To do this we require
					// the type of the column.
					// Also fill in indoption for each column to indicate if the index
					// is ASC/DESC and if nulls appear first/last.
					collationOids := tree.NewDArray(types.Oid)
					indoption := tree.NewDArray(types.Int)

					colIDs := make([]descpb.ColumnID, 0, index.NumColumns())
					for i := index.IndexDesc().ExplicitColumnStartIdx(); i < index.NumColumns(); i++ {
						columnID := index.GetColumnID(i)
						colIDs = append(colIDs, columnID)
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
						if index.GetColumnDirection(i) == descpb.IndexDescriptor_ASC {
							thisIndOption = indoptionNullsFirst
						} else {
							thisIndOption = indoptionDesc
						}
						if err := indoption.Append(tree.NewDInt(thisIndOption)); err != nil {
							return err
						}
					}
					indkey, err := colIDArrayToVector(colIDs)
					if err != nil {
						return err
					}
					collationOidVector := tree.NewDOidVectorFromDArray(collationOids)
					indoptionIntVector := tree.NewDIntVectorFromDArray(indoption)
					// TODO(bram): #27763 indclass still needs to be populated but it
					// requires pg_catalog.pg_opclass first.
					indclass, err := makeZeroedOidVector(len(colIDs))
					if err != nil {
						return err
					}
					return addRow(
						h.IndexOid(table.GetID(), index.GetID()),     // indexrelid
						tableOid,                                     // indrelid
						tree.NewDInt(tree.DInt(index.NumColumns())),  // indnatts
						tree.MakeDBool(tree.DBool(index.IsUnique())), // indisunique
						tree.MakeDBool(tree.DBool(index.Primary())),  // indisprimary
						tree.DBoolFalse,                              // indisexclusion
						tree.MakeDBool(tree.DBool(index.IsUnique())), // indimmediate
						tree.DBoolFalse,                              // indisclustered
						tree.MakeDBool(tree.DBool(!isMutation)),      // indisvalid
						tree.DBoolFalse,                              // indcheckxmin
						tree.MakeDBool(tree.DBool(isReady)),          // indisready
						tree.DBoolTrue,                               // indislive
						tree.DBoolFalse,                              // indisreplident
						indkey,                                       // indkey
						collationOidVector,                           // indcollation
						indclass,                                     // indclass
						indoptionIntVector,                           // indoption
						tree.DNull,                                   // indexprs
						tree.DNull,                                   // indpred
					)
				})
			})
	},
}

var pgCatalogIndexesTable = virtualSchemaTable{
	comment: `index creation statements
https://www.postgresql.org/docs/9.5/view-pg-indexes.html`,
	schema: vtable.PGCatalogIndexes,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual, /* virtual tables do not have indexes */
			func(db *dbdesc.Immutable, scName string, table catalog.TableDescriptor, tableLookup tableLookupFn) error {
				scNameName := tree.NewDName(scName)
				tblName := tree.NewDName(table.GetName())
				return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
					def, err := indexDefFromDescriptor(ctx, p, db, table, index.IndexDesc(), tableLookup)
					if err != nil {
						return err
					}
					return addRow(
						h.IndexOid(table.GetID(), index.GetID()), // oid
						scNameName,                               // schemaname
						tblName,                                  // tablename
						tree.NewDName(index.GetName()),           // indexname
						tree.DNull,                               // tablespace
						tree.NewDString(def),                     // indexdef
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
	db *dbdesc.Immutable,
	table catalog.TableDescriptor,
	index *descpb.IndexDescriptor,
	tableLookup tableLookupFn,
) (string, error) {
	colNames := index.ColumnNames[index.ExplicitColumnStartIdx():]
	indexDef := tree.CreateIndex{
		Name:     tree.Name(index.Name),
		Table:    tree.MakeTableName(tree.Name(db.GetName()), tree.Name(table.GetName())),
		Unique:   index.Unique,
		Columns:  make(tree.IndexElemList, len(colNames)),
		Storing:  make(tree.NameList, len(index.StoreColumnNames)),
		Inverted: index.Type == descpb.IndexDescriptor_INVERTED,
	}
	for i, name := range colNames {
		elem := tree.IndexElem{
			Column:    tree.Name(name),
			Direction: tree.Ascending,
		}
		if index.ColumnDirections[index.ExplicitColumnStartIdx()+i] == descpb.IndexDescriptor_DESC {
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
		parentDb, err := tableLookup.getDatabaseByID(parentTable.GetParentID())
		if err != nil {
			return "", err
		}
		var sharedPrefixLen int
		for _, ancestor := range intl.Ancestors {
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
		}
		fields := colNames[:sharedPrefixLen]
		intlDef := &tree.InterleaveDef{
			Parent: tree.MakeTableName(tree.Name(parentDb.GetName()),
				tree.Name(parentTable.GetName())),
			Fields: make(tree.NameList, len(fields)),
		}
		for i, field := range fields {
			intlDef.Fields[i] = tree.Name(field)
		}
		indexDef.Interleave = intlDef
	}
	if index.IsPartial() {
		// Format the raw predicate for display in order to resolve user-defined
		// types to a human readable form.
		//
		// TODO(mgartner): Avoid parsing the predicate expression twice. It is
		// parsed in schemaexpr.FormatExprForDisplay and again here.
		formattedPred, err := schemaexpr.FormatExprForDisplay(ctx, table, index.Predicate, p.SemaCtx(), tree.FmtPGCatalog)
		if err != nil {
			return "", err
		}

		pred, err := parser.ParseExpr(formattedPred)
		if err != nil {
			return "", err
		}
		indexDef.Predicate = pred
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtPGCatalog)
	fmtCtx.FormatNode(&indexDef)
	return fmtCtx.String(), nil
}

var pgCatalogInheritsTable = virtualSchemaTable{
	comment: `table inheritance hierarchy (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-inherits.html`,
	schema: vtable.PGCatalogInherits,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Table inheritance is not supported.
		return nil
	},
}

var pgCatalogLanguageTable = virtualSchemaTable{
	comment: `available languages (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-language.html`,
	schema: vtable.PGCatalogLanguage,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Languages to write functions and stored procedures are not supported.
		return nil
	},
}

var pgCatalogLocksTable = virtualSchemaTable{
	comment: `locks held by active processes (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/view-pg-locks.html`,
	schema: vtable.PGCatalogLocks,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogMatViewsTable = virtualSchemaTable{
	comment: `available materialized views (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/view-pg-matviews.html`,
	schema: vtable.PGCatalogMatViews,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual,
			func(db *dbdesc.Immutable, scName string, desc catalog.TableDescriptor) error {
				if !desc.MaterializedView() {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition
				// returned, unlike postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(SQL Features): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					tree.NewDName(scName),         // schemaname
					tree.NewDName(desc.GetName()), // matviewname
					getOwnerName(desc),            // matviewowner
					tree.DNull,                    // tablespace
					tree.MakeDBool(len(desc.TableDesc().Indexes) > 0), // hasindexes
					tree.DBoolTrue,                       // ispopulated,
					tree.NewDString(desc.GetViewQuery()), // definition
				)
			})
	},
}

var pgCatalogNamespaceTable = virtualSchemaTable{
	comment: `available namespaces (incomplete; namespaces and databases are congruent in CockroachDB)
https://www.postgresql.org/docs/9.5/catalog-pg-namespace.html`,
	schema: vtable.PGCatalogNamespace,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(db *dbdesc.Immutable) error {
				return forEachSchema(ctx, p, db, func(sc catalog.ResolvedSchema) error {
					ownerOID := tree.DNull
					if sc.Kind == catalog.SchemaUserDefined {
						ownerOID = getOwnerOID(sc.Desc)
					} else if sc.Kind == catalog.SchemaPublic {
						// admin is the owner of the public schema.
						ownerOID = h.UserOid(security.MakeSQLUsernameFromPreNormalizedString("admin"))
					}
					return addRow(
						h.NamespaceOid(db.GetID(), sc.Name), // oid
						tree.NewDString(sc.Name),            // nspname
						ownerOID,                            // nspowner
						tree.DNull,                          // nspacl
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

var pgCatalogOpclassTable = virtualSchemaTable{
	comment: `opclass (empty - Operator classes not supported yet)
https://www.postgresql.org/docs/12/catalog-pg-opclass.html`,
	schema: vtable.PGCatalogOpclass,
	populate: func(ctx context.Context, p *planner, db *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogOperatorTable = virtualSchemaTable{
	comment: `operators (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-operator.html`,
	schema: vtable.PGCatalogOperator,
	populate: func(ctx context.Context, p *planner, db *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		nspOid := h.NamespaceOid(db.GetID(), pgCatalogName)
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
				panic(errors.AssertionFailedf("unexpected operator %s with %d params",
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
	schema: vtable.PGCatalogPreparedXacts,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogPreparedStatements,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogProc,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *dbdesc.Immutable) error {
				nspOid := h.NamespaceOid(db.GetID(), pgCatalogName)
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
	schema: vtable.PGCatalogRange,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// We currently do not support any range types, so this table is empty.
		// This table should be populated when any range types are added to
		// oidToDatum (and therefore pg_type).
		return nil
	},
}

var pgCatalogRewriteTable = virtualSchemaTable{
	comment: `rewrite rules (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-rewrite.html`,
	schema: vtable.PGCatalogRewrite,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Rewrite rules are not supported.
		return nil
	},
}

var pgCatalogRolesTable = virtualSchemaTable{
	comment: `database roles
https://www.postgresql.org/docs/9.5/view-pg-roles.html`,
	schema: vtable.PGCatalogRoles,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// We intentionally do not check if the user has access to system.user.
		// Because Postgres allows access to pg_roles by non-privileged users, we
		// need to do the same. This shouldn't be an issue, because pg_roles doesn't
		// include sensitive information such as password hashes.
		h := makeOidHasher()
		return forEachRole(ctx, p,
			func(username security.SQLUsername, isRole bool, noLogin bool, rolValidUntil *time.Time) error {
				isRoot := tree.DBool(username.IsRootUser() || username.IsAdminRole())
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
					h.UserOid(username),                  // oid
					tree.NewDName(username.Normalized()), // rolname
					tree.MakeDBool(isRoot),               // rolsuper
					tree.MakeDBool(isRoleDBool),          // rolinherit. Roles inherit by default.
					tree.MakeDBool(isRoot),               // rolcreaterole
					tree.MakeDBool(isRoot),               // rolcreatedb
					tree.DBoolFalse,                      // rolcatupdate
					tree.MakeDBool(roleCanLogin),         // rolcanlogin.
					tree.DBoolFalse,                      // rolreplication
					negOneVal,                            // rolconnlimit
					passwdStarString,                     // rolpassword
					roleValidUntilValue,                  // rolvaliduntil
					tree.DBoolFalse,                      // rolbypassrls
					tree.DNull,                           // rolconfig
				)
			})
	},
}

var pgCatalogSecLabelsTable = virtualSchemaTable{
	comment: `security labels (empty)
https://www.postgresql.org/docs/9.6/view-pg-seclabels.html`,
	schema: vtable.PGCatalogSecLabels,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogSequencesTable = virtualSchemaTable{
	comment: `sequences (see also information_schema.sequences)
https://www.postgresql.org/docs/9.5/catalog-pg-sequence.html`,
	schema: vtable.PGCatalogSequence,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual schemas do not have indexes */
			func(db *dbdesc.Immutable, scName string, table catalog.TableDescriptor) error {
				if !table.IsSequence() {
					return nil
				}
				opts := table.GetSequenceOpts()
				return addRow(
					tableOid(table.GetID()),                 // seqrelid
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
	schema: vtable.PGCatalogSettings,
	populate: func(_ context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogShdepend,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogTablesTable = virtualSchemaTable{
	comment: `tables summary (see also information_schema.tables, pg_catalog.pg_class)
https://www.postgresql.org/docs/9.5/view-pg-tables.html`,
	schema: vtable.PGCatalogTables,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Note: pg_catalog.pg_tables is not well-defined if the dbContext is
		// empty -- listing tables across databases can yield duplicate
		// schema/table names.
		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *dbdesc.Immutable, scName string, table catalog.TableDescriptor) error {
				if !table.IsTable() {
					return nil
				}
				return addRow(
					tree.NewDName(scName),          // schemaname
					tree.NewDName(table.GetName()), // tablename
					getOwnerName(table),            // tableowner
					tree.DNull,                     // tablespace
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
	schema: vtable.PGCatalogTablespace,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	schema: vtable.PGCatalogTrigger,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
	h oidHasher, nspOid tree.Datum, owner tree.Datum, typ *types.T, addRow func(...tree.Datum) error,
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
		owner,                              // typowner
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

var pgCatalogTypeTable = virtualSchemaTable{
	comment: `scalar types (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-type.html`,
	schema: vtable.PGCatalogType,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *dbdesc.Immutable) error {
				nspOid := h.NamespaceOid(db.GetID(), pgCatalogName)

				// Generate rows for all predefined types.
				for _, typ := range types.OidToType {
					if err := addPGTypeRow(h, nspOid, tree.DNull /* owner */, typ, addRow); err != nil {
						return err
					}
				}

				// Now generate rows for user defined types in this database.
				return forEachTypeDesc(ctx, p, dbContext, func(_ *dbdesc.Immutable, _ string, typDesc *typedesc.Immutable) error {
					sc, err := p.Descriptors().GetImmutableSchemaByID(
						ctx, p.txn, typDesc.ParentSchemaID, tree.SchemaLookupFlags{})
					if err != nil {
						return err
					}
					nspOid := h.NamespaceOid(db.GetID(), sc.Name)
					typ, err := typDesc.MakeTypesT(ctx, tree.NewUnqualifiedTypeName(tree.Name(typDesc.GetName())), p)
					if err != nil {
						return err
					}
					return addPGTypeRow(h, nspOid, getOwnerOID(typDesc), typ, addRow)
				})
			})
	},
	indexes: []virtualIndex{
		{
			partial: false,
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, db *dbdesc.Immutable,
				addRow func(...tree.Datum) error) (bool, error) {

				h := makeOidHasher()
				nspOid := h.NamespaceOid(db.GetID(), pgCatalogName)
				coid := tree.MustBeDOid(constraint)
				ooid := oid.Oid(int(coid.DInt))

				// Check if it is a predefined type.
				typ, ok := types.OidToType[ooid]
				if ok {
					if err := addPGTypeRow(h, nspOid, tree.DNull /* owner */, typ, addRow); err != nil {
						return false, err
					}
					return true, nil
				}

				// This oid is not a user-defined type and we didn't find it in the
				// map of predefined types, return false. Note that in common usage we
				// only really expect the value 0 here (which cockroach uses internally
				// in the typelem field amongst others). Users, however, may join on
				// this index with any value.
				if ooid <= oidext.CockroachPredefinedOIDMax {
					return false, nil
				}

				// Check if it is a user defined type.
				id := typedesc.UserDefinedTypeOIDToID(ooid)
				typDesc, err := p.Descriptors().GetImmutableTypeByID(ctx, p.txn, id, tree.ObjectLookupFlags{})
				if err != nil {
					if errors.Is(err, catalog.ErrDescriptorNotFound) {
						return false, nil
					}
					if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
						return false, nil
					}
					return false, err
				}
				sc, err := p.Descriptors().GetImmutableSchemaByID(
					ctx, p.txn, typDesc.ParentSchemaID, tree.SchemaLookupFlags{})
				if err != nil {
					return false, err
				}
				nspOid = h.NamespaceOid(db.GetID(), sc.Name)
				typ, err = typDesc.MakeTypesT(ctx, tree.NewUnqualifiedTypeName(tree.Name(typDesc.GetName())), p)
				if err != nil {
					return false, err
				}
				if err := addPGTypeRow(h, nspOid, getOwnerOID(typDesc), typ, addRow); err != nil {
					return false, err
				}

				// No errors and matches.
				return false, nil
			},
		},
	},
}

var pgCatalogUserTable = virtualSchemaTable{
	comment: `database users
https://www.postgresql.org/docs/9.5/view-pg-user.html`,
	schema: vtable.PGCatalogUser,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRole(ctx, p,
			func(username security.SQLUsername, isRole bool, noLogin bool, rolValidUntil *time.Time) error {
				if isRole {
					return nil
				}
				isRoot := tree.DBool(username.IsRootUser())
				return addRow(
					tree.NewDName(username.Normalized()), // usename
					h.UserOid(username),                  // usesysid
					tree.MakeDBool(isRoot),               // usecreatedb
					tree.MakeDBool(isRoot),               // usesuper
					tree.DBoolFalse,                      // userepl
					tree.DBoolFalse,                      // usebypassrls
					passwdStarString,                     // passwd
					tree.DNull,                           // valuntil
					tree.DNull,                           // useconfig
				)
			})
	},
}

var pgCatalogUserMappingTable = virtualSchemaTable{
	comment: `local to remote user mapping (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-user-mapping.html`,
	schema: vtable.PGCatalogUserMapping,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// This table stores the mapping to foreign server users.
		// Foreign servers are not supported.
		return nil
	},
}

var pgCatalogStatActivityTable = virtualSchemaTable{
	comment: `backend access statistics (empty - monitoring works differently in CockroachDB)
https://www.postgresql.org/docs/9.6/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW`,
	schema: vtable.PGCatalogStatActivity,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogSecurityLabelTable = virtualSchemaTable{
	comment: `security labels (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-seclabel.html`,
	schema: vtable.PGCatalogSecurityLabel,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var pgCatalogSharedSecurityLabelTable = virtualSchemaTable{
	comment: `shared security labels (empty - feature not supported)
https://www.postgresql.org/docs/9.5/catalog-pg-shseclabel.html`,
	schema: vtable.PGCatalogSharedSecurityLabel,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
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
		return h.CollationOid(tree.DefaultCollationTag)
	case types.CollatedStringFamily:
		return h.CollationOid(typ.Locale())
	}

	if typ.Equivalent(types.StringArray) {
		return h.CollationOid(tree.DefaultCollationTag)
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
	types.Box2DFamily:       typCategoryUserDefined,
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
	schema: vtable.PGCatalogViews,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		// Note: pg_views is not well defined if the dbContext is empty,
		// because it does not distinguish views in separate databases.
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /*virtual schemas do not have views*/
			func(db *dbdesc.Immutable, scName string, desc catalog.TableDescriptor) error {
				if !desc.IsView() || desc.MaterializedView() {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition
				// returned, unlike postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(SQL Features): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					tree.NewDName(scName),                // schemaname
					tree.NewDName(desc.GetName()),        // viewname
					getOwnerName(desc),                   // viewowner
					tree.NewDString(desc.GetViewQuery()), // definition
				)
			})
	},
}

var pgCatalogAggregateTable = virtualSchemaTable{
	comment: `aggregated built-in functions (incomplete)
https://www.postgresql.org/docs/9.6/catalog-pg-aggregate.html`,
	schema: vtable.PGCatalogAggregate,
	populate: func(ctx context.Context, p *planner, dbContext *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *dbdesc.Immutable) error {
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

func (h oidHasher) writeDB(dbID descpb.ID) {
	h.writeUInt32(uint32(dbID))
}

func (h oidHasher) writeSchema(scName string) {
	h.writeStr(scName)
}

func (h oidHasher) writeTable(tableID descpb.ID) {
	h.writeUInt32(uint32(tableID))
}

func (h oidHasher) writeIndex(indexID descpb.IndexID) {
	h.writeUInt32(uint32(indexID))
}

func (h oidHasher) writeUniqueConstraint(uc *descpb.UniqueWithoutIndexConstraint) {
	h.writeUInt32(uint32(uc.TableID))
	h.writeStr(uc.Name)
}

func (h oidHasher) writeCheckConstraint(check *descpb.TableDescriptor_CheckConstraint) {
	h.writeStr(check.Name)
	h.writeStr(check.Expr)
}

func (h oidHasher) writeForeignKeyConstraint(fk *descpb.ForeignKeyConstraint) {
	h.writeUInt32(uint32(fk.ReferencedTableID))
	h.writeStr(fk.Name)
}

func (h oidHasher) NamespaceOid(dbID descpb.ID, scName string) *tree.DOid {
	h.writeTypeTag(namespaceTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scName)
	return h.getOid()
}

func (h oidHasher) IndexOid(tableID descpb.ID, indexID descpb.IndexID) *tree.DOid {
	h.writeTypeTag(indexTypeTag)
	h.writeTable(tableID)
	h.writeIndex(indexID)
	return h.getOid()
}

func (h oidHasher) ColumnOid(tableID descpb.ID, columnID descpb.ColumnID) *tree.DOid {
	h.writeTypeTag(columnTypeTag)
	h.writeUInt32(uint32(tableID))
	h.writeUInt32(uint32(columnID))
	return h.getOid()
}

func (h oidHasher) CheckConstraintOid(
	dbID descpb.ID, scName string, tableID descpb.ID, check *descpb.TableDescriptor_CheckConstraint,
) *tree.DOid {
	h.writeTypeTag(checkConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scName)
	h.writeTable(tableID)
	h.writeCheckConstraint(check)
	return h.getOid()
}

func (h oidHasher) PrimaryKeyConstraintOid(
	dbID descpb.ID, scName string, tableID descpb.ID, pkey *descpb.IndexDescriptor,
) *tree.DOid {
	h.writeTypeTag(pKeyConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scName)
	h.writeTable(tableID)
	h.writeIndex(pkey.ID)
	return h.getOid()
}

func (h oidHasher) ForeignKeyConstraintOid(
	dbID descpb.ID, scName string, tableID descpb.ID, fk *descpb.ForeignKeyConstraint,
) *tree.DOid {
	h.writeTypeTag(fkConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scName)
	h.writeTable(tableID)
	h.writeForeignKeyConstraint(fk)
	return h.getOid()
}

func (h oidHasher) UniqueWithoutIndexConstraintOid(
	dbID descpb.ID, scName string, tableID descpb.ID, uc *descpb.UniqueWithoutIndexConstraint,
) *tree.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scName)
	h.writeTable(tableID)
	h.writeUniqueConstraint(uc)
	return h.getOid()
}

func (h oidHasher) UniqueConstraintOid(
	dbID descpb.ID, scName string, tableID descpb.ID, indexID descpb.IndexID,
) *tree.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scName)
	h.writeTable(tableID)
	h.writeIndex(indexID)
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

func (h oidHasher) UserOid(username security.SQLUsername) *tree.DOid {
	h.writeTypeTag(userTypeTag)
	h.writeStr(username.Normalized())
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

func tableOid(id descpb.ID) *tree.DOid {
	return tree.NewDOid(tree.DInt(id))
}

func dbOid(id descpb.ID) *tree.DOid {
	return tree.NewDOid(tree.DInt(id))
}

func stringOid(s string) *tree.DOid {
	h := makeOidHasher()
	h.writeStr(s)
	return h.getOid()
}
