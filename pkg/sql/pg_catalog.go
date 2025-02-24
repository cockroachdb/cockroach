// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catformat"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vtable"
	"github.com/cockroachdb/cockroach/pkg/util/collatedstring"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var (
	oidZero        = tree.NewDOid(0)
	regProcOidZero = tree.NewDOidWithTypeAndName(0, types.RegProc, "-")
	zeroVal        = tree.DZero
	negOneVal      = tree.NewDInt(-1)

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

// RewriteEvTypes could be a enumeration if rewrite rules gets implemented
type RewriteEvTypes string

const evTypeSelect RewriteEvTypes = "1"

// PGShDependType is an enumeration that lists pg_shdepend deptype column values
type PGShDependType string

const (
	// sharedDependencyOwner is a deptype used to reference an object with the
	// owner of the dependent object.
	sharedDependencyOwner PGShDependType = "o"

	// sharedDependencyACL in postgres The referenced object is mentioned in the
	// ACL (access control list, i.e., privileges list) of the dependent object.
	// (A SHARED_DEPENDENCY_ACL entry is not made for the owner of the object,
	// since the owner will have a SHARED_DEPENDENCY_OWNER entry anyway.)
	// For cockroachDB deptype is sharedDependencyACL if the role is not the
	// owner neither a pinned role.
	sharedDependencyACL PGShDependType = "a"

	// sharedDependencyPin in postgres for this deptype there is no dependent
	// object; this type of entry is a signal that the system itself depends on
	// the referenced object, and so that object must never be deleted. Entries
	// of this type are created only by initdb.
	// In cockroachDB the similar roles are root and admin.
	sharedDependencyPin PGShDependType = "p"
)

var forwardIndexOid = stringOid(indexTypeForwardIndex)
var invertedIndexOid = stringOid(indexTypeInvertedIndex)

// pgCatalog contains a set of system tables mirroring PostgreSQL's pg_catalog schema.
// This code attempts to comply as closely as possible to the system catalogs documented
// in https://www.postgresql.org/docs/9.6/static/catalogs.html.
var pgCatalog = virtualSchema{
	name: pgCatalogName,
	undefinedTables: buildStringSet(
		// Generated with:
		// select distinct '"'||table_name||'",' from information_schema.tables
		//    where table_schema='pg_catalog' order by table_name;
		"pg_pltemplate",
	),
	tableDefs: map[descpb.ID]virtualSchemaDef{
		catconstants.PgCatalogAggregateTableID:                  pgCatalogAggregateTable,
		catconstants.PgCatalogAmTableID:                         pgCatalogAmTable,
		catconstants.PgCatalogAmopTableID:                       pgCatalogAmopTable,
		catconstants.PgCatalogAmprocTableID:                     pgCatalogAmprocTable,
		catconstants.PgCatalogAttrDefTableID:                    pgCatalogAttrDefTable,
		catconstants.PgCatalogAttributeTableID:                  pgCatalogAttributeTable,
		catconstants.PgCatalogAuthIDTableID:                     pgCatalogAuthIDTable,
		catconstants.PgCatalogAuthMembersTableID:                pgCatalogAuthMembersTable,
		catconstants.PgCatalogAvailableExtensionVersionsTableID: pgCatalogAvailableExtensionVersionsTable,
		catconstants.PgCatalogAvailableExtensionsTableID:        pgCatalogAvailableExtensionsTable,
		catconstants.PgCatalogCastTableID:                       pgCatalogCastTable,
		catconstants.PgCatalogClassTableID:                      pgCatalogClassTable,
		catconstants.PgCatalogCollationTableID:                  pgCatalogCollationTable,
		catconstants.PgCatalogConfigTableID:                     pgCatalogConfigTable,
		catconstants.PgCatalogConstraintTableID:                 pgCatalogConstraintTable,
		catconstants.PgCatalogConversionTableID:                 pgCatalogConversionTable,
		catconstants.PgCatalogCursorsTableID:                    pgCatalogCursorsTable,
		catconstants.PgCatalogDatabaseTableID:                   pgCatalogDatabaseTable,
		catconstants.PgCatalogDbRoleSettingTableID:              pgCatalogDbRoleSettingTable,
		catconstants.PgCatalogDefaultACLTableID:                 pgCatalogDefaultACLTable,
		catconstants.PgCatalogDependTableID:                     pgCatalogDependTable,
		catconstants.PgCatalogDescriptionTableID:                pgCatalogDescriptionView,
		catconstants.PgCatalogEnumTableID:                       pgCatalogEnumTable,
		catconstants.PgCatalogEventTriggerTableID:               pgCatalogEventTriggerTable,
		catconstants.PgCatalogExtensionTableID:                  pgCatalogExtensionTable,
		catconstants.PgCatalogFileSettingsTableID:               pgCatalogFileSettingsTable,
		catconstants.PgCatalogForeignDataWrapperTableID:         pgCatalogForeignDataWrapperTable,
		catconstants.PgCatalogForeignServerTableID:              pgCatalogForeignServerTable,
		catconstants.PgCatalogForeignTableTableID:               pgCatalogForeignTableTable,
		catconstants.PgCatalogGroupTableID:                      pgCatalogGroupTable,
		catconstants.PgCatalogHbaFileRulesTableID:               pgCatalogHbaFileRulesTable,
		catconstants.PgCatalogIndexTableID:                      pgCatalogIndexTable,
		catconstants.PgCatalogIndexesTableID:                    pgCatalogIndexesTable,
		catconstants.PgCatalogInheritsTableID:                   pgCatalogInheritsTable,
		catconstants.PgCatalogInitPrivsTableID:                  pgCatalogInitPrivsTable,
		catconstants.PgCatalogLanguageTableID:                   pgCatalogLanguageTable,
		catconstants.PgCatalogLargeobjectMetadataTableID:        pgCatalogLargeobjectMetadataTable,
		catconstants.PgCatalogLargeobjectTableID:                pgCatalogLargeobjectTable,
		catconstants.PgCatalogLocksTableID:                      pgCatalogLocksTable,
		catconstants.PgCatalogMatViewsTableID:                   pgCatalogMatViewsTable,
		catconstants.PgCatalogNamespaceTableID:                  pgCatalogNamespaceTable,
		catconstants.PgCatalogOpclassTableID:                    pgCatalogOpclassTable,
		catconstants.PgCatalogOperatorTableID:                   pgCatalogOperatorTable,
		catconstants.PgCatalogOpfamilyTableID:                   pgCatalogOpfamilyTable,
		catconstants.PgCatalogPartitionedTableTableID:           pgCatalogPartitionedTableTable,
		catconstants.PgCatalogPoliciesTableID:                   pgCatalogPoliciesTable,
		catconstants.PgCatalogPolicyTableID:                     pgCatalogPolicyTable,
		catconstants.PgCatalogPreparedStatementsTableID:         pgCatalogPreparedStatementsTable,
		catconstants.PgCatalogPreparedXactsTableID:              pgCatalogPreparedXactsTable,
		catconstants.PgCatalogProcTableID:                       pgCatalogProcTable,
		catconstants.PgCatalogPublicationRelTableID:             pgCatalogPublicationRelTable,
		catconstants.PgCatalogPublicationTableID:                pgCatalogPublicationTable,
		catconstants.PgCatalogPublicationTablesTableID:          pgCatalogPublicationTablesTable,
		catconstants.PgCatalogRangeTableID:                      pgCatalogRangeTable,
		catconstants.PgCatalogReplicationOriginStatusTableID:    pgCatalogReplicationOriginStatusTable,
		catconstants.PgCatalogReplicationOriginTableID:          pgCatalogReplicationOriginTable,
		catconstants.PgCatalogReplicationSlotsTableID:           pgCatalogReplicationSlotsTable,
		catconstants.PgCatalogRewriteTableID:                    pgCatalogRewriteTable,
		catconstants.PgCatalogRolesTableID:                      pgCatalogRolesTable,
		catconstants.PgCatalogRulesTableID:                      pgCatalogRulesTable,
		catconstants.PgCatalogSecLabelsTableID:                  pgCatalogSecLabelsTable,
		catconstants.PgCatalogSecurityLabelTableID:              pgCatalogSecurityLabelTable,
		catconstants.PgCatalogSequenceTableID:                   pgCatalogSequenceTable,
		catconstants.PgCatalogSequencesTableID:                  pgCatalogSequencesTable,
		catconstants.PgCatalogSettingsTableID:                   pgCatalogSettingsTable,
		catconstants.PgCatalogShadowTableID:                     pgCatalogShadowTable,
		catconstants.PgCatalogSharedDescriptionTableID:          pgCatalogSharedDescriptionView,
		catconstants.PgCatalogSharedSecurityLabelTableID:        pgCatalogSharedSecurityLabelTable,
		catconstants.PgCatalogShdependTableID:                   pgCatalogShdependTable,
		catconstants.PgCatalogShmemAllocationsTableID:           pgCatalogShmemAllocationsTable,
		catconstants.PgCatalogStatActivityTableID:               pgCatalogStatActivityTable,
		catconstants.PgCatalogStatAllIndexesTableID:             pgCatalogStatAllIndexesTable,
		catconstants.PgCatalogStatAllTablesTableID:              pgCatalogStatAllTablesTable,
		catconstants.PgCatalogStatArchiverTableID:               pgCatalogStatArchiverTable,
		catconstants.PgCatalogStatBgwriterTableID:               pgCatalogStatBgwriterTable,
		catconstants.PgCatalogStatDatabaseConflictsTableID:      pgCatalogStatDatabaseConflictsTable,
		catconstants.PgCatalogStatDatabaseTableID:               pgCatalogStatDatabaseTable,
		catconstants.PgCatalogStatGssapiTableID:                 pgCatalogStatGssapiTable,
		catconstants.PgCatalogStatProgressAnalyzeTableID:        pgCatalogStatProgressAnalyzeTable,
		catconstants.PgCatalogStatProgressBasebackupTableID:     pgCatalogStatProgressBasebackupTable,
		catconstants.PgCatalogStatProgressClusterTableID:        pgCatalogStatProgressClusterTable,
		catconstants.PgCatalogStatProgressCreateIndexTableID:    pgCatalogStatProgressCreateIndexTable,
		catconstants.PgCatalogStatProgressVacuumTableID:         pgCatalogStatProgressVacuumTable,
		catconstants.PgCatalogStatReplicationTableID:            pgCatalogStatReplicationTable,
		catconstants.PgCatalogStatSlruTableID:                   pgCatalogStatSlruTable,
		catconstants.PgCatalogStatSslTableID:                    pgCatalogStatSslTable,
		catconstants.PgCatalogStatSubscriptionTableID:           pgCatalogStatSubscriptionTable,
		catconstants.PgCatalogStatSysIndexesTableID:             pgCatalogStatSysIndexesTable,
		catconstants.PgCatalogStatSysTablesTableID:              pgCatalogStatSysTablesTable,
		catconstants.PgCatalogStatUserFunctionsTableID:          pgCatalogStatUserFunctionsTable,
		catconstants.PgCatalogStatUserIndexesTableID:            pgCatalogStatUserIndexesTable,
		catconstants.PgCatalogStatUserTablesTableID:             pgCatalogStatUserTablesTable,
		catconstants.PgCatalogStatWalReceiverTableID:            pgCatalogStatWalReceiverTable,
		catconstants.PgCatalogStatXactAllTablesTableID:          pgCatalogStatXactAllTablesTable,
		catconstants.PgCatalogStatXactSysTablesTableID:          pgCatalogStatXactSysTablesTable,
		catconstants.PgCatalogStatXactUserFunctionsTableID:      pgCatalogStatXactUserFunctionsTable,
		catconstants.PgCatalogStatXactUserTablesTableID:         pgCatalogStatXactUserTablesTable,
		catconstants.PgCatalogStatioAllIndexesTableID:           pgCatalogStatioAllIndexesTable,
		catconstants.PgCatalogStatioAllSequencesTableID:         pgCatalogStatioAllSequencesTable,
		catconstants.PgCatalogStatioAllTablesTableID:            pgCatalogStatioAllTablesTable,
		catconstants.PgCatalogStatioSysIndexesTableID:           pgCatalogStatioSysIndexesTable,
		catconstants.PgCatalogStatioSysSequencesTableID:         pgCatalogStatioSysSequencesTable,
		catconstants.PgCatalogStatioSysTablesTableID:            pgCatalogStatioSysTablesTable,
		catconstants.PgCatalogStatioUserIndexesTableID:          pgCatalogStatioUserIndexesTable,
		catconstants.PgCatalogStatioUserSequencesTableID:        pgCatalogStatioUserSequencesTable,
		catconstants.PgCatalogStatioUserTablesTableID:           pgCatalogStatioUserTablesTable,
		catconstants.PgCatalogStatisticExtDataTableID:           pgCatalogStatisticExtDataTable,
		catconstants.PgCatalogStatisticExtTableID:               pgCatalogStatisticExtTable,
		catconstants.PgCatalogStatisticTableID:                  pgCatalogStatisticTable,
		catconstants.PgCatalogStatsExtTableID:                   pgCatalogStatsExtTable,
		catconstants.PgCatalogStatsTableID:                      pgCatalogStatsTable,
		catconstants.PgCatalogSubscriptionRelTableID:            pgCatalogSubscriptionRelTable,
		catconstants.PgCatalogSubscriptionTableID:               pgCatalogSubscriptionTable,
		catconstants.PgCatalogTablesTableID:                     pgCatalogTablesTable,
		catconstants.PgCatalogTablespaceTableID:                 pgCatalogTablespaceTable,
		catconstants.PgCatalogTimezoneAbbrevsTableID:            pgCatalogTimezoneAbbrevsTable,
		catconstants.PgCatalogTimezoneNamesTableID:              pgCatalogTimezoneNamesTable,
		catconstants.PgCatalogTransformTableID:                  pgCatalogTransformTable,
		catconstants.PgCatalogTriggerTableID:                    pgCatalogTriggerTable,
		catconstants.PgCatalogTsConfigMapTableID:                pgCatalogTsConfigMapTable,
		catconstants.PgCatalogTsConfigTableID:                   pgCatalogTsConfigTable,
		catconstants.PgCatalogTsDictTableID:                     pgCatalogTsDictTable,
		catconstants.PgCatalogTsParserTableID:                   pgCatalogTsParserTable,
		catconstants.PgCatalogTsTemplateTableID:                 pgCatalogTsTemplateTable,
		catconstants.PgCatalogTypeTableID:                       pgCatalogTypeTable,
		catconstants.PgCatalogUserMappingTableID:                pgCatalogUserMappingTable,
		catconstants.PgCatalogUserMappingsTableID:               pgCatalogUserMappingsTable,
		catconstants.PgCatalogUserTableID:                       pgCatalogUserTable,
		catconstants.PgCatalogViewsTableID:                      pgCatalogViewsTable,
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
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	func(ctx context.Context, p *planner, h oidHasher,
		db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, table catalog.TableDescriptor,
		lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error,
	) error {
		for _, column := range table.PublicColumns() {
			if !column.HasDefault() && !column.IsComputed() {
				// pg_attrdef only expects rows for columns with default values
				// or computed expressions.
				continue
			}
			expr := column.GetDefaultExpr()
			if column.IsComputed() {
				expr = column.GetComputeExpr()
			}
			displayExpr, err := schemaexpr.FormatExprForDisplay(
				ctx, table, expr, p.EvalContext(), &p.semaCtx, p.SessionData(), tree.FmtPGCatalog,
			)
			if err != nil {
				return err
			}
			defSrc := tree.NewDString(displayExpr)
			if err := addRow(
				h.ColumnOid(table.GetID(), column.GetID()),          // oid
				tableOid(table.GetID()),                             // adrelid
				tree.NewDInt(tree.DInt(column.GetPGAttributeNum())), // adnum
				defSrc, // adbin
				defSrc, // adsrc
			); err != nil {
				return err
			}
		}
		return nil
	},
	nil)

var pgCatalogAttributeTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`table columns (incomplete - see also information_schema.columns)
https://www.postgresql.org/docs/12/catalog-pg-attribute.html`,
	vtable.PGCatalogAttribute,
	virtualMany, true, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
		table catalog.TableDescriptor,
		lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error,
	) error {
		populatedColumns := intsets.Fast{}
		maxPGAttributeNum := 0
		// addColumn adds either a table or an index column to the pg_attribute table.
		addColumn := func(column catalog.Column, attRelID tree.Datum, attNum uint32) error {
			if int(column.GetID()) > maxPGAttributeNum {
				maxPGAttributeNum = int(column.GetPGAttributeNum())
			}
			populatedColumns.Add(int(column.GetPGAttributeNum()))
			colTyp := column.GetType()
			// Sets the attgenerated column to 's' if the column is generated/
			// computed stored, "v" if virtual, zero byte otherwise.
			var isColumnComputed string
			if column.IsComputed() && !column.IsVirtual() {
				isColumnComputed = "s"
			} else if column.IsComputed() {
				isColumnComputed = "v"
			} else {
				isColumnComputed = ""
			}

			// Sets the attidentity column to 'a' if the column is generated
			// always as identity, "b" if generated by default as identity,
			// zero byte otherwise.
			var generatedAsIdentityType string
			if column.IsGeneratedAsIdentity() {
				if column.IsGeneratedAlwaysAsIdentity() {
					generatedAsIdentityType = "a"
				} else if column.IsGeneratedByDefaultAsIdentity() {
					generatedAsIdentityType = "d"
				} else {
					return errors.AssertionFailedf(
						"column %s is of wrong generated as identity type (neither ALWAYS nor BY DEFAULT)",
						column.GetName(),
					)
				}
			} else {
				generatedAsIdentityType = ""
			}

			return addRow(
				attRelID,                        // attrelid
				tree.NewDName(column.GetName()), // attname
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
				tree.MakeDBool(tree.DBool(!column.IsNullable())), // attnotnull
				tree.MakeDBool(tree.DBool(column.HasDefault() ||
					column.IsComputed())), // atthasdef
				tree.NewDString(generatedAsIdentityType), // attidentity
				tree.NewDString(isColumnComputed),        // attgenerated
				tree.DBoolFalse,                          // attisdropped
				tree.DBoolTrue,                           // attislocal
				zeroVal,                                  // attinhcount
				typColl(colTyp, h),                       // attcollation
				tree.DNull,                               // attacl
				tree.DNull,                               // attoptions
				tree.DNull,                               // attfdwoptions
				// These columns were automatically created by pg_catalog_test's missing column generator.
				tree.DNull, // atthasmissing
				// These columns were automatically created by pg_catalog_test's missing column generator.
				tree.DNull, // attmissingval
				tree.MakeDBool(tree.DBool(column.IsHidden())), // attishidden
			)
		}

		// Columns for table.
		tableID := tableOid(table.GetID())
		for _, column := range table.AccessibleColumns() {
			if err := addColumn(column, tableID, uint32(column.GetPGAttributeNum())); err != nil {
				return err
			}
		}
		// The next column ID may not be populated on certain relations like
		// sequences, so only use if it's available.
		if int(table.GetNextColumnID()) > maxPGAttributeNum {
			// We need to check the maximum in the table in case columns
			// at the end were dropped.
			maxPGAttributeNum = int(table.GetNextColumnID() - 1)
		}
		// Add a dropped entry for any attribute numbers in the middle that are
		// missing, assuming there are any numeric gaps in the number of columns
		// observed.
		missingColumnType := types.AnyElement
		if populatedColumns.Len() != maxPGAttributeNum {
			for colOrdinal := 1; colOrdinal <= maxPGAttributeNum; colOrdinal++ {
				if populatedColumns.Contains(colOrdinal) {
					continue
				}
				colName := fmt.Sprintf("........pg.dropped.%d........", colOrdinal)
				if err := addRow(
					tableID,                             // attrelid
					tree.NewDName(colName),              // attname
					typOid(missingColumnType),           // atttypid
					zeroVal,                             // attstattarget
					typLen(missingColumnType),           // attlen
					tree.NewDInt(tree.DInt(colOrdinal)), // attnum
					zeroVal,                             // attndims
					negOneVal,                           // attcacheoff
					tree.NewDInt(tree.DInt(missingColumnType.TypeModifier())), // atttypmod
					tree.DNull,                    // attbyval (see pg_type.typbyval)
					tree.DNull,                    // attstorage
					tree.DNull,                    // attalign
					tree.DBoolFalse,               // attnotnull
					tree.DBoolFalse,               // atthasdef
					tree.NewDString(""),           // attidentity
					tree.NewDString(""),           // attgenerated
					tree.DBoolTrue,                // attisdropped
					tree.DBoolTrue,                // attislocal
					zeroVal,                       // attinhcount
					typColl(missingColumnType, h), // attcollation
					tree.DNull,                    // attacl
					tree.DNull,                    // attoptions
					tree.DNull,                    // attfdwoptions
					// These columns were automatically created by pg_catalog_test's missing column generator.
					tree.DNull, // atthasmissing
					// These columns were automatically created by pg_catalog_test's missing column generator.
					tree.DNull, // attmissingval
					tree.DNull, // attishidden
				); err != nil {
					return err
				}
			}
		}

		// Columns for each index.
		columnIdxMap := catalog.ColumnIDToOrdinalMap(table.PublicColumns())
		return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
			idxID := h.IndexOid(table.GetID(), index.GetID())

			for i := 0; i < index.NumKeyColumns(); i++ {
				colID := index.GetKeyColumnID(i)
				column := table.PublicColumns()[columnIdxMap.GetDefault(colID)]
				// The attnum for columns in an index is the order it appears in the
				// index definition and is not related to the attnum the column has in
				// the table.
				if err := addColumn(column, idxID, uint32(i+1)); err != nil {
					return err
				}
			}
			// pg_attribute only includes stored columns for secondary indexes, not
			// for primary indexes
			for i := 0; i < index.NumSecondaryStoredColumns(); i++ {
				colID := index.GetStoredColumnID(i)
				column := table.PublicColumns()[columnIdxMap.GetDefault(colID)]
				// The attnum for columns in an index is the order it appears in the
				// index definition and is not related to the attnum the column has in
				// the table.
				if err := addColumn(column, idxID, uint32(i+1+index.NumKeyColumns())); err != nil {
					return err
				}
			}
			return nil
		})
	},
	addPGAttributeRowForCompositeType)

var pgCatalogCastTable = virtualSchemaTable{
	comment: `casts (empty - needs filling out)
https://www.postgresql.org/docs/9.6/catalog-pg-cast.html`,
	schema: vtable.PGCatalogCast,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		cast.ForEachCast(func(src, tgt oid.Oid, cCtx cast.Context, ctxOrigin cast.ContextOrigin, _ volatility.V) {
			if ctxOrigin == cast.ContextOriginPgCast {
				castCtx := cCtx.PGString()

				castFunc := tree.DNull
				if srcTyp, ok := types.OidToType[src]; ok {
					if v, ok := builtins.CastBuiltinOIDs[tgt][srcTyp.Family()]; ok {
						castFunc = tree.NewDOid(v)
					}
				}
				_ = addRow(
					h.CastOid(src, tgt),      // oid
					tree.NewDOid(src),        // cast source
					tree.NewDOid(tgt),        // casttarget
					castFunc,                 // castfunc
					tree.NewDString(castCtx), // castcontext
					tree.DNull,               // castmethod
				)
			}
		})
		return nil
	},
}

func userIsSuper(
	ctx context.Context, p *planner, userName username.SQLUsername,
) (tree.DBool, error) {
	isSuper, err := p.UserHasAdminRole(ctx, userName)
	return tree.DBool(isSuper), err
}

var pgCatalogAuthIDTable = virtualSchemaTable{
	comment: `authorization identifiers - differs from postgres as we do not display passwords, 
and thus do not require admin privileges for access. 
https://www.postgresql.org/docs/9.5/catalog-pg-authid.html`,
	schema: vtable.PGCatalogAuthID,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRoleAtCacheReadTS(ctx, p, func(ctx context.Context, userName username.SQLUsername, isRole bool, options roleOptions, _ tree.Datum) error {
			isRoot := tree.DBool(userName.IsRootUser() || userName.IsAdminRole())
			// Currently, all users and roles inherit the privileges of roles they are
			// members of. See https://github.com/cockroachdb/cockroach/issues/69583.
			roleInherits := tree.DBool(true)
			noLogin, err := options.noLogin()
			if err != nil {
				return err
			}
			roleCanLogin := !noLogin
			createDB, err := options.createDB()
			if err != nil {
				return err
			}
			rolValidUntil, err := options.validUntil(p)
			if err != nil {
				return err
			}
			createRole, err := options.createRole()
			if err != nil {
				return err
			}

			isSuper, err := userIsSuper(ctx, p, userName)
			if err != nil {
				return err
			}

			return addRow(
				h.UserOid(userName),                  // oid
				tree.NewDName(userName.Normalized()), // rolname
				tree.MakeDBool(isRoot || isSuper),    // rolsuper
				tree.MakeDBool(roleInherits),         // rolinherit
				tree.MakeDBool(isRoot || createRole), // rolcreaterole
				tree.MakeDBool(isRoot || createDB),   // rolcreatedb
				tree.MakeDBool(roleCanLogin),         // rolcanlogin.
				tree.DBoolFalse,                      // rolreplication
				tree.DBoolFalse,                      // rolbypassrls
				negOneVal,                            // rolconnlimit
				passwdStarString,                     // rolpassword
				rolValidUntil,                        // rolvaliduntil
			)
		})
	},
}

var pgCatalogAuthMembersTable = virtualSchemaTable{
	comment: `role membership
https://www.postgresql.org/docs/9.5/catalog-pg-auth-members.html`,
	schema: vtable.PGCatalogAuthMembers,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRoleMembershipAtCacheReadTS(ctx, p,
			func(ctx context.Context, roleName, memberName username.SQLUsername, isAdmin bool) error {
				return addRow(
					h.UserOid(roleName),                 // roleid
					h.UserOid(memberName),               // member
					tree.DNull,                          // grantor
					tree.MakeDBool(tree.DBool(isAdmin)), // admin_option
				)
			},
		)
	},
}

var pgCatalogAvailableExtensionsTable = virtualSchemaTable{
	comment: `available extensions
https://www.postgresql.org/docs/9.6/view-pg-available-extensions.html`,
	schema: vtable.PGCatalogAvailableExtensions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// We support no extensions.
		return nil
	},
	unimplemented: true,
}

func getOwnerOID(ctx context.Context, p *planner, desc catalog.Descriptor) (tree.Datum, error) {
	owner, err := p.getOwnerOfPrivilegeObject(ctx, desc)
	if err != nil {
		return nil, err
	}
	h := makeOidHasher()
	return h.UserOid(owner), nil
}

func getOwnerName(ctx context.Context, p *planner, desc catalog.Descriptor) (tree.Datum, error) {
	owner, err := p.getOwnerOfPrivilegeObject(ctx, desc)
	if err != nil {
		return nil, err
	}
	return tree.NewDName(owner.Normalized()), nil
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
	func(ctx context.Context, p *planner, h oidHasher, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
		table catalog.TableDescriptor, _ simpleSchemaResolver, addRow func(...tree.Datum) error,
	) error {
		// The only difference between tables, views and sequences are the relkind and relam columns.
		relKind := relKindTable
		relAm := forwardIndexOid
		replIdent := "d" // default;
		if table.IsView() {
			relKind = relKindView
			if table.MaterializedView() {
				relKind = relKindMaterializedView
			} else {
				replIdent = "n"
			}
			relAm = oidZero
		} else if table.IsSequence() {
			relKind = relKindSequence
			relAm = oidZero
			replIdent = "n"
		}
		relPersistence := relPersistencePermanent
		if table.IsTemporary() {
			relPersistence = relPersistenceTemporary
		}
		var relOptions tree.Datum = tree.DNull
		if storageParams := table.GetStorageParams(false /* spaceBetweenEqual */); len(storageParams) > 0 {
			relOptionsArr := tree.NewDArray(types.String)
			for _, storageParam := range storageParams {
				if err := relOptionsArr.Append(tree.NewDString(storageParam)); err != nil {
					return err
				}
			}
			relOptions = relOptionsArr
		}
		ownerOid, err := getOwnerOID(ctx, p, table)
		if err != nil {
			return err
		}
		implicitTypOID := typedesc.TableIDToImplicitTypeOID(table.GetID())
		namespaceOid := schemaOid(sc.GetID())
		if err := addRow(
			tableOid(table.GetID()),        // oid
			tree.NewDName(table.GetName()), // relname
			namespaceOid,                   // relnamespace
			tree.NewDOid(implicitTypOID),   // reltype (PG creates a composite type in pg_type for each table)
			oidZero,                        // reloftype (used for type tables, which is unsupported)
			ownerOid,                       // relowner
			relAm,                          // relam
			oidZero,                        // relfilenode
			oidZero,                        // reltablespace
			tree.DNull,                     // relpages
			tree.DNull,                     // reltuples
			zeroVal,                        // relallvisible
			oidZero,                        // reltoastrelid
			tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhasindex
			tree.DBoolFalse, // relisshared
			relPersistence,  // relpersistence
			tree.MakeDBool(tree.DBool(table.IsTemporary())), // relistemp
			relKind, // relkind
			tree.NewDInt(tree.DInt(len(table.AccessibleColumns()))),        // relnatts
			tree.NewDInt(tree.DInt(len(table.EnforcedCheckConstraints()))), // relchecks
			tree.DBoolFalse, // relhasoids
			tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhaspkey
			tree.DBoolFalse, // relhasrules
			tree.DBoolFalse, // relhastriggers
			tree.DBoolFalse, // relhassubclass
			zeroVal,         // relfrozenxid
			tree.DNull,      // relacl
			relOptions,      // reloptions
			// These columns were automatically created by pg_catalog_test's missing column generator.
			tree.MakeDBool(tree.DBool(table.IsRowLevelSecurityForced())), // relforcerowsecurity
			tree.DNull,                 // relispartition
			tree.DNull,                 // relispopulated
			tree.NewDString(replIdent), // relreplident
			tree.DNull,                 // relrewrite
			tree.MakeDBool(tree.DBool(table.IsRowLevelSecurityEnabled())), // relrowsecurity
			tree.DNull, // relpartbound
			// These columns were automatically created by pg_catalog_test's missing column generator.
			tree.DNull, // relminmxid
		); err != nil {
			return err
		}

		// Skip adding indexes for sequences (their table descriptors have a primary
		// index to make them comprehensible to backup/restore, but PG doesn't include
		// an index in pg_class).
		if table.IsSequence() {
			return nil
		}

		// Indexes.
		return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
			indexType := forwardIndexOid
			if index.GetType() == idxtype.INVERTED {
				indexType = invertedIndexOid
			}
			ownerOid, err := getOwnerOID(ctx, p, table)
			if err != nil {
				return err
			}
			return addRow(
				h.IndexOid(table.GetID(), index.GetID()), // oid
				tree.NewDName(index.GetName()),           // relname
				namespaceOid,                             // relnamespace
				oidZero,                                  // reltype
				oidZero,                                  // reloftype
				ownerOid,                                 // relowner
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
				tree.NewDInt(tree.DInt(index.NumKeyColumns())), // relnatts
				zeroVal,         // relchecks
				tree.DBoolFalse, // relhasoids
				tree.DBoolFalse, // relhaspkey
				tree.DBoolFalse, // relhasrules
				tree.DBoolFalse, // relhastriggers
				tree.DBoolFalse, // relhassubclass
				zeroVal,         // relfrozenxid
				tree.DNull,      // relacl
				tree.DNull,      // reloptions
				// These columns were automatically created by pg_catalog_test's missing column generator.
				tree.DBoolFalse,      // relforcerowsecurity
				tree.DNull,           // relispartition
				tree.DNull,           // relispopulated
				tree.NewDString("n"), // relreplident
				tree.DNull,           // relrewrite
				tree.DBoolFalse,      // relrowsecurity
				tree.DNull,           // relpartbound
				// These columns were automatically created by pg_catalog_test's missing column generator.
				tree.DNull, // relminmxid
			)
		})
	},
	addPGClassRowForCompositeType)

var pgCatalogCollationTable = virtualSchemaTable{
	comment: `available collations (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-collation.html`,
	schema: vtable.PGCatalogCollation,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false /* requiresPrivileges */, func(ctx context.Context, db catalog.DatabaseDescriptor) error {
			namespaceOid := tree.NewDOid(catconstants.PgCatalogID)
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
					// These columns were automatically created by pg_catalog_test's missing column generator.
					tree.DNull, // collprovider
					tree.DNull, // collversion
					tree.DNull, // collisdeterministic
				)
			}
			for _, tag := range collatedstring.Supported() {
				if err := add(tag); err != nil {
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

	fkActionMap = map[semenumpb.ForeignKeyAction]tree.Datum{
		semenumpb.ForeignKeyAction_NO_ACTION:   fkActionNone,
		semenumpb.ForeignKeyAction_RESTRICT:    fkActionRestrict,
		semenumpb.ForeignKeyAction_CASCADE:     fkActionCascade,
		semenumpb.ForeignKeyAction_SET_NULL:    fkActionSetNull,
		semenumpb.ForeignKeyAction_SET_DEFAULT: fkActionSetDefault,
	}

	fkMatchTypeFull    = tree.NewDString("f")
	fkMatchTypePartial = tree.NewDString("p")
	fkMatchTypeSimple  = tree.NewDString("s")

	fkMatchMap = map[semenumpb.Match]tree.Datum{
		semenumpb.Match_SIMPLE:  fkMatchTypeSimple,
		semenumpb.Match_FULL:    fkMatchTypeFull,
		semenumpb.Match_PARTIAL: fkMatchTypePartial,
	}
)

func populateTableConstraints(
	ctx context.Context,
	p *planner,
	h oidHasher,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	table catalog.TableDescriptor,
	tableLookup simpleSchemaResolver,
	addRow func(...tree.Datum) error,
) error {
	namespaceOid := schemaOid(sc.GetID())
	tblOid := tableOid(table.GetID())
	for _, c := range table.AllConstraints() {
		// Ignore constraints that are being dropped. When a column is dropped alongside
		// a constraint, the DSC may insert a placeholder name into the constraint,
		// which can interfere with column name lookups in this function.
		if c.GetConstraintValidity() == descpb.ConstraintValidity_Dropping {
			continue
		}

		conoid := tree.DNull
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
		if uwi := c.AsUniqueWithIndex(); uwi != nil {
			conindid = h.IndexOid(table.GetID(), uwi.GetID())
			var err error
			if conkey, err = colIDArrayToDatum(uwi.IndexDesc().KeyColumnIDs); err != nil {
				return err
			}
			if uwi.Primary() {
				if !p.SessionData().ShowPrimaryKeyConstraintOnNotVisibleColumns {
					allHidden := true
					for _, col := range table.IndexKeyColumns(uwi) {
						if !col.IsHidden() {
							allHidden = false
							break
						}
					}
					if allHidden {
						continue
					}
				}
				conoid = h.PrimaryKeyConstraintOid(db.GetID(), sc.GetID(), table.GetID(), uwi)
				contype = conTypePKey
				condef = tree.NewDString(tabledesc.PrimaryKeyString(table))
			} else {
				f := tree.NewFmtCtx(tree.FmtSimple)
				conoid = h.UniqueConstraintOid(db.GetID(), sc.GetID(), table.GetID(), uwi)
				contype = conTypeUnique
				f.WriteString("UNIQUE (")
				if err := catformat.FormatIndexElements(
					ctx, table, uwi.IndexDesc(), f, p.EvalContext(), p.SemaCtx(), p.SessionData(),
				); err != nil {
					return err
				}
				f.WriteByte(')')
				if uwi.IsPartial() {
					pred, err := schemaexpr.FormatExprForDisplay(ctx, table, uwi.GetPredicate(), p.EvalContext(), p.SemaCtx(), p.SessionData(), tree.FmtPGCatalog)
					if err != nil {
						return err
					}
					f.WriteString(fmt.Sprintf(" WHERE (%s)", pred))
				}
				condef = tree.NewDString(f.CloseAndGetString())
			}
		} else if fk := c.AsForeignKey(); fk != nil {
			conoid = h.ForeignKeyConstraintOid(db.GetID(), sc.GetID(), table.GetID(), fk)
			contype = conTypeFK
			// Foreign keys don't have a single linked index. Pick the first one
			// that matches on the referenced table.
			referencedTable, err := tableLookup.getTableByID(fk.GetReferencedTableID())
			if err != nil {
				return err
			}
			if refConstraint, err := catalog.FindFKReferencedUniqueConstraint(referencedTable, fk); err != nil {
				// We couldn't find a unique constraint that matched. This shouldn't
				// happen.
				log.Warningf(ctx, "broken fk reference: %v", err)
			} else if idx := refConstraint.AsUniqueWithIndex(); idx != nil {
				conindid = h.IndexOid(referencedTable.GetID(), idx.GetID())
			}
			confrelid = tableOid(referencedTable.GetID())
			if r, ok := fkActionMap[fk.OnUpdate()]; ok {
				confupdtype = r
			}
			if r, ok := fkActionMap[fk.OnDelete()]; ok {
				confdeltype = r
			}
			if r, ok := fkMatchMap[fk.Match()]; ok {
				confmatchtype = r
			}
			if conkey, err = colIDArrayToDatum(fk.ForeignKeyDesc().OriginColumnIDs); err != nil {
				return err
			}
			if confkey, err = colIDArrayToDatum(fk.ForeignKeyDesc().ReferencedColumnIDs); err != nil {
				return err
			}
			var buf bytes.Buffer
			if err := showForeignKeyConstraint(
				&buf, db.GetName(),
				table, fk.ForeignKeyDesc(),
				tableLookup,
				p.extendedEvalCtx.SessionData().SearchPath,
			); err != nil {
				return err
			}
			condef = tree.NewDString(buf.String())
		} else if uwoi := c.AsUniqueWithoutIndex(); uwoi != nil {
			contype = conTypeUnique
			f := tree.NewFmtCtx(tree.FmtSimple)
			conoid = h.UniqueWithoutIndexConstraintOid(
				db.GetID(), sc.GetID(), table.GetID(), uwoi,
			)
			f.WriteString("UNIQUE WITHOUT INDEX (")
			colNames, err := catalog.ColumnNamesForIDs(table, uwoi.UniqueWithoutIndexDesc().ColumnIDs)
			if err != nil {
				return err
			}
			f.WriteString(strings.Join(colNames, ", "))
			f.WriteByte(')')
			if !uwoi.IsConstraintValidated() {
				f.WriteString(" NOT VALID")
			}
			if uwoi.GetPredicate() != "" {
				pred, err := schemaexpr.FormatExprForDisplay(ctx, table, uwoi.GetPredicate(), p.EvalContext(), p.SemaCtx(), p.SessionData(), tree.FmtPGCatalog)
				if err != nil {
					return err
				}
				f.WriteString(fmt.Sprintf(" WHERE (%s)", pred))
			}
			condef = tree.NewDString(f.CloseAndGetString())
		} else if ck := c.AsCheck(); ck != nil {
			conoid = h.CheckConstraintOid(db.GetID(), sc.GetID(), table.GetID(), ck)
			contype = conTypeCheck
			if conkey, err = colIDArrayToDatum(ck.CheckDesc().ColumnIDs); err != nil {
				return err
			}
			displayExpr, err := schemaexpr.FormatExprForDisplay(ctx, table, ck.GetExpr(), p.EvalContext(), &p.semaCtx, p.SessionData(), tree.FmtPGCatalog)
			if err != nil {
				return err
			}
			consrc = tree.NewDString(fmt.Sprintf("(%s)", displayExpr))
			conbin = consrc
			validity := ""
			if !ck.IsConstraintValidated() {
				validity = " NOT VALID"
			}
			condef = tree.NewDString(fmt.Sprintf("CHECK ((%s))%s", displayExpr, validity))
		}

		if err := addRow(
			conoid,                   // oid
			dNameOrNull(c.GetName()), // conname
			namespaceOid,             // connamespace
			contype,                  // contype
			tree.DBoolFalse,          // condeferrable
			tree.DBoolFalse,          // condeferred
			tree.MakeDBool(tree.DBool(!c.IsConstraintUnvalidated())), // convalidated
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
			oidZero,        // conparentid
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
	id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := r.p.Descriptors().ByIDWithLeased(r.p.txn).WithoutNonPublic().Get().Database(r.ctx, id)
	return desc, err
}

func (r oneAtATimeSchemaResolver) getTableByID(id descpb.ID) (catalog.TableDescriptor, error) {
	table, err := r.p.LookupTableByID(r.ctx, id)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (r oneAtATimeSchemaResolver) getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error) {
	return r.p.Descriptors().ByIDWithoutLeased(r.p.txn).Get().Schema(r.ctx, id)
}

// makeAllRelationsVirtualTableWithDescriptorIDIndex creates a virtual table that searches through
// all table and type descriptors in the system. It automatically adds a virtual index implementation
// to the table/type id column as well. The input schema must have a single INDEX definition
// with a single column, which must be the column that contains the table/type id.
// includesIndexEntries should be set to true if the indexed column produces
// index ids as well as just ordinary table/type descriptor ids. In this case, the
// caller must pass true for this variable to prevent failed lookups.
func makeAllRelationsVirtualTableWithDescriptorIDIndex(
	comment string,
	schemaDef string,
	virtualOpts virtualOpts,
	includesIndexEntries bool,
	populateFromTable func(ctx context.Context, p *planner, h oidHasher, db catalog.DatabaseDescriptor,
		sc catalog.SchemaDescriptor, table catalog.TableDescriptor, lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error,
	) error,
	populateFromType func(h oidHasher, nspOid tree.Datum, owner tree.Datum, typ *types.T, addRow func(...tree.Datum) error,
	) error,
) virtualSchemaTable {
	includesCompositeTypes := populateFromType != nil
	populateAll := func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()

		opts := forEachTableDescOptions{virtualOpts: virtualOpts} /* no constraints in virtual tables */
		if err := forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table, lookup := descCtx.database, descCtx.schema, descCtx.table, descCtx.tableLookup
				return populateFromTable(ctx, p, h, db, sc, table, lookup, addRow)
			},
		); err != nil {
			return err
		}

		// Loop through all user defined types (enums and composite types).
		if includesCompositeTypes {
			return forEachTypeDesc(
				ctx,
				p,
				dbContext,
				func(ctx context.Context, _ catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, typeDesc catalog.TypeDescriptor) error {
					nspOid := schemaOid(sc.GetID())
					tn := tree.NewQualifiedTypeName(dbContext.GetName(), sc.GetName(), typeDesc.GetName())
					typ, err := typedesc.HydratedTFromDesc(ctx, tn, typeDesc, p)
					if err != nil {
						return err
					}
					ownerOid, err := getOwnerOID(ctx, p, typeDesc)
					if err != nil {
						return err
					}
					// Generate rows for some/all user defined types (depending on function populateFromType).
					return populateFromType(h, nspOid, ownerOid, typ, addRow)
				},
			)
		}

		return nil
	}

	return virtualSchemaTable{
		comment: comment,
		schema:  schemaDef,
		indexes: []virtualIndex{
			{
				incomplete: includesIndexEntries,
				populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, db catalog.DatabaseDescriptor,
					addRow func(...tree.Datum) error) (bool, error) {
					var id descpb.ID
					switch t := unwrappedConstraint.(type) {
					case *tree.DOid:
						id = descpb.ID(t.Oid)
					case *tree.DInt:
						id = descpb.ID(*t)
					default:
						return false, errors.AssertionFailedf("unexpected type %T for table id column in virtual table %s",
							unwrappedConstraint, schemaDef)
					}
					maybeID := id
					if includesCompositeTypes {
						maybeID = catid.UserDefinedOIDToID(oid.Oid(id))
					}
					desc, err := p.byIDGetterBuilder().WithoutNonPublic().Get().Desc(ctx, id)
					if err != nil {
						if errors.Is(err, catalog.ErrDescriptorNotFound) ||
							catalog.HasInactiveDescriptorError(err) {
							// There is a possibility that we are not finding the descriptor because it points to
							// the OID of a user-defined type. If we are including user-defined composite types in
							// the catalog table being populated, we should use the converted maybeID as the id for
							// our lookup.
							if includesCompositeTypes && maybeID != catid.InvalidDescID {
								desc, err = p.byIDGetterBuilder().WithoutNonPublic().Get().Desc(ctx, maybeID)
								if err != nil {
									if errors.Is(err, catalog.ErrDescriptorNotFound) ||
										catalog.HasInactiveDescriptorError(err) {
										//nolint:returnerrcheck
										return !IsMaybeHashedOid(oid.Oid(id)), nil
									}
									return false, err
								}
							} else {
								// No table found, so no rows. If we know this value is
								// not a hashed OID we can safely say the table was populated
								// and skip an expensive step populating the full tables.
								//nolint:returnerrcheck
								return !IsMaybeHashedOid(oid.Oid(id)), nil
							}
						} else {
							return false, err
						}
					}
					// If the descriptor is not a table or a composite type, then we have a complete result
					// from this virtual index. We can mark the result as populated,
					// because we know the underlying descriptor will generate no rows, since
					// the ID being queried is *not* a table or a composite type.
					switch d := desc.(type) {
					case catalog.TableDescriptor:
						return populateVirtualIndexForTable(ctx, p, db, d, addRow, populateFromTable)
					case catalog.TypeDescriptor:
						if !includesCompositeTypes {
							return true, nil
						}
						return populateVirtualIndexForType(ctx, p, db, d, addRow, populateFromType)
					default:
						return true, nil
					}
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
	populateTableConstraints,
	nil)

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
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogDatabaseTable = virtualSchemaTable{
	comment: `available databases (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-database.html`,
	schema: vtable.PGCatalogDatabase,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /*all databases*/, false, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				ownerOid, err := getOwnerOID(ctx, p, db)
				if err != nil {
					return err
				}
				return addRow(
					dbOid(db.GetID()),           // oid
					tree.NewDName(db.GetName()), // datname
					ownerOid,                    // datdba
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
	comment: `default ACLs; these are the privileges that will be assigned to newly created objects
https://www.postgresql.org/docs/13/catalog-pg-default-acl.html`,
	schema: vtable.PGCatalogDefaultACL,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()

		populatePrivilegeRow := func(schemaID descpb.ID) func(defaultPrivilegesForRole catpb.DefaultPrivilegesForRole) error {

			return func(defaultPrivilegesForRole catpb.DefaultPrivilegesForRole) error {
				objectTypes := privilege.GetTargetObjectTypes()
				for _, objectType := range objectTypes {
					privs, ok := defaultPrivilegesForRole.DefaultPrivilegesPerObject[objectType]
					if !ok || len(privs.Users) == 0 {
						// If the default privileges default state has been altered,
						// we use an empty entry to signify that the user has no privileges.
						// We only omit the row entirely if the default privileges are
						// in its default state. This is PG's behavior.
						// Note that if ForAllRoles is true, we can skip adding an entry
						// since ForAllRoles cannot be a grantee - therefore we can ignore
						// the RoleHasAllPrivilegesOnX flag and skip. We still have to take
						// into consideration the PublicHasUsageOnTypes flag.
						if objectType == privilege.Types {
							// if the objectType is Types, we only omit the entry
							// if both the role has ALL privileges AND public has USAGE.
							// This is the "default" state for default privileges on types
							// in Postgres.
							if (!defaultPrivilegesForRole.IsExplicitRole() ||
								catprivilege.GetRoleHasAllPrivilegesOnTargetObject(&defaultPrivilegesForRole, privilege.Types)) &&
								catprivilege.GetPublicHasUsageOnTypes(&defaultPrivilegesForRole) {
								continue
							}
						} else if objectType == privilege.Routines {
							// if the objectType is Routines, we only omit the entry
							// if both the role has ALL privileges AND public has EXECUTE.
							// This is the "default" state for default privileges on routines
							// in Postgres.
							if (!defaultPrivilegesForRole.IsExplicitRole() ||
								catprivilege.GetRoleHasAllPrivilegesOnTargetObject(&defaultPrivilegesForRole, privilege.Routines)) &&
								catprivilege.GetPublicHasExecuteOnFunctions(&defaultPrivilegesForRole) {
								continue
							}
						} else if !defaultPrivilegesForRole.IsExplicitRole() ||
							catprivilege.GetRoleHasAllPrivilegesOnTargetObject(&defaultPrivilegesForRole, objectType) {
							continue
						}
					}

					// Type of object this entry is for:
					// r = relation (table, view), S = sequence, f = function, T = type, n = schema.
					var c string
					switch objectType {
					case privilege.Tables:
						c = "r"
					case privilege.Sequences:
						c = "S"
					case privilege.Types:
						c = "T"
					case privilege.Schemas:
						c = "n"
					case privilege.Routines:
						c = "f"
					}
					privilegeObjectType := targetObjectToPrivilegeObject[objectType]
					arr := tree.NewDArray(types.String)
					for _, userPrivs := range privs.Users {
						var user string
						if userPrivs.UserProto.Decode().IsPublicRole() {
							// Postgres represents Public in defacl as an empty string.
							user = ""
						} else {
							user = userPrivs.UserProto.Decode().Normalized()
						}

						privileges, err := privilege.ListFromBitField(
							userPrivs.Privileges, privilegeObjectType,
						)
						if err != nil {
							return err
						}
						grantOptions, err := privilege.ListFromBitField(
							userPrivs.WithGrantOption, privilegeObjectType,
						)
						if err != nil {
							return err
						}
						defaclItem, err := createDefACLItem(user, privileges, grantOptions, privilegeObjectType)
						if err != nil {
							return err
						}
						if err := arr.Append(
							tree.NewDString(defaclItem)); err != nil {
							return err
						}
					}

					// Special cases to handle for types and functions.
					// If one of RoleHasAllPrivilegesOnTypes or PublicHasUsageOnTypes is false
					// and the other is true, we do not omit the entry since the default
					// state has changed. We have to produce an entry by expanding the
					// privileges. Similarly, we need to check EXECUTE for functions.
					if defaultPrivilegesForRole.IsExplicitRole() {
						publicHasUsage := false
						roleHasAllPrivileges := false
						privilegeKind := privilege.USAGE

						switch objectType {
						case privilege.Types:
							publicHasUsage = !catprivilege.GetRoleHasAllPrivilegesOnTargetObject(&defaultPrivilegesForRole, privilege.Types) &&
								catprivilege.GetPublicHasUsageOnTypes(&defaultPrivilegesForRole)
							privilegeKind = privilege.USAGE
							roleHasAllPrivileges = !catprivilege.GetPublicHasUsageOnTypes(&defaultPrivilegesForRole) &&
								defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTypes
						case privilege.Routines:
							publicHasUsage = !catprivilege.GetRoleHasAllPrivilegesOnTargetObject(&defaultPrivilegesForRole, privilege.Routines) &&
								catprivilege.GetPublicHasExecuteOnFunctions(&defaultPrivilegesForRole)
							privilegeKind = privilege.EXECUTE
							roleHasAllPrivileges = !catprivilege.GetPublicHasExecuteOnFunctions(&defaultPrivilegesForRole) &&
								defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnFunctions
						default:
							if len(privs.Users) == 0 && schemaID != descpb.InvalidID {
								continue
							}
						}

						// publicHasUsage and roleHasAllPrivileges will always be false
						// when objectType is not privilege.Types or privilege.Routines
						if publicHasUsage {
							defaclItem, err := createDefACLItem(
								"" /* public role */, privilege.List{privilegeKind}, privilege.List{}, privilegeObjectType,
							)
							if err != nil {
								return err
							}
							if err := arr.Append(tree.NewDString(defaclItem)); err != nil {
								return err
							}
						} else if roleHasAllPrivileges {
							defaclItem, err := createDefACLItem(
								defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode().Normalized(),
								privilege.List{privilege.ALL}, privilege.List{}, privilegeObjectType,
							)
							if err != nil {
								return err
							}
							if err := arr.Append(tree.NewDString(defaclItem)); err != nil {
								return err
							}
						} else if len(privs.Users) == 0 && schemaID != descpb.InvalidID {
							continue
						}
					}

					// If ForAllRoles is specified, we use an empty string as the normalized
					// role name to create the row hash.
					normalizedName := ""
					roleOid := oidZero

					if defaultPrivilegesForRole.IsExplicitRole() {
						roleOid = h.UserOid(defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode())
						normalizedName = defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode().Normalized()
					}
					rowOid := h.DBSchemaRoleOid(
						dbContext.GetID(),
						schemaID,
						normalizedName,
					)
					if err := addRow(
						rowOid,              // row identifier oid
						roleOid,             // defaclrole oid
						schemaOid(schemaID), // defaclnamespace oid
						tree.NewDString(c),  // defaclobjtype char
						arr,                 // defaclacl aclitem[]
					); err != nil {
						return err
					}
				}
				return nil
			}
		}
		err := dbContext.ForEachSchema(func(id descpb.ID, name string) error {
			schemaDescriptor, err := p.Descriptors().ByIDWithoutLeased(p.txn).Get().Schema(ctx, id)
			if err != nil {
				return err
			}

			err = schemaDescriptor.GetDefaultPrivilegeDescriptor().ForEachDefaultPrivilegeForRole(populatePrivilegeRow(id))

			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			return err
		}

		return dbContext.GetDefaultPrivilegeDescriptor().ForEachDefaultPrivilegeForRole(populatePrivilegeRow(descpb.InvalidID /* schemaID */))
	},
}

func createDefACLItem(
	user string,
	privileges privilege.List,
	grantOptions privilege.List,
	privilegeObjectType privilege.ObjectType,
) (string, error) {
	acl, err := privileges.ListToACL(
		grantOptions,
		privilegeObjectType,
	)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%s=%s/%s`,
		user,
		acl,
		// TODO(richardjcai): CockroachDB currently does not track grantors
		//    See: https://github.com/cockroachdb/cockroach/issues/67442.
		"", /* grantor */
	), nil
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

	pgAuthIDTableName      = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_authid"))
	pgConstraintsTableName = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_constraint"))
	pgClassTableName       = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_class"))
	pgDatabaseTableName    = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_database"))
	pgRewriteTableName     = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_rewrite"))
	pgProcTableName        = tree.MakeTableNameWithSchema("", tree.Name(pgCatalogName), tree.Name("pg_proc"))
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
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		vt := p.getVirtualTabler()
		pgConstraintsDesc, err := vt.getVirtualTableDesc(&pgConstraintsTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_constraint")
		}
		pgClassDesc, err := vt.getVirtualTableDesc(&pgClassTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_class")
		}
		pgRewriteDesc, err := vt.getVirtualTableDesc(&pgRewriteTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_rewrite")
		}
		pgProcDesc, err := vt.getVirtualTableDesc(&pgProcTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_rewrite")
		}
		h := makeOidHasher()
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /*virtual tables have no constraints*/
		err = forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			db, sc, table, tableLookup := descCtx.database, descCtx.schema, descCtx.table, descCtx.tableLookup
			pgConstraintTableOid := tableOid(pgConstraintsDesc.GetID())
			pgClassTableOid := tableOid(pgClassDesc.GetID())
			pgRewriteTableOid := tableOid(pgRewriteDesc.GetID())
			if table.IsSequence() &&
				!table.GetSequenceOpts().SequenceOwner.Equal(descpb.TableDescriptor_SequenceOpts_SequenceOwner{}) {
				refObjID := tableOid(table.GetSequenceOpts().SequenceOwner.OwnerTableID)
				refObjSubID := tree.NewDInt(tree.DInt(table.GetSequenceOpts().SequenceOwner.OwnerColumnID))
				objID := tableOid(table.GetID())
				return addRow(
					pgClassTableOid, // classid
					objID,           // objid
					zeroVal,         // objsubid
					pgClassTableOid, // refclassid
					refObjID,        // refobjid
					refObjSubID,     // refobjsubid
					depTypeAuto,     // deptype
				)
			}

			// In the case of table/view relationship, In PostgreSQL pg_depend.objid refers to
			// pg_rewrite.oid, then pg_rewrite ev_class refers to the dependent object.
			reportViewDependency := func(dep *descpb.TableDescriptor_Reference) error {
				refObjOid := tableOid(table.GetID())
				objID := h.rewriteOid(table.GetID(), dep.ID)
				for _, colID := range dep.ColumnIDs {
					if err := addRow(
						pgRewriteTableOid,              // classid
						objID,                          // objid
						zeroVal,                        // objsubid
						pgClassTableOid,                // refclassid
						refObjOid,                      // refobjid
						tree.NewDInt(tree.DInt(colID)), // refobjsubid
						depTypeNormal,                  // deptype
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

			for _, fk := range table.OutboundForeignKeys() {
				// Foreign keys don't have a single linked index. Pick the first one
				// that matches on the referenced table.
				referencedTable, err := tableLookup.getTableByID(fk.GetReferencedTableID())
				if err != nil {
					return err
				}
				refObjID := oidZero
				if refConstraint, err := catalog.FindFKReferencedUniqueConstraint(referencedTable, fk); err != nil {
					// We couldn't find a unique constraint that matched. This shouldn't
					// happen.
					log.Warningf(ctx, "broken fk reference: %v", err)
				} else if idx := refConstraint.AsUniqueWithIndex(); idx != nil {
					refObjID = h.IndexOid(referencedTable.GetID(), idx.GetID())
				}
				constraintOid := h.ForeignKeyConstraintOid(db.GetID(), sc.GetID(), table.GetID(), fk)

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
		if err != nil {
			return err
		}
		return forEachSchema(ctx, p, dbContext, true, func(ctx context.Context, sc catalog.SchemaDescriptor) error {
			pgProcTableOid := tableOid(pgProcDesc.GetID())
			return sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
				funcDesc, err := p.Descriptors().ByIDWithoutLeased(p.txn).Get().Function(ctx, sig.ID)
				if err != nil {
					return err
				}
				sourceID := catid.FuncIDToOID(funcDesc.GetID())
				for _, otherFunction := range funcDesc.GetDependsOnFunctions() {
					destID := catid.FuncIDToOID(otherFunction)
					if err := addRow(
						pgProcTableOid,         // classid
						tree.NewDOid(sourceID), // objid
						zeroVal,                // objsubid
						pgProcTableOid,         // refclassid
						tree.NewDOid(destID),   // refobjid
						zeroVal,                // refobjsubid
						depTypeNormal,          // deptype
					); err != nil {
						return err
					}
				}
				return nil
			})
		})

	},
}

var pgCatalogDescriptionView = virtualSchemaView{
	// Note, the query uses `crdb_internal.kv_catalog_comments` without
	// a database prefix. This is intentional: this ensures
	// kv_catalog_comments only conains rows for the current database,
	// which is what pg_description expects.
	schema: vtable.PGCatalogDescription,
	resultColumns: colinfo.ResultColumns{
		{Name: "objoid", Typ: types.Oid},
		{Name: "classoid", Typ: types.Oid},
		{Name: "objsubid", Typ: types.Int4},
		{Name: "description", Typ: types.String},
	},
	comment: `object comments
https://www.postgresql.org/docs/9.5/catalog-pg-description.html`,
}

func getOIDFromConstraint(
	constraint catalog.Constraint, dbID descpb.ID, scID descpb.ID, tableDesc catalog.TableDescriptor,
) *tree.DOid {
	hasher := makeOidHasher()
	tableID := tableDesc.GetID()
	var oid *tree.DOid
	if ck := constraint.AsCheck(); ck != nil {
		oid = hasher.CheckConstraintOid(
			dbID,
			scID,
			tableID,
			ck,
		)
	} else if fk := constraint.AsForeignKey(); fk != nil {
		oid = hasher.ForeignKeyConstraintOid(
			dbID,
			scID,
			tableID,
			fk,
		)
	} else if uc := constraint.AsUniqueWithoutIndex(); uc != nil {
		oid = hasher.UniqueWithoutIndexConstraintOid(
			dbID,
			scID,
			tableID,
			uc,
		)
	} else if ic := constraint.AsUniqueWithIndex(); ic != nil {
		if ic.GetID() == tableDesc.GetPrimaryIndexID() {
			oid = hasher.PrimaryKeyConstraintOid(
				dbID,
				scID,
				tableID,
				ic,
			)
		} else {
			oid = hasher.UniqueConstraintOid(
				dbID,
				scID,
				tableID,
				ic,
			)
		}
	}
	return oid
}

// Database comments.
// https://www.postgresql.org/docs/9.5/catalog-pg-shdescription.html,
var pgCatalogSharedDescriptionView = virtualSchemaView{
	schema: vtable.PGCatalogSharedDescription,
	resultColumns: colinfo.ResultColumns{
		{Name: "objoid", Typ: types.Oid},
		{Name: "classoid", Typ: types.Oid},
		{Name: "description", Typ: types.String},
	},
	comment: `shared object comments
https://www.postgresql.org/docs/9.5/catalog-pg-shdescription.html`,
}

var pgCatalogEnumTable = virtualSchemaTable{
	comment: `enum types and labels (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-enum.html`,
	schema: vtable.PGCatalogEnum,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()

		return forEachTypeDesc(ctx, p, dbContext, func(ctx context.Context, _ catalog.DatabaseDescriptor, _ catalog.SchemaDescriptor, typDesc catalog.TypeDescriptor) error {
			e := typDesc.AsEnumTypeDescriptor()
			if e == nil {
				// We only want to iterate over ENUM types and multi-region enums.
				return nil
			}
			// Generate a row for each member of the enum. We don't represent enums
			// internally using floats for ordering like Postgres, so just pick a
			// float entry for the rows.
			typOID := tree.NewDOid(catid.TypeIDToOID(e.GetID()))
			for i := 0; i < e.NumEnumMembers(); i++ {
				if err := addRow(
					h.EnumEntryOid(typOID, e.GetMemberPhysicalRepresentation(i)),
					typOID,
					tree.NewDFloat(tree.DFloat(float64(i))),
					tree.NewDString(e.GetMemberLogicalRepresentation(i)),
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
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Event triggers are not currently supported.
		return nil
	},
	unimplemented: true,
}

var pgCatalogExtensionTable = virtualSchemaTable{
	comment: `installed extensions (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-extension.html`,
	schema: vtable.PGCatalogExtension,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Extensions are not supported.
		return nil
	},
	unimplemented: true,
}

var pgCatalogForeignDataWrapperTable = virtualSchemaTable{
	comment: `foreign data wrappers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-data-wrapper.html`,
	schema: vtable.PGCatalogForeignDataWrapper,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Foreign data wrappers are not supported.
		return nil
	},
	unimplemented: true,
}

var pgCatalogForeignServerTable = virtualSchemaTable{
	comment: `foreign servers (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-server.html`,
	schema: vtable.PGCatalogForeignServer,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Foreign servers are not supported.
		return nil
	},
	unimplemented: true,
}

var pgCatalogForeignTableTable = virtualSchemaTable{
	comment: `foreign tables (empty  - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-foreign-table.html`,
	schema: vtable.PGCatalogForeignTable,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Foreign tables are not supported.
		return nil
	},
	unimplemented: true,
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
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* virtual tables do not have indexes */
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				table := descCtx.table
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
					indoption := tree.NewDArray(types.Int2)

					colAttNums := make([]descpb.ColumnID, 0, index.NumKeyColumns())
					exprs := make([]string, 0, index.NumKeyColumns())
					for i, col := range table.IndexKeyColumns(index) {
						// The indkey for an expression element in an index
						// should be 0.
						if col.IsExpressionIndexColumn() {
							colAttNums = append(colAttNums, 0)
							formattedExpr, err := schemaexpr.FormatExprForDisplay(
								ctx, table, col.GetComputeExpr(), p.EvalContext(), p.SemaCtx(), p.SessionData(), tree.FmtPGCatalog,
							)
							if err != nil {
								return err
							}
							exprs = append(exprs, fmt.Sprintf("(%s)", formattedExpr))
						} else {
							colAttNums = append(colAttNums, descpb.ColumnID(col.GetPGAttributeNum()))
						}
						if err := collationOids.Append(typColl(col.GetType(), h)); err != nil {
							return err
						}
						// Currently, nulls always appear first if the order is ascending,
						// and always appear last if the order is descending.
						var thisIndOption tree.DInt
						if index.GetKeyColumnDirection(i) == catenumpb.IndexColumn_ASC {
							thisIndOption = indoptionNullsFirst
						} else {
							thisIndOption = indoptionDesc
						}
						if err := indoption.Append(tree.NewDInt(thisIndOption)); err != nil {
							return err
						}
					}
					// indnkeyatts is the number of attributes without INCLUDED columns.
					indnkeyatts := len(colAttNums)
					for i := 0; i < index.NumSecondaryStoredColumns(); i++ {
						col, err := catalog.MustFindColumnByID(table, index.GetStoredColumnID(i))
						if err != nil {
							return err
						}
						colAttNums = append(colAttNums, descpb.ColumnID(col.GetPGAttributeNum()))
					}
					// indnatts is the number of attributes with INCLUDED columns.
					indnatts := len(colAttNums)
					indkey, err := colIDArrayToVector(colAttNums)
					if err != nil {
						return err
					}
					collationOidVector := tree.NewDOidVectorFromDArray(collationOids)
					indoptionIntVector := tree.NewDIntVectorFromDArray(indoption)
					// TODO(bram): #27763 indclass still needs to be populated but it
					// requires pg_catalog.pg_opclass first.
					indclass, err := makeZeroedOidVector(indnkeyatts)
					if err != nil {
						return err
					}
					indpred := tree.DNull
					if index.IsPartial() {
						formattedPred, err := schemaexpr.FormatExprForDisplay(
							ctx, table, index.GetPredicate(), p.EvalContext(), p.SemaCtx(), p.SessionData(), tree.FmtPGCatalog,
						)
						if err != nil {
							return err
						}
						indpred = tree.NewDString(formattedPred)
					}
					indexprs := tree.DNull
					if len(exprs) > 0 {
						// The column contains multiple elements, but must be stored as a
						// string. Similar to Postgres, this is a list with one element for
						// each zero entry in indkey.
						arr := tree.NewDArray(types.String)
						for _, expr := range exprs {
							if err := arr.Append(tree.NewDString(expr)); err != nil {
								return err
							}
						}
						indexprs = tree.NewDString(tree.AsStringWithFlags(arr, tree.FmtPgwireText))
					}
					return addRow(
						h.IndexOid(table.GetID(), index.GetID()),     // indexrelid
						tableOid,                                     // indrelid
						tree.NewDInt(tree.DInt(indnatts)),            // indnatts
						tree.MakeDBool(tree.DBool(index.IsUnique())), // indisunique
						tree.DBoolFalse,                              // indnullsnotdistinct
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
						indexprs,                                     // indexprs
						indpred,                                      // indpred
						tree.NewDInt(tree.DInt(indnkeyatts)),         // indnkeyatts
					)
				})
			})
	},
}

var pgCatalogIndexesTable = virtualSchemaTable{
	comment: `index creation statements
https://www.postgresql.org/docs/9.5/view-pg-indexes.html`,
	schema: vtable.PGCatalogIndexes,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* virtual tables do not have indexes */
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				db, sc, table := descCtx.database, descCtx.schema, descCtx.table
				scNameName := tree.NewDName(sc.GetName())
				tblName := tree.NewDName(table.GetName())
				return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
					def, err := indexDefFromDescriptor(ctx, p, db, sc, table, index)
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
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	table catalog.TableDescriptor,
	index catalog.Index,
) (string, error) {
	tableName := tree.MakeTableNameWithSchema(tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(table.GetName()))
	partitionStr := ""
	fmtStr, err := catformat.IndexForDisplay(
		ctx,
		table,
		&tableName,
		index,
		partitionStr,
		tree.FmtPGCatalog,
		p.EvalContext(),
		p.SemaCtx(),
		p.SessionData(),
		catformat.IndexDisplayShowCreate,
	)
	if err != nil {
		return "", err
	}
	return fmtStr, nil
}

var pgCatalogInheritsTable = virtualSchemaTable{
	comment: `table inheritance hierarchy (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-inherits.html`,
	schema: vtable.PGCatalogInherits,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Table inheritance is not supported.
		return nil
	},
	unimplemented: true,
}

// Match the OIDs that Postgres uses for languages.
var languageInternalOid = tree.NewDOidWithTypeAndName(oid.Oid(12), types.Oid, "internal")
var languageSqlOid = tree.NewDOidWithTypeAndName(oid.Oid(14), types.Oid, "sql")
var languagePlpgsqlOid = tree.NewDOidWithTypeAndName(oid.Oid(14024), types.Oid, "plpgsql")

var pgCatalogLanguageTable = virtualSchemaTable{
	comment: `available languages
https://www.postgresql.org/docs/9.5/catalog-pg-language.html`,
	schema: vtable.PGCatalogLanguage,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		for _, lang := range []*tree.DOid{languageInternalOid, languageSqlOid, languagePlpgsqlOid} {
			isPl := tree.DBoolFalse
			if lang == languagePlpgsqlOid {
				isPl = tree.DBoolTrue
			}

			isTrusted := tree.DBoolFalse
			if lang == languagePlpgsqlOid || lang == languageSqlOid {
				isTrusted = tree.DBoolTrue
			}
			if err := addRow(
				lang,                                // oid
				tree.NewDString(lang.Name()),        // lanname
				h.UserOid(username.AdminRoleName()), // lanowner
				isPl,                                // lanispl
				isTrusted,                           // lanpltrusted
				tree.NewDOid(tree.UnknownOidValue),  // lanplcallfoid
				tree.NewDOid(tree.UnknownOidValue),  // laninline
				tree.NewDOid(tree.UnknownOidValue),  // lanvalidator
				tree.DNull,                          // lanacl
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogLocksTable = virtualSchemaTable{
	comment: `locks held by active processes (empty - feature does not exist)
https://www.postgresql.org/docs/9.6/view-pg-locks.html`,
	schema: vtable.PGCatalogLocks,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogMatViewsTable = virtualSchemaTable{
	comment: `available materialized views
https://www.postgresql.org/docs/9.6/view-pg-matviews.html`,
	schema: vtable.PGCatalogMatViews,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				sc, desc := descCtx.schema, descCtx.table
				if !desc.MaterializedView() {
					return nil
				}
				owner, err := getOwnerName(ctx, p, desc)
				if err != nil {
					return err
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
					tree.NewDName(sc.GetName()),   // schemaname
					tree.NewDName(desc.GetName()), // matviewname
					owner,                         // matviewowner
					tree.DNull,                    // tablespace
					tree.MakeDBool(len(desc.PublicNonPrimaryIndexes()) > 0), // hasindexes
					tree.DBoolTrue,                       // ispopulated,
					tree.NewDString(desc.GetViewQuery()), // definition
				)
			})
	},
}

var adminOID = makeOidHasher().UserOid(username.AdminRoleName())
var nodeOID = makeOidHasher().UserOid(username.NodeUserName())

var pgCatalogNamespaceTable = virtualSchemaTable{
	comment: `available namespaces
https://www.postgresql.org/docs/9.5/catalog-pg-namespace.html`,
	schema: vtable.PGCatalogNamespace,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				return forEachSchema(ctx, p, db, true /* requiresPrivileges */, func(ctx context.Context, sc catalog.SchemaDescriptor) error {
					ownerOID := tree.DNull
					if sc.SchemaKind() == catalog.SchemaUserDefined {
						var err error
						ownerOID, err = getOwnerOID(ctx, p, sc)
						if err != nil {
							return err
						}
					} else if sc.SchemaKind() == catalog.SchemaPublic {
						// admin is the owner of the public schema.
						ownerOID = adminOID
					} else if sc.SchemaKind() == catalog.SchemaVirtual {
						ownerOID = nodeOID
					}
					return addRow(
						schemaOid(sc.GetID()),         // oid
						tree.NewDString(sc.GetName()), // nspname
						ownerOID,                      // nspowner
						tree.DNull,                    // nspacl
					)
				})
			})
	},
	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, db catalog.DatabaseDescriptor,
				addRow func(...tree.Datum) error,
			) (bool, error) {
				coid := tree.MustBeDOid(unwrappedConstraint)
				ooid := coid.Oid
				sc, ok, err := func() (_ catalog.SchemaDescriptor, found bool, _ error) {
					// The system database still does not have a physical public schema.
					if !db.HasPublicSchemaWithDescriptor() && ooid == keys.SystemPublicSchemaID {
						return schemadesc.GetPublicSchema(), true, nil
					}
					if sc, ok := schemadesc.GetVirtualSchemaByID(descpb.ID(ooid)); ok {
						return sc, true, nil
					}
					if sc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Schema(ctx, descpb.ID(ooid)); err == nil {
						return sc, true, nil
					} else if !sqlerrors.IsUndefinedSchemaError(err) {
						return nil, false, err
					}
					// Fallback to looking for temporary schemas.
					var tempSchema catalog.SchemaDescriptor
					if err := forEachSchema(ctx, p, db, false /* requiresPrivileges */, func(ctx context.Context, schema catalog.SchemaDescriptor) error {
						if schema.GetID() != descpb.ID(ooid) {
							return nil
						}
						tempSchema = schema
						return iterutil.StopIteration()
					}); err != nil {
						return nil, false, err
					}
					if tempSchema != nil {
						return tempSchema, true, nil
					}
					return nil, false, nil
				}()
				if !ok || err != nil {
					return false, err
				}
				ownerOID := tree.DNull
				if sc.SchemaKind() == catalog.SchemaUserDefined {
					var err error
					ownerOID, err = getOwnerOID(ctx, p, sc)
					if err != nil {
						return false, err
					}
				} else if sc.SchemaKind() == catalog.SchemaPublic {
					// admin is the owner of the public schema.
					ownerOID = adminOID
				} else if sc.SchemaKind() == catalog.SchemaVirtual {
					ownerOID = nodeOID
				}
				if err := addRow(
					schemaOid(sc.GetID()),         // oid
					tree.NewDString(sc.GetName()), // nspname
					ownerOID,                      // nspowner
					tree.DNull,                    // nspacl
				); err != nil {
					return false, err
				}
				return true, nil
			},
		},
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
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogOperatorTable = virtualSchemaTable{
	comment: `operators (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-operator.html`,
	schema: vtable.PGCatalogOperator,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		nspOid := tree.NewDOid(catconstants.PgCatalogID)
		addOp := func(opName string, kind tree.Datum, params tree.TypeList, returnTyper tree.ReturnTyper) error {
			var leftType, rightType *tree.DOid
			switch params.Length() {
			case 1:
				leftType = oidZero
				rightType = tree.NewDOid(params.Types()[0].Oid())
			case 2:
				leftType = tree.NewDOid(params.Types()[0].Oid())
				rightType = tree.NewDOid(params.Types()[1].Oid())
			default:
				panic(errors.AssertionFailedf("unexpected operator %s with %d params",
					opName, params.Length()))
			}
			returnType := tree.NewDOid(returnTyper(nil).Oid())
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
			if cmpOp == treecmp.In {
				continue
			}
			if err := overloads.ForEachCmpOp(func(overload *tree.CmpOp) error {
				params, returnType := tree.GetParamsAndReturnType(overload)
				if err := addOp(cmpOp.String(), infixKind, params, returnType); err != nil {
					return err
				}
				if inverse, ok := tree.CmpOpInverse(cmpOp); ok {
					if err := addOp(inverse.String(), infixKind, params, returnType); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		for binOp, overloads := range tree.BinOps {
			if err := overloads.ForEachBinOp(func(overload *tree.BinOp) error {
				params, returnType := tree.GetParamsAndReturnType(overload)
				return addOp(binOp.String(), infixKind, params, returnType)
			}); err != nil {
				return err
			}
		}
		for unaryOp, overloads := range tree.UnaryOps {
			if err := overloads.ForEachUnaryOp(func(overload *tree.UnaryOp) error {
				params, returnType := tree.GetParamsAndReturnType(overload)
				return addOp(unaryOp.String(), prefixKind, params, returnType)
			}); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogPreparedXactsTable = virtualSchemaTable{
	comment: `prepared transactions
https://www.postgresql.org/docs/9.6/view-pg-prepared-xacts.html`,
	schema: vtable.PGCatalogPreparedXacts,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if !p.IsActive(ctx, clusterversion.V25_1_PreparedTransactionsTable) {
			// TODO(nvanbenschoten): Remove this logic when mixed-version support
			// with v24.3 is no longer necessary.
			return nil
		}
		rows, err := p.InternalSQLTxn().QueryBufferedEx(
			ctx,
			"select-prepared-transactions",
			p.Txn(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT global_id, prepared, owner, database FROM system.prepared_transactions`,
		)
		if err != nil {
			return err
		}
		for _, row := range rows {
			// NOTE: we can't map a 128-bit CockroachDB transaction ID to a 32-bit
			// Postgres xid, so we just return zero for each transaction's xid. This
			// is acceptable, as the gid is the important part of pg_prepared_xacts.
			transaction := zeroVal
			globalID := row[0]
			prepared := row[1]
			owner := tree.NewDName(string(tree.MustBeDString(row[2])))
			database := tree.NewDName(string(tree.MustBeDString(row[3])))
			if err := addRow(
				transaction, // transaction
				globalID,    // gid
				prepared,    // prepared
				owner,       // owner
				database,    // database
			); err != nil {
				return err
			}
		}
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
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for name, stmt := range p.preparedStatements.List() {
			placeholderTypes := stmt.PrepareMetadata.PlaceholderTypesInfo.Types
			paramTypes := tree.NewDArray(types.RegType)
			paramTypes.Array = make(tree.Datums, len(placeholderTypes))
			paramNames := make([]string, len(placeholderTypes))

			for i, placeholderType := range placeholderTypes {
				paramTypes.Array[i] = tree.NewDOidWithTypeAndName(
					placeholderType.Oid(),
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

var (
	proKindFunction  = tree.NewDString("f")
	proKindAggregate = tree.NewDString("a")
	proKindWindow    = tree.NewDString("w")
	proKindProcedure = tree.NewDString("p")
)

func addPgProcBuiltinRow(name string, addRow func(...tree.Datum) error) error {
	_, overloads := builtinsregistry.GetBuiltinProperties(name)
	nspOid := tree.NewDOid(catconstants.PgCatalogID)
	const crdbInternal = catconstants.CRDBInternalSchemaName + "."
	const infoSchema = catconstants.InformationSchemaName + "."
	if strings.HasPrefix(name, crdbInternal) {
		nspOid = tree.NewDOid(catconstants.CrdbInternalID)
		name = name[len(crdbInternal):]
	} else if strings.HasPrefix(name, infoSchema) {
		nspOid = tree.NewDOid(catconstants.InformationSchemaID)
		name = name[len(infoSchema):]
	}

	for _, builtin := range overloads {
		dName := tree.NewDName(name)
		dSrc := tree.NewDString(name)

		var kind tree.Datum
		switch {
		case builtin.Class == tree.AggregateClass:
			kind = proKindAggregate
		case builtin.Class == tree.WindowClass:
			kind = proKindWindow
		default:
			kind = proKindFunction
		}

		var retType tree.Datum
		isRetSet := builtin.IsGenerator()
		if fixedRetType := builtin.FixedReturnType(); fixedRetType != nil {
			var retOid oid.Oid
			if fixedRetType.Family() == types.TupleFamily && builtin.IsGenerator() {
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
			retType = tree.NewDOid(retOid)
		}

		argTypes := builtin.Types
		dArgTypes := tree.NewDArray(types.Oid)
		for _, argType := range argTypes.Types() {
			if err := dArgTypes.Append(tree.NewDOid(argType.Oid())); err != nil {
				return err
			}
		}

		getVariadicStringArray := func() tree.Datum {
			return &tree.DArray{ParamTyp: types.String, Array: tree.Datums{proArgModeVariadic}}
		}

		var argmodes tree.Datum
		var variadicType tree.Datum
		switch v := argTypes.(type) {
		case tree.VariadicType:
			if len(v.FixedTypes) == 0 {
				argmodes = getVariadicStringArray()
			} else {
				ary := tree.NewDArray(types.String)
				for range v.FixedTypes {
					if err := ary.Append(proArgModeIn); err != nil {
						return err
					}
				}
				if err := ary.Append(proArgModeVariadic); err != nil {
					return err
				}
				argmodes = ary
			}
			variadicType = tree.NewDOid(v.VarType.Oid())
		case tree.HomogeneousType:
			argmodes = getVariadicStringArray()
			variadicType = tree.NewDOid(types.AnyElement.Oid())
		default:
			argmodes = tree.DNull
			variadicType = oidZero
		}
		provolatile, proleakproof := builtin.Volatility.ToPostgres()
		proisstrict := !builtin.CalledOnNullInput

		err := addRow(
			tree.NewDOid(builtin.Oid),                // oid
			dName,                                    // proname
			nspOid,                                   // pronamespace
			tree.DNull,                               // proowner
			languageInternalOid,                      // prolang
			tree.DNull,                               // procost
			tree.DNull,                               // prorows
			variadicType,                             // provariadic
			tree.DNull,                               // prosupport
			kind,                                     // prokind
			tree.DBoolFalse,                          // prosecdef
			tree.MakeDBool(tree.DBool(proleakproof)), // proleakproof
			tree.MakeDBool(tree.DBool(proisstrict)),  // proisstrict
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
			tree.DNull,                                      // prosqlbody
			tree.DNull,                                      // proconfig
			tree.DNull,                                      // proacl
		)
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	proArgModeIn       = tree.NewDString("i")
	proArgModeOut      = tree.NewDString("o")
	proArgModeInOut    = tree.NewDString("b")
	proArgModeVariadic = tree.NewDString("v")
)

func addPgProcUDFRow(
	h oidHasher,
	scDesc catalog.SchemaDescriptor,
	fnDesc catalog.FunctionDescriptor,
	addRow func(...tree.Datum) error,
) error {
	kind := proKindFunction
	if fnDesc.IsProcedure() {
		kind = proKindProcedure
	}

	lang := languageInternalOid
	if fnDesc.GetLanguage() == catpb.Function_PLPGSQL {
		lang = languagePlpgsqlOid
	} else if fnDesc.GetLanguage() == catpb.Function_SQL {
		lang = languageSqlOid
	}

	argTypes, allArgTypesArray := tree.NewDArray(types.Oid), tree.NewDArray(types.Oid)
	argModesArray, argNamesArray := tree.NewDArray(types.String), tree.NewDArray(types.String)
	onlyINArgs := true
	var foundAnyArgNames bool
	var nArgs, nArgDefaults int
	var argDefaultsBuilder strings.Builder
	for _, param := range fnDesc.GetParams() {
		class := funcdesc.ToTreeRoutineParamClass(param.Class)
		if tree.IsInParamClass(class) {
			// nArgs tracks only the number of input arguments.
			nArgs++
			// argTypes only includes input arguments.
			if err := argTypes.Append(tree.NewDOid(param.Type.Oid())); err != nil {
				return err
			}
		}
		if param.DefaultExpr != nil {
			nArgDefaults++
			if nArgDefaults > 1 {
				argDefaultsBuilder.WriteString(", ")
			}
			// Postgres has a special type pg_node_tree for proargdefaults
			// column where the values are of the form:
			//  ({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 55 :constvalue 4 [ 2 0 0 0 0 0 0 0 ]})
			// We make our string roughly resemble that format.
			argDefaultsBuilder.WriteString("{")
			argDefaultsBuilder.WriteString(*param.DefaultExpr)
			argDefaultsBuilder.WriteString("}")
		}
		// allArgTypesArray includes all arguments.
		if err := allArgTypesArray.Append(tree.NewDOid(param.Type.Oid())); err != nil {
			return err
		}
		onlyINArgs = onlyINArgs && (class == tree.RoutineParamDefault || class == tree.RoutineParamIn)
		var argMode tree.Datum
		switch class {
		case tree.RoutineParamDefault, tree.RoutineParamIn:
			argMode = proArgModeIn
		case tree.RoutineParamOut:
			argMode = proArgModeOut
		case tree.RoutineParamInOut:
			argMode = proArgModeInOut
		case tree.RoutineParamVariadic:
			argMode = proArgModeVariadic
		default:
			return errors.AssertionFailedf("unknown parameter class %d", class)
		}
		if err := argModesArray.Append(argMode); err != nil {
			return err
		}
		foundAnyArgNames = foundAnyArgNames || len(param.Name) > 0
		if err := argNamesArray.Append(tree.NewDString(param.Name)); err != nil {
			return err
		}
	}
	allArgTypes, argModes := tree.DNull, tree.DNull
	if !onlyINArgs {
		// When all arguments are IN arguments, then proallargtypes and
		// proargmodes are NULL.
		allArgTypes = allArgTypesArray
		argModes = argModesArray
	}
	argNames := tree.DNull
	if foundAnyArgNames {
		// If none of the arguments have a name, then proargnames is NULL.
		argNames = argNamesArray
	}
	argDefaults := tree.DNull
	if nArgDefaults > 0 {
		argDefaults = tree.NewDString("(" + argDefaultsBuilder.String() + ")")
	}
	return addRow(
		tree.NewDOid(catid.FuncIDToOID(fnDesc.GetID())), // oid
		tree.NewDName(fnDesc.GetName()),                 // proname
		schemaOid(scDesc.GetID()),                       // pronamespace
		h.UserOid(fnDesc.GetPrivileges().Owner()),       // proowner
		lang,            // prolang
		tree.DNull,      // procost
		tree.DNull,      // prorows
		oidZero,         // provariadic // TODO(88947): this might need an adjustment.
		tree.DNull,      // prosupport
		kind,            // prokind
		tree.DBoolFalse, // prosecdef
		tree.MakeDBool(tree.DBool(fnDesc.GetLeakProof())),                                    // proleakproof
		tree.MakeDBool(fnDesc.GetNullInputBehavior() != catpb.Function_CALLED_ON_NULL_INPUT), // proisstrict
		tree.MakeDBool(tree.DBool(fnDesc.GetReturnType().ReturnSet)),                         // proretset
		tree.NewDString(funcVolatility(fnDesc.GetVolatility())),                              // provolatile
		tree.DNull,                                      // proparallel
		tree.NewDInt(tree.DInt(nArgs)),                  // pronargs
		tree.NewDInt(tree.DInt(nArgDefaults)),           // pronargdefaults
		tree.NewDOid(fnDesc.GetReturnType().Type.Oid()), // prorettype
		tree.NewDOidVectorFromDArray(argTypes),          // proargtypes
		allArgTypes,                                     // proallargtypes
		argModes,                                        // proargmodes
		argNames,                                        // proargnames
		argDefaults,                                     // proargdefaults
		tree.DNull,                                      // protrftypes
		tree.NewDString(fnDesc.GetFunctionBody()),       // prosrc
		tree.DNull,                                      // probin
		tree.DNull,                                      // prosqlbody
		tree.DNull,                                      // proconfig
		tree.DNull,                                      // proacl
	)
}

var pgCatalogProcTable = virtualSchemaTable{
	comment: `built-in functions (incomplete)
https://www.postgresql.org/docs/16/catalog-pg-proc.html`,
	schema: vtable.PGCatalogProc,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		// Build rows for builtin function. Normally, dbContext is not nil. So only
		// dbContext is looked at and used to generate the NamespaceOid. However,
		// the downside is that the NamespaceOid would change if pg_catalog.pg_proc
		// is selected from a different database. But this is probably fine for
		// builtin function since they don't really belong to any database.

		err := forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				for _, name := range builtins.AllBuiltinNames() {
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
					err := addPgProcBuiltinRow(name, addRow)
					if err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil {
			return err
		}
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(ctx context.Context, dbDesc catalog.DatabaseDescriptor) error {
				return forEachSchema(ctx, p, dbDesc, true /* requiresPrivileges */, func(ctx context.Context, scDesc catalog.SchemaDescriptor) error {
					return scDesc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
						fnDesc, err := p.Descriptors().ByIDWithoutLeased(p.Txn()).WithoutNonPublic().Get().Function(ctx, sig.ID)
						if err != nil {
							return err
						}

						return addPgProcUDFRow(h, scDesc, fnDesc, addRow)
					})
				})
			})
	},
	indexes: []virtualIndex{
		{
			incomplete: false,
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, dbContext catalog.DatabaseDescriptor,
				addRow func(...tree.Datum) error) (bool, error) {
				h := makeOidHasher()
				coid := tree.MustBeDOid(unwrappedConstraint)
				ooid := coid.Oid

				if funcdesc.IsOIDUserDefinedFunc(ooid) {
					fnDesc, err := p.Descriptors().ByIDWithoutLeased(p.Txn()).WithoutNonPublic().Get().Function(ctx, funcdesc.UserDefinedFunctionOIDToID(ooid))
					if err != nil {
						if errors.Is(err, tree.ErrRoutineUndefined) {
							return false, nil //nolint:returnerrcheck
						}
						return false, err
					}

					scDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Schema(ctx, fnDesc.GetParentSchemaID())
					if err != nil {
						return false, err
					}
					if fnDesc.Dropped() || fnDesc.GetParentID() != dbContext.GetID() {
						return false, nil
					}

					err = addPgProcUDFRow(h, scDesc, fnDesc, addRow)
					if err != nil {
						return false, err
					}
					return true, nil

				} else {
					name, _, err := p.ResolveFunctionByOID(ctx, ooid)
					if err != nil {
						if errors.Is(err, tree.ErrRoutineUndefined) {
							return false, nil //nolint:returnerrcheck
						}
						return false, err
					}

					err = addPgProcBuiltinRow(name.Object(), addRow)
					if err != nil {
						return false, err
					}
					return true, nil
				}
			},
		},
	},
}

var pgCatalogRangeTable = virtualSchemaTable{
	comment: `range types (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-range.html`,
	schema: vtable.PGCatalogRange,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// We currently do not support any range types, so this table is empty.
		// This table should be populated when any range types are added to
		// oidToDatum (and therefore pg_type).
		return nil
	},
	unimplemented: true,
}

var pgCatalogRewriteTable = virtualSchemaTable{
	comment: `rewrite rules (only for referencing on pg_depend for table-view dependencies)
https://www.postgresql.org/docs/9.5/catalog-pg-rewrite.html`,
	schema: vtable.PGCatalogRewrite,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		ruleName := tree.NewDString("_RETURN")
		evType := tree.NewDString(string(evTypeSelect))
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /*virtual tables have no constraints*/
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			table := descCtx.table
			if !table.IsTable() && !table.IsView() {
				return nil
			}

			return table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
				rewriteOid := h.rewriteOid(table.GetID(), dep.ID)
				evClass := tableOid(dep.ID)
				return addRow(
					rewriteOid,     // oid
					ruleName,       // rulename
					evClass,        // ev_class
					evType,         // ev_type
					tree.DNull,     // ev_enabled
					tree.DBoolTrue, // is_instead
					tree.DNull,     // ev_qual
					tree.DNull,     // ev_action
				)
			})
		})
	},
}

var pgCatalogRolesTable = virtualSchemaTable{
	comment: `database roles
https://www.postgresql.org/docs/9.5/view-pg-roles.html`,
	schema: vtable.PGCatalogRoles,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// We intentionally do not check if the user has access to system.user.
		// Because Postgres allows access to pg_roles by non-privileged users, we
		// need to do the same. This shouldn't be an issue, because pg_roles doesn't
		// include sensitive information such as password hashes.
		h := makeOidHasher()
		return forEachRoleAtCacheReadTS(ctx, p,
			func(ctx context.Context, userName username.SQLUsername, isRole bool, options roleOptions, settings tree.Datum) error {
				isRoot := tree.DBool(userName.IsRootUser() || userName.IsAdminRole())
				// Currently, all users and roles inherit the privileges of roles they are
				// members of. See https://github.com/cockroachdb/cockroach/issues/69583.
				roleInherits := tree.DBool(true)
				noLogin, err := options.noLogin()
				if err != nil {
					return err
				}
				roleCanLogin := isRoot || !noLogin
				createDB, err := options.createDB()
				if err != nil {
					return err
				}
				rolValidUntil, err := options.validUntil(p)
				if err != nil {
					return err
				}
				createRole, err := options.createRole()
				if err != nil {
					return err
				}
				isSuper, err := userIsSuper(ctx, p, userName)
				if err != nil {
					return err
				}

				return addRow(
					h.UserOid(userName),                   // oid
					tree.NewDName(userName.Normalized()),  // rolname
					tree.MakeDBool(isRoot || isSuper),     // rolsuper
					tree.MakeDBool(roleInherits),          // rolinherit
					tree.MakeDBool(isSuper || createRole), // rolcreaterole
					tree.MakeDBool(isSuper || createDB),   // rolcreatedb
					tree.DBoolFalse,                       // rolcatupdate
					tree.MakeDBool(roleCanLogin),          // rolcanlogin.
					tree.DBoolFalse,                       // rolreplication
					negOneVal,                             // rolconnlimit
					passwdStarString,                      // rolpassword
					rolValidUntil,                         // rolvaliduntil
					tree.DBoolFalse,                       // rolbypassrls
					settings,                              // rolconfig
				)
			})
	},
}

var pgCatalogSecLabelsTable = virtualSchemaTable{
	comment: `security labels (empty)
https://www.postgresql.org/docs/9.6/view-pg-seclabels.html`,
	schema: vtable.PGCatalogSecLabels,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogSequenceTable = virtualSchemaTable{
	comment: `sequences (see also information_schema.sequences)
https://www.postgresql.org/docs/9.5/catalog-pg-sequence.html`,
	schema: vtable.PGCatalogSequence,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* virtual schemas do not have indexes */
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				table := descCtx.table
				if !table.IsSequence() {
					return nil
				}
				opts := table.GetSequenceOpts()
				return addRow(
					tableOid(table.GetID()),                 // seqrelid
					tree.NewDOid(oid.T_int8),                // seqtypid
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
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			if gen.Hidden {
				continue
			}
			value, err := gen.Get(&p.extendedEvalCtx, p.Txn())
			if err != nil {
				return err
			}
			valueDatum := tree.NewDString(value)
			var bootDatum tree.Datum = tree.DNull
			var resetDatum tree.Datum = tree.DNull
			if gen.Set == nil && gen.RuntimeSet == nil && gen.SetWithPlanner == nil {
				// RESET/SET will leave the variable unchanged. Announce the
				// current value as boot/reset value.
				bootDatum = valueDatum
				resetDatum = bootDatum
			} else {
				if gen.GlobalDefault != nil {
					globalDefVal := gen.GlobalDefault(&p.EvalContext().Settings.SV)
					bootDatum = tree.NewDString(globalDefVal)
				}
				if hasDefault, defVal := getSessionVarDefaultString(
					vName,
					gen,
					p.sessionDataMutatorIterator.sessionDataMutatorBase,
				); hasDefault {
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
	comment: `Shared Dependencies (Roles depending on objects). 
https://www.postgresql.org/docs/9.6/catalog-pg-shdepend.html`,
	schema: vtable.PGCatalogShdepend,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		vt := p.getVirtualTabler()
		h := makeOidHasher()

		pgClassDesc, err := vt.getVirtualTableDesc(&pgClassTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_class")
		}
		pgClassOid := tableOid(pgClassDesc.GetID())

		pgAuthIDDesc, err := vt.getVirtualTableDesc(&pgAuthIDTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_authid")
		}
		pgAuthIDOid := tableOid(pgAuthIDDesc.GetID())

		pgDatabaseDesc, err := vt.getVirtualTableDesc(&pgDatabaseTableName, p)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_database")
		}
		pgDatabaseOid := tableOid(pgDatabaseDesc.GetID())

		// There is no dependent object for pinned roles; In postgres this type of
		// entry is a signal that the system itself depends on the referenced
		// object, and so that object must never be deleted. The columns for the
		// dependent object contain zeroes.
		pinnedRoles := map[string]username.SQLUsername{
			username.RootUser:  username.RootUserName(),
			username.AdminRole: username.AdminRoleName(),
		}

		// Function commonly used to add table and database dependencies.
		addSharedDependency := func(
			dbID *tree.DOid, classID *tree.DOid, objID *tree.DOid, refClassID *tree.DOid, user, owner username.SQLUsername,
		) error {
			// As stated above, where pinned roles is declared: pinned roles
			// does not have dependent objects.
			if _, ok := pinnedRoles[user.Normalized()]; ok {
				return nil
			}

			depType := sharedDependencyACL
			if owner.Normalized() == user.Normalized() {
				depType = sharedDependencyOwner
			}

			return addRow(
				dbID,                             // dbid
				classID,                          // classid
				objID,                            // objid
				zeroVal,                          // objsubid
				refClassID,                       // refclassid
				h.UserOid(user),                  // refobjid
				tree.NewDString(string(depType)), // deptype
			)
		}

		// Populating table descriptor dependencies with roles
		opts := forEachTableDescOptions{virtualOpts: virtualMany}
		if err = forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				db, table := descCtx.database, descCtx.table
				privDesc, err := p.getPrivilegeDescriptor(ctx, table)
				if err != nil {
					return err
				}
				owner := privDesc.Owner()
				showPrivs, err := privDesc.Show(privilege.Table, true /* showImplicitOwnerPrivs */)
				if err != nil {
					return err
				}
				for _, u := range showPrivs {
					if err := addSharedDependency(
						dbOid(db.GetID()),       // dbid
						pgClassOid,              // classid
						tableOid(table.GetID()), // objid
						pgAuthIDOid,             // refclassid
						u.User,                  // refobjid
						owner,
					); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			return err
		}

		// Databases dependencies with roles
		if err = forEachDatabaseDesc(ctx, p, nil /*all databases*/, false, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				owner := db.GetPrivileges().Owner()
				showPrivs, err := db.GetPrivileges().Show(privilege.Database, true /* showImplicitOwnerPrivs */)
				if err != nil {
					return err
				}
				for _, u := range showPrivs {
					if err := addSharedDependency(
						tree.NewDOid(0),   // dbid
						pgDatabaseOid,     // classid
						dbOid(db.GetID()), // objid
						pgAuthIDOid,       // refclassid
						u.User,            // refobjid
						owner,
					); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			return err
		}

		// Pinned roles, as stated above, pinned roles only have rows with zeros.
		for _, role := range pinnedRoles {
			if err := addRow(
				tree.NewDOid(0), // dbid
				tree.NewDOid(0), // classid
				tree.NewDOid(0), // objid
				zeroVal,         // objsubid
				pgAuthIDOid,     // refclassid
				h.UserOid(role), // refobjid
				tree.NewDString(string(sharedDependencyPin)), // deptype
			); err != nil {
				return err
			}
		}

		return nil
	},
}

var pgCatalogTablesTable = virtualSchemaTable{
	comment: `tables summary (see also information_schema.tables, pg_catalog.pg_class)
https://www.postgresql.org/docs/9.5/view-pg-tables.html`,
	schema: vtable.PGCatalogTables,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Note: pg_catalog.pg_tables is not well-defined if the dbContext is
		// empty -- listing tables across databases can yield duplicate
		// schema/table names.
		opts := forEachTableDescOptions{virtualOpts: virtualMany}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				sc, table := descCtx.schema, descCtx.table
				if !table.IsTable() {
					return nil
				}
				owner, err := getOwnerName(ctx, p, table)
				if err != nil {
					return err
				}
				return addRow(
					tree.NewDName(sc.GetName()),    // schemaname
					tree.NewDName(table.GetName()), // tablename
					owner,                          // tableowner
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
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Triggers are unsupported.
		return nil
	},
	unimplemented: true,
}

var (
	typTypeBase      = tree.NewDString("b")
	typTypeComposite = tree.NewDString("c")
	typTypeDomain    = tree.NewDString("d")
	typTypeEnum      = tree.NewDString("e")
	typTypePseudo    = tree.NewDString("p")
	typTypeRange     = tree.NewDString("r")

	// Avoid unused warning for constants.
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
	_ = typCategoryEnum
	_ = typCategoryGeometric
	_ = typCategoryRange
	_ = typCategoryBitString

	commaTypDelim = tree.NewDString(",")
)

func addPGTypeRowForTable(
	ctx context.Context,
	p *planner,
	h oidHasher,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	table catalog.TableDescriptor,
	addRow func(...tree.Datum) error,
) error {
	nspOid := schemaOid(sc.GetID())
	ownerOID, err := getOwnerOID(ctx, p, table)
	if err != nil {
		return err
	}
	implicitTypOid := typedesc.TableIDToImplicitTypeOID(table.GetID())
	return addRow(
		tree.NewDOid(implicitTypOid),   // oid
		tree.NewDName(table.GetName()), // typname
		nspOid,                         // typnamespace
		ownerOID,                       // typowner
		negOneVal,                      // typlen
		tree.DBoolFalse,                // typbyval (is it fixedlen or not)
		typTypeComposite,               // typtype
		typCategoryComposite,           // typcategory
		tree.DBoolFalse,                // typispreferred
		tree.DBoolTrue,                 // typisdefined
		commaTypDelim,                  // typdelim
		tableOid(table.GetID()),        // typrelid
		oidZero,                        // typelem
		// NOTE: we do not add the array type or OID here.
		// We unfortunately do not reserve a descriptor ID for an array of the given class,
		// and there is no safe "range" to reserve for arrays left.
		oidZero, // typarray

		h.RegProc("record_in"),   // typinput
		h.RegProc("record_out"),  // typoutput
		h.RegProc("record_recv"), // typreceive
		h.RegProc("record_send"), // typsend
		oidZero,                  // typmodin
		oidZero,                  // typmodout
		oidZero,                  // typanalyze

		tree.DNull,      // typalign
		tree.DNull,      // typstorage
		tree.DBoolFalse, // typnotnull
		oidZero,         // typbasetype
		negOneVal,       // typtypmod
		zeroVal,         // typndims
		oidZero,         // typcollation
		tree.DNull,      // typdefaultbin
		tree.DNull,      // typdefault
		tree.DNull,      // typacl
	)
}

func addPGTypeRow(
	h oidHasher,
	nspOid tree.Datum,
	owner tree.Datum,
	typ *types.T,
	isUDT bool,
	addRow func(...tree.Datum) error,
) error {
	cat := typCategory(typ)
	typType := typTypeBase
	typElem := oidZero
	typArray := oidZero
	builtinPrefix := builtins.PGIOBuiltinPrefix(typ)
	typrelid := oidZero
	switch typ.Family() {
	case types.ArrayFamily:
		switch typ.Oid() {
		case oid.T_int2vector:
			// IntVector needs a special case because it's a special snowflake
			// type that behaves in some ways like a scalar type and in others
			// like an array type.
			typElem = tree.NewDOid(oid.T_int2)
			typArray = tree.NewDOid(oid.T__int2vector)
		case oid.T_oidvector:
			// Same story as above for OidVector.
			typElem = tree.NewDOid(oid.T_oid)
			typArray = tree.NewDOid(oid.T__oidvector)
		case oid.T_anyarray:
			// AnyArray does not use a prefix or element type.
		default:
			builtinPrefix = "array_"
			typElem = tree.NewDOid(typ.ArrayContents().Oid())
		}
	case types.EnumFamily:
		builtinPrefix = "enum_"
		typType = typTypeEnum
		typArray = tree.NewDOid(types.CalcArrayOid(typ))
	case types.TupleFamily:
		builtinPrefix = "record_"
		typType = typTypeComposite
		typArray = tree.NewDOid(types.CalcArrayOid(typ))
		// Predefined composite types still have a typrelid of 0.
		if isUDT {
			typrelid = tree.NewDOid(typ.Oid())
		}
	case types.VoidFamily:
		// void does not have an array type.
	case types.TriggerFamily:
		// trigger does not have an array type.
	default:
		typArray = tree.NewDOid(types.CalcArrayOid(typ))
	}
	if cat == typCategoryPseudo {
		typType = typTypePseudo
	}
	typname := typ.PGName()
	typDelim := tree.NewDString(typ.Delimiter())
	return addRow(
		tree.NewDOid(typ.Oid()), // oid
		tree.NewDName(typname),  // typname
		nspOid,                  // typnamespace
		owner,                   // typowner
		typLen(typ),             // typlen
		typByVal(typ),           // typbyval (is it fixedlen or not)
		typType,                 // typtype
		cat,                     // typcategory
		tree.DBoolFalse,         // typispreferred
		tree.DBoolTrue,          // typisdefined
		typDelim,                // typdelim
		typrelid,                // typrelid
		typElem,                 // typelem
		typArray,                // typarray

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

// addPGClassRowForCompositeType is utilized to populate rows in the pg_class table; it will
// add a row iff the type we are looking at is a composite type (it does not add rows for enum types).
func addPGClassRowForCompositeType(
	h oidHasher, nspOid tree.Datum, owner tree.Datum, typ *types.T, addRow func(...tree.Datum) error,
) error {
	var typType *tree.DString
	var tupLen int

	switch typ.Family() {
	case types.TupleFamily:
		typType = typTypeComposite

		tupLen = len(typ.TupleContents())
	default:
		return nil
	}
	typname := typ.PGName()
	return addRow(
		tree.NewDOid(typ.Oid()),         // oid
		tree.NewDName(typname),          // relname
		nspOid,                          // relnamespace
		tree.NewDOid(typ.Oid()),         // reltype
		oidZero,                         // reloftype (used for type tables, which us unsupported)
		owner,                           // relowner
		oidZero,                         // relam
		oidZero,                         // refilenode
		oidZero,                         // reltablespace
		tree.DNull,                      // relpages
		tree.DNull,                      // reltuples
		zeroVal,                         // relallvisible
		oidZero,                         // reltoastrelid
		tree.DBoolFalse,                 // relhasindex (composite types implemented as virtual tables - no indexes)
		tree.DBoolFalse,                 // relisshared
		relPersistencePermanent,         // relpersistance
		tree.DBoolFalse,                 // relistemp
		typType,                         // relkind
		tree.NewDInt(tree.DInt(tupLen)), // relnatts
		zeroVal,                         // relchecks
		tree.DBoolFalse,                 // relhasoids
		tree.DBoolFalse,                 // relhaspkey
		tree.DBoolFalse,                 // relhasrules
		tree.DBoolFalse,                 // relhastriggers
		tree.DBoolFalse,                 // relhassubclass
		zeroVal,                         // relfrozenxid
		tree.DNull,                      // relacl
		tree.DNull,                      // reloptions
		tree.DBoolFalse,                 // relforcerowsecurity
		tree.DNull,                      // relispartition
		tree.DNull,                      // relispopulated
		tree.NewDString("n"),            // relreplident (compositite types are views)
		tree.DNull,                      // relrewrite
		tree.DBoolFalse,                 // relrowsecurity
		tree.DNull,                      // relpartbound
		tree.DNull,                      // relminmxid
	)
}

// addPGAttributeRowForCompositeType is utilized to populate rows in the pg_attribute table; it will
// add a row (per type in list) iff the type we are looking at is a composite type (it does not add rows for enum types).
func addPGAttributeRowForCompositeType(
	h oidHasher, nspOid tree.Datum, owner tree.Datum, typ *types.T, addRow func(...tree.Datum) error,
) error {
	var tupLabels []string
	var tupContents []*types.T

	switch typ.Family() {
	case types.TupleFamily:
		tupLabels = typ.TupleLabels()
		tupContents = typ.TupleContents()
	default:
		return nil
	}

	for i, colTyp := range tupContents {
		if err := addRow(
			tree.NewDOid(typ.Oid()),      // attrelid
			tree.NewDName(tupLabels[i]),  // attname
			typOid(colTyp),               // atttypid
			zeroVal,                      // attstattarget
			typLen(colTyp),               // attlen
			tree.NewDInt(tree.DInt(i+1)), // attnum
			zeroVal,                      // attndims
			negOneVal,                    // attcacheoff
			tree.NewDInt(tree.DInt(colTyp.TypeModifier())), // atttypmod
			tree.DNull,          // attbyval (see pg_type.typbyval)
			tree.DNull,          // attstorage
			tree.DNull,          // attalign
			tree.DBoolFalse,     // attnotnull
			tree.DBoolFalse,     // atthasdef
			tree.NewDString(""), // attidentity
			tree.NewDString(""), // attgenerated
			tree.DBoolFalse,     // attisdropped
			tree.DBoolTrue,      // attislocal
			zeroVal,             // attinhcount
			typColl(colTyp, h),  // attcollation
			tree.DNull,          // attacl
			tree.DNull,          // attoptions
			tree.DNull,          // attfdwoptions
			// These columns were automatically created by pg_catalog_test's missing column generator.
			tree.DNull, // atthasmissing
			// These columns were automatically created by pg_catalog_test's missing column generator.
			tree.DNull,      // attmissingval
			tree.DBoolFalse, // attishidden
		); err != nil {
			return err
		}
	}
	return nil
}

func getSchemaAndTypeByTypeID(
	ctx context.Context, p *planner, id descpb.ID,
) (catalog.SchemaDescriptor, catalog.TypeDescriptor, error) {
	typDesc, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Type(ctx, id)
	if err != nil {
		// If the type was not found, it may be a table.
		if !(errors.Is(err, catalog.ErrDescriptorNotFound) || pgerror.GetPGCode(err) == pgcode.UndefinedObject) {
			return nil, nil, err
		}
		return nil, nil, nil
	}

	sc, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Schema(ctx, typDesc.GetParentSchemaID())
	if err != nil {
		return nil, nil, err
	}
	return sc, typDesc, nil
}

var pgCatalogTypeTable = virtualSchemaTable{
	comment: `scalar types (incomplete)
https://www.postgresql.org/docs/9.5/catalog-pg-type.html`,
	schema: vtable.PGCatalogType,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				nspOid := tree.NewDOid(catconstants.PgCatalogID)

				// Generate rows for all predefined types.
				for _, typ := range types.OidToType {
					if err := addPGTypeRow(h, nspOid, tree.DNull /* owner */, typ, false /* isUDT */, addRow); err != nil {
						return err
					}
				}

				// Each table has a corresponding pg_type row.

				opts := forEachTableDescOptions{virtualOpts: virtualCurrentDB}
				if err := forEachTableDesc(ctx, p, dbContext, opts, func(
					ctx context.Context, descCtx tableDescContext) error {
					return addPGTypeRowForTable(ctx, p, h, descCtx.database, descCtx.schema, descCtx.table, addRow)
				},
				); err != nil {
					return err
				}

				// Now generate rows for user defined types in this database.
				return forEachTypeDesc(
					ctx,
					p,
					db,
					func(ctx context.Context, _ catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, typDesc catalog.TypeDescriptor) error {
						nspOid := schemaOid(sc.GetID())
						tn := tree.NewQualifiedTypeName(db.GetName(), sc.GetName(), typDesc.GetName())
						typ, err := typedesc.HydratedTFromDesc(ctx, tn, typDesc, p)
						if err != nil {
							return err
						}
						ownerOid, err := getOwnerOID(ctx, p, typDesc)
						if err != nil {
							return err
						}
						return addPGTypeRow(h, nspOid, ownerOid, typ, true /* isUDT */, addRow)
					},
				)
			},
		)
	},
	indexes: []virtualIndex{
		{
			incomplete: false,
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, db catalog.DatabaseDescriptor,
				addRow func(...tree.Datum) error) (bool, error) {

				h := makeOidHasher()
				nspOid := tree.NewDOid(catconstants.PgCatalogID)
				coid := tree.MustBeDOid(unwrappedConstraint)
				ooid := coid.Oid

				// Check if it is a predefined type.
				typ, ok := types.OidToType[ooid]
				if ok {
					if err := addPGTypeRow(h, nspOid, tree.DNull /* owner */, typ, false /* isUDT */, addRow); err != nil {
						return false, err
					}
					return true, nil
				}

				// Check if it is a user defined type.
				if !types.IsOIDUserDefinedType(ooid) {
					// This oid is not a user-defined type and we didn't find it in the
					// map of predefined types, return false. Note that in common usage we
					// only really expect the value 0 here (which cockroach uses internally
					// in the typelem field amongst others). Users, however, may join on
					// this index with any value.
					return false, nil
				}

				id := typedesc.UserDefinedTypeOIDToID(ooid)

				sc, typDesc, err := getSchemaAndTypeByTypeID(ctx, p, id)
				if err != nil || typDesc == nil {
					return false, err
				}

				if typDesc.Dropped() {
					return false, nil
				}

				// It's an entry for the implicit record type created on behalf of each
				// table. We have special logic for this case.
				if typDesc.AsTableImplicitRecordTypeDescriptor() != nil {
					table, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, id)
					if err != nil {
						if errors.Is(err, catalog.ErrDescriptorNotFound) ||
							pgerror.GetPGCode(err) == pgcode.UndefinedObject ||
							pgerror.GetPGCode(err) == pgcode.UndefinedTable {
							return false, nil
						}
						return false, err
					}
					if !table.IsVirtualTable() && table.GetParentID() != db.GetID() {
						// If we're looking an implicit record type for a virtual table, we
						// always return the row regardless of the parent DB. But for real
						// tables, we only return a row if we're in the same DB as the table.
						return false, nil
					}
					if err := addPGTypeRowForTable(
						ctx,
						p,
						h,
						db,
						sc,
						table,
						addRow,
					); err != nil {
						return false, err
					}
					return true, nil
				}

				if typDesc.GetParentID() != db.GetID() {
					// Don't return types that aren't in our database.
					return false, nil
				}

				nspOid = schemaOid(sc.GetID())
				typ, err = typedesc.HydratedTFromDesc(ctx, tree.NewUnqualifiedTypeName(typDesc.GetName()), typDesc, p)
				if err != nil {
					return false, err
				}
				ownerOid, err := getOwnerOID(ctx, p, typDesc)
				if err != nil {
					return false, err
				}
				if err := addPGTypeRow(h, nspOid, ownerOid, typ, true /* isUDT */, addRow); err != nil {
					return false, err
				}

				return true, nil
			},
		},
	},
}

var pgCatalogUserTable = virtualSchemaTable{
	comment: `database users
https://www.postgresql.org/docs/9.5/view-pg-user.html`,
	schema: vtable.PGCatalogUser,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRoleAtCacheReadTS(ctx, p,
			func(ctx context.Context, userName username.SQLUsername, isRole bool, options roleOptions, settings tree.Datum) error {
				if isRole {
					return nil
				}
				isRoot := tree.DBool(userName.IsRootUser())
				createDB, err := options.createDB()
				if err != nil {
					return err
				}
				validUntil, err := options.validUntil(p)
				if err != nil {
					return err
				}
				isSuper, err := userIsSuper(ctx, p, userName)
				if err != nil {
					return err
				}

				return addRow(
					tree.NewDName(userName.Normalized()), // usename
					h.UserOid(userName),                  // usesysid
					tree.MakeDBool(isSuper || createDB),  // usecreatedb
					tree.MakeDBool(isRoot || isSuper),    // usesuper
					tree.DBoolFalse,                      // userepl
					tree.DBoolFalse,                      // usebypassrls
					passwdStarString,                     // passwd
					validUntil,                           // valuntil
					settings,                             // useconfig
				)
			})
	},
}

var pgCatalogUserMappingTable = virtualSchemaTable{
	comment: `local to remote user mapping (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-user-mapping.html`,
	schema: vtable.PGCatalogUserMapping,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// This table stores the mapping to foreign server users.
		// Foreign servers are not supported.
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatActivityTable = virtualSchemaTable{
	comment: `backend access statistics (empty - monitoring works differently in CockroachDB)
https://www.postgresql.org/docs/9.6/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW`,
	schema: vtable.PGCatalogStatActivity,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogSecurityLabelTable = virtualSchemaTable{
	comment: `security labels (empty - feature does not exist)
https://www.postgresql.org/docs/9.5/catalog-pg-seclabel.html`,
	schema: vtable.PGCatalogSecurityLabel,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogSharedSecurityLabelTable = virtualSchemaTable{
	comment: `shared security labels (empty - feature not supported)
https://www.postgresql.org/docs/9.5/catalog-pg-shseclabel.html`,
	schema: vtable.PGCatalogSharedSecurityLabel,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogDbRoleSettingTable = virtualSchemaTable{
	comment: `contains the default values that have been configured for session variables
https://www.postgresql.org/docs/13/catalog-pg-db-role-setting.html`,
	schema: vtable.PgCatalogDbRoleSetting,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		rows, err := p.InternalSQLTxn().QueryBufferedEx(
			ctx,
			"select-db-role-settings",
			p.Txn(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT database_id, role_name, settings FROM system.public.database_role_settings`,
		)
		if err != nil {
			return err
		}
		h := makeOidHasher()
		for _, row := range rows {
			databaseID := tree.MustBeDOid(row[0])
			roleName := tree.MustBeDString(row[1])
			roleID := oidZero
			if roleName != "" {
				roleID = h.UserOid(username.MakeSQLUsernameFromPreNormalizedString(string(roleName)))
			}
			settings := tree.MustBeDArray(row[2])
			if err := addRow(
				settings,
				databaseID,
				roleID,
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogShadowTable = virtualSchemaTable{
	comment: `pg_shadow lists properties for roles that are marked as rolcanlogin in pg_authid
https://www.postgresql.org/docs/13/view-pg-shadow.html`,
	schema: vtable.PgCatalogShadow,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRoleAtCacheReadTS(ctx, p, func(ctx context.Context, userName username.SQLUsername, isRole bool, options roleOptions, settings tree.Datum) error {
			noLogin, err := options.noLogin()
			if err != nil {
				return err
			}
			if noLogin {
				return nil
			}

			isRoot := tree.DBool(userName.IsRootUser() || userName.IsAdminRole())
			createDB, err := options.createDB()
			if err != nil {
				return err
			}
			rolValidUntil, err := options.validUntil(p)
			if err != nil {
				return err
			}
			isSuper, err := userIsSuper(ctx, p, userName)
			if err != nil {
				return err
			}

			return addRow(
				tree.NewDName(userName.Normalized()), // usename
				h.UserOid(userName),                  // usesysid
				tree.MakeDBool(isRoot || createDB),   // usecreatedb
				tree.MakeDBool(isRoot || isSuper),    // usesuper
				tree.DBoolFalse,                      // userepl
				tree.DBoolFalse,                      // usebypassrls
				passwdStarString,                     // passwd
				rolValidUntil,                        // valuntil
				settings,                             // useconfig
			)
		})
	},
}

var pgCatalogStatisticExtTable = virtualSchemaTable{
	comment: `pg_statistic_ext has the statistics objects created with CREATE STATISTICS
https://www.postgresql.org/docs/13/catalog-pg-statistic-ext.html`,
	schema: vtable.PgCatalogStatisticExt,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {

		//  '{d}' refers to Postgres code for n-distinct statistics or multi-column
		//  statistics.
		query := `SELECT "tableID", name, "columnIDs", "statisticID", '{d}'::"char"[] FROM system.table_statistics;`

		rows, err := p.InternalSQLTxn().QueryBufferedEx(
			ctx, "read-statistics-objects", p.txn,
			sessiondata.NodeUserSessionDataOverride,
			query,
		)
		if err != nil {
			return err
		}
		h := makeOidHasher()
		statTgt := tree.NewDInt(-1)

		for _, row := range rows {
			tableID := tree.MustBeDInt(row[0])
			columnIDs := tree.MustBeDArray(row[2])
			statisticsID := tree.MustBeDInt(row[3])
			statisticsKind := tree.MustBeDArray(row[4])

			// The statisticsID is generated from unique_rowid() so it won't fit in a
			// uint32.
			h.writeUInt64(uint64(statisticsID))
			statisticsOID := h.getOid()

			tbl, err := p.Descriptors().ByIDWithLeased(p.Txn()).Get().Table(ctx, descpb.ID(tableID))
			if err != nil {
				if pgerror.GetPGCode(err) == pgcode.UndefinedTable {
					// The system.table_statistics could contain stale rows that
					// reference a descriptor that no longer exists.
					continue
				}
				return err
			}
			canSeeDescriptor, err := userCanSeeDescriptor(
				ctx, p, tbl, db, false /* allowAdding */, false /* includeDropped */)

			if err != nil {
				return err
			}
			if !canSeeDescriptor {
				continue
			}
			if err != nil {
				return err
			}
			tn, err := descs.GetObjectName(ctx, p.Txn(), p.Descriptors(), tbl)
			if err != nil {
				return err
			}

			statSchema := db.GetSchemaID(tn.Schema())

			if err := addRow(
				statisticsOID,                // oid
				tableOid(descpb.ID(tableID)), // stxrelid
				row[1],                       // stxname
				schemaOid(statSchema),        // stxnamespace
				tree.DNull,                   // stxowner
				statTgt,                      // stxstattarget
				columnIDs,                    // stxkeys
				statisticsKind,               // stxkind
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogSequencesTable = virtualSchemaTable{
	comment: `pg_sequences is very similar as pg_sequence.
https://www.postgresql.org/docs/13/view-pg-sequences.html
`,
	schema: vtable.PgCatalogSequences,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {

		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /* virtual schemas do not have indexes */
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				sc, table := descCtx.schema, descCtx.table
				if !table.IsSequence() {
					return nil
				}
				opts := table.GetSequenceOpts()
				lastValue := tree.DNull
				sequenceValue, err := p.GetSequenceValue(ctx, p.execCfg.Codec, table)
				if err != nil {
					return err
				}

				// Before using for the first time, sequenceValue will be:
				// opts.Start - opts.Increment.
				if sequenceValue != opts.Start-opts.Increment {
					lastValue = tree.NewDInt(tree.DInt(sequenceValue))
				}
				owner, err := getOwnerName(ctx, p, table)
				if err != nil {
					return err
				}
				// sequenceowner refers to the username that owns the sequence which is
				// available in the table descriptor that can be changed by ALTER
				// SEQUENCE sequencename OWNER TO username. Sequence opts have a
				// table.column owner which is the value that can be modifyied by ALTER
				// SEQUENE sequencename OWNED BY table.column, This is not the expected
				// value on sequenceowner.
				return addRow(
					tree.NewDString(sc.GetName()),           // schemaname
					tree.NewDString(table.GetName()),        // sequencename
					owner,                                   // sequenceowner
					tree.NewDOid(oid.T_int8),                // data_type
					tree.NewDInt(tree.DInt(opts.Start)),     // start_value
					tree.NewDInt(tree.DInt(opts.MinValue)),  // min_value
					tree.NewDInt(tree.DInt(opts.MaxValue)),  // max_value
					tree.NewDInt(tree.DInt(opts.Increment)), // increment_by
					tree.DBoolFalse,                         // cycle
					tree.NewDInt(tree.DInt(opts.CacheSize)), // cache_size
					lastValue,                               // last_value
				)
			},
		)
	},
}

var pgCatalogInitPrivsTable = virtualSchemaTable{
	comment: "pg_init_privs was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogInitPrivs,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatProgressCreateIndexTable = virtualSchemaTable{
	comment: "pg_stat_progress_create_index was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatProgressCreateIndex,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogOpfamilyTable = virtualSchemaTable{
	comment: "pg_opfamily was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogOpfamily,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioAllSequencesTable = virtualSchemaTable{
	comment: "pg_statio_all_sequences was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioAllSequences,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogPoliciesTable = virtualSchemaTable{
	comment: "pg_policies was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogPolicies,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatsExtTable = virtualSchemaTable{
	comment: "pg_stats_ext was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatsExt,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogUserMappingsTable = virtualSchemaTable{
	comment: "pg_user_mappings was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogUserMappings,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatGssapiTable = virtualSchemaTable{
	comment: "pg_stat_gssapi was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatGssapi,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatDatabaseTable = virtualSchemaTable{
	comment: "pg_stat_database was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatDatabase,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioUserIndexesTable = virtualSchemaTable{
	comment: "pg_statio_user_indexes was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioUserIndexes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatSslTable = virtualSchemaTable{
	comment: "pg_stat_ssl was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatSsl,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioAllIndexesTable = virtualSchemaTable{
	comment: "pg_statio_all_indexes was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioAllIndexes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTimezoneAbbrevsTable = virtualSchemaTable{
	comment: "pg_timezone_abbrevs was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTimezoneAbbrevs,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatSysTablesTable = virtualSchemaTable{
	comment: "pg_stat_sys_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatSysTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioSysSequencesTable = virtualSchemaTable{
	comment: "pg_statio_sys_sequences was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioSysSequences,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatAllTablesTable = virtualSchemaTable{
	comment: "pg_stat_all_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatAllTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioSysTablesTable = virtualSchemaTable{
	comment: "pg_statio_sys_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioSysTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTsConfigTable = virtualSchemaTable{
	comment: "pg_ts_config was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTsConfig,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatsTable = virtualSchemaTable{
	comment: "pg_stats was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStats,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatProgressBasebackupTable = virtualSchemaTable{
	comment: "pg_stat_progress_basebackup was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatProgressBasebackup,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogPolicyTable = virtualSchemaTable{
	comment: "pg_policy was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogPolicy,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatArchiverTable = virtualSchemaTable{
	comment: "pg_stat_archiver was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatArchiver,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatXactUserFunctionsTable = virtualSchemaTable{
	comment: "pg_stat_xact_user_functions was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatXactUserFunctions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatUserFunctionsTable = virtualSchemaTable{
	comment: "pg_stat_user_functions was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatUserFunctions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogPublicationTable = virtualSchemaTable{
	comment: "pg_publication was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogPublication,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogAmprocTable = virtualSchemaTable{
	comment: "pg_amproc was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogAmproc,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatProgressAnalyzeTable = virtualSchemaTable{
	comment: "pg_stat_progress_analyze was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatProgressAnalyze,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatXactAllTablesTable = virtualSchemaTable{
	comment: "pg_stat_xact_all_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatXactAllTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogHbaFileRulesTable = virtualSchemaTable{
	comment: "pg_hba_file_rules was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogHbaFileRules,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogCursorsTable = virtualSchemaTable{
	comment: `contains currently active SQL cursors created with DECLARE
https://www.postgresql.org/docs/14/view-pg-cursors.html`,
	schema: vtable.PgCatalogCursors,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for name, c := range p.sqlCursors.list() {
			tz, err := tree.MakeDTimestampTZ(c.created, time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDString(string(name)),          /* name */
				tree.NewDString(c.statement),           /* statement */
				tree.MakeDBool(tree.DBool(c.withHold)), /* is_holdable */
				tree.DBoolFalse,                        /* is_binary */
				tree.DBoolFalse,                        /* is_scrollable */
				tz,                                     /* creation_date */
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var pgCatalogStatSlruTable = virtualSchemaTable{
	comment: "pg_stat_slru was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatSlru,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogFileSettingsTable = virtualSchemaTable{
	comment: "pg_file_settings was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogFileSettings,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatUserIndexesTable = virtualSchemaTable{
	comment: "pg_stat_user_indexes was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatUserIndexes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogRulesTable = virtualSchemaTable{
	comment: "pg_rules was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogRules,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioUserSequencesTable = virtualSchemaTable{
	comment: "pg_statio_user_sequences was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioUserSequences,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatAllIndexesTable = virtualSchemaTable{
	comment: "pg_stat_all_indexes was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatAllIndexes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTsConfigMapTable = virtualSchemaTable{
	comment: "pg_ts_config_map was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTsConfigMap,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatBgwriterTable = virtualSchemaTable{
	comment: "pg_stat_bgwriter was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatBgwriter,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTransformTable = virtualSchemaTable{
	comment: "pg_transform was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTransform,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatXactUserTablesTable = virtualSchemaTable{
	comment: "pg_stat_xact_user_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatXactUserTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogPublicationTablesTable = virtualSchemaTable{
	comment: "pg_publication_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogPublicationTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatProgressClusterTable = virtualSchemaTable{
	comment: "pg_stat_progress_cluster was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatProgressCluster,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogGroupTable = virtualSchemaTable{
	comment: "pg_group was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogGroup,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogLargeobjectMetadataTable = virtualSchemaTable{
	comment: "pg_largeobject_metadata was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogLargeobjectMetadata,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogReplicationSlotsTable = virtualSchemaTable{
	comment: "pg_replication_slots was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogReplicationSlots,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogSubscriptionRelTable = virtualSchemaTable{
	comment: "pg_subscription_rel was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogSubscriptionRel,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTsParserTable = virtualSchemaTable{
	comment: "pg_ts_parser was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTsParser,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatisticExtDataTable = virtualSchemaTable{
	comment: "pg_statistic_ext_data was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatisticExtData,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogPartitionedTableTable = virtualSchemaTable{
	comment: "pg_partitioned_table was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogPartitionedTable,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioSysIndexesTable = virtualSchemaTable{
	comment: "pg_statio_sys_indexes was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioSysIndexes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogConfigTable = virtualSchemaTable{
	comment: "pg_config was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogConfig,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioUserTablesTable = virtualSchemaTable{
	comment: "pg_statio_user_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioUserTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTimezoneNamesTable = virtualSchemaTable{
	comment: "pg_timezone_names lists all the timezones that are supported by SET timezone",
	schema:  vtable.PgCatalogTimezoneNames,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, tz := range timeutil.TimeZones() {
			loc, err := timeutil.LoadLocation(tz)
			if err != nil {
				return err
			}
			if err := addRowForTimezoneNames(tz, p.extendedEvalCtx.StmtTimestamp.In(loc), addRow); err != nil {
				return err
			}
		}
		return nil
	},
	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, db catalog.DatabaseDescriptor,
				addRow func(...tree.Datum) error,
			) (bool, error) {
				tz := string(tree.MustBeDString(unwrappedConstraint))
				loc, err := timeutil.LoadLocation(tz)
				if err != nil {
					return false, nil //nolint:returnerrcheck
				}
				return true, addRowForTimezoneNames(tz, p.extendedEvalCtx.StmtTimestamp.In(loc), addRow)
			},
		},
	},
}

func addRowForTimezoneNames(tz string, t time.Time, addRow func(...tree.Datum) error) error {
	abbrev, offset := t.Zone()
	utcOffsetInterval := duration.MakeDuration(int64(offset)*int64(time.Second), 0, 0)
	return addRow(
		tree.NewDString(tz),     // name
		tree.NewDString(abbrev), // abbrev
		tree.NewDInterval(utcOffsetInterval, types.DefaultIntervalTypeMetadata), // utc_offset
		tree.MakeDBool(tree.DBool(t.IsDST())),                                   // is_dst
	)
}

var pgCatalogTsDictTable = virtualSchemaTable{
	comment: "pg_ts_dict was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTsDict,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatUserTablesTable = virtualSchemaTable{
	comment: "pg_stat_user_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatUserTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogSubscriptionTable = virtualSchemaTable{
	comment: "pg_subscription was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogSubscription,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogShmemAllocationsTable = virtualSchemaTable{
	comment: "pg_shmem_allocations was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogShmemAllocations,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatWalReceiverTable = virtualSchemaTable{
	comment: "pg_stat_wal_receiver was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatWalReceiver,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatSubscriptionTable = virtualSchemaTable{
	comment: "pg_stat_subscription was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatSubscription,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogLargeobjectTable = virtualSchemaTable{
	comment: "pg_largeobject was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogLargeobject,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogReplicationOriginStatusTable = virtualSchemaTable{
	comment: "pg_replication_origin_status was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogReplicationOriginStatus,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogAmopTable = virtualSchemaTable{
	comment: "pg_amop was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogAmop,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatProgressVacuumTable = virtualSchemaTable{
	comment: "pg_stat_progress_vacuum was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatProgressVacuum,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatSysIndexesTable = virtualSchemaTable{
	comment: "pg_stat_sys_indexes was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatSysIndexes,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatioAllTablesTable = virtualSchemaTable{
	comment: "pg_statio_all_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatioAllTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatDatabaseConflictsTable = virtualSchemaTable{
	comment: "pg_stat_database_conflicts was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatDatabaseConflicts,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogReplicationOriginTable = virtualSchemaTable{
	comment: "pg_replication_origin was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogReplicationOrigin,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatisticTable = virtualSchemaTable{
	comment: "pg_statistic was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatistic,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatXactSysTablesTable = virtualSchemaTable{
	comment: "pg_stat_xact_sys_tables was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatXactSysTables,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogTsTemplateTable = virtualSchemaTable{
	comment: "pg_ts_template was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogTsTemplate,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogStatReplicationTable = virtualSchemaTable{
	comment: "pg_stat_replication was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogStatReplication,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogPublicationRelTable = virtualSchemaTable{
	comment: "pg_publication_rel was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogPublicationRel,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

var pgCatalogAvailableExtensionVersionsTable = virtualSchemaTable{
	comment: "pg_available_extension_versions was created for compatibility and is currently unimplemented",
	schema:  vtable.PgCatalogAvailableExtensionVersions,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

// typOid is the only OID generation approach that does not use oidHasher, because
// object identifiers for types are not arbitrary, but instead need to be kept in
// sync with Postgres.
func typOid(typ *types.T) tree.Datum {
	return tree.NewDOid(typ.Oid())
}

func typLen(typ *types.T) *tree.DInt {
	return tree.NewDInt(tree.DInt(tree.PGWireTypeSize(typ)))
}

func typByVal(typ *types.T) tree.Datum {
	if typ.Identical(types.Trigger) {
		return tree.DBoolTrue
	}
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
		return h.CollationOid(collatedstring.DefaultCollationTag)
	case types.CollatedStringFamily:
		return h.CollationOid(typ.Locale())
	}

	if typ.Equivalent(types.StringArray) {
		return h.CollationOid(collatedstring.DefaultCollationTag)
	}
	return oidZero
}

// This mapping should be kept in sync with PG's categorization.
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
	types.JsonpathFamily:    typCategoryUserDefined,
	types.DecimalFamily:     typCategoryNumeric,
	types.StringFamily:      typCategoryString,
	types.TimestampFamily:   typCategoryDateTime,
	types.TimestampTZFamily: typCategoryDateTime,
	types.TSQueryFamily:     typCategoryUserDefined,
	types.TSVectorFamily:    typCategoryUserDefined,
	types.ArrayFamily:       typCategoryArray,
	types.TupleFamily:       typCategoryPseudo,
	types.OidFamily:         typCategoryNumeric,
	types.PGLSNFamily:       typCategoryUserDefined,
	types.PGVectorFamily:    typCategoryUserDefined,
	types.RefCursorFamily:   typCategoryUserDefined,
	types.UuidFamily:        typCategoryUserDefined,
	types.INetFamily:        typCategoryNetworkAddr,
	types.UnknownFamily:     typCategoryUnknown,
	types.VoidFamily:        typCategoryPseudo,
	types.TriggerFamily:     typCategoryPseudo,
}

func typCategory(typ *types.T) tree.Datum {
	// Special case ARRAY of ANY.
	if typ.Family() == types.ArrayFamily && typ.ArrayContents().Family() == types.AnyFamily {
		return typCategoryPseudo
	}
	if typ.UserDefined() && typ.Family() == types.TupleFamily {
		return typCategoryComposite
	}
	return datumToTypeCategory[typ.Family()]
}

var pgCatalogViewsTable = virtualSchemaTable{
	comment: `view definitions (incomplete - see also information_schema.views)
https://www.postgresql.org/docs/9.5/view-pg-views.html`,
	schema: vtable.PGCatalogViews,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Note: pg_views is not well defined if the dbContext is empty,
		// because it does not distinguish views in separate databases.

		opts := forEachTableDescOptions{virtualOpts: hideVirtual} /*virtual schemas do not have views*/
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				sc, desc := descCtx.schema, descCtx.table
				if !desc.IsView() || desc.MaterializedView() {
					return nil
				}
				owner, err := getOwnerName(ctx, p, desc)
				if err != nil {
					return err
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
					tree.NewDName(sc.GetName()),          // schemaname
					tree.NewDName(desc.GetName()),        // viewname
					owner,                                // viewowner
					tree.NewDString(desc.GetViewQuery()), // definition
				)
			})
	},
}

var pgCatalogAggregateTable = virtualSchemaTable{
	comment: `aggregated built-in functions (incomplete)
https://www.postgresql.org/docs/9.6/catalog-pg-aggregate.html`,
	schema: vtable.PGCatalogAggregate,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				for _, name := range builtins.AllAggregateBuiltinNames() {
					if name == builtins.AnyNotNull {
						// any_not_null is treated as a special case.
						continue
					}
					_, overloads := builtinsregistry.GetBuiltinProperties(name)
					for _, overload := range overloads {
						params, _ := tree.GetParamsAndReturnType(overload)
						sortOperatorOid := oidZero
						aggregateKind := tree.NewDString("n")
						aggNumDirectArgs := zeroVal
						if params.Length() != 0 {
							argType := tree.NewDOid(params.Types()[0].Oid())
							returnType := tree.NewDOid(oid.T_bool)
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
						err := addRow(
							tree.NewDOid(overload.Oid).AsRegProc(name), // aggfnoid
							aggregateKind,    // aggkind
							aggNumDirectArgs, // aggnumdirectargs
							regProcOidZero,   // aggtransfn
							regProcOidZero,   // aggfinalfn
							regProcOidZero,   // aggcombinefn
							regProcOidZero,   // aggserialfn
							regProcOidZero,   // aggdeserialfn
							regProcOidZero,   // aggmtransfn
							regProcOidZero,   // aggminvtransfn
							regProcOidZero,   // aggmfinalfn
							tree.DBoolFalse,  // aggfinalextra
							tree.DBoolFalse,  // aggmfinalextra
							sortOperatorOid,  // aggsortop
							tree.DNull,       // aggtranstype
							tree.DNull,       // aggtransspace
							tree.DNull,       // aggmtranstype
							tree.DNull,       // aggmtransspace
							tree.DNull,       // agginitval
							tree.DNull,       // aggminitval
							// These columns were automatically created by pg_catalog_test's missing column generator.
							tree.DNull, // aggfinalmodify
							tree.DNull, // aggmfinalmodify
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
// sequentially generated. Postgres does not guarantee database-wide uniqueness
// of OIDs, especially in large databases, but they are frequently used as the
// key of unique indexes for pg_catalog tables.
// See: https://www.postgresql.org/docs/9.6/static/datatype-oid.html.
// Because Cockroach does not have an equivalent concept, we generate arbitrary
// fingerprints for database objects with the only requirements being that they
// are 32 bits and that they are stable across accesses.
//
// The type has a few layers of methods:
//   - write<go_type> methods write concrete types to the underlying running hash.
//   - write<db_object> methods account for single database objects like TableDescriptors
//     or IndexDescriptors in the running hash. These methods aim to write information
//     that would uniquely fingerprint the object to the hash using the first layer of
//     methods.
//   - <DB_Object>Oid methods use the second layer of methods to construct a unique
//     object identifier for the provided database object. This object identifier will
//     be returned as a *tree.DInt, and the running hash will be reset. These are the
//     only methods that are part of the oidHasher's external facing interface.
type oidHasher struct {
	h hash.Hash32
}

// IsMaybeHashedOid returns if the OID value is possibly a hashed value.
func IsMaybeHashedOid(o oid.Oid) bool {
	return o > oidext.CockroachPredefinedOIDMax
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
	h.writeUInt64(uint64(oid.Oid))
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
	rewriteTypeTag
	dbSchemaRoleTypeTag
	castTypeTag
)

func (h oidHasher) writeTypeTag(tag oidTypeTag) {
	h.writeUInt8(uint8(tag))
}

func (h oidHasher) getOid() *tree.DOid {
	i := h.h.Sum32()
	// Ensure generated OID hashes are above the pre-defined max limit.
	if i <= oidext.CockroachPredefinedOIDMax {
		i += oidext.CockroachPredefinedOIDMax
	}
	h.h.Reset()
	return tree.NewDOid(oid.Oid(i))
}

func (h oidHasher) writeDB(dbID descpb.ID) {
	h.writeUInt32(uint32(dbID))
}

func (h oidHasher) writeSchema(scID descpb.ID) {
	h.writeUInt32(uint32(scID))
}

func (h oidHasher) writeTable(tableID descpb.ID) {
	h.writeUInt32(uint32(tableID))
}

func (h oidHasher) writeIndex(indexID descpb.IndexID) {
	h.writeUInt32(uint32(indexID))
}

func (h oidHasher) writeUniqueConstraint(uc catalog.UniqueWithoutIndexConstraint) {
	h.writeUInt32(uint32(uc.ParentTableID()))
	h.writeStr(uc.GetName())
}

func (h oidHasher) writeCheckConstraint(check catalog.CheckConstraint) {
	h.writeStr(check.GetName())
	h.writeStr(check.GetExpr())
}

func (h oidHasher) writeForeignKeyConstraint(fk catalog.ForeignKeyConstraint) {
	h.writeUInt32(uint32(fk.GetReferencedTableID()))
	h.writeStr(fk.GetName())
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
	dbID descpb.ID, scID descpb.ID, tableID descpb.ID, check catalog.CheckConstraint,
) *tree.DOid {
	h.writeTypeTag(checkConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scID)
	h.writeTable(tableID)
	h.writeCheckConstraint(check)
	return h.getOid()
}

func (h oidHasher) PrimaryKeyConstraintOid(
	dbID descpb.ID, scID descpb.ID, tableID descpb.ID, pkey catalog.UniqueWithIndexConstraint,
) *tree.DOid {
	h.writeTypeTag(pKeyConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scID)
	h.writeTable(tableID)
	h.writeIndex(pkey.GetID())
	return h.getOid()
}

func (h oidHasher) ForeignKeyConstraintOid(
	dbID descpb.ID, scID descpb.ID, tableID descpb.ID, fk catalog.ForeignKeyConstraint,
) *tree.DOid {
	h.writeTypeTag(fkConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scID)
	h.writeTable(tableID)
	h.writeForeignKeyConstraint(fk)
	return h.getOid()
}

func (h oidHasher) UniqueWithoutIndexConstraintOid(
	dbID descpb.ID, scID descpb.ID, tableID descpb.ID, uc catalog.UniqueWithoutIndexConstraint,
) *tree.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scID)
	h.writeTable(tableID)
	h.writeUniqueConstraint(uc)
	return h.getOid()
}

func (h oidHasher) UniqueConstraintOid(
	dbID descpb.ID, scID descpb.ID, tableID descpb.ID, uwi catalog.UniqueWithIndexConstraint,
) *tree.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scID)
	h.writeTable(tableID)
	h.writeIndex(uwi.GetID())
	return h.getOid()
}

// RegProc can only be used to construct RegProc datum for builtin functions.
// It's currently only be used to construct rows in pg_catalog.pg_type which
// requires type-relevant builtin functions.
func (h oidHasher) RegProc(name string) tree.Datum {
	_, overloads := builtinsregistry.GetBuiltinProperties(name)
	if len(overloads) == 0 {
		return regProcOidZero
	}
	return tree.NewDOid(overloads[0].Oid).AsRegProc(name)
}

func (h oidHasher) UserOid(userName username.SQLUsername) *tree.DOid {
	h.writeTypeTag(userTypeTag)
	h.writeStr(userName.Normalized())
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

func (h oidHasher) rewriteOid(source descpb.ID, depended descpb.ID) *tree.DOid {
	h.writeTypeTag(rewriteTypeTag)
	h.writeUInt32(uint32(source))
	h.writeUInt32(uint32(depended))
	return h.getOid()
}

// DBSchemaRoleOid creates an OID based on the combination of a db/schema/role.
// This is used to generate a unique row identifier for pg_default_acl.
func (h oidHasher) DBSchemaRoleOid(
	dbID descpb.ID, scID descpb.ID, normalizedRole string,
) *tree.DOid {
	h.writeTypeTag(dbSchemaRoleTypeTag)
	h.writeDB(dbID)
	h.writeSchema(scID)
	h.writeStr(normalizedRole)
	return h.getOid()
}

func tableOid(id descpb.ID) *tree.DOid {
	return tree.NewDOid(oid.Oid(id))
}

func schemaOid(id descpb.ID) *tree.DOid {
	return tree.NewDOid(oid.Oid(id))
}

func dbOid(id descpb.ID) *tree.DOid {
	return tree.NewDOid(oid.Oid(id))
}

func stringOid(s string) *tree.DOid {
	h := makeOidHasher()
	h.writeStr(s)
	return h.getOid()
}

func (h oidHasher) CastOid(srcID oid.Oid, tgtID oid.Oid) *tree.DOid {
	h.writeTypeTag(castTypeTag)
	h.writeUInt32(uint32(srcID))
	h.writeUInt32(uint32(tgtID))
	return h.getOid()
}

func funcVolatility(v catpb.Function_Volatility) string {
	switch v {
	case catpb.Function_IMMUTABLE:
		return "i"
	case catpb.Function_STABLE:
		return "s"
	case catpb.Function_VOLATILE:
		return "v"
	default:
		return ""
	}
}

// populateVirtualIndexForTable is used to populate the virtual index with context of the given table descriptor.
func populateVirtualIndexForTable(
	ctx context.Context,
	p *planner,
	dbContext catalog.DatabaseDescriptor,
	tableDesc catalog.TableDescriptor,
	addRow func(...tree.Datum) error,
	populateFromTable func(ctx context.Context, p *planner, h oidHasher, db catalog.DatabaseDescriptor,
		sc catalog.SchemaDescriptor, table catalog.TableDescriptor, lookup simpleSchemaResolver,
		addRow func(...tree.Datum) error,
	) error,
) (bool, error) {

	// Don't include tables that aren't in the current database unless
	// they're virtual, dropped tables, or ones that the user can't see.
	canSeeDescriptor, err := userCanSeeDescriptor(
		ctx, p, tableDesc, dbContext, true /*allowAdding*/, false /* includeDropped */)
	if err != nil {
		return false, err
	}
	// Skip over tables from a different DB, ones which aren't visible
	// or are dropped. From a virtual index viewpoint, we will consider
	// this result set as populated, since the underlying full table will
	// also skip the same descriptors.
	if (!tableDesc.IsVirtualTable() && dbContext != nil && tableDesc.GetParentID() != dbContext.GetID()) ||
		tableDesc.Dropped() ||
		!canSeeDescriptor {
		return true, nil
	}
	h := makeOidHasher()
	scResolver := oneAtATimeSchemaResolver{p: p, ctx: ctx}
	var sc catalog.SchemaDescriptor
	if tableDesc.IsTemporary() {
		// Temp tables from other sessions should still be visible here.
		// Ideally, the catalog API would be able to return the temporary
		// schemas from other sessions, but it cannot right now. See
		// https://github.com/cockroachdb/cockroach/issues/97822.
		if err := forEachSchema(ctx, p, dbContext, false /* requiresPrivileges*/, func(ctx context.Context, schema catalog.SchemaDescriptor) error {
			if schema.GetID() == tableDesc.GetParentSchemaID() {
				sc = schema
			}
			return nil
		}); err != nil {
			return false, err
		}
	}
	if sc == nil {
		sc, err = p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Schema(ctx, tableDesc.GetParentSchemaID())
		if err != nil {
			return false, err
		}
	}
	db := dbContext
	if db == nil {
		db, err = p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Database(ctx, tableDesc.GetParentID())
		if err != nil {
			return false, err
		}
	}
	if err := populateFromTable(ctx, p, h, db, sc, tableDesc, scResolver, addRow); err != nil {
		return false, err
	}
	return true, nil
}

// populateVirtualIndexForType is used to populate the virtual index with context of the given type descriptor.
func populateVirtualIndexForType(
	ctx context.Context,
	p *planner,
	db catalog.DatabaseDescriptor,
	typeDesc catalog.TypeDescriptor,
	addRow func(...tree.Datum) error,
	populateFromType func(h oidHasher, nspOid tree.Datum, owner tree.Datum,
		typ *types.T, addRow func(...tree.Datum) error) error,
) (bool, error) {
	// Skip over types from a different DB.
	if typeDesc.GetParentID() != db.GetID() {
		return true, nil
	}
	h := makeOidHasher()
	sc, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Schema(ctx, typeDesc.GetParentSchemaID())
	if err != nil {
		return false, err
	}

	nspOid := schemaOid(sc.GetID())
	tn := tree.NewQualifiedTypeName(db.GetName(), sc.GetName(), typeDesc.GetName())
	typ, err := typedesc.HydratedTFromDesc(ctx, tn, typeDesc, p)
	if err != nil {
		return false, err
	}
	ownerOid, err := getOwnerOID(ctx, p, typeDesc)
	if err != nil {
		return false, err
	}
	if err := populateFromType(h, nspOid, ownerOid, typ, addRow); err != nil {
		return false, err
	}
	return true, nil
}
