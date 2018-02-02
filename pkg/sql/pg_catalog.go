// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"golang.org/x/text/collate"

	"bytes"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var (
	oidZero   = tree.NewDOid(0)
	zeroVal   = tree.DZero
	negOneVal = tree.NewDInt(-1)

	passwdStarString = tree.NewDString("********")
)

const (
	cockroachIndexEncoding = "prefix"
	defaultCollationTag    = "en-US"
)

// pgCatalog contains a set of system tables mirroring PostgreSQL's pg_catalog schema.
// This code attempts to comply as closely as possible to the system catalogs documented
// in https://www.postgresql.org/docs/9.6/static/catalogs.html.
var pgCatalog = virtualSchema{
	name: pgCatalogName,
	tables: []virtualSchemaTable{
		pgCatalogAmTable,
		pgCatalogAttrDefTable,
		pgCatalogAttributeTable,
		pgCatalogAuthMembersTable,
		pgCatalogClassTable,
		pgCatalogCollationTable,
		pgCatalogConstraintTable,
		pgCatalogDatabaseTable,
		pgCatalogDependTable,
		pgCatalogDescriptionTable,
		pgCatalogEnumTable,
		pgCatalogExtensionTable,
		pgCatalogForeignDataWrapperTable,
		pgCatalogForeignServerTable,
		pgCatalogForeignTableTable,
		pgCatalogIndexTable,
		pgCatalogIndexesTable,
		pgCatalogInheritsTable,
		pgCatalogNamespaceTable,
		pgCatalogOperatorTable,
		pgCatalogProcTable,
		pgCatalogRangeTable,
		pgCatalogRewriteTable,
		pgCatalogRolesTable,
		pgCatalogSequencesTable,
		pgCatalogSettingsTable,
		pgCatalogUserTable,
		pgCatalogUserMappingTable,
		pgCatalogTablesTable,
		pgCatalogTablespaceTable,
		pgCatalogTriggerTable,
		pgCatalogTypeTable,
		pgCatalogViewsTable,
	},
}

// See: https://www.postgresql.org/docs/current/static/catalog-pg-am.html.
var pgCatalogAmTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_am (
	oid OID,
	amname NAME,
	amhandler OID,
	amtype CHAR
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		h.writeStr(cockroachIndexEncoding)
		return addRow(
			h.getOid(),
			tree.NewDName(cockroachIndexEncoding),
			tree.DNull,
			tree.NewDString("i"),
		)
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-attrdef.html.
var pgCatalogAttrDefTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_attrdef (
	oid OID,
	adrelid OID,
	adnum INT,
	adbin STRING,
	adsrc STRING
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			colNum := 0
			return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				colNum++
				if column.DefaultExpr == nil {
					// pg_attrdef only expects rows for columns with default values.
					return nil
				}
				defSrc := tree.NewDString(*column.DefaultExpr)
				return addRow(
					h.ColumnOid(db, table, column),  // oid
					h.TableOid(db, table),           // adrelid
					tree.NewDInt(tree.DInt(colNum)), // adnum
					defSrc, // adbin
					defSrc, // adsrc
				)
			})
		})
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-attribute.html.
var pgCatalogAttributeTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_attribute (
	attrelid OID,
	attname NAME,
	atttypid OID,
	attstattarget INT,
	attlen INT,
	attnum INT,
	attndims INT,
	attcacheoff INT,
	atttypmod INT,
	attbyval BOOL,
	attstorage CHAR,
	attalign CHAR,
	attnotnull BOOL,
	atthasdef BOOL,
	attisdropped BOOL,
	attislocal BOOL,
	attinhcount INT,
	attcollation OID,
	attacl STRING[],
	attoptions STRING[],
	attfdwoptions STRING[]
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			// addColumn adds adds either a table or a index column to the pg_attribute table.
			addColumn := func(column *sqlbase.ColumnDescriptor, attRelID tree.Datum, colNum int) error {
				colTyp := column.Type.ToDatumType()
				return addRow(
					attRelID,                        // attrelid
					tree.NewDName(column.Name),      // attname
					typOid(colTyp),                  // atttypid
					zeroVal,                         // attstattarget
					typLen(colTyp),                  // attlen
					tree.NewDInt(tree.DInt(colNum)), // attnum
					zeroVal,    // attndims
					negOneVal,  // attcacheoff
					negOneVal,  // atttypmod
					tree.DNull, // attbyval (see pg_type.typbyval)
					tree.DNull, // attstorage
					tree.DNull, // attalign
					tree.MakeDBool(tree.DBool(!column.Nullable)),          // attnotnull
					tree.MakeDBool(tree.DBool(column.DefaultExpr != nil)), // atthasdef
					tree.DBoolFalse,                                       // attisdropped
					tree.DBoolTrue,                                        // attislocal
					zeroVal,                                               // attinhcount
					typColl(colTyp, h),                                    // attcollation
					tree.DNull,                                            // attacl
					tree.DNull,                                            // attoptions
					tree.DNull,                                            // attfdwoptions
				)
			}

			// Columns for table.
			colNum := 0
			if err := forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				colNum++
				tableID := h.TableOid(db, table)
				return addColumn(column, tableID, colNum)
			}); err != nil {
				return err
			}

			// Columns for each index.
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				colNum := 0
				return forEachColumnInIndex(table, index,
					func(column *sqlbase.ColumnDescriptor) error {
						colNum++
						idxID := h.IndexOid(db, table, index)
						return addColumn(column, idxID, colNum)
					},
				)
			})
		})
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-auth-members.html.
var pgCatalogAuthMembersTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_auth_members (
	roleid OID,
	member OID,
	grantor OID,
	admin_option BOOL
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
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

var (
	relKindTable    = tree.NewDString("r")
	relKindIndex    = tree.NewDString("i")
	relKindView     = tree.NewDString("v")
	relKindSequence = tree.NewDString("S")

	relPersistencePermanent = tree.NewDString("p")
)

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-class.html.
var pgCatalogClassTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_class (
	oid OID,
	relname NAME NOT NULL,
	relnamespace OID,
	reltype OID,
	relowner OID,
	relam OID,
	relfilenode OID,
	reltablespace OID,
	relpages INT,
	reltuples FLOAT,
	relallvisible INT,
	reltoastrelid OID,
	relhasindex BOOL,
	relisshared BOOL,
	relpersistence CHAR,
	relistemp BOOL,
	relkind CHAR,
	relnatts INT,
	relchecks INT,
	relhasoids BOOL,
	relhaspkey BOOL,
	relhasrules BOOL,
	relhastriggers BOOL,
	relhassubclass BOOL,
	relfrozenxid INT,
	relacl STRING[],
	reloptions STRING[]
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			// The only difference between tables, views and sequences is the relkind column.
			relKind := relKindTable
			if table.IsView() {
				relKind = relKindView
			} else if table.IsSequence() {
				relKind = relKindSequence
			}
			if err := addRow(
				h.TableOid(db, table),       // oid
				tree.NewDName(table.Name),   // relname
				pgNamespaceForDB(db, h).Oid, // relnamespace
				oidZero,                     // reltype (PG creates a composite type in pg_type for each table)
				tree.DNull,                  // relowner
				tree.DNull,                  // relam
				oidZero,                     // relfilenode
				oidZero,                     // reltablespace
				tree.DNull,                  // relpages
				tree.DNull,                  // reltuples
				zeroVal,                     // relallvisible
				oidZero,                     // reltoastrelid
				tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhasindex
				tree.DBoolFalse,                                     // relisshared
				relPersistencePermanent,                             // relPersistence
				tree.DBoolFalse,                                     // relistemp
				relKind,                                             // relkind
				tree.NewDInt(tree.DInt(len(table.Columns))),         // relnatts
				tree.NewDInt(tree.DInt(len(table.Checks))),          // relchecks
				tree.DBoolFalse,                                     // relhasoids
				tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // relhaspkey
				tree.DBoolFalse,                                     // relhasrules
				tree.DBoolFalse,                                     // relhastriggers
				tree.DBoolFalse,                                     // relhassubclass
				zeroVal,                                             // relfrozenxid
				tree.DNull,                                          // relacl
				tree.DNull,                                          // reloptions
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
				return addRow(
					h.IndexOid(db, table, index),                    // oid
					tree.NewDName(index.Name),                       // relname
					pgNamespaceForDB(db, h).Oid,                     // relnamespace
					oidZero,                                         // reltype
					tree.DNull,                                      // relowner
					tree.DNull,                                      // relam
					oidZero,                                         // relfilenode
					oidZero,                                         // reltablespace
					tree.DNull,                                      // relpages
					tree.DNull,                                      // reltuples
					zeroVal,                                         // relallvisible
					oidZero,                                         // reltoastrelid
					tree.DBoolFalse,                                 // relhasindex
					tree.DBoolFalse,                                 // relisshared
					relPersistencePermanent,                         // relPersistence
					tree.DBoolFalse,                                 // relistemp
					relKindIndex,                                    // relkind
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
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-collation.html.
var pgCatalogCollationTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_collation (
  oid OID,
  collname STRING,
  collnamespace OID,
  collowner OID,
  collencoding INT,
  collcollate STRING,
  collctype STRING
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		for _, tag := range collate.Supported() {
			collName := tag.String()
			if err := addRow(
				h.CollationOid(collName),  // oid
				tree.NewDString(collName), // collname
				pgNamespacePGCatalog.Oid,  // collnamespace
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

	// Avoid unused warning for constants.
	_ = fkActionRestrict
	_ = fkActionCascade
	_ = fkActionSetNull
	_ = fkActionSetDefault

	fkMatchTypeFull    = tree.NewDString("f")
	fkMatchTypePartial = tree.NewDString("p")
	fkMatchTypeSimple  = tree.NewDString("s")

	// Avoid unused warning for constants.
	_ = fkMatchTypeFull
	_ = fkMatchTypePartial
)

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-constraint.html.
var pgCatalogConstraintTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_constraint (
	oid OID,
	conname NAME,
	connamespace OID,
	contype STRING,
	condeferrable BOOL,
	condeferred BOOL,
	convalidated BOOL,
	conrelid OID,
	contypid OID,
	conindid OID,
	confrelid OID,
	confupdtype STRING,
	confdeltype STRING,
	confmatchtype STRING,
	conislocal BOOL,
	coninhcount INT,
	connoinherit BOOL,
	conkey INT[],
	confkey INT[],
	conpfeqop OID[],
	conppeqop OID[],
	conffeqop OID[],
	conexclop OID[],
	conbin STRING,
	consrc STRING,
	-- condef is a CockroachDB extension that provides a SHOW CREATE CONSTRAINT
	-- style string, for use by pg_get_constraintdef().
	condef STRING
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, prefix, func(
			db *sqlbase.DatabaseDescriptor,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			info, err := table.GetConstraintInfoWithLookup(tableLookup.tableOrErr)
			if err != nil {
				return err
			}

			for name, c := range info {
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
				switch c.Kind {
				case sqlbase.ConstraintTypePK:
					oid = h.PrimaryKeyConstraintOid(db, table, c.Index)
					contype = conTypePKey
					conindid = h.IndexOid(db, table, c.Index)

					var err error
					conkey, err = colIDArrayToDatum(c.Index.ColumnIDs)
					if err != nil {
						return err
					}
					condef = tree.NewDString(table.PrimaryKeyString())

				case sqlbase.ConstraintTypeFK:
					referencedDB, _ := tableLookup(c.ReferencedTable.ID)
					if referencedDB == nil {
						panic(fmt.Sprintf("could not find database of %+v", c.ReferencedTable))
					}

					oid = h.ForeignKeyConstraintOid(db, table, c.FK)
					contype = conTypeFK
					conindid = h.IndexOid(referencedDB, c.ReferencedTable, c.ReferencedIndex)
					confrelid = h.TableOid(referencedDB, c.ReferencedTable)
					confupdtype = fkActionNone
					confdeltype = fkActionNone
					confmatchtype = fkMatchTypeSimple
					var err error
					conkey, err = colIDArrayToDatum(c.Index.ColumnIDs)
					if err != nil {
						return err
					}
					confkey, err = colIDArrayToDatum(c.ReferencedIndex.ColumnIDs)
					if err != nil {
						return err
					}
					var buf bytes.Buffer
					if err := p.printForeignKeyConstraint(ctx, &buf, db.Name, c.Index); err != nil {
						return err
					}
					condef = tree.NewDString(buf.String())

				case sqlbase.ConstraintTypeUnique:
					oid = h.UniqueConstraintOid(db, table, c.Index)
					contype = conTypeUnique
					conindid = h.IndexOid(db, table, c.Index)
					var err error
					conkey, err = colIDArrayToDatum(c.Index.ColumnIDs)
					if err != nil {
						return err
					}
					f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
					f.WriteString("UNIQUE (")
					c.Index.ColNamesFormat(f)
					f.WriteByte(')')
					condef = tree.NewDString(f.CloseAndGetString())

				case sqlbase.ConstraintTypeCheck:
					oid = h.CheckConstraintOid(db, table, c.CheckConstraint)
					contype = conTypeCheck
					// TODO(nvanbenschoten): We currently do not store the referenced columns for a check
					// constraint. We should add an array of column indexes to
					// sqlbase.TableDescriptor_CheckConstraint and use that here.
					consrc = tree.NewDString(c.Details)
					conbin = consrc
					condef = tree.NewDString(fmt.Sprintf("CHECK (%s)", c.Details))
				}

				if err := addRow(
					oid,                                        // oid
					dNameOrNull(name),                          // conname
					pgNamespaceForDB(db, h).Oid,                // connamespace
					contype,                                    // contype
					tree.DBoolFalse,                            // condeferrable
					tree.DBoolFalse,                            // condeferred
					tree.MakeDBool(tree.DBool(!c.Unvalidated)), // convalidated
					h.TableOid(db, table),                      // conrelid
					oidZero,                                    // contypid
					conindid,                                   // conindid
					confrelid,                                  // confrelid
					confupdtype,                                // confupdtype
					confdeltype,                                // confdeltype
					confmatchtype,                              // confmatchtype
					tree.DBoolTrue,                             // conislocal
					zeroVal,                                    // coninhcount
					tree.DBoolTrue,                             // connoinherit
					conkey,                                     // conkey
					confkey,                                    // confkey
					tree.DNull,                                 // conpfeqop
					tree.DNull,                                 // conppeqop
					tree.DNull,                                 // conffeqop
					tree.DNull,                                 // conexclop
					conbin,                                     // conbin
					consrc,                                     // consrc
					condef,                                     // condef
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

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

// See https://www.postgresql.org/docs/9.6/static/catalog-pg-database.html.
var pgCatalogDatabaseTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_database (
	oid OID,
	datname Name,
	datdba OID,
	encoding INT,
	datcollate STRING,
	datctype STRING,
	datistemplate BOOL,
	datallowconn BOOL,
	datconnlimit INT,
	datlastsysoid OID,
	datfrozenxid INT,
	datminmxid INT,
	dattablespace OID,
	datacl STRING[]
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor) error {
			return addRow(
				h.DBOid(db),                // oid
				tree.NewDName(db.Name),     // datname
				tree.DNull,                 // datdba
				builtins.DatEncodingUTFId,  // encoding
				builtins.DatEncodingEnUTF8, // datcollate
				builtins.DatEncodingEnUTF8, // datctype
				tree.DBoolFalse,            // datistemplate
				tree.DBoolTrue,             // datallowconn
				negOneVal,                  // datconnlimit
				tree.DNull,                 // datlastsysoid
				tree.DNull,                 // datfrozenxid
				tree.DNull,                 // datminmxid
				oidZero,                    // dattablespace
				tree.DNull,                 // datacl
			)
		})
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
)

// See https://www.postgresql.org/docs/9.6/static/catalog-pg-depend.html.
//
// pg_depend is a fairly complex table that details many different kinds of
// relationships between database objects. We do not implement the vast
// majority of this table, as it is mainly used by pgjdbc to address a
// deficiency in pg_constraint that was removed in postgres v9.0 with the
// addition of the conindid column. To provide backward compatibility with
// pgjdbc drivers before https://github.com/pgjdbc/pgjdbc/pull/689, we
// provide those rows in pg_depend that track the dependency of foreign key
// constraints on their supporting index entries in pg_class.
var pgCatalogDependTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_depend (
  classid OID,
  objid OID,
  objsubid INT,
  refclassid OID,
  refobjid OID,
  refobjsubid INT,
  deptype CHAR
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		db, err := getDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), pgCatalogName)
		if err != nil {
			return errors.New("could not find pg_catalog")
		}
		pgConstraintDesc, err := getTableDesc(
			ctx,
			p.txn,
			p.getVirtualTabler(),
			tree.NewTableName(pgCatalogName, tree.Name("pg_constraint")),
		)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_constraint")
		}
		pgConstraintTableOid := h.TableOid(db, pgConstraintDesc)

		pgClassDesc, err := getTableDesc(
			ctx,
			p.txn,
			p.getVirtualTabler(),
			tree.NewTableName(pgCatalogName, tree.Name("pg_class")),
		)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_class")
		}
		pgClassTableOid := h.TableOid(db, pgClassDesc)

		return forEachTableDescWithTableLookup(ctx, p, prefix, func(
			db *sqlbase.DatabaseDescriptor,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			info, err := table.GetConstraintInfoWithLookup(tableLookup.tableOrErr)
			if err != nil {
				return err
			}
			for _, c := range info {
				if c.Kind != sqlbase.ConstraintTypeFK {
					continue
				}
				referencedDB, _ := tableLookup(c.ReferencedTable.ID)
				if referencedDB == nil {
					panic(fmt.Sprintf("could not find database of %+v", c.ReferencedTable))
				}

				constraintOid := h.ForeignKeyConstraintOid(db, table, c.FK)
				refObjID := h.IndexOid(referencedDB, c.ReferencedTable, c.ReferencedIndex)

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

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-description.html.
var pgCatalogDescriptionTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_description (
	objoid OID,
	classoid OID,
	objsubid INT,
	description STRING
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Comments on database objects are not currently supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-enum.html.
var pgCatalogEnumTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_enum (
  oid OID,
  enumtypid OID,
  enumsortorder FLOAT,
  enumlabel STRING
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Enum types are not currently supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-extension.html.
var pgCatalogExtensionTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_extension (
  extname NAME,
  extowner OID,
  extnamespace OID,
  extrelocatable BOOL,
  extversion STRING,
  extconfig STRING,
  extcondition STRING
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Extensions are not supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/10/static/catalog-pg-foreign-data-wrapper.html
var pgCatalogForeignDataWrapperTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_foreign_data_wrapper (
  oid OID,
  fdwname NAME,
  fdwowner OID,
  fdwhandler OID,
  fdwvalidator OID,
  fdwacl STRING[],
  fdwoptions STRING[]
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Foreign data wrappers are not supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-foreign-server.html.
var pgCatalogForeignServerTable = virtualSchemaTable{
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
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Foreign servers are not supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-foreign-table.html.
var pgCatalogForeignTableTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_foreign_table (
  ftrelid OID,
  ftserver OID,
  ftoptions STRING[]
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Foreign tables are not supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-index.html.
var pgCatalogIndexTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_index (
    indexrelid OID,
    indrelid OID,
    indnatts INT,
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
    indcollation INT,
    indclass INT,
    indoption INT,
    indexprs STRING,
    indpred STRING
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			tableOid := h.TableOid(db, table)
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				isMutation, isWriteOnly :=
					table.GetIndexMutationCapabilities(index.ID)
				isReady := isMutation && isWriteOnly
				indkey, err := colIDArrayToVector(index.ColumnIDs)
				if err != nil {
					return err
				}
				return addRow(
					h.IndexOid(db, table, index), // indexrelid
					tableOid,                     // indrelid
					tree.NewDInt(tree.DInt(len(index.ColumnNames))),                                          // indnatts
					tree.MakeDBool(tree.DBool(index.Unique)),                                                 // indisunique
					tree.MakeDBool(tree.DBool(table.IsPhysicalTable() && index.ID == table.PrimaryIndex.ID)), // indisprimary
					tree.DBoolFalse,                          // indisexclusion
					tree.MakeDBool(tree.DBool(index.Unique)), // indimmediate
					tree.DBoolFalse,                          // indisclustered
					tree.MakeDBool(tree.DBool(!isMutation)),  // indisvalid
					tree.DBoolFalse,                          // indcheckxmin
					tree.MakeDBool(tree.DBool(isReady)),      // indisready
					tree.DBoolTrue,                           // indislive
					tree.DBoolFalse,                          // indisreplident
					indkey,                                   // indkey
					zeroVal,                                  // indcollation
					zeroVal,                                  // indclass
					zeroVal,                                  // indoption
					tree.DNull,                               // indexprs
					tree.DNull,                               // indpred
				)
			})
		})
	},
}

// See: https://www.postgresql.org/docs/9.6/static/view-pg-indexes.html.
//
// Note that crdb_oid is an extension of the schema to much more easily map
// index OIDs to the corresponding index definition.
var pgCatalogIndexesTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_indexes (
	crdb_oid OID,
	schemaname NAME,
	tablename NAME,
	indexname NAME,
	tablespace NAME,
	indexdef STRING
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				def, err := indexDefFromDescriptor(ctx, p, db, table, index)
				if err != nil {
					return err
				}
				return addRow(
					h.IndexOid(db, table, index),    // oid
					pgNamespaceForDB(db, h).NameStr, // schemaname
					tree.NewDName(table.Name),       // tablename
					tree.NewDName(index.Name),       // indexname
					tree.DNull,                      // tablespace
					tree.NewDString(def),            // indexdef
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
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
) (string, error) {
	indexDef := tree.CreateIndex{
		Name: tree.Name(index.Name),
		Table: tree.NormalizableTableName{
			TableNameReference: tree.NewTableName(tree.Name(db.Name), tree.Name(table.Name)),
		},
		Unique:  index.Unique,
		Columns: make(tree.IndexElemList, len(index.ColumnNames)),
		Storing: make(tree.NameList, len(index.StoreColumnNames)),
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
		parentTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
		if err != nil {
			return "", err
		}
		parentDb, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, parentTable.ParentID)
		if err != nil {
			return "", err
		}
		var sharedPrefixLen int
		for _, ancestor := range intl.Ancestors {
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
		}
		fields := index.ColumnNames[:sharedPrefixLen]
		intlDef := &tree.InterleaveDef{
			Parent: &tree.NormalizableTableName{
				TableNameReference: tree.NewTableName(
					tree.Name(parentDb.Name), tree.Name(parentTable.Name)),
			},
			Fields: make(tree.NameList, len(fields)),
		}
		for i, field := range fields {
			intlDef.Fields[i] = tree.Name(field)
		}
		indexDef.Interleave = intlDef
	}
	return indexDef.String(), nil
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-inherits.html.
var pgCatalogInheritsTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_inherits (
	inhrelid OID,
	inhparent OID,
	inhseqno INT
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Table inheritance is not supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-namespace.html.
var pgCatalogNamespaceTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_namespace (
	oid OID,
	nspname NAME NOT NULL,
	nspowner OID,
	nspacl STRING[]
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor) error {
			return addRow(
				h.NamespaceOid(db.Name),  // oid
				tree.NewDString(db.Name), // nspname
				tree.DNull,               // nspowner
				tree.DNull,               // nspacl
			)
		})
	},
}

// See: https://www.postgresql.org/docs/10/static/catalog-pg-operator.html.
var pgCatalogOperatorTable = virtualSchemaTable{
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
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
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

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-proc.html
var pgCatalogProcTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_proc (
	oid OID,
	proname NAME,
	pronamespace OID,
	proowner OID,
	prolang OID,
	procost FLOAT,
	prorows FLOAT,
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
	pronargs INT,
	pronargdefaults INT,
	prorettype OID,
	proargtypes STRING,
	proallargtypes STRING[],
	proargmodes STRING[],
	proargnames STRING[],
	proargdefaults STRING,
	protrftypes OID[],
	prosrc STRING,
	probin STRING,
	proconfig STRING[],
	proacl STRING[]
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		dbDesc, err := getDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), pgCatalogName)
		if err != nil {
			return err
		}
		nspOid := pgNamespaceForDB(dbDesc, h).Oid
		for name, builtins := range builtins.Builtins {
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
			for _, builtin := range builtins {
				dName := tree.NewDName(name)
				dSrc := tree.NewDString(name)
				isAggregate := builtin.Class == tree.AggregateClass
				isWindow := builtin.Class == tree.WindowClass

				var retType tree.Datum
				isRetSet := false
				if fixedRetType := builtin.FixedReturnType(); fixedRetType != nil {
					var retOid oid.Oid
					if t, ok := fixedRetType.(types.TTable); ok {
						isRetSet = true
						// Functions returning tables with zero, or more than one
						// columns are marked to return "anyelement"
						// (e.g. `unnest`)
						retOid = oid.T_anyelement
						if len(t.Cols) == 1 {
							// Functions returning tables with exactly one column
							// are marked to return the type of that column
							// (e.g. `generate_series`).
							retOid = t.Cols[0].Oid()
						}
					} else {
						retOid = fixedRetType.Oid()
					}
					retType = tree.NewDOid(tree.DInt(retOid))
				}

				argTypes := builtin.Types
				dArgTypes := make([]string, len(argTypes.Types()))
				for i, argType := range argTypes.Types() {
					dArgType := argType.Oid()
					dArgTypes[i] = strconv.Itoa(int(dArgType))
				}
				dArgTypeString := strings.Join(dArgTypes, ", ")

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
				err := addRow(
					h.BuiltinOid(name, &builtin), // oid
					dName,                                       // proname
					nspOid,                                      // pronamespace
					tree.DNull,                                  // proowner
					oidZero,                                     // prolang
					tree.DNull,                                  // procost
					tree.DNull,                                  // prorows
					variadicType,                                // provariadic
					tree.DNull,                                  // protransform
					tree.MakeDBool(tree.DBool(isAggregate)),     // proisagg
					tree.MakeDBool(tree.DBool(isWindow)),        // proiswindow
					tree.DBoolFalse,                             // prosecdef
					tree.MakeDBool(tree.DBool(!builtin.Impure)), // proleakproof
					tree.DBoolFalse,                             // proisstrict
					tree.MakeDBool(tree.DBool(isRetSet)),        // proretset
					tree.DNull,                                  // provolatile
					tree.DNull,                                  // proparallel
					tree.NewDInt(tree.DInt(builtin.Types.Length())), // pronargs
					tree.NewDInt(tree.DInt(0)),                      // pronargdefaults
					retType,                                         // prorettype
					tree.NewDString(dArgTypeString), // proargtypes
					tree.DNull,                      // proallargtypes
					argmodes,                        // proargmodes
					tree.DNull,                      // proargnames
					tree.DNull,                      // proargdefaults
					tree.DNull,                      // protrftypes
					dSrc,                            // prosrc
					tree.DNull,                      // probin
					tree.DNull,                      // proconfig
					tree.DNull,                      // proacl
				)
				if err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-range.html.
var pgCatalogRangeTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_range (
	rngtypid OID,
	rngsubtype OID,
	rngcollation OID,
	rngsubopc OID,
	rngcanonical INT,
	rngsubdiff INT
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// We currently do not support any range types, so this table is empty.
		// This table should be populated when any range types are added to
		// oidToDatum (and therefore pg_type).
		return nil
	},
}

// See: https://www.postgresql.org/docs/10/static/catalog-pg-rewrite.html.
var pgCatalogRewriteTable = virtualSchemaTable{
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
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// Rewrite rules are not supported.
		return nil
	},
}

// See: https://www.postgresql.org/docs/9.6/static/view-pg-roles.html.
var pgCatalogRolesTable = virtualSchemaTable{
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
	rolconnlimit INT,
	rolpassword STRING,
	rolvaliduntil TIMESTAMPTZ,
	rolbypassrls BOOL,
	rolconfig STRING[]
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// We intentionally do not check if the user has access to system.user.
		// Because Postgres allows access to pg_roles by non-privileged users, we
		// need to do the same. This shouldn't be an issue, because pg_roles doesn't
		// include sensitive information such as password hashes.
		h := makeOidHasher()
		return forEachRole(ctx, p,
			func(username string, isRole bool) error {
				isRoot := tree.DBool(username == security.RootUser || username == sqlbase.AdminRole)
				isRoleDBool := tree.DBool(isRole)
				return addRow(
					h.UserOid(username),          // oid
					tree.NewDName(username),      // rolname
					tree.MakeDBool(isRoot),       // rolsuper
					tree.MakeDBool(isRoleDBool),  // rolinherit. Roles inherit by default.
					tree.MakeDBool(isRoot),       // rolcreaterole
					tree.MakeDBool(isRoot),       // rolcreatedb
					tree.DBoolFalse,              // rolcatupdate
					tree.MakeDBool(!isRoleDBool), // rolcanlogin. Only users can login.
					tree.DBoolFalse,              // rolreplication
					negOneVal,                    // rolconnlimit
					passwdStarString,             // rolpassword
					tree.DNull,                   // rolvaliduntil
					tree.DBoolFalse,              // rolbypassrls
					tree.DNull,                   // rolconfig
				)
			})
	},
}

// See: https://www.postgresql.org/docs/10/static/catalog-pg-sequence.html
var pgCatalogSequencesTable = virtualSchemaTable{
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
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			if !table.IsSequence() {
				return nil
			}
			opts := table.SequenceOpts
			return addRow(
				h.TableOid(db, table),                   // seqrelid
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

// See: https://www.postgresql.org/docs/9.6/static/view-pg-settings.html.
var pgCatalogSettingsTable = virtualSchemaTable{
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
    sourceline INT,
    pending_restart BOOL
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			value := gen.Get(&p.extendedEvalCtx)
			valueDatum := tree.NewDString(value)
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
				valueDatum,                              // boot_val
				valueDatum,                              // reset_val
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

// See: https://www.postgresql.org/docs/9.6/static/view-pg-tables.html.
var pgCatalogTablesTable = virtualSchemaTable{
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
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			if table.IsView() {
				return nil
			}
			return addRow(
				tree.NewDName(db.Name),    // schemaname
				tree.NewDName(table.Name), // tablename
				tree.DNull,                // tableowner
				tree.DNull,                // tablespace
				tree.MakeDBool(tree.DBool(table.IsPhysicalTable())), // hasindexes
				tree.DBoolFalse,                                     // hasrules
				tree.DBoolFalse,                                     // hastriggers
				tree.DBoolFalse,                                     // rowsecurity
			)
		})
	},
}

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-tablespace.html
var pgCatalogTablespaceTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_tablespace (
  oid OID,
  spcname NAME,
  spcowner OID,
  spclocation TEXT,
  spcacl TEXT[],
  spcoptions TEXT[]
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		return addRow(
			oidZero, // oid
			tree.NewDString("pg_default"), // spcname
			tree.DNull,                    // spcowner
			tree.DNull,                    // spclocation
			tree.DNull,                    // spcacl
			tree.DNull,                    // spcoptions
		)
	},
}

// See: https://www.postgresql.org/docs/10/static/catalog-pg-trigger.html
var pgCatalogTriggerTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_trigger (
  oid OID,
  tgrelid OID,
  tgname NAME,
  tgfoid OID,
  tgtype INT,
  tgenabled TEXT,
  tgisinternal BOOL,
  tgconstrrelid OID,
  tgconstrindid OID,
  tgconstraint OID,
  tgdeferrable BOOL,
  tginitdeferred BOOL,
  tgnargs INT,
  tgattr INT2VECTOR,
  tgargs BYTEA,
  tgqual TEXT,
  tgoldtable NAME,
  tgnewtable NAME
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
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
	_ = typTypeEnum
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
	_ = typCategoryArray
	_ = typCategoryComposite
	_ = typCategoryEnum
	_ = typCategoryGeometric
	_ = typCategoryNetworkAddr
	_ = typCategoryPseudo
	_ = typCategoryRange
	_ = typCategoryBitString
	_ = typCategoryUnknown

	typDelim = tree.NewDString(",")
)

// See: https://www.postgresql.org/docs/9.6/static/catalog-pg-type.html.
var pgCatalogTypeTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_type (
	oid OID,
	typname NAME NOT NULL,
	typnamespace OID,
	typowner OID,
	typlen INT,
	typbyval BOOL,
	typtype CHAR,
	typcategory CHAR,
	typispreferred BOOL,
	typisdefined BOOL,
	typdelim CHAR,
	typrelid OID,
	typelem OID,
	typarray OID,
	typinput OID,
	typoutput OID,
	typreceive OID,
	typsend OID,
	typmodin OID,
	typmodout OID,
	typanalyze OID,
	typalign CHAR,
	typstorage CHAR,
	typnotnull BOOL,
	typbasetype OID,
	typtypmod INT,
	typndims INT,
	typcollation OID,
	typdefaultbin STRING,
	typdefault STRING,
	typacl STRING[]
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		for o, typ := range types.OidToType {
			cat := typCategory(typ)
			typElem := oidZero
			typArray := oidZero
			builtinPrefix := builtins.PGIOBuiltinPrefix(typ)
			if cat == typCategoryArray {
				if typ == types.IntVector {
					// IntVector needs a special case because its a special snowflake
					// type. It's just like an Int2Array, but it has its own OID. We
					// can't just wrap our Int2Array type in an OID wrapper, though,
					// because Int2Array is not an exported, first-class type - it's an
					// input-only type that translates immediately to int8array. This
					// would go away if we decided to export Int2Array as a real type.
					typElem = tree.NewDOid(tree.DInt(oid.T_int2))
				} else {
					builtinPrefix = "array_"
					typElem = tree.NewDOid(tree.DInt(types.UnwrapType(typ).(types.TArray).Typ.Oid()))
				}
			} else {
				typArray = tree.NewDOid(tree.DInt(types.TArray{Typ: typ}.Oid()))
			}
			typname := types.PGDisplayName(typ)

			if err := addRow(
				tree.NewDOid(tree.DInt(o)), // oid
				tree.NewDName(typname),     // typname
				pgNamespacePGCatalog.Oid,   // typnamespace
				tree.DNull,                 // typowner
				typLen(typ),                // typlen
				typByVal(typ),              // typbyval
				typTypeBase,                // typtype
				cat,                        // typcategory
				tree.DBoolFalse,            // typispreferred
				tree.DBoolTrue,             // typisdefined
				typDelim,                   // typdelim
				oidZero,                    // typrelid
				typElem,                    // typelem
				typArray,                   // typarray

				// regproc references
				h.RegProc(builtinPrefix+"in"),   // typinput
				h.RegProc(builtinPrefix+"out"),  // typoutput
				h.RegProc(builtinPrefix+"recv"), // typreceive
				h.RegProc(builtinPrefix+"send"), // typsend
				oidZero, // typmodin
				oidZero, // typmodout
				oidZero, // typanalyze

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
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// See: https://www.postgresql.org/docs/10/static/view-pg-user.html
var pgCatalogUserTable = virtualSchemaTable{
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
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachRole(ctx, p,
			func(username string, isRole bool) error {
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

// See: https://www.postgresql.org/docs/10/static/view-pg-user.html
var pgCatalogUserMappingTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_user_mapping (
  oid OID,
  umuser OID,
  umserver OID,
  umoptions TEXT[]
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		// This table stores the mapping to foreign server users.
		// Foreign servers are not supported.
		return nil
	},
}

// typOid is the only OID generation approach that does not use oidHasher, because
// object identifiers for types are not arbitrary, but instead need to be kept in
// sync with Postgres.
func typOid(typ types.T) tree.Datum {
	return tree.NewDOid(tree.DInt(typ.Oid()))
}

func typLen(typ types.T) *tree.DInt {
	if sz, variable := tree.DatumTypeSize(typ); !variable {
		return tree.NewDInt(tree.DInt(sz))
	}
	return negOneVal
}

func typByVal(typ types.T) tree.Datum {
	_, variable := tree.DatumTypeSize(typ)
	return tree.MakeDBool(tree.DBool(!variable))
}

// typColl returns the collation OID for a given type.
// The default collation is en-US, which is equivalent to but spelled
// differently than the default database collation, en_US.utf8.
func typColl(typ types.T, h oidHasher) tree.Datum {
	if typ.FamilyEqual(types.Any) {
		return oidZero
	} else if typ.Equivalent(types.String) || typ.Equivalent(types.TArray{Typ: types.String}) {
		return h.CollationOid(defaultCollationTag)
	} else if typ.FamilyEqual(types.FamCollatedString) {
		return h.CollationOid(typ.(types.TCollatedString).Locale)
	}
	return oidZero
}

// This mapping should be kept sync with PG's categorization.
var datumToTypeCategory = map[reflect.Type]*tree.DString{
	reflect.TypeOf(types.Any):         typCategoryPseudo,
	reflect.TypeOf(types.Bool):        typCategoryBoolean,
	reflect.TypeOf(types.Bytes):       typCategoryUserDefined,
	reflect.TypeOf(types.Date):        typCategoryDateTime,
	reflect.TypeOf(types.Time):        typCategoryDateTime,
	reflect.TypeOf(types.Float):       typCategoryNumeric,
	reflect.TypeOf(types.Int):         typCategoryNumeric,
	reflect.TypeOf(types.Interval):    typCategoryTimespan,
	reflect.TypeOf(types.JSON):        typCategoryUserDefined,
	reflect.TypeOf(types.Decimal):     typCategoryNumeric,
	reflect.TypeOf(types.String):      typCategoryString,
	reflect.TypeOf(types.Timestamp):   typCategoryDateTime,
	reflect.TypeOf(types.TimestampTZ): typCategoryDateTime,
	reflect.TypeOf(types.FamTuple):    typCategoryPseudo,
	reflect.TypeOf(types.FamTable):    typCategoryPseudo,
	reflect.TypeOf(types.Oid):         typCategoryNumeric,
	reflect.TypeOf(types.UUID):        typCategoryUserDefined,
	reflect.TypeOf(types.INet):        typCategoryNetworkAddr,
}

func typCategory(typ types.T) tree.Datum {
	if typ.FamilyEqual(types.FamArray) {
		return typCategoryArray
	}
	return datumToTypeCategory[reflect.TypeOf(types.UnwrapType(typ))]
}

// See: https://www.postgresql.org/docs/9.6/static/view-pg-views.html.
var pgCatalogViewsTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_views (
	schemaname NAME,
	viewname NAME,
	viewowner STRING,
	definition STRING
);
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, prefix, func(db *sqlbase.DatabaseDescriptor, desc *sqlbase.TableDescriptor) error {
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
				tree.NewDName(db.Name),          // schemaname
				tree.NewDName(desc.Name),        // viewname
				tree.DNull,                      // viewowner
				tree.NewDString(desc.ViewQuery), // definition
			)
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

type oidTypeTag uint8

const (
	_ oidTypeTag = iota
	namespaceTypeTag
	databaseTypeTag
	tableTypeTag
	indexTypeTag
	columnTypeTag
	checkConstraintTypeTag
	fkConstraintTypeTag
	pKeyConstraintTypeTag
	uniqueConstraintTypeTag
	functionTypeTag
	userTypeTag
	collationTypeTag
)

func (h oidHasher) writeTypeTag(tag oidTypeTag) {
	h.writeUInt8(uint8(tag))
}

func (h oidHasher) getOid() *tree.DOid {
	i := h.h.Sum32()
	h.h.Reset()
	return tree.NewDOid(tree.DInt(i))
}

func (h oidHasher) writeDB(db *sqlbase.DatabaseDescriptor) {
	h.writeUInt32(uint32(db.ID))
	h.writeStr(db.Name)
}

func (h oidHasher) writeTable(table *sqlbase.TableDescriptor) {
	h.writeUInt32(uint32(table.ID))
	h.writeStr(table.Name)
}

func (h oidHasher) writeIndex(index *sqlbase.IndexDescriptor) {
	h.writeUInt32(uint32(index.ID))
}

func (h oidHasher) writeColumn(column *sqlbase.ColumnDescriptor) {
	h.writeUInt32(uint32(column.ID))
	h.writeStr(column.Name)
}

func (h oidHasher) writeCheckConstraint(check *sqlbase.TableDescriptor_CheckConstraint) {
	h.writeStr(check.Name)
	h.writeStr(check.Expr)
}

func (h oidHasher) writeForeignKeyReference(fk *sqlbase.ForeignKeyReference) {
	h.writeUInt32(uint32(fk.Table))
	h.writeUInt32(uint32(fk.Index))
	h.writeStr(fk.Name)
}

func (h oidHasher) NamespaceOid(namespace string) *tree.DOid {
	h.writeTypeTag(namespaceTypeTag)
	h.writeStr(namespace)
	return h.getOid()
}

func (h oidHasher) DBOid(db *sqlbase.DatabaseDescriptor) *tree.DOid {
	h.writeTypeTag(databaseTypeTag)
	h.writeDB(db)
	return h.getOid()
}

func (h oidHasher) TableOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor,
) *tree.DOid {
	h.writeTypeTag(tableTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	return h.getOid()
}

func (h oidHasher) IndexOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) *tree.DOid {
	h.writeTypeTag(indexTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}

func (h oidHasher) ColumnOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, column *sqlbase.ColumnDescriptor,
) *tree.DOid {
	h.writeTypeTag(columnTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeColumn(column)
	return h.getOid()
}

func (h oidHasher) CheckConstraintOid(
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	check *sqlbase.TableDescriptor_CheckConstraint,
) *tree.DOid {
	h.writeTypeTag(checkConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeCheckConstraint(check)
	return h.getOid()
}

func (h oidHasher) PrimaryKeyConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, pkey *sqlbase.IndexDescriptor,
) *tree.DOid {
	h.writeTypeTag(pKeyConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(pkey)
	return h.getOid()
}

func (h oidHasher) ForeignKeyConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, fk *sqlbase.ForeignKeyReference,
) *tree.DOid {
	h.writeTypeTag(fkConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeForeignKeyReference(fk)
	return h.getOid()
}

func (h oidHasher) UniqueConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) *tree.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}

func (h oidHasher) BuiltinOid(name string, builtin *tree.Builtin) *tree.DOid {
	h.writeTypeTag(functionTypeTag)
	h.writeStr(name)
	h.writeStr(builtin.Types.String())
	return h.getOid()
}

func (h oidHasher) RegProc(name string) tree.Datum {
	builtin, ok := builtins.Builtins[name]
	if !ok {
		return tree.DNull
	}
	return h.BuiltinOid(name, &builtin[0]).AsRegProc(name)
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

// pgNamespace represents a PostgreSQL-style namespace, which is the structure
// underlying SQL schemas: "each namespace can have a separate collection of
// relations, types, etc. without name conflicts."
//
// CockroachDB does not have a notion of namespaces, but some clients rely on
// a relationship between databases and namespaces existing in pg_catalog. To
// accommodate this use case, we mock out the existence of namespaces, splitting
// databases into one of four namespaces. These namespaces mirror Postgres, with
// the addition of a "system" namespace so that only user-created objects exist
// in the "public" namespace.
type pgNamespace struct {
	name string

	NameStr tree.Datum
	Oid     tree.Datum
}

var (
	pgNamespaceSystem            = &pgNamespace{name: "system"}
	pgNamespacePGCatalog         = &pgNamespace{name: "pg_catalog"}
	pgNamespaceInformationSchema = &pgNamespace{name: "information_schema"}

	pgNamespaces = []*pgNamespace{
		pgNamespaceSystem,
		pgNamespacePGCatalog,
		pgNamespaceInformationSchema,
	}
)

func init() {
	h := makeOidHasher()
	for _, nsp := range pgNamespaces {
		nsp.NameStr = tree.NewDName(nsp.name)
		nsp.Oid = h.NamespaceOid(nsp.name)
	}
}

// pgNamespaceForDB maps a DatabaseDescriptor to its corresponding pgNamespace.
// See the comment above pgNamespace for more details.
func pgNamespaceForDB(db *sqlbase.DatabaseDescriptor, h oidHasher) *pgNamespace {
	switch db.Name {
	case sqlbase.SystemDB.Name:
		return pgNamespaceSystem
	case pgCatalogName:
		return pgNamespacePGCatalog
	case informationSchemaName:
		return pgNamespaceInformationSchema
	default:
		return &pgNamespace{name: db.Name, NameStr: tree.NewDName(db.Name), Oid: h.NamespaceOid(db.Name)}
	}
}
