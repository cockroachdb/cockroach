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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
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
	"golang.org/x/net/context"
	"golang.org/x/text/collate"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var (
	oidZero   = parser.NewDOid(0)
	zeroVal   = parser.DZero
	negOneVal = parser.NewDInt(-1)
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
		pgCatalogClassTable,
		pgCatalogCollationTable,
		pgCatalogConstraintTable,
		pgCatalogDatabaseTable,
		pgCatalogDependTable,
		pgCatalogDescriptionTable,
		pgCatalogEnumTable,
		pgCatalogExtensionTable,
		pgCatalogForeignServerTable,
		pgCatalogForeignTableTable,
		pgCatalogIndexTable,
		pgCatalogIndexesTable,
		pgCatalogInheritsTable,
		pgCatalogNamespaceTable,
		pgCatalogProcTable,
		pgCatalogRangeTable,
		pgCatalogRolesTable,
		pgCatalogSettingsTable,
		pgCatalogTablesTable,
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		h.writeStr(cockroachIndexEncoding)
		return addRow(
			h.getOid(),
			parser.NewDName(cockroachIndexEncoding),
			parser.DNull,
			parser.NewDString("i"),
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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			colNum := 0
			return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				colNum++
				if column.DefaultExpr == nil {
					// pg_attrdef only expects rows for columns with default values.
					return nil
				}
				defSrc := parser.NewDString(*column.DefaultExpr)
				return addRow(
					h.ColumnOid(db, table, column),      // oid
					h.TableOid(db, table),               // adrelid
					parser.NewDInt(parser.DInt(colNum)), // adnum
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
	attacl STRING,
	attoptions STRING,
	attfdwoptions STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			// addColumn adds adds either a table or a index column to the pg_attribute table.
			addColumn := func(column *sqlbase.ColumnDescriptor, attRelID parser.Datum, colNum int) error {
				colTyp := column.Type.ToDatumType()
				return addRow(
					attRelID,                            // attrelid
					parser.NewDName(column.Name),        // attname
					typOid(colTyp),                      // atttypid
					zeroVal,                             // attstattarget
					typLen(colTyp),                      // attlen
					parser.NewDInt(parser.DInt(colNum)), // attnum
					zeroVal,      // attndims
					negOneVal,    // attcacheoff
					negOneVal,    // atttypmod
					parser.DNull, // attbyval (see pg_type.typbyval)
					parser.DNull, // attstorage
					parser.DNull, // attalign
					parser.MakeDBool(parser.DBool(!column.Nullable)),          // attnotnull
					parser.MakeDBool(parser.DBool(column.DefaultExpr != nil)), // atthasdef
					parser.MakeDBool(false),                                   // attisdropped
					parser.MakeDBool(true),                                    // attislocal
					zeroVal,                                                   // attinhcount
					typColl(colTyp, h),                                        // attcollation
					parser.DNull,                                              // attacl
					parser.DNull,                                              // attoptions
					parser.DNull,                                              // attfdwoptions
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

var (
	relKindTable = parser.NewDString("r")
	relKindIndex = parser.NewDString("i")
	relKindView  = parser.NewDString("v")
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
	relacl STRING,
	reloptions STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			// Table.
			relKind := relKindTable
			if table.IsView() {
				// The only difference between tables and views is the relkind column.
				relKind = relKindView
			}
			if err := addRow(
				h.TableOid(db, table),       // oid
				parser.NewDName(table.Name), // relname
				pgNamespaceForDB(db, h).Oid, // relnamespace
				oidZero,                     // reltype (PG creates a composite type in pg_type for each table)
				parser.DNull,                // relowner
				parser.DNull,                // relam
				oidZero,                     // relfilenode
				oidZero,                     // reltablespace
				parser.DNull,                // relpages
				parser.DNull,                // reltuples
				zeroVal,                     // relallvisible
				oidZero,                     // reltoastrelid
				parser.MakeDBool(parser.DBool(table.IsPhysicalTable())), // relhasindex
				parser.MakeDBool(false),                                 // relisshared
				parser.MakeDBool(false),                                 // relistemp
				relKind,                                                 // relkind
				parser.NewDInt(parser.DInt(len(table.Columns))),         // relnatts
				parser.NewDInt(parser.DInt(len(table.Checks))),          // relchecks
				parser.MakeDBool(false),                                 // relhasoids
				parser.MakeDBool(parser.DBool(table.IsPhysicalTable())), // relhaspkey
				parser.MakeDBool(false),                                 // relhasrules
				parser.MakeDBool(false),                                 // relhastriggers
				parser.MakeDBool(false),                                 // relhassubclass
				zeroVal,                                                 // relfrozenxid
				parser.DNull,                                            // relacl
				parser.DNull,                                            // reloptions
			); err != nil {
				return err
			}

			// Indexes.
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				return addRow(
					h.IndexOid(db, table, index), // oid
					parser.NewDName(index.Name),  // relname
					pgNamespaceForDB(db, h).Oid,  // relnamespace
					oidZero,                      // reltype
					parser.DNull,                 // relowner
					parser.DNull,                 // relam
					oidZero,                      // relfilenode
					oidZero,                      // reltablespace
					parser.DNull,                 // relpages
					parser.DNull,                 // reltuples
					zeroVal,                      // relallvisible
					oidZero,                      // reltoastrelid
					parser.MakeDBool(false),      // relhasindex
					parser.MakeDBool(false),      // relisshared
					parser.MakeDBool(false),      // relistemp
					relKindIndex,                 // relkind
					parser.NewDInt(parser.DInt(len(index.ColumnNames))), // relnatts
					zeroVal,                 // relchecks
					parser.MakeDBool(false), // relhasoids
					parser.MakeDBool(false), // relhaspkey
					parser.MakeDBool(false), // relhasrules
					parser.MakeDBool(false), // relhastriggers
					parser.MakeDBool(false), // relhassubclass
					zeroVal,                 // relfrozenxid
					parser.DNull,            // relacl
					parser.DNull,            // reloptions
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		for _, tag := range collate.Supported() {
			collName := tag.String()
			if err := addRow(
				h.CollationOid(collName),    // oid
				parser.NewDString(collName), // collname
				pgNamespacePGCatalog.Oid,    // collnamespace
				parser.DNull,                // collowner
				datEncodingUTFId,            // collencoding
				// It's not clear how to translate a Go collation tag into the format
				// required by LC_COLLATE and LC_CTYPE.
				parser.DNull, // collcollate
				parser.DNull, // collctype
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var (
	conTypeCheck     = parser.NewDString("c")
	conTypeFK        = parser.NewDString("f")
	conTypePKey      = parser.NewDString("p")
	conTypeUnique    = parser.NewDString("u")
	conTypeTrigger   = parser.NewDString("t")
	conTypeExclusion = parser.NewDString("x")

	// Avoid unused warning for constants.
	_ = conTypeTrigger
	_ = conTypeExclusion

	fkActionNone       = parser.NewDString("a")
	fkActionRestrict   = parser.NewDString("r")
	fkActionCascade    = parser.NewDString("c")
	fkActionSetNull    = parser.NewDString("n")
	fkActionSetDefault = parser.NewDString("d")

	// Avoid unused warning for constants.
	_ = fkActionRestrict
	_ = fkActionCascade
	_ = fkActionSetNull
	_ = fkActionSetDefault

	fkMatchTypeFull    = parser.NewDString("f")
	fkMatchTypePartial = parser.NewDString("p")
	fkMatchTypeSimple  = parser.NewDString("s")

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
	conpfeqop STRING,
	conppeqop STRING,
	conffeqop STRING,
	conexclop STRING,
	conbin STRING,
	consrc STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, func(
			db *sqlbase.DatabaseDescriptor,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			info, err := table.GetConstraintInfoWithLookup(tableLookup.tableOrErr)
			if err != nil {
				return err
			}

			for name, c := range info {
				oid := parser.DNull
				contype := parser.DNull
				conindid := oidZero
				confrelid := oidZero
				confupdtype := parser.DNull
				confdeltype := parser.DNull
				confmatchtype := parser.DNull
				conkey := parser.DNull
				confkey := parser.DNull
				consrc := parser.DNull

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

				case sqlbase.ConstraintTypeUnique:
					oid = h.UniqueConstraintOid(db, table, c.Index)
					contype = conTypeUnique
					conindid = h.IndexOid(db, table, c.Index)
					var err error
					conkey, err = colIDArrayToDatum(c.Index.ColumnIDs)
					if err != nil {
						return err
					}

				case sqlbase.ConstraintTypeCheck:
					oid = h.CheckConstraintOid(db, table, c.CheckConstraint)
					contype = conTypeCheck
					// TODO(nvanbenschoten): We currently do not store the referenced columns for a check
					// constraint. We should add an array of column indexes to
					// sqlbase.TableDescriptor_CheckConstraint and use that here.
					consrc = parser.NewDString(c.Details)
				}

				if err := addRow(
					oid,                                            // oid
					dNameOrNull(name),                              // conname
					pgNamespaceForDB(db, h).Oid,                    // connamespace
					contype,                                        // contype
					parser.MakeDBool(false),                        // condeferrable
					parser.MakeDBool(false),                        // condeferred
					parser.MakeDBool(parser.DBool(!c.Unvalidated)), // convalidated
					h.TableOid(db, table),                          // conrelid
					oidZero,                                        // contypid
					conindid,                                       // conindid
					confrelid,                                      // confrelid
					confupdtype,                                    // confupdtype
					confdeltype,                                    // confdeltype
					confmatchtype,                                  // confmatchtype
					parser.MakeDBool(true),                         // conislocal
					zeroVal,                                        // coninhcount
					parser.MakeDBool(true),                         // connoinherit
					conkey,                                         // conkey
					confkey,                                        // confkey
					parser.DNull,                                   // conpfeqop
					parser.DNull,                                   // conppeqop
					parser.DNull,                                   // conffeqop
					parser.DNull,                                   // conexclop
					consrc,                                         // conbin
					consrc,                                         // consrc
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
func colIDArrayToDatum(arr []sqlbase.ColumnID) (parser.Datum, error) {
	if len(arr) == 0 {
		return parser.DNull, nil
	}
	d := parser.NewDArray(parser.TypeInt)
	for _, val := range arr {
		if err := d.Append(parser.NewDInt(parser.DInt(val))); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// colIDArrayToVector returns an INT2VECTOR containing the ColumnIDs, or NULL if
// there are no ColumnIDs.
func colIDArrayToVector(arr []sqlbase.ColumnID) (parser.Datum, error) {
	dArr, err := colIDArrayToDatum(arr)
	if err != nil {
		return nil, err
	}
	if dArr == parser.DNull {
		return dArr, nil
	}
	return parser.NewDIntVectorFromDArray(parser.MustBeDArray(dArr)), nil
}

var (
	// http://doxygen.postgresql.org/pg__wchar_8h.html#a22e0c8b9f59f6e226a5968620b4bb6a9aac3b065b882d3231ba59297524da2f23
	datEncodingUTFId  = parser.NewDInt(6)
	datEncodingEnUTF8 = parser.NewDString("en_US.utf8")
)

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
	datacl STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor) error {
			return addRow(
				h.DBOid(db),              // oid
				parser.NewDName(db.Name), // datname
				parser.DNull,             // datdba
				datEncodingUTFId,         // encoding
				datEncodingEnUTF8,        // datcollate
				datEncodingEnUTF8,        // datctype
				parser.MakeDBool(false),  // datistemplate
				parser.MakeDBool(true),   // datallowconn
				negOneVal,                // datconnlimit
				parser.DNull,             // datlastsysoid
				parser.DNull,             // datfrozenxid
				parser.DNull,             // datminmxid
				parser.DNull,             // dattablespace
				parser.DNull,             // datacl
			)
		})
	},
}
var (
	depTypeNormal        = parser.NewDString("n")
	depTypeAuto          = parser.NewDString("a")
	depTypeInternal      = parser.NewDString("i")
	depTypeExtension     = parser.NewDString("e")
	depTypeAutoExtension = parser.NewDString("x")
	depTypePin           = parser.NewDString("p")

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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		db, err := getDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), pgCatalogName)
		if err != nil {
			return errors.New("could not find pg_catalog")
		}
		pgConstraintDesc, err := getTableDesc(
			ctx,
			p.txn,
			p.getVirtualTabler(),
			&parser.TableName{
				DatabaseName: pgCatalogName,
				TableName:    "pg_constraint"},
		)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_constraint")
		}
		pgConstraintTableOid := h.TableOid(db, pgConstraintDesc)

		pgClassDesc, err := getTableDesc(
			ctx,
			p.txn,
			p.getVirtualTabler(),
			&parser.TableName{
				DatabaseName: pgCatalogName,
				TableName:    "pg_class"},
		)
		if err != nil {
			return errors.New("could not find pg_catalog.pg_class")
		}
		pgClassTableOid := h.TableOid(db, pgClassDesc)

		return forEachTableDescWithTableLookup(ctx, p, func(
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		// Extensions are not supported.
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
  srvacl STRING,
  srvoptions STRING
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
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
  ftoptions STRING
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			tableOid := h.TableOid(db, table)
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				isValid := true
				isReady := false
				for _, mutation := range table.Mutations {
					if mutationIndex := mutation.GetIndex(); mutationIndex != nil {
						if mutationIndex.ID == index.ID {
							isValid = false
							if mutation.State == sqlbase.DescriptorMutation_WRITE_ONLY {
								isReady = true
							}
							break
						}
					}
				}
				indkey, err := colIDArrayToVector(index.ColumnIDs)
				if err != nil {
					return err
				}
				return addRow(
					h.IndexOid(db, table, index), // indexrelid
					tableOid,                     // indrelid
					parser.NewDInt(parser.DInt(len(index.ColumnNames))),                                          // indnatts
					parser.MakeDBool(parser.DBool(index.Unique)),                                                 // indisunique
					parser.MakeDBool(parser.DBool(table.IsPhysicalTable() && index.ID == table.PrimaryIndex.ID)), // indisprimary
					parser.MakeDBool(false),                      // indisexclusion
					parser.MakeDBool(parser.DBool(index.Unique)), // indimmediate
					parser.MakeDBool(false),                      // indisclustered
					parser.MakeDBool(parser.DBool(isValid)),      // indisvalid
					parser.MakeDBool(false),                      // indcheckxmin
					parser.MakeDBool(parser.DBool(isReady)),      // indisready
					parser.MakeDBool(true),                       // indislive
					parser.MakeDBool(false),                      // indisreplident
					indkey,                                       // indkey
					zeroVal,                                      // indcollation
					zeroVal,                                      // indclass
					zeroVal,                                      // indoption
					parser.DNull,                                 // indexprs
					parser.DNull,                                 // indpred
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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				def, err := indexDefFromDescriptor(ctx, p, db, table, index)
				if err != nil {
					return err
				}
				return addRow(
					h.IndexOid(db, table, index),    // oid
					pgNamespaceForDB(db, h).NameStr, // schemaname
					parser.NewDName(table.Name),     // tablename
					parser.NewDName(index.Name),     // indexname
					parser.DNull,                    // tablespace
					parser.NewDString(def),          // indexdef
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
	indexDef := parser.CreateIndex{
		Name: parser.Name(index.Name),
		Table: parser.NormalizableTableName{
			TableNameReference: &parser.TableName{
				DatabaseName: parser.Name(db.Name),
				TableName:    parser.Name(table.Name),
			},
		},
		Unique:  index.Unique,
		Columns: make(parser.IndexElemList, len(index.ColumnNames)),
		Storing: make(parser.NameList, len(index.StoreColumnNames)),
	}
	for i, name := range index.ColumnNames {
		elem := parser.IndexElem{
			Column:    parser.Name(name),
			Direction: parser.Ascending,
		}
		if index.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC {
			elem.Direction = parser.Descending
		}
		indexDef.Columns[i] = elem
	}
	for i, name := range index.StoreColumnNames {
		indexDef.Storing[i] = parser.Name(name)
	}
	if len(index.Interleave.Ancestors) > 0 {
		intl := index.Interleave
		parentTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
		if err != nil {
			return "", err
		}
		var sharedPrefixLen int
		for _, ancestor := range intl.Ancestors {
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
		}
		fields := index.ColumnNames[:sharedPrefixLen]
		intlDef := &parser.InterleaveDef{
			Parent: parser.NormalizableTableName{
				TableNameReference: &parser.TableName{
					TableName: parser.Name(parentTable.Name),
				},
			},
			Fields: make(parser.NameList, len(fields)),
		}
		for i, field := range fields {
			intlDef.Fields[i] = parser.Name(field)
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
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
	aclitem STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor) error {
			return addRow(
				h.NamespaceOid(db.Name),    // oid
				parser.NewDString(db.Name), // nspname
				parser.DNull,               // nspowner
				parser.DNull,               // aclitem
			)
		})
	},
}

var (
	proArgModeInOut    = parser.NewDString("b")
	proArgModeIn       = parser.NewDString("i")
	proArgModeOut      = parser.NewDString("o")
	proArgModeTable    = parser.NewDString("t")
	proArgModeVariadic = parser.NewDString("v")

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
	proallargtypes STRING,
	proargmodes STRING,
	proargnames STRING,
	proargdefaults STRING,
	protrftypes STRING,
	prosrc STRING,
	probin STRING,
	proconfig STRING,
	proacl STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		dbDesc, err := getDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), pgCatalogName)
		if err != nil {
			return err
		}
		nspOid := pgNamespaceForDB(dbDesc, h).Oid
		for name, builtins := range parser.Builtins {
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
				dName := parser.NewDName(name)
				dSrc := parser.NewDString(name)
				isAggregate := builtin.Class() == parser.AggregateClass
				isWindow := builtin.Class() == parser.WindowClass

				var retType parser.Datum
				isRetSet := false
				if fixedRetType := builtin.FixedReturnType(); fixedRetType != nil {
					var retOid oid.Oid
					if t, ok := fixedRetType.(parser.TTable); ok {
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
					retType = parser.NewDOid(parser.DInt(retOid))
				}

				argTypes := builtin.Types
				dArgTypes := make([]string, len(argTypes.Types()))
				for i, argType := range argTypes.Types() {
					dArgType := argType.Oid()
					dArgTypes[i] = strconv.Itoa(int(dArgType))
				}
				dArgTypeString := strings.Join(dArgTypes, ", ")

				var argmodes parser.Datum
				var variadicType parser.Datum
				switch argTypes.(type) {
				case parser.VariadicType:
					argmodes = proArgModeVariadic
					argType := argTypes.Types()[0]
					oid := argType.Oid()
					variadicType = parser.NewDOid(parser.DInt(oid))
				case parser.HomogeneousType:
					argmodes = proArgModeVariadic
					argType := parser.TypeAny
					oid := argType.Oid()
					variadicType = parser.NewDOid(parser.DInt(oid))
				default:
					argmodes = parser.DNull
					variadicType = oidZero
				}
				err := addRow(
					h.BuiltinOid(name, &builtin), // oid
					dName,                                             // proname
					nspOid,                                            // pronamespace
					parser.DNull,                                      // proowner
					oidZero,                                           // prolang
					parser.DNull,                                      // procost
					parser.DNull,                                      // prorows
					variadicType,                                      // provariadic
					parser.DNull,                                      // protransform
					parser.MakeDBool(parser.DBool(isAggregate)),       // proisagg
					parser.MakeDBool(parser.DBool(isWindow)),          // proiswindow
					parser.MakeDBool(false),                           // prosecdef
					parser.MakeDBool(parser.DBool(!builtin.Impure())), // proleakproof
					parser.MakeDBool(false),                           // proisstrict
					parser.MakeDBool(parser.DBool(isRetSet)),          // proretset
					parser.DNull,                                      // provolatile
					parser.DNull,                                      // proparallel
					parser.NewDInt(parser.DInt(builtin.Types.Length())), // pronargs
					parser.NewDInt(parser.DInt(0)),                      // pronargdefaults
					retType, // prorettype
					parser.NewDString(dArgTypeString), // proargtypes
					parser.DNull,                      // proallargtypes
					argmodes,                          // proargmodes
					parser.DNull,                      // proargnames
					parser.DNull,                      // proargdefaults
					parser.DNull,                      // protrftypes
					dSrc,                              // prosrc
					parser.DNull,                      // probin
					parser.DNull,                      // proconfig
					parser.DNull,                      // proacl
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		// We currently do not support any range types, so this table is empty.
		// This table should be populated when any range types are added to
		// oidToDatum (and therefore pg_type).
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
	rolconnlimit INT,
	rolpassword STRING,
	rolvaliduntil TIMESTAMPTZ,
	rolconfig STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		// We intentionally do not check if the user has access to system.user.
		// Because Postgres allows access to pg_roles by non-privileged users, we
		// need to do the same. This shouldn't be an issue, because pg_roles doesn't
		// include sensitive information such as password hashes.
		h := makeOidHasher()
		return forEachUser(ctx, p,
			func(username string) error {
				isRoot := parser.DBool(username == security.RootUser)
				return addRow(
					h.UserOid(username),           // oid
					parser.NewDName(username),     // rolname
					parser.MakeDBool(isRoot),      // rolsuper
					parser.MakeDBool(false),       // rolinherit
					parser.MakeDBool(isRoot),      // rolcreaterole
					parser.MakeDBool(isRoot),      // rolcreatedb
					parser.MakeDBool(false),       // rolcatupdate
					parser.MakeDBool(true),        // rolcanlogin
					negOneVal,                     // rolconnlimit
					parser.NewDString("********"), // rolpassword
					parser.DNull,                  // rolvaliduntil
					parser.NewDString("{}"),       // rolconfig
				)
			})
	},
}

var (
	varTypeString   = parser.NewDString("string")
	settingsCtxUser = parser.NewDString("user")
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			value := gen.Get(p)
			valueDatum := parser.NewDString(value)
			if err := addRow(
				parser.NewDString(strings.ToLower(vName)), // name
				valueDatum,              // setting
				parser.DNull,            // unit
				parser.DNull,            // category
				parser.DNull,            // short_desc
				parser.DNull,            // extra_desc
				settingsCtxUser,         // context
				varTypeString,           // vartype
				parser.DNull,            // source
				parser.DNull,            // min_val
				parser.DNull,            // max_val
				parser.DNull,            // enumvals
				valueDatum,              // boot_val
				valueDatum,              // reset_val
				parser.DNull,            // sourcefile
				parser.DNull,            // sourceline
				parser.MakeDBool(false), // pending_restart
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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			if table.IsView() {
				return nil
			}
			return addRow(
				parser.NewDName(db.Name),    // schemaname
				parser.NewDName(table.Name), // tablename
				parser.DNull,                // tableowner
				parser.DNull,                // tablespace
				parser.MakeDBool(parser.DBool(table.IsPhysicalTable())), // hasindexes
				parser.MakeDBool(false),                                 // hasrules
				parser.MakeDBool(false),                                 // hastriggers
				parser.MakeDBool(false),                                 // rowsecurity
			)
		})
	},
}

var (
	typTypeBase      = parser.NewDString("b")
	typTypeComposite = parser.NewDString("c")
	typTypeDomain    = parser.NewDString("d")
	typTypeEnum      = parser.NewDString("e")
	typTypePseudo    = parser.NewDString("p")
	typTypeRange     = parser.NewDString("r")

	// Avoid unused warning for constants.
	_ = typTypeComposite
	_ = typTypeDomain
	_ = typTypeEnum
	_ = typTypePseudo
	_ = typTypeRange

	// See https://www.postgresql.org/docs/9.6/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE.
	typCategoryArray       = parser.NewDString("A")
	typCategoryBoolean     = parser.NewDString("B")
	typCategoryComposite   = parser.NewDString("C")
	typCategoryDateTime    = parser.NewDString("D")
	typCategoryEnum        = parser.NewDString("E")
	typCategoryGeometric   = parser.NewDString("G")
	typCategoryNetworkAddr = parser.NewDString("I")
	typCategoryNumeric     = parser.NewDString("N")
	typCategoryPseudo      = parser.NewDString("P")
	typCategoryRange       = parser.NewDString("R")
	typCategoryString      = parser.NewDString("S")
	typCategoryTimespan    = parser.NewDString("T")
	typCategoryUserDefined = parser.NewDString("U")
	typCategoryBitString   = parser.NewDString("V")
	typCategoryUnknown     = parser.NewDString("X")

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

	typDelim = parser.NewDString(",")
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
	typacl STRING
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		for o, typ := range parser.OidToType {
			cat := typCategory(typ)
			typElem := oidZero
			builtinPrefix := parser.PGIOBuiltinPrefix(typ)
			if cat == typCategoryArray {
				if typ == parser.TypeIntVector {
					// IntVector needs a special case because its a special snowflake
					// type. It's just like an Int2Array, but it has its own OID. We
					// can't just wrap our Int2Array type in an OID wrapper, though,
					// because Int2Array is not an exported, first-class type - it's an
					// input-only type that translates immediately to int8array. This
					// would go away if we decided to export Int2Array as a real type.
					typElem = parser.NewDOid(parser.DInt(oid.T_int2))
				} else {
					builtinPrefix = "array_"
					typElem = parser.NewDOid(parser.DInt(parser.UnwrapType(typ).(parser.TArray).Typ.Oid()))
				}
			}
			typname := parser.PGDisplayName(typ)

			if err := addRow(
				parser.NewDOid(parser.DInt(o)), // oid
				parser.NewDName(typname),       // typname
				pgNamespacePGCatalog.Oid,       // typnamespace
				parser.DNull,                   // typowner
				typLen(typ),                    // typlen
				typByVal(typ),                  // typbyval
				typTypeBase,                    // typtype
				cat,                            // typcategory
				parser.MakeDBool(false), // typispreferred
				parser.MakeDBool(true),  // typisdefined
				typDelim,                // typdelim
				oidZero,                 // typrelid
				typElem,                 // typelem
				oidZero,                 // typarray

				// regproc references
				h.RegProc(builtinPrefix+"in"),   // typinput
				h.RegProc(builtinPrefix+"out"),  // typoutput
				h.RegProc(builtinPrefix+"recv"), // typreceive
				h.RegProc(builtinPrefix+"send"), // typsend
				oidZero, // typmodin
				oidZero, // typmodout
				oidZero, // typanalyze

				parser.DNull,            // typalign
				parser.DNull,            // typstorage
				parser.MakeDBool(false), // typnotnull
				oidZero,                 // typbasetype
				negOneVal,               // typtypmod
				zeroVal,                 // typndims
				typColl(typ, h),         // typcollation
				parser.DNull,            // typdefaultbin
				parser.DNull,            // typdefault
				parser.DNull,            // typacl
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// typOid is the only OID generation approach that does not use oidHasher, because
// object identifiers for types are not arbitrary, but instead need to be kept in
// sync with Postgres.
func typOid(typ parser.Type) parser.Datum {
	return parser.NewDOid(parser.DInt(typ.Oid()))
}

func typLen(typ parser.Type) *parser.DInt {
	if sz, variable := typ.Size(); !variable {
		return parser.NewDInt(parser.DInt(sz))
	}
	return negOneVal
}

func typByVal(typ parser.Type) parser.Datum {
	_, variable := typ.Size()
	return parser.MakeDBool(parser.DBool(!variable))
}

// typColl returns the collation OID for a given type.
// The default collation is en-US, which is equivalent to but spelled
// differently than the default database collation, en_US.utf8.
func typColl(typ parser.Type, h oidHasher) parser.Datum {
	if typ.FamilyEqual(parser.TypeAny) {
		return oidZero
	} else if typ.Equivalent(parser.TypeString) || typ.Equivalent(parser.TypeStringArray) {
		return h.CollationOid(defaultCollationTag)
	} else if typ.FamilyEqual(parser.TypeCollatedString) {
		return h.CollationOid(typ.(parser.TCollatedString).Locale)
	}
	return oidZero
}

// This mapping should be kept sync with PG's categorization.
var datumToTypeCategory = map[reflect.Type]*parser.DString{
	reflect.TypeOf(parser.TypeAny):         typCategoryPseudo,
	reflect.TypeOf(parser.TypeStringArray): typCategoryArray,
	reflect.TypeOf(parser.TypeIntArray):    typCategoryArray,
	reflect.TypeOf(parser.TypeBool):        typCategoryBoolean,
	reflect.TypeOf(parser.TypeBytes):       typCategoryUserDefined,
	reflect.TypeOf(parser.TypeDate):        typCategoryDateTime,
	reflect.TypeOf(parser.TypeFloat):       typCategoryNumeric,
	reflect.TypeOf(parser.TypeInt):         typCategoryNumeric,
	reflect.TypeOf(parser.TypeInterval):    typCategoryTimespan,
	reflect.TypeOf(parser.TypeDecimal):     typCategoryNumeric,
	reflect.TypeOf(parser.TypeString):      typCategoryString,
	reflect.TypeOf(parser.TypeTimestamp):   typCategoryDateTime,
	reflect.TypeOf(parser.TypeTimestampTZ): typCategoryDateTime,
	reflect.TypeOf(parser.TypeTuple):       typCategoryPseudo,
	reflect.TypeOf(parser.TypeTable):       typCategoryPseudo,
	reflect.TypeOf(parser.TypeOid):         typCategoryNumeric,
}

func typCategory(typ parser.Type) parser.Datum {
	return datumToTypeCategory[reflect.TypeOf(parser.UnwrapType(typ))]
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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(ctx, p, func(db *sqlbase.DatabaseDescriptor, desc *sqlbase.TableDescriptor) error {
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
				parser.NewDName(db.Name),          // schemaname
				parser.NewDName(desc.Name),        // viewname
				parser.DNull,                      // viewowner
				parser.NewDString(desc.ViewQuery), // definition
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
//   be returned as a *parser.DInt, and the running hash will be reset. These are the
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

func (h oidHasher) getOid() *parser.DOid {
	i := h.h.Sum32()
	h.h.Reset()
	return parser.NewDOid(parser.DInt(i))
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

func (h oidHasher) NamespaceOid(namespace string) *parser.DOid {
	h.writeTypeTag(namespaceTypeTag)
	h.writeStr(namespace)
	return h.getOid()
}

func (h oidHasher) DBOid(db *sqlbase.DatabaseDescriptor) *parser.DOid {
	h.writeTypeTag(databaseTypeTag)
	h.writeDB(db)
	return h.getOid()
}

func (h oidHasher) TableOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor,
) *parser.DOid {
	h.writeTypeTag(tableTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	return h.getOid()
}

func (h oidHasher) IndexOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) *parser.DOid {
	h.writeTypeTag(indexTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}

func (h oidHasher) ColumnOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, column *sqlbase.ColumnDescriptor,
) *parser.DOid {
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
) *parser.DOid {
	h.writeTypeTag(checkConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeCheckConstraint(check)
	return h.getOid()
}

func (h oidHasher) PrimaryKeyConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, pkey *sqlbase.IndexDescriptor,
) *parser.DOid {
	h.writeTypeTag(pKeyConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(pkey)
	return h.getOid()
}

func (h oidHasher) ForeignKeyConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, fk *sqlbase.ForeignKeyReference,
) *parser.DOid {
	h.writeTypeTag(fkConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeForeignKeyReference(fk)
	return h.getOid()
}

func (h oidHasher) UniqueConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) *parser.DOid {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}

func (h oidHasher) BuiltinOid(name string, builtin *parser.Builtin) *parser.DOid {
	h.writeTypeTag(functionTypeTag)
	h.writeStr(name)
	h.writeStr(builtin.Types.String())
	return h.getOid()
}

func (h oidHasher) RegProc(name string) parser.Datum {
	builtin, ok := parser.Builtins[name]
	if !ok {
		return parser.DNull
	}
	return h.BuiltinOid(name, &builtin[0]).AsRegProc(name)
}

func (h oidHasher) UserOid(username string) *parser.DOid {
	h.writeTypeTag(userTypeTag)
	h.writeStr(username)
	return h.getOid()
}

func (h oidHasher) CollationOid(collation string) *parser.DOid {
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

	NameStr parser.Datum
	Oid     parser.Datum
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
		nsp.NameStr = parser.NewDName(nsp.name)
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
		return &pgNamespace{name: db.Name, NameStr: parser.NewDName(db.Name), Oid: h.NamespaceOid(db.Name)}
	}
}
