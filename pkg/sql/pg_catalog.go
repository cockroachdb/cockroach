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
	"bytes"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var (
	oidZero   = parser.NewDInt(0)
	zeroVal   = oidZero
	negOneVal = parser.NewDInt(-1)
)

// pgCatalog contains a set of system tables mirroring PostgreSQL's pg_catalog schema.
var pgCatalog = virtualSchema{
	name: "pg_catalog",
	tables: []virtualSchemaTable{
		pgCatalogAttrDefTable,
		pgCatalogAttributeTable,
		pgCatalogClassTable,
		pgCatalogConstraintTable,
		pgCatalogIndexesTable,
		pgCatalogNamespaceTable,
		pgCatalogTablesTable,
	},
}

var pgCatalogAttrDefTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_attrdef (
	oid INT,
	adrelid INT,
	adnum INT,
	adbin STRING,
	adsrc STRING
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				colNum := 0
				return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
					colNum++
					if column.DefaultExpr == nil {
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
			},
		)
	},
}

var pgCatalogAttributeTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_attribute (
	attrelid INT,
	attname STRING,
	atttypid INT,
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
	attacl STRING,
	attoptions STRING,
	attfdwoptions STRING
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				addColumn := func(column *sqlbase.ColumnDescriptor, attRelID parser.Datum, colNum int) error {
					colTyp := column.Type.ToDatumType()
					return addRow(
						attRelID,                            // attrelid
						parser.NewDString(column.Name),      // attname
						h.TypeOid(colTyp),                   // atttypid
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
			},
		)
	},
}

var (
	relKindTable = parser.NewDString("r")
	relKindIndex = parser.NewDString("i")
	relKindView  = parser.NewDString("v")
)

var pgCatalogClassTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_class (
	oid INT,
	relname STRING NOT NULL DEFAULT '',
	relnamespace INT,
	reltype INT,
	relowner INT,
	relam INT,
	relfilenode INT,
	reltablespace INT,
	relpages INT,
	reltuples FLOAT,
	relallvisible INT,
	reltoastrelid INT,
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
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				// Table.
				relKind := relKindTable
				if table.IsView() {
					relKind = relKindView
				}
				if err := addRow(
					h.TableOid(db, table),         // oid
					parser.NewDString(table.Name), // relname
					h.DBOid(db),                   // relnamespace
					oidZero,                       // reltype (PG creates a composite type in pg_type for each table)
					parser.DNull,                  // relowner
					parser.DNull,                  // relam
					oidZero,                       // relfilenode
					oidZero,                       // reltablespace
					parser.DNull,                  // relpages
					parser.DNull,                  // reltuples
					oidZero,                       // relallvisible
					oidZero,                       // reltoastrelid
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
						h.IndexOid(db, table, index),  // oid
						parser.NewDString(index.Name), // relname
						h.DBOid(db),                   // relnamespace
						oidZero,                       // reltype
						parser.DNull,                  // relowner
						parser.DNull,                  // relam
						oidZero,                       // relfilenode
						oidZero,                       // reltablespace
						parser.DNull,                  // relpages
						parser.DNull,                  // reltuples
						oidZero,                       // relallvisible
						oidZero,                       // reltoastrelid
						parser.MakeDBool(false),       // relhasindex
						parser.MakeDBool(false),       // relisshared
						parser.MakeDBool(false),       // relistemp
						relKindIndex,                  // relkind
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
			},
		)
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
)

var pgCatalogConstraintTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_constraint (
	oid INT,
	conname STRING,
	connamespace INT,
	contype STRING,
	condeferrable BOOL,
	condeferred BOOL,
	convalidated BOOL,
	conrelid INT,
	contypid INT,
	conindid INT,
	confrelid INT,
	confupdtype STRING,
	confdeltype STRING,
	confmatchtype STRING,
	conislocal BOOL,
	coninhcount INT,
	connoinherit BOOL,
	conkey STRING,
	confkey STRING,
	conpfeqop STRING,
	conppeqop STRING,
	conffeqop STRING,
	conexclop STRING,
	conbin STRING,
	consrc STRING
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(p,
			func(
				db *sqlbase.DatabaseDescriptor,
				table *sqlbase.TableDescriptor,
				tableLookup tableLookupFn,
			) error {
				if err := addCheckConstraintsForTable(db, table, &h, addRow); err != nil {
					return err
				}
				if err := addFKConstraintsForTable(db, table, tableLookup, &h, addRow); err != nil {
					return err
				}
				if err := addPKeyAndUniqueConstraintsForTable(db, table, &h, addRow); err != nil {
					return err
				}
				return nil
			},
		)
	},
}

// colIDArrayToDatum returns a mock int[] as a DString for a slice of ColumnIDs.
// TODO(nvanbenschoten) use real int arrays when they are supported.
func colIDArrayToDatum(arr []sqlbase.ColumnID) parser.Datum {
	if len(arr) == 0 {
		return parser.DNull
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, val := range arr {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(strconv.Itoa(int(val)))
	}
	buf.WriteByte('}')
	return parser.NewDString(buf.String())
}

func addCheckConstraintsForTable(
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	h *oidHasher,
	addRow func(...parser.Datum) error,
) error {
	for _, check := range table.Checks {
		src := parser.NewDString(check.Expr)
		if err := addRow(
			h.CheckConstraintOid(db, table, check),                                   // oid
			dStringOrNull(check.Name),                                                // conname
			h.DBOid(db),                                                              // connamespace
			conTypeCheck,                                                             // contype
			parser.MakeDBool(false),                                                  // condeferrable
			parser.MakeDBool(false),                                                  // condeferred
			parser.MakeDBool(check.Validity == sqlbase.ConstraintValidity_Validated), // convalidated
			h.TableOid(db, table),                                                    // conrelid
			zeroVal,                                                                  // contypid
			zeroVal,                                                                  // conindid
			zeroVal,                                                                  // confrelid
			parser.DNull,                                                             // confupdtype
			parser.DNull,                                                             // confdeltype
			parser.DNull,                                                             // confmatchtype
			parser.MakeDBool(true),                                                   // conislocal
			zeroVal,                                                                  // coninhcount
			parser.MakeDBool(true),                                                   // connoinherit
			// TODO(nvanbenschoten) We currently do not store the referenced columns for a check
			// constraint. We should add an array of column indexes to
			// sqlbase.TableDescriptor_CheckConstraint and use that here.
			parser.DNull, // conkey
			parser.DNull, // confkey
			parser.DNull, // conpfeqop
			parser.DNull, // conppeqop
			parser.DNull, // conffeqop
			parser.DNull, // conexclop
			src,          // conbin
			src,          // consrc
		); err != nil {
			return err
		}
	}
	return nil
}

var (
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

func addFKConstraintsForTable(
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	tableLookup tableLookupFn,
	h *oidHasher,
	addRow func(...parser.Datum) error,
) error {
	return forEachForeignKeyInTable(table,
		func(index *sqlbase.IndexDescriptor, fk *sqlbase.ForeignKeyReference) error {
			// Perform a lookup for the refereced db, table, and index of the foreign key.
			fkDB, fkTable := tableLookup(fk.Table)
			if fkTable == nil {
				return errors.Errorf("could not find referenced table for foreign key %+v", fk)
			}
			fkIndex, err := fkTable.FindIndexByID(fk.Index)
			if err != nil {
				return err
			}

			return addRow(
				h.ForeignKeyConstraintOid(db, table, fk), // oid
				dStringOrNull(fk.Name),                   // conname
				h.DBOid(db),                              // connamespace
				conTypeFK,                                // contype
				parser.MakeDBool(false),                  // condeferrable
				parser.MakeDBool(false),                  // condeferred
				parser.MakeDBool(true),                   // convalidated
				h.TableOid(db, table),                    // conrelid
				zeroVal,                                  // contypid
				h.IndexOid(fkDB, fkTable, fkIndex),   // conindid
				h.TableOid(fkDB, fkTable),            // confrelid
				fkActionNone,                         // confupdtype
				fkActionNone,                         // confdeltype
				fkMatchTypeSimple,                    // confmatchtype
				parser.MakeDBool(true),               // conislocal
				zeroVal,                              // coninhcount
				parser.MakeDBool(true),               // connoinherit
				colIDArrayToDatum(index.ColumnIDs),   // conkey
				colIDArrayToDatum(fkIndex.ColumnIDs), // confkey
				parser.DNull,                         // conpfeqop
				parser.DNull,                         // conppeqop
				parser.DNull,                         // conffeqop
				parser.DNull,                         // conexclop
				parser.DNull,                         // conbin
				parser.DNull,                         // consrc
			)
		})
}

func addPKeyAndUniqueConstraintsForTable(
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	h *oidHasher,
	addRow func(...parser.Datum) error,
) error {
	return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
		oidFunc := h.UniqueConstraintOid
		conType := conTypeUnique
		if index == &table.PrimaryIndex {
			oidFunc = h.PrimaryKeyConstraintOid
			conType = conTypePKey
		} else if !index.Unique {
			return nil
		}
		return addRow(
			oidFunc(db, table, index), // oid
			dStringOrNull(index.Name), // conname
			h.DBOid(db),               // connamespace
			conType,                   // contype
			parser.MakeDBool(false),   // condeferrable
			parser.MakeDBool(false),   // condeferred
			parser.MakeDBool(true),    // convalidated
			h.TableOid(db, table),     // conrelid
			zeroVal,                   // contypid
			h.IndexOid(db, table, index), // conindid
			zeroVal,                            // confrelid
			parser.DNull,                       // confupdtype
			parser.DNull,                       // confdeltype
			parser.DNull,                       // confmatchtype
			parser.MakeDBool(true),             // conislocal
			zeroVal,                            // coninhcount
			parser.MakeDBool(true),             // connoinherit
			colIDArrayToDatum(index.ColumnIDs), // conkey
			parser.DNull,                       // confkey
			parser.DNull,                       // conpfeqop
			parser.DNull,                       // conppeqop
			parser.DNull,                       // conffeqop
			parser.DNull,                       // conexclop
			parser.DNull,                       // conbin
			parser.DNull,                       // consrc
		)
	})
}

var pgCatalogIndexesTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_indexes (
	schemaname STRING,
	tablename STRING,
	indexname STRING,
	tablespace STRING,
	indexdef STRING
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					def, err := indexDefFromDescriptor(p, db, table, index)
					if err != nil {
						return err
					}
					return addRow(
						parser.NewDString(db.Name),    // schemaname
						parser.NewDString(table.Name), // tablename
						parser.NewDString(index.Name), // indexname
						parser.DNull,                  // tablespace
						parser.NewDString(def),        // indexdef
					)
				})
			},
		)
	},
}

func indexDefFromDescriptor(
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
		parentTable, err := sqlbase.GetTableDescFromID(p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
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

var pgCatalogNamespaceTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_namespace (
	oid INT,
	nspname STRING NOT NULL DEFAULT '',
	nspowner INT,
	aclitem STRING
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		h := makeOidHasher()
		return forEachDatabaseDesc(p, func(db *sqlbase.DatabaseDescriptor) error {
			return addRow(
				h.DBOid(db),                // oid
				parser.NewDString(db.Name), // nspname
				parser.DNull,               // nspowner
				parser.DNull,               // aclitem
			)
		})
	},
}

var pgCatalogTablesTable = virtualSchemaTable{
	schema: `
CREATE TABLE pg_catalog.pg_tables (
	schemaname STRING,
	tablename STRING,
	tableowner STRING,
	tablespace STRING,
	hasindexes BOOL,
	hasrules BOOL,
	hastriggers BOOL,
	rowsecurity BOOL
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				if table.IsView() {
					return nil
				}
				return addRow(
					parser.NewDString(db.Name),    // schemaname
					parser.NewDString(table.Name), // tablename
					parser.DNull,                  // tableowner
					parser.DNull,                  // tablespace
					parser.MakeDBool(parser.DBool(table.IsPhysicalTable())), // hasindexes
					parser.MakeDBool(false),                                 // hasrules
					parser.MakeDBool(false),                                 // hastriggers
					parser.MakeDBool(false),                                 // rowsecurity
				)
			},
		)
	},
}

func typLen(typ parser.Type) parser.Datum {
	if sz, variable := typ.Size(); !variable {
		return parser.NewDInt(parser.DInt(sz))
	}
	return negOneVal
}

// oidHasher provides a consistent hashing mechanism for object identifiers in
// pg_catalog tables, allowing for reliable joins across tables.
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
	databaseTypeTag
	tableTypeTag
	indexTypeTag
	columnTypeTag
	checkConstraintTypeTag
	fkConstraintTypeTag
	pKeyConstraintTypeTag
	uniqueConstraintTypeTag
	typeTypeTag
)

func (h oidHasher) writeTypeTag(tag oidTypeTag) {
	h.writeUInt8(uint8(tag))
}

func (h oidHasher) getOid() *parser.DInt {
	i := h.h.Sum32()
	h.h.Reset()
	return parser.NewDInt(parser.DInt(i))
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

func (h oidHasher) writeType(typ parser.Type) {
	h.writeStr(typ.String())
}

func (h oidHasher) DBOid(db *sqlbase.DatabaseDescriptor) *parser.DInt {
	h.writeTypeTag(databaseTypeTag)
	h.writeDB(db)
	return h.getOid()
}

func (h oidHasher) TableOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor,
) *parser.DInt {
	h.writeTypeTag(tableTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	return h.getOid()
}

func (h oidHasher) IndexOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) *parser.DInt {
	h.writeTypeTag(indexTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}

func (h oidHasher) ColumnOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, column *sqlbase.ColumnDescriptor,
) *parser.DInt {
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
) *parser.DInt {
	h.writeTypeTag(checkConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeCheckConstraint(check)
	return h.getOid()
}

func (h oidHasher) PrimaryKeyConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, pkey *sqlbase.IndexDescriptor,
) *parser.DInt {
	h.writeTypeTag(pKeyConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(pkey)
	return h.getOid()
}

func (h oidHasher) ForeignKeyConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, fk *sqlbase.ForeignKeyReference,
) *parser.DInt {
	h.writeTypeTag(fkConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeForeignKeyReference(fk)
	return h.getOid()
}

func (h oidHasher) UniqueConstraintOid(
	db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) *parser.DInt {
	h.writeTypeTag(uniqueConstraintTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}

func (h oidHasher) TypeOid(typ parser.Type) *parser.DInt {
	h.writeTypeTag(typeTypeTag)
	h.writeType(typ)
	return h.getOid()
}
