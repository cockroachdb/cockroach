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
	"hash"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
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

func typLen(typ parser.Datum) parser.Datum {
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

func (h oidHasher) writeType(typ parser.Datum) {
	h.writeStr(typ.Type())
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

func (h oidHasher) TypeOid(typ parser.Datum) *parser.DInt {
	h.writeTypeTag(typeTypeTag)
	h.writeType(typ)
	return h.getOid()
}
