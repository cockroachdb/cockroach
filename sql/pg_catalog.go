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
	"github.com/cockroachdb/cockroach/util/hlc"
)

var (
	oidZero = parser.NewDInt(0)
	zeroVal = oidZero
)

// pgCatalog contains a set of system tables mirroring PostgreSQL's pg_catalog schema.
var pgCatalog = virtualSchema{
	name: "pg_catalog",
	tables: []virtualSchemaTable{
		pgCatalogClassTable,
		pgCatalogNamespaceTable,
		pgCatalogTablesTable,
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
	desc: sqlbase.TableDescriptor{Name: "pg_class", ID: 0xffffffff, ParentID: 0x0, Version: 0x1, UpVersion: false, ModificationTime: hlc.Timestamp{WallTime: 0, Logical: 0}, Columns: []sqlbase.ColumnDescriptor{{Name: "oid", ID: 0x1, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relname", ID: 0x2, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: false, DefaultExpr: &emptyStr, Hidden: false}, {Name: "relnamespace", ID: 0x3, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "reltype", ID: 0x4, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relowner", ID: 0x5, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relam", ID: 0x6, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relfilenode", ID: 0x7, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "reltablespace", ID: 0x8, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relpages", ID: 0x9, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "reltuples", ID: 0xa, Type: sqlbase.ColumnType{Kind: 2, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relallvisible", ID: 0xb, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "reltoastrelid", ID: 0xc, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relhasindex", ID: 0xd, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relisshared", ID: 0xe, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relistemp", ID: 0xf, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relkind", ID: 0x10, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relnatts", ID: 0x11, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relchecks", ID: 0x12, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relhasoids", ID: 0x13, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relhaspkey", ID: 0x14, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relhasrules", ID: 0x15, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relhastriggers", ID: 0x16, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relhassubclass", ID: 0x17, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relfrozenxid", ID: 0x18, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "relacl", ID: 0x19, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "reloptions", ID: 0x1a, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}}, NextColumnID: 0x1b, Families: []sqlbase.ColumnFamilyDescriptor(nil), NextFamilyID: 0x0, PrimaryIndex: sqlbase.IndexDescriptor{Name: "", ID: 0x0, Unique: false, ColumnNames: []string(nil), ColumnDirections: []sqlbase.IndexDescriptor_Direction(nil), StoreColumnNames: []string(nil), ColumnIDs: []sqlbase.ColumnID(nil), ImplicitColumnIDs: []sqlbase.ColumnID(nil), ForeignKey: sqlbase.ForeignKeyReference{Table: 0x0, Index: 0x0, Name: "", Validity: 0}, ReferencedBy: []sqlbase.ForeignKeyReference(nil), Interleave: sqlbase.InterleaveDescriptor{Ancestors: []sqlbase.InterleaveDescriptor_Ancestor(nil)}, InterleavedBy: []sqlbase.ForeignKeyReference(nil)}, Indexes: []sqlbase.IndexDescriptor(nil), NextIndexID: 0x0, Privileges: emptyPrivileges, Mutations: []sqlbase.DescriptorMutation(nil), Lease: (*sqlbase.TableDescriptor_SchemaChangeLease)(nil), NextMutationID: 0x1, FormatVersion: 0x3, State: 0, Checks: []*sqlbase.TableDescriptor_CheckConstraint(nil), Renames: []sqlbase.TableDescriptor_RenameInfo(nil), ViewQuery: ""},
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
	desc: sqlbase.TableDescriptor{Name: "pg_namespace", ID: 0xffffffff, ParentID: 0x0, Version: 0x1, UpVersion: false, ModificationTime: hlc.Timestamp{WallTime: 0, Logical: 0}, Columns: []sqlbase.ColumnDescriptor{{Name: "oid", ID: 0x1, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "nspname", ID: 0x2, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: false, DefaultExpr: &emptyStr, Hidden: false}, {Name: "nspowner", ID: 0x3, Type: sqlbase.ColumnType{Kind: 1, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "aclitem", ID: 0x4, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}}, NextColumnID: 0x5, Families: []sqlbase.ColumnFamilyDescriptor(nil), NextFamilyID: 0x0, PrimaryIndex: sqlbase.IndexDescriptor{Name: "", ID: 0x0, Unique: false, ColumnNames: []string(nil), ColumnDirections: []sqlbase.IndexDescriptor_Direction(nil), StoreColumnNames: []string(nil), ColumnIDs: []sqlbase.ColumnID(nil), ImplicitColumnIDs: []sqlbase.ColumnID(nil), ForeignKey: sqlbase.ForeignKeyReference{Table: 0x0, Index: 0x0, Name: "", Validity: 0}, ReferencedBy: []sqlbase.ForeignKeyReference(nil), Interleave: sqlbase.InterleaveDescriptor{Ancestors: []sqlbase.InterleaveDescriptor_Ancestor(nil)}, InterleavedBy: []sqlbase.ForeignKeyReference(nil)}, Indexes: []sqlbase.IndexDescriptor(nil), NextIndexID: 0x0, Privileges: emptyPrivileges, Mutations: []sqlbase.DescriptorMutation(nil), Lease: (*sqlbase.TableDescriptor_SchemaChangeLease)(nil), NextMutationID: 0x1, FormatVersion: 0x3, State: 0, Checks: []*sqlbase.TableDescriptor_CheckConstraint(nil), Renames: []sqlbase.TableDescriptor_RenameInfo(nil), ViewQuery: ""},
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
	desc: sqlbase.TableDescriptor{Name: "pg_tables", ID: 0xffffffff, ParentID: 0x0, Version: 0x1, UpVersion: false, ModificationTime: hlc.Timestamp{WallTime: 0, Logical: 0}, Columns: []sqlbase.ColumnDescriptor{{Name: "schemaname", ID: 0x1, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "tablename", ID: 0x2, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "tableowner", ID: 0x3, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "tablespace", ID: 0x4, Type: sqlbase.ColumnType{Kind: 7, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "hasindexes", ID: 0x5, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "hasrules", ID: 0x6, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "hastriggers", ID: 0x7, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}, {Name: "rowsecurity", ID: 0x8, Type: sqlbase.ColumnType{Kind: 0, Width: 0, Precision: 0}, Nullable: true, DefaultExpr: (*string)(nil), Hidden: false}}, NextColumnID: 0x9, Families: []sqlbase.ColumnFamilyDescriptor(nil), NextFamilyID: 0x0, PrimaryIndex: sqlbase.IndexDescriptor{Name: "", ID: 0x0, Unique: false, ColumnNames: []string(nil), ColumnDirections: []sqlbase.IndexDescriptor_Direction(nil), StoreColumnNames: []string(nil), ColumnIDs: []sqlbase.ColumnID(nil), ImplicitColumnIDs: []sqlbase.ColumnID(nil), ForeignKey: sqlbase.ForeignKeyReference{Table: 0x0, Index: 0x0, Name: "", Validity: 0}, ReferencedBy: []sqlbase.ForeignKeyReference(nil), Interleave: sqlbase.InterleaveDescriptor{Ancestors: []sqlbase.InterleaveDescriptor_Ancestor(nil)}, InterleavedBy: []sqlbase.ForeignKeyReference(nil)}, Indexes: []sqlbase.IndexDescriptor(nil), NextIndexID: 0x0, Privileges: emptyPrivileges, Mutations: []sqlbase.DescriptorMutation(nil), Lease: (*sqlbase.TableDescriptor_SchemaChangeLease)(nil), NextMutationID: 0x1, FormatVersion: 0x3, State: 0, Checks: []*sqlbase.TableDescriptor_CheckConstraint(nil), Renames: []sqlbase.TableDescriptor_RenameInfo(nil), ViewQuery: ""},
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

func (h oidHasher) DBOid(db *sqlbase.DatabaseDescriptor) *parser.DInt {
	h.writeTypeTag(databaseTypeTag)
	h.writeDB(db)
	return h.getOid()
}

func (h oidHasher) TableOid(
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
) *parser.DInt {
	h.writeTypeTag(tableTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	return h.getOid()
}

func (h oidHasher) IndexOid(
	db *sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
) *parser.DInt {
	h.writeTypeTag(indexTypeTag)
	h.writeDB(db)
	h.writeTable(table)
	h.writeIndex(index)
	return h.getOid()
}
