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

var pgCatalog = virtualSchema{
	name: "pg_catalog",
	tables: []virtualSchemaTable{
		pgCatalogNamespaceTable,
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

func (h oidHasher) DBOid(db *sqlbase.DatabaseDescriptor) *parser.DInt {
	h.writeTypeTag(databaseTypeTag)
	h.writeDB(db)
	return h.getOid()
}
