// Copyright 2018 The Cockroach Authors.
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

package xform

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type testColumn struct {
	optbase.Column

	isNullable bool
	colName    string
	datumType  types.T
}

func (c *testColumn) IsNullable() bool {
	return c.isNullable
}

func (c *testColumn) ColName() string {
	return c.colName
}

func (c *testColumn) DatumType() types.T {
	return c.datumType
}

type testTable struct {
	optbase.Table

	tabName string
	columns []*testColumn
}

func (t *testTable) TabName() string {
	return t.tabName
}

func (t *testTable) NumColumns() int {
	return len(t.columns)
}

func (t *testTable) Column(i int) optbase.Column {
	return t.columns[i]
}

type testCatalog struct {
	optbase.Catalog

	tables map[string]*testTable
}

func (c *testCatalog) FindTable(ctx context.Context, name *tree.TableName) (optbase.Table, error) {
	return c.tables[name.Table()], nil
}

func (c *testCatalog) addTable(tbl *testTable) {
	if c.tables == nil {
		c.tables = make(map[string]*testTable)
	}
	c.tables[tbl.tabName] = tbl
}

func TestMetadataColumns(t *testing.T) {
	cat := &testCatalog{}
	md := newMetadata(cat)
	if md.Catalog() != cat {
		t.Fatal("metadata catalog didn't match catalog passed to newMetadata")
	}

	// Add standalone column.
	colIndex := md.AddColumn("alias")
	if colIndex != 1 {
		t.Fatalf("unexpected column index: %d", colIndex)
	}

	label := md.ColumnLabel(colIndex)
	if label != "alias" {
		t.Fatalf("unexpected column label: %s", label)
	}

	// Add another column.
	colIndex = md.AddColumn("alias2")
	if colIndex != 2 {
		t.Fatalf("unexpected column index: %d", colIndex)
	}

	label = md.ColumnLabel(colIndex)
	if label != "alias2" {
		t.Fatalf("unexpected column label: %s", label)
	}
}

func TestMetadataTables(t *testing.T) {
	cat := &testCatalog{}
	md := newMetadata(cat)
	if md.Catalog() != cat {
		t.Fatal("metadata catalog didn't match catalog passed to newMetadata")
	}

	// Add a table reference to the metadata.
	a := &testTable{tabName: "a"}
	a.columns = append(a.columns, &testColumn{colName: "x"}, &testColumn{colName: "y"})

	tblIndex := md.AddTable(a)
	if tblIndex != 1 {
		t.Fatalf("unexpected table index: %d", tblIndex)
	}

	tbl := md.Table(tblIndex)
	if tbl != a {
		t.Fatal("table didn't match table added to metadata")
	}

	colIndex := md.TableColumn(tblIndex, 0)
	if colIndex != 1 {
		t.Fatalf("unexpected column index: %d", colIndex)
	}

	label := md.ColumnLabel(colIndex)
	if label != "a.x" {
		t.Fatalf("unexpected column label: %s", label)
	}

	// Add a table reference without a name to the metadata.
	b := &testTable{}
	b.columns = append(b.columns, &testColumn{colName: "x"})

	tblIndex = md.AddTable(b)
	if tblIndex != 3 {
		t.Fatalf("unexpected table index: %d", tblIndex)
	}

	label = md.ColumnLabel(md.TableColumn(tblIndex, 0))
	if label != "x" {
		t.Fatalf("unexpected column label: %s", label)
	}
}

type testMetadataGen struct {
	cat *testCatalog
	mem *memo
}

func newTestMetadataGen() *testMetadataGen {
	cat := &testCatalog{}
	return &testMetadataGen{cat: cat, mem: newMemo(cat)}
}

// CREATE TABLE a (x INT PRIMARY KEY, y INT)
func (g *testMetadataGen) addTableA() TableIndex {
	a := &testTable{tabName: "a"}
	a.columns = append(
		a.columns,
		&testColumn{colName: "x"},
		&testColumn{colName: "y", isNullable: true},
	)

	g.cat.addTable(a)
	return g.mem.metadata.AddTable(a)
}

// CREATE TABLE b (x INT, z INT NOT NULL, FOREIGN KEY (x) REFERENCES a (x))
func (g *testMetadataGen) addTableB() TableIndex {
	b := &testTable{tabName: "b"}
	b.columns = append(
		b.columns,
		&testColumn{colName: "x"},
		&testColumn{colName: "z"},
	)

	g.cat.addTable(b)
	return g.mem.metadata.AddTable(b)
}
