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

package opt

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestMetadataColumns(t *testing.T) {
	cat := testutils.NewTestCatalog()
	md := NewMetadata(cat)
	if md.Catalog() != cat {
		t.Fatal("metadata catalog didn't match catalog passed to newMetadata")
	}

	// Add standalone column.
	colIndex := md.AddColumn("alias", types.Int)
	if colIndex != 1 {
		t.Fatalf("unexpected column index: %d", colIndex)
	}

	label := md.ColumnLabel(colIndex)
	if label != "alias" {
		t.Fatalf("unexpected column label: %s", label)
	}

	typ := md.ColumnType(colIndex)
	if typ != types.Int {
		t.Fatalf("unexpected column type: %s", typ)
	}

	// Add another column.
	colIndex = md.AddColumn("alias2", types.String)
	if colIndex != 2 {
		t.Fatalf("unexpected column index: %d", colIndex)
	}

	label = md.ColumnLabel(colIndex)
	if label != "alias2" {
		t.Fatalf("unexpected column label: %s", label)
	}

	typ = md.ColumnType(colIndex)
	if typ != types.String {
		t.Fatalf("unexpected column type: %s", typ)
	}
}

func TestMetadataTables(t *testing.T) {
	cat := testutils.NewTestCatalog()
	md := NewMetadata(cat)
	if md.Catalog() != cat {
		t.Fatal("metadata catalog didn't match catalog passed to newMetadata")
	}

	// Add a table reference to the metadata.
	a := &testutils.TestTable{Name: "a"}
	x := &testutils.TestColumn{Name: "x"}
	y := &testutils.TestColumn{Name: "y"}
	a.Columns = append(a.Columns, x, y)

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
	b := &testutils.TestTable{}
	b.Columns = append(b.Columns, &testutils.TestColumn{Name: "x"})

	tblIndex = md.AddTable(b)
	if tblIndex != 3 {
		t.Fatalf("unexpected table index: %d", tblIndex)
	}

	label = md.ColumnLabel(md.TableColumn(tblIndex, 0))
	if label != "x" {
		t.Fatalf("unexpected column label: %s", label)
	}
}
