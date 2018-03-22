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

package opt_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestMetadataColumns(t *testing.T) {
	md := opt.NewMetadata()

	// Add standalone column.
	colID := md.AddColumn("alias", types.Int)
	if colID != 1 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	label := md.ColumnLabel(colID)
	if label != "alias" {
		t.Fatalf("unexpected column label: %s", label)
	}

	typ := md.ColumnType(colID)
	if typ != types.Int {
		t.Fatalf("unexpected column type: %s", typ)
	}

	if n := md.NumColumns(); n != 1 {
		t.Fatalf("unexpected num columns: %d", n)
	}

	// Add another column.
	colID = md.AddColumn("alias2", types.String)
	if colID != 2 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	label = md.ColumnLabel(colID)
	if label != "alias2" {
		t.Fatalf("unexpected column label: %s", label)
	}

	typ = md.ColumnType(colID)
	if typ != types.String {
		t.Fatalf("unexpected column type: %s", typ)
	}

	if n := md.NumColumns(); n != 2 {
		t.Fatalf("unexpected num columns: %d", n)
	}
}

func TestMetadataTables(t *testing.T) {
	md := opt.NewMetadata()

	// Add a table reference to the metadata.
	a := &testutils.TestTable{Name: "a"}
	x := &testutils.TestColumn{Name: "x"}
	y := &testutils.TestColumn{Name: "y"}
	a.Columns = append(a.Columns, x, y)

	tabID := md.AddTable(a)
	if tabID != 1 {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	tab := md.Table(tabID)
	if tab != a {
		t.Fatal("table didn't match table added to metadata")
	}

	colID := md.TableColumn(tabID, 0)
	if colID != 1 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	label := md.ColumnLabel(colID)
	if label != "a.x" {
		t.Fatalf("unexpected column label: %s", label)
	}

	// Add a table reference without a name to the metadata.
	b := &testutils.TestTable{}
	b.Columns = append(b.Columns, &testutils.TestColumn{Name: "x"})

	tabID = md.AddTable(b)
	if tabID != 3 {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	label = md.ColumnLabel(md.TableColumn(tabID, 0))
	if label != "x" {
		t.Fatalf("unexpected column label: %s", label)
	}
}
