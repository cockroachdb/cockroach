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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestMetadataColumns(t *testing.T) {
	var md opt.Metadata

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
	var md opt.Metadata

	// Add a table reference to the metadata.
	a := &testcat.Table{StableID: 1}
	a.TabName = tree.MakeUnqualifiedTableName(tree.Name("a"))
	x := &testcat.Column{Name: "x"}
	y := &testcat.Column{Name: "y"}
	a.Columns = append(a.Columns, x, y)

	tabID := md.AddTable(a)
	if tabID == 0 {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	tab := md.Table(tabID)
	if tab != a {
		t.Fatal("table didn't match table added to metadata")
	}

	colID := tabID.ColumnID(0)
	if colID == 0 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	label := md.ColumnLabel(colID)
	if label != "x" {
		t.Fatalf("unexpected column label: %s", label)
	}

	// Add another table reference to the metadata.
	b := &testcat.Table{StableID: 1}
	b.TabName = tree.MakeUnqualifiedTableName(tree.Name("b"))
	b.Columns = append(b.Columns, &testcat.Column{Name: "x"})

	otherTabID := md.AddTable(b)
	if otherTabID == tabID {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	label = md.ColumnLabel(otherTabID.ColumnID(0))
	if label != "x" {
		t.Fatalf("unexpected column label: %s", label)
	}
}

// TestIndexColumns tests that we can extract a set of columns from an index ordinal.
func TestIndexColumns(t *testing.T) {
	cat := testcat.New()
	_, err := cat.ExecuteDDL(
		"CREATE TABLE a (" +
			"k INT PRIMARY KEY, " +
			"i INT, " +
			"s STRING, " +
			"f FLOAT, " +
			"INDEX (i, k), " +
			"INDEX (s DESC) STORING(f))")
	if err != nil {
		t.Fatal(err)
	}

	var md opt.Metadata
	a := md.AddTable(cat.Table(tree.NewUnqualifiedTableName("a")))

	k := int(a.ColumnID(0))
	i := int(a.ColumnID(1))
	s := int(a.ColumnID(2))
	f := int(a.ColumnID(3))

	testCases := []struct {
		index        int
		expectedCols opt.ColSet
	}{
		{1, util.MakeFastIntSet(k, i)},
		{2, util.MakeFastIntSet(s, f, k)},
	}

	for _, tc := range testCases {
		actual := md.IndexColumns(a, tc.index)
		if !tc.expectedCols.Equals(actual) {
			t.Errorf("expected %v, got %v", tc.expectedCols, actual)
		}
	}
}
