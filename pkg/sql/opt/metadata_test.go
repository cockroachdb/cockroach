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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	a := &testutils.TestTable{}
	a.Name = tree.MakeUnqualifiedTableName(tree.Name("a"))
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

func TestMetadataWeakKeys(t *testing.T) {
	test := func(weakKeys opt.WeakKeys, expected string) {
		t.Helper()
		actual := fmt.Sprintf("%v", weakKeys)
		if actual != expected {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}

	testContains := func(weakKeys opt.WeakKeys, cs opt.ColSet, expected bool) {
		t.Helper()
		actual := weakKeys.ContainsSubsetOf(cs)
		if actual != expected {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	}

	md := opt.NewMetadata()

	// Create table with the following interesting indexes:
	//   1. Primary key index with multiple columns.
	//   2. Single column index.
	//   3. Storing values (should have no impact).
	//   4. Non-unique index (should always be superset of primary key).
	//   5. Unique index that has subset of cols of another unique index, but
	//      which is defined afterwards (triggers removal of previous weak key).
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(t,
		"CREATE TABLE a ("+
			"k INT, "+
			"i INT, "+
			"d DECIMAL, "+
			"f FLOAT, "+
			"s STRING, "+
			"PRIMARY KEY (k, i), "+
			"UNIQUE INDEX (f) STORING (s, i),"+
			"UNIQUE INDEX (d DESC, i, s),"+
			"UNIQUE INDEX (d, i DESC) STORING (f),"+
			"INDEX (s DESC, i))",
		cat)
	a := md.AddTable(cat.Table("a"))

	wk := md.TableWeakKeys(a)
	test(wk, "[(1,2) (4) (2,3)]")

	// Test ContainsSubsetOf method.
	testContains(wk, util.MakeFastIntSet(1, 2), true)
	testContains(wk, util.MakeFastIntSet(1, 2, 3), true)
	testContains(wk, util.MakeFastIntSet(4), true)
	testContains(wk, util.MakeFastIntSet(4, 3, 2, 1), true)
	testContains(wk, util.MakeFastIntSet(1), false)
	testContains(wk, util.MakeFastIntSet(1, 3), false)
	testContains(wk, util.MakeFastIntSet(5), false)

	// Add additional weak keys to additionally verify Add method.
	wk.Add(util.MakeFastIntSet(1, 2, 3))
	test(wk, "[(1,2) (4) (2,3)]")

	wk.Add(util.MakeFastIntSet(2, 1))
	test(wk, "[(1,2) (4) (2,3)]")

	wk.Add(util.MakeFastIntSet(2))
	test(wk, "[(4) (2)]")
}
