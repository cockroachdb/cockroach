// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestMetadata(t *testing.T) {
	var md opt.Metadata
	schID := md.AddSchema(&testcat.Schema{})
	colID := md.AddColumn("col", types.Int)
	tabID := md.AddTable(&testcat.Table{}, &tree.TableName{})

	// Call Init and add objects from catalog, verifying that IDs have been reset.
	testCat := testcat.New()
	tab := &testcat.Table{Revoked: true}
	testCat.AddTable(tab)

	md.Init()
	if md.AddSchema(testCat.Schema()) != schID {
		t.Fatalf("unexpected schema id")
	}
	if md.AddColumn("col2", types.Int) != colID {
		t.Fatalf("unexpected column id")
	}
	if md.AddTable(tab, &tree.TableName{}) != tabID {
		t.Fatalf("unexpected table id")
	}

	md.AddDependency(opt.DepByName(&tab.TabName), tab, privilege.CREATE)
	depsUpToDate, err := md.CheckDependencies(context.TODO(), testCat)
	if err == nil || depsUpToDate {
		t.Fatalf("expected table privilege to be revoked")
	}

	// Call CopyFrom and verify that same objects are present in new metadata.
	var mdNew opt.Metadata
	mdNew.CopyFrom(&md)
	if mdNew.Schema(schID) != testCat.Schema() {
		t.Fatalf("unexpected schema")
	}
	if mdNew.ColumnMeta(colID).Alias != "col2" {
		t.Fatalf("unexpected column")
	}

	if mdNew.TableMeta(tabID).Table != tab {
		t.Fatalf("unexpected table")
	}

	depsUpToDate, err = md.CheckDependencies(context.TODO(), testCat)
	if err == nil || depsUpToDate {
		t.Fatalf("expected table privilege to be revoked in metadata copy")
	}
}

func TestMetadataSchemas(t *testing.T) {
	var md opt.Metadata

	sch := &testcat.Schema{
		SchemaID:   1,
		SchemaName: cat.SchemaName{CatalogName: "db", SchemaName: "schema"},
	}

	schID := md.AddSchema(sch)
	lookup := md.Schema(schID)
	if lookup.ID() != 1 {
		t.Fatalf("unexpected schema id: %d", lookup.ID())
	}
	if lookup.Name().String() != sch.SchemaName.String() {
		t.Fatalf("unexpected schema name: %s", lookup.Name())
	}
}

func TestMetadataColumns(t *testing.T) {
	var md opt.Metadata

	// Add standalone column.
	colID := md.AddColumn("alias", types.Int)
	if colID != 1 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	colMeta := md.ColumnMeta(colID)
	if colMeta.Alias != "alias" {
		t.Fatalf("unexpected column alias: %s", colMeta.Alias)
	}

	if colMeta.Type.Family() != types.IntFamily {
		t.Fatalf("unexpected column type: %s", colMeta.Type)
	}

	if n := md.NumColumns(); n != 1 {
		t.Fatalf("unexpected num columns: %d", n)
	}

	// Add another column.
	colID = md.AddColumn("alias2", types.String)
	if colID != 2 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	colMeta = md.ColumnMeta(colID)
	if colMeta.Alias != "alias2" {
		t.Fatalf("unexpected column alias: %s", colMeta.Alias)
	}

	if colMeta.Type.Family() != types.StringFamily {
		t.Fatalf("unexpected column type: %s", colMeta.Type)
	}

	if n := md.NumColumns(); n != 2 {
		t.Fatalf("unexpected num columns: %d", n)
	}
}

func TestMetadataTables(t *testing.T) {
	var md opt.Metadata

	// Add a table reference to the metadata.
	a := &testcat.Table{TabID: 1}
	a.TabName = tree.MakeUnqualifiedTableName(tree.Name("a"))
	x := &testcat.Column{Name: "x"}
	y := &testcat.Column{Name: "y"}
	a.Columns = append(a.Columns, x, y)

	tabID := md.AddTable(a, &tree.TableName{})
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

	colMeta := md.ColumnMeta(colID)
	if colMeta.Alias != "x" {
		t.Fatalf("unexpected column alias: %s", colMeta.Alias)
	}

	// Add another table reference to the metadata.
	b := &testcat.Table{TabID: 1}
	b.TabName = tree.MakeUnqualifiedTableName(tree.Name("b"))
	b.Columns = append(b.Columns, &testcat.Column{Name: "x"})

	otherTabID := md.AddTable(b, &tree.TableName{})
	if otherTabID == tabID {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	alias := md.ColumnMeta(otherTabID.ColumnID(0)).Alias
	if alias != "x" {
		t.Fatalf("unexpected column alias: %s", alias)
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
	tn := tree.NewUnqualifiedTableName("a")
	a := md.AddTable(cat.Table(tn), tn)

	k := a.ColumnID(0)
	i := a.ColumnID(1)
	s := a.ColumnID(2)
	f := a.ColumnID(3)

	testCases := []struct {
		index        int
		expectedCols opt.ColSet
	}{
		{1, opt.MakeColSet(k, i)},
		{2, opt.MakeColSet(s, f, k)},
	}

	for _, tc := range testCases {
		actual := md.TableMeta(a).IndexColumns(tc.index)
		if !tc.expectedCols.Equals(actual) {
			t.Errorf("expected %v, got %v", tc.expectedCols, actual)
		}
	}
}
