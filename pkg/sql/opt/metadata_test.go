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
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestMetadata(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	f.Init(&evalCtx, nil /* catalog */)
	md := f.Metadata()

	schID := md.AddSchema(&testcat.Schema{})
	colID := md.AddColumn("col", types.Int)
	cmpID := md.AddColumn("cmp", types.Bool)
	tabID := md.AddTable(&testcat.Table{}, &tree.TableName{})
	seqID := md.AddSequence(&testcat.Sequence{})
	md.AddView(&testcat.View{})
	md.AddUserDefinedType(types.MakeEnum(152100, 154180))

	// Call Init and add objects from catalog, verifying that IDs have been reset.
	testCat := testcat.New()
	tab := &testcat.Table{Revoked: true}
	testCat.AddTable(tab)

	// Create a (col = 1) scalar expression.
	scalar := &memo.EqExpr{
		Left: &memo.VariableExpr{
			Col: colID,
			Typ: types.Int,
		},
		Right: &memo.ConstExpr{
			Value: tree.NewDInt(1),
			Typ:   types.Int,
		},
	}

	md.Init()
	if md.AddSchema(testCat.Schema()) != schID {
		t.Fatalf("unexpected schema id")
	}
	if md.AddColumn("col2", types.Int) != colID {
		t.Fatalf("unexpected column id")
	}
	if md.AddColumn("cmp2", types.Bool) != cmpID {
		t.Fatalf("unexpected column id")
	}
	if md.AddTable(tab, &tree.TableName{}) != tabID {
		t.Fatalf("unexpected table id")
	}
	tabMeta := md.TableMeta(tabID)
	tabMeta.SetConstraints(scalar)
	tabMeta.AddComputedCol(cmpID, scalar)
	tabMeta.AddPartialIndexPredicate(0, scalar)
	if md.AddSequence(&testcat.Sequence{SeqID: 100}) != seqID {
		t.Fatalf("unexpected sequence id")
	}

	md.AddView(&testcat.View{ViewID: 101})
	if len(md.AllViews()) != 1 {
		t.Fatalf("unexpected views")
	}

	md.AddUserDefinedType(types.MakeEnum(151500, 152510))
	if len(md.AllUserDefinedTypes()) != 1 {
		fmt.Println(md)
		t.Fatalf("unexpected types")
	}

	md.AddDependency(opt.DepByName(&tab.TabName), tab, privilege.CREATE)
	depsUpToDate, err := md.CheckDependencies(context.Background(), testCat)
	if err == nil || depsUpToDate {
		t.Fatalf("expected table privilege to be revoked")
	}

	// Call CopyFrom and verify that same objects are present in new metadata.
	expr := &memo.ProjectExpr{}
	md.AddWithBinding(1, expr)
	var mdNew opt.Metadata
	mdNew.CopyFrom(md, f.CopyScalarWithoutPlaceholders)

	if mdNew.Schema(schID) != testCat.Schema() {
		t.Fatalf("unexpected schema")
	}
	if mdNew.ColumnMeta(colID).Alias != "col2" {
		t.Fatalf("unexpected column")
	}

	tabMetaNew := mdNew.TableMeta(tabID)
	if tabMetaNew.Table != tab {
		t.Fatalf("unexpected table")
	}

	if tabMetaNew.Constraints == scalar {
		t.Fatalf("expected constraints to be copied")
	}

	compColsPtr := reflect.ValueOf(tabMeta.ComputedCols).Pointer()
	newCompColsPtr := reflect.ValueOf(tabMetaNew.ComputedCols).Pointer()
	if newCompColsPtr == compColsPtr {
		t.Fatalf("expected computed columns map to be copied, not shared")
	}

	if tabMetaNew.ComputedCols[cmpID] == scalar {
		t.Fatalf("expected computed column expression to be copied")
	}

	partialIdxPredPtr := reflect.ValueOf(tabMeta.PartialIndexPredicatesUnsafe()).Pointer()
	newPartialIdxPredPtr := reflect.ValueOf(tabMetaNew.PartialIndexPredicatesUnsafe()).Pointer()
	if newPartialIdxPredPtr == partialIdxPredPtr {
		t.Fatalf("expected partial index predicates map to be copied, not shared")
	}

	if tabMetaNew.PartialIndexPredicatesUnsafe()[0] == scalar {
		t.Fatalf("expected partial index predicate to be copied")
	}

	if mdNew.Sequence(seqID).(*testcat.Sequence).SeqID != 100 {
		t.Fatalf("unexpected sequence")
	}

	if v := mdNew.AllViews(); len(v) != 1 && v[0].(*testcat.View).ViewID != 101 {
		t.Fatalf("unexpected view")
	}

	if ts := mdNew.AllUserDefinedTypes(); len(ts) != 1 && ts[151500].Equal(types.MakeEnum(151500, 152510)) {
		t.Fatalf("unexpected type")
	}

	depsUpToDate, err = md.CheckDependencies(context.Background(), testCat)
	if err == nil || depsUpToDate {
		t.Fatalf("expected table privilege to be revoked in metadata copy")
	}

	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		mdNew.WithBinding(1)
	}()
	if !panicked {
		t.Fatalf("with bindings should not be copied!")
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

	mkCol := func(ordinal int, name string) cat.Column {
		var c cat.Column
		c.InitNonVirtual(
			ordinal,
			cat.StableID(ordinal+1),
			tree.Name(name),
			cat.Ordinary,
			types.Int,
			false, /* nullable */
			cat.Visible,
			nil, /* defaultExpr */
			nil, /* computedExpr */
		)
		return c
	}
	x := mkCol(0, "x")
	y := mkCol(1, "y")
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
	b.Columns = append(b.Columns, mkCol(0, "x"))

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

// TestDuplicateTable tests that we can extract a set of columns from an index ordinal.
func TestDuplicateTable(t *testing.T) {
	cat := testcat.New()
	_, err := cat.ExecuteDDL("CREATE TABLE a (b BOOL, b2 BOOL, INDEX (b2) WHERE b)")
	if err != nil {
		t.Fatal(err)
	}

	var md opt.Metadata
	tn := tree.NewUnqualifiedTableName("a")
	a := md.AddTable(cat.Table(tn), tn)
	b := a.ColumnID(0)
	b2 := a.ColumnID(1)

	tabMeta := md.TableMeta(a)
	tabMeta.SetConstraints(&memo.VariableExpr{Col: b})
	tabMeta.AddComputedCol(b2, &memo.VariableExpr{Col: b})
	tabMeta.AddPartialIndexPredicate(1, &memo.VariableExpr{Col: b})

	// remap is a simple function that can only remap column IDs in a
	// memo.VariableExpr, useful in the context of this test only.
	remap := func(e opt.ScalarExpr, colMap opt.ColMap) opt.ScalarExpr {
		v := e.(*memo.VariableExpr)
		newColID, ok := colMap.Get(int(v.Col))
		if !ok {
			return e
		}
		return &memo.VariableExpr{Col: opt.ColumnID(newColID)}
	}

	// Duplicate the table.
	dupA := md.DuplicateTable(a, remap)
	dupTabMeta := md.TableMeta(dupA)
	dupB := dupA.ColumnID(0)
	dupB2 := dupA.ColumnID(1)

	if dupTabMeta.Constraints == nil {
		t.Fatalf("expected constraints to be duplicated")
	}

	col := dupTabMeta.Constraints.(*memo.VariableExpr).Col
	if col == b {
		t.Errorf("expected constraints to reference new column ID %d, got %d", dupB, col)
	}

	if dupTabMeta.ComputedCols == nil || dupTabMeta.ComputedCols[dupB2] == nil {
		t.Fatalf("expected computed cols to be duplicated")
	}

	col = dupTabMeta.ComputedCols[dupB2].(*memo.VariableExpr).Col
	if col == b {
		t.Errorf("expected computed column to reference new column ID %d, got %d", dupB, col)
	}

	pred, isPartialIndex := dupTabMeta.PartialIndexPredicate(1)
	if !isPartialIndex {
		t.Fatalf("expected partial index predicates to be duplicated")
	}

	colMeta := md.ColumnMeta(dupB)
	if colMeta.Table != dupA {
		t.Fatalf("expected new column to reference new table ID")
	}

	colMeta = md.ColumnMeta(dupB2)
	if colMeta.Table != dupA {
		t.Fatalf("expected new column to reference new table ID")
	}

	col = pred.(*memo.VariableExpr).Col
	if col == b {
		t.Errorf("expected partial index predicate to reference new column ID %d, got %d", dupB, col)
	}
}
