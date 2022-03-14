// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TestDuplicateTableWithCheckConstraint tests that we can accurately duplicate
// the optimizer's representation check constraint filters when calling
// DuplicateTable.
func TestDuplicateTableWithCheckConstraint(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	cat := testcat.New()
	f.Init(&evalCtx, cat /* catalog */)
	var c norm.CustomFuncs
	c.Init(&f)
	semaCtx := tree.MakeSemaContext()
	ctx := context.Background()
	builder := optbuilder.New(ctx, &semaCtx, &evalCtx, cat, &f, nil)

	_, err := cat.ExecuteDDL("CREATE TABLE a (i INT CHECK (i IN (5,2,8,5,9)), i2 INT CHECK (i = i2), PRIMARY KEY (i2, i))")
	if err != nil {
		t.Fatal(err)
	}
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("a")
	a := md.AddTable(cat.Table(tn), tn)

	tabMeta := md.TableMeta(a)
	builder.AddCheckConstraintsForTable(tabMeta)

	// Duplicate the table.
	dupA := md.DuplicateTable(a, c.RemapCols)
	dupTabMeta := md.TableMeta(dupA)

	if dupTabMeta.Constraints == nil {
		t.Fatalf("expected constraints to be duplicated")
	}

	origConstraints := *tabMeta.Constraints.(*memo.FiltersExpr)
	dupedConstraints := *dupTabMeta.Constraints.(*memo.FiltersExpr)
	if len(origConstraints) != len(dupedConstraints) {
		t.Fatalf("Length of original constraints doesn't match length of duped constraints")
	}
	for i := range origConstraints {
		if origConstraints[i].ScalarProps().Equals(dupedConstraints[i].ScalarProps(), &evalCtx) {
			t.Fatalf("Scalar properties of duplicated constraints including remapping not expected to match original constraints")
		}
	}

	// Now remap back to original column ids and check for equality.
	colMap := makeColMap(dupTabMeta, tabMeta)
	doublyRemappedConstraints := *c.RemapCols(&dupedConstraints, colMap).(*memo.FiltersExpr)

	for i := range origConstraints {
		if !origConstraints[i].ScalarProps().Equals(doublyRemappedConstraints[i].ScalarProps(), &evalCtx) {
			t.Fatalf("Scalar properties of doubly-remapped, duplicated constraints do not match the original constraints")
		}
	}
}

// BenchmarkCopyConstructConstraints compares the cost of copying scalar
// properties of a copied filter with constructing the copied filter from
// scratch.
func BenchmarkCopyConstructConstraints(b *testing.B) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	cat := testcat.New()
	f.Init(&evalCtx, cat /* catalog */)
	var c norm.CustomFuncs
	c.Init(&f)
	semaCtx := tree.MakeSemaContext()
	ctx := context.Background()
	builder := optbuilder.New(ctx, &semaCtx, &evalCtx, cat, &f, nil)

	_, err := cat.ExecuteDDL(
		`CREATE TABLE a (i INT
		CHECK (i IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,28,29,30,
					31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
					61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,
					91,92,93,94,95,96,97,98,99,100,
					101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,
					121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,
					141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,
					161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,
					181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,
					201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,
					221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240))
		, i2 INT CHECK (i = i2), PRIMARY KEY(i2, i))`)
	if err != nil {
		b.Fatal(err)
	}
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("a")
	a := md.AddTable(cat.Table(tn), tn)

	tabMeta := md.TableMeta(a)
	builder.AddCheckConstraintsForTable(tabMeta)

	// Duplicate the table.
	dupA := md.DuplicateTable(a, c.RemapCols)
	dupTabMeta := md.TableMeta(dupA)

	if dupTabMeta.Constraints == nil {
		b.Fatalf("expected constraints to be duplicated")
	}
	origConstraints := *tabMeta.Constraints.(*memo.FiltersExpr)

	colMap := makeColMap(tabMeta, dupTabMeta)
	replace := c.ColumnRemapFunction(colMap)

	var cols opt.ColSet
	for i := 0; i < tabMeta.Table.ColumnCount(); i++ {
		cols.Add(tabMeta.MetaID.ColumnID(i))
	}
	sp := memo.ScanPrivate{Table: tabMeta.MetaID, Cols: cols}
	selectExpr := f.ConstructSelect(f.ConstructScan(&sp), origConstraints)

	// Run the operation 10000 times per execution of the benchmark function. This
	// causes GC pressure to become a factor and a huge discrepancy in the number
	// of times the test can be run in one second is apparent. Example results:
	//                          # of executions in 1 sec     Time per execution
	// ------------------------------------------------------------------------
	// ConstructFilterTest-32         	               7        146437858 ns/op
	// CopyConstructFilterTest-32             1000000000           0.1694 ns/op

	const numOps = 10000
	runBench1 := func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < numOps; i++ {
			_ = replace(selectExpr)
		}
		b.StopTimer()
	}
	runBench2 := func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < numOps; i++ {
			_ = replace(&origConstraints)
		}
		b.StopTimer()
	}

	// This is the slower way to construct a copy of filters.
	b.Run(`ConstructFilterTest`, func(b *testing.B) {
		runBench1(b)
	})

	// Copy constructing filters should be faster.
	b.Run(`CopyConstructFilterTest`, func(b *testing.B) {
		runBench2(b)
	})

}

func makeColMap(fromTable *opt.TableMeta, toTable *opt.TableMeta) opt.ColMap {
	colMap := opt.ColMap{}
	for i := 0; i < fromTable.Table.ColumnCount(); i++ {
		colMap.Set(int(fromTable.MetaID.ColumnID(i)), int(toTable.MetaID.ColumnID(i)))
	}
	return colMap
}
