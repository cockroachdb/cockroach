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

package memo

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestInternColumnID(t *testing.T) {
	var ps privateStorage
	ps.init()

	id1 := ps.internColumnID(opt.ColumnID(1))
	id2 := ps.internColumnID(opt.ColumnID(1))
	if id1 != id2 {
		t.Errorf("same column ID didn't return same private ID")
	}

	if ps.internColumnID(opt.ColumnID(1)) == ps.internColumnID(opt.ColumnID(2)) {
		t.Errorf("different column IDs didn't return different private IDs")
	}
}

func TestInternColSet(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right opt.ColSet, expected bool) {
		t.Helper()
		leftID := ps.internColSet(left)
		rightID := ps.internColSet(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(util.MakeFastIntSet(), util.MakeFastIntSet(), true)
	test(util.MakeFastIntSet(0), util.MakeFastIntSet(0), true)
	test(util.MakeFastIntSet(0, 1, 2, 10, 63), util.MakeFastIntSet(2, 63, 1, 10, 0), true)
	test(util.MakeFastIntSet(64, 100, 1000), util.MakeFastIntSet(1000, 100, 64), true)
	test(util.MakeFastIntSet(1, 10, 100, 1000), util.MakeFastIntSet(10, 100, 1, 1000), true)
	test(util.MakeFastIntSet(1), util.MakeFastIntSet(2), false)
	test(util.MakeFastIntSet(1, 2), util.MakeFastIntSet(1, 2, 1000), false)
}

func TestInternColList(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right opt.ColList, expected bool) {
		t.Helper()
		leftID := ps.internColList(left)
		rightID := ps.internColList(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(opt.ColList{}, opt.ColList{}, true)
	test(opt.ColList{1, 2, 3}, opt.ColList{1, 2, 3}, true)
	test(opt.ColList{1, 10, 100}, opt.ColList{1, 10, 100}, true)
	test(opt.ColList{1, 2, 3}, opt.ColList{1, 3, 2}, false)
	test(opt.ColList{1, 2}, opt.ColList{1, 2, 3}, false)
}

func TestInternScanOpDef(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right *ScanOpDef, expected bool) {
		t.Helper()
		leftID := ps.internScanOpDef(left)
		rightID := ps.internScanOpDef(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	scanDef1 := &ScanOpDef{Table: 0, Index: 1, Cols: util.MakeFastIntSet(1, 2)}
	scanDef2 := &ScanOpDef{Table: 0, Index: 1, Cols: util.MakeFastIntSet(2, 1)}
	test(scanDef1, scanDef2, true)
	scanDef3 := &ScanOpDef{Table: 1, Index: 1, Cols: util.MakeFastIntSet(1, 2)}
	test(scanDef1, scanDef3, false)
	scanDef4 := &ScanOpDef{Table: 1, Index: 1, Cols: util.MakeFastIntSet(1, 2, 3)}
	test(scanDef3, scanDef4, false)
	scanDef5 := &ScanOpDef{Table: 1, Index: 2, Cols: util.MakeFastIntSet(1, 2)}
	test(scanDef3, scanDef5, false)
}

func TestInternFuncOpDef(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right *FuncOpDef, expected bool) {
		t.Helper()
		leftID := ps.internFuncOpDef(left)
		rightID := ps.internFuncOpDef(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	ttuple1 := types.TTuple{types.Int}
	ttuple2 := types.TTuple{types.Decimal}
	funcDef1 := &FuncOpDef{Name: "foo", Type: ttuple1, Overload: &builtins.Builtins["now"][0]}
	funcDef2 := &FuncOpDef{Name: "bar", Type: ttuple2, Overload: &builtins.Builtins["now"][0]}
	test(funcDef1, funcDef2, true)
	funcDef3 := &FuncOpDef{Name: "bar", Type: ttuple2, Overload: &builtins.Builtins["now"][1]}
	test(funcDef2, funcDef3, false)
}

func TestInternSetOpColMap(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right *SetOpColMap, expected bool) {
		t.Helper()
		leftID := ps.internSetOpColMap(left)
		rightID := ps.internSetOpColMap(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	list1 := opt.ColList{1, 2}
	list2 := opt.ColList{3, 4}
	list3 := opt.ColList{5, 6}
	list4 := opt.ColList{1, 2}
	list5 := opt.ColList{3, 4}
	list6 := opt.ColList{5, 6}
	test(&SetOpColMap{list1, list2, list3}, &SetOpColMap{list4, list5, list6}, true)
	test(&SetOpColMap{list1, list2, list3}, &SetOpColMap{list1, list3, list2}, false)
}

func TestInternOrdering(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right Ordering, expected bool) {
		t.Helper()
		leftID := ps.internOrdering(left)
		rightID := ps.internOrdering(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(Ordering{}, Ordering{}, true)
	test(Ordering{1, -1, 0}, Ordering{1, -1, 0}, true)
	test(Ordering{1, -1, 0}, Ordering{-1, 1, 0}, false)
	test(Ordering{1, -1, 0}, Ordering{-1, 0, 1}, false)
	test(Ordering{1, 2}, Ordering{1, 2, 3}, false)
}

func TestInternDatum(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right tree.Datum, expected bool) {
		t.Helper()
		leftID := ps.internDatum(left)
		rightID := ps.internDatum(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(tree.DNull, tree.DNull, true)
	test(tree.DBoolTrue, tree.DBoolFalse, false)
	test(tree.NewDFloat(1.0), tree.NewDFloat(1e0), true)
	test(tree.NewDString("foo"), tree.NewDString("foo"), true)

	dec1, _ := tree.ParseDDecimal("1.0")
	dec2, _ := tree.ParseDDecimal("1.0")
	test(dec1, dec2, true)
	dec3, _ := tree.ParseDDecimal("1")
	test(dec2, dec3, false)
	test(dec3, tree.NewDInt(1), false)

	arr1 := tree.NewDArray(types.Int)
	arr1.Array = tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}
	arr2 := tree.NewDArray(types.Int)
	arr2.Array = tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}
	test(arr1, arr2, true)
}

func TestInternType(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right types.T, expected bool) {
		t.Helper()
		leftID := ps.internType(left)
		rightID := ps.internType(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(types.Unknown, types.Unknown, true)
	test(types.Int, types.Int, true)
	test(types.Timestamp, types.TimestampTZ, false)
	test(types.AnyArray, types.TArray{Typ: types.Any}, true)
	test(types.TArray{Typ: types.Int}, types.TArray{Typ: types.Int}, true)
	test(types.TArray{Typ: types.Int}, types.TArray{Typ: types.Decimal}, false)

	tarr1 := types.TArray{Typ: types.TTuple{types.Int, types.String}}
	tarr2 := types.TArray{Typ: types.TTuple{types.Int, types.String}}
	test(tarr1, tarr2, true)

	test(types.TTuple{types.Int, types.Decimal}, types.TTuple{types.Decimal, types.Int}, false)

	// TTable type ignores labels as part of key.
	ttuple := types.TTuple{types.Bytes, types.JSON}
	test(types.TTable{Cols: ttuple, Labels: []string{"a"}}, types.TTable{Cols: ttuple}, true)
}

func TestInternTypedExpr(t *testing.T) {
	var ps privateStorage
	ps.init()

	d := tree.NewDInt(1)
	id1 := ps.internTypedExpr(d)
	id2 := ps.internTypedExpr(d)
	if id1 != id2 {
		t.Errorf("same datum instance didn't return same private ID")
	}

	id1 = ps.internTypedExpr(tree.NewDInt(1))
	id2 = ps.internTypedExpr(tree.NewDInt(1))
	if id1 == id2 {
		t.Errorf("different datum instances didn't return different private IDs")
	}
}

// Ensure that interning values that already exist does not cause unexpected
// allocations.
func TestPrivateStorageAllocations(t *testing.T) {
	var ps privateStorage
	ps.init()

	colID := opt.ColumnID(1)
	colSet := util.MakeFastIntSet(1, 2, 3)
	colList := opt.ColList{3, 2, 1}
	ordering := Ordering{1, -2, 3}
	funcOpDef := &FuncOpDef{
		Name:     "concat",
		Type:     types.String,
		Overload: &builtins.Builtins["concat"][0],
	}
	scanOpDef := &ScanOpDef{Table: 1, Index: 2, Cols: colSet}
	setOpColMap := &SetOpColMap{Left: colList, Right: colList, Out: colList}
	datum := tree.NewDInt(1)
	typ := types.Int

	result := testutils.TestNoMallocs(func() {
		ps.internColumnID(colID)
		ps.internColSet(colSet)
		ps.internColList(colList)
		ps.internOrdering(ordering)
		ps.internFuncOpDef(funcOpDef)
		ps.internScanOpDef(scanOpDef)
		ps.internSetOpColMap(setOpColMap)
		ps.internDatum(datum)
		ps.internType(typ)
		ps.internTypedExpr(datum)
	})

	if !result {
		t.Errorf("intern methods should not allocate after initial add")
	}
}

func BenchmarkPrivateStorage(b *testing.B) {
	var ps privateStorage
	ps.init()

	colID := opt.ColumnID(1)
	colSet := util.MakeFastIntSet(1, 2, 3)
	colList := opt.ColList{3, 2, 1}
	ordering := Ordering{1, -2, 3}
	funcOpDef := &FuncOpDef{
		Name:     "concat",
		Type:     types.String,
		Overload: &builtins.Builtins["concat"][0],
	}
	scanOpDef := &ScanOpDef{Table: 1, Index: 2, Cols: colSet}
	setOpColMap := &SetOpColMap{Left: colList, Right: colList, Out: colList}
	datum := tree.NewDInt(1)
	typ := types.Int

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.internColumnID(colID)
		ps.internColSet(colSet)
		ps.internColList(colList)
		ps.internOrdering(ordering)
		ps.internFuncOpDef(funcOpDef)
		ps.internScanOpDef(scanOpDef)
		ps.internSetOpColMap(setOpColMap)
		ps.internDatum(datum)
		ps.internType(typ)
		ps.internTypedExpr(datum)
	}
}
