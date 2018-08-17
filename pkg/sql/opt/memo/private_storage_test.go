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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
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

func TestInternProjectionsOpDef(t *testing.T) {
	var ps privateStorage
	ps.init()

	defs := [...]ProjectionsOpDef{
		1: {PassthroughCols: util.MakeFastIntSet(1, 2)},
		2: {PassthroughCols: util.MakeFastIntSet(2, 1)},
		3: {SynthesizedCols: opt.ColList{1, 2}},
		4: {SynthesizedCols: opt.ColList{5, 6, 7}},
		5: {SynthesizedCols: opt.ColList{5, 7, 6}},
		6: {
			SynthesizedCols: opt.ColList{5, 6, 7},
			PassthroughCols: util.MakeFastIntSet(1, 2),
		},
		7: {
			SynthesizedCols: opt.ColList{5, 6, 7},
			PassthroughCols: util.MakeFastIntSet(2, 1),
		},
		8: {
			SynthesizedCols: opt.ColList{5, 7, 6},
			PassthroughCols: util.MakeFastIntSet(1, 2),
		},
		9: {
			SynthesizedCols: opt.ColList{5, 6, 7},
			PassthroughCols: util.MakeFastIntSet(1, 3),
		},
		10: {
			SynthesizedCols: opt.ColList{5, 6, 7},
			PassthroughCols: util.MakeFastIntSet(8),
		},
		11: {
			SynthesizedCols: opt.ColList{5, 6},
			PassthroughCols: util.MakeFastIntSet(7, 8),
		},
	}

	// These are the pairs that are equal. Any other pairs are not equal.
	equalPairs := map[[2]int]bool{
		{1, 2}: true,
		{6, 7}: true,
	}

	for i := 1; i < len(defs); i++ {
		for j := i; j < len(defs); j++ {
			expected := (i == j) || equalPairs[[2]int{i, j}]
			leftID := ps.internProjectionsOpDef(&defs[i])
			rightID := ps.internProjectionsOpDef(&defs[j])
			if (leftID == rightID) != expected {
				t.Errorf("%v == %v, expected %v, got %v", defs[i], defs[j], expected, !expected)
			}
		}
	}
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
	scanDef6 := &ScanOpDef{Table: 1, Index: 2, Cols: util.MakeFastIntSet(1, 2), HardLimit: 10}
	test(scanDef5, scanDef6, false)
	scanDef7 := &ScanOpDef{Table: 1, Index: 2, Cols: util.MakeFastIntSet(1, 2), HardLimit: -10}
	test(scanDef5, scanDef7, false)
	scanDef8 := &ScanOpDef{Table: 1, Index: 2, Cols: util.MakeFastIntSet(1, 2), Flags: ScanFlags{NoIndexJoin: true}}
	test(scanDef5, scanDef8, false)
	scanDef9 := &ScanOpDef{Table: 1, Index: 2, Cols: util.MakeFastIntSet(1, 2), Flags: ScanFlags{ForceIndex: true}}
	test(scanDef5, scanDef9, false)
	test(scanDef8, scanDef9, false)
	scanDef10 := &ScanOpDef{Table: 1, Index: 2, Cols: util.MakeFastIntSet(1, 2), Flags: ScanFlags{ForceIndex: true, Index: 1}}
	test(scanDef5, scanDef10, false)
	test(scanDef8, scanDef10, false)
	test(scanDef9, scanDef10, false)
}

func TestInternGroupByDef(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right *GroupByDef, expected bool) {
		t.Helper()
		leftID := ps.internGroupByDef(left)
		rightID := ps.internGroupByDef(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	groupByDef1 := &GroupByDef{util.MakeFastIntSet(1, 2), props.ParseOrderingChoice("+1,-2")}
	groupByDef2 := &GroupByDef{util.MakeFastIntSet(2, 1), props.ParseOrderingChoice("+1,-2")}
	groupByDef3 := &GroupByDef{util.MakeFastIntSet(1), props.ParseOrderingChoice("+1,+2")}
	groupByDef4 := &GroupByDef{util.MakeFastIntSet(), props.ParseOrderingChoice("+1,+2,+3")}

	test(groupByDef1, groupByDef2, true)
	test(groupByDef1, groupByDef3, false)
	test(groupByDef1, groupByDef4, false)
	test(groupByDef3, groupByDef4, false)
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

	ttuple1 := types.TTuple{Types: []types.T{types.Int}}
	ttuple2 := types.TTuple{Types: []types.T{types.Decimal}}
	nowProps, nowOvls := builtins.GetBuiltinProperties("now")
	funcDef1 := &FuncOpDef{Name: "foo", Type: ttuple1, Properties: nowProps, Overload: &nowOvls[0]}
	funcDef2 := &FuncOpDef{Name: "bar", Type: ttuple2, Properties: nowProps, Overload: &nowOvls[0]}
	test(funcDef1, funcDef2, true)
	funcDef3 := &FuncOpDef{Name: "bar", Type: ttuple2, Properties: nowProps, Overload: &nowOvls[1]}
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

func TestInternOperator(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right opt.Operator, expected bool) {
		t.Helper()
		leftID := ps.internOperator(left)
		rightID := ps.internOperator(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(opt.PlusOp, opt.PlusOp, true)
	test(opt.PlusOp, opt.MinusOp, false)
	test(opt.MinusOp, opt.PlusOp, false)
}

func TestInternOrdering(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right opt.Ordering, expected bool) {
		t.Helper()
		leftID := ps.internOrdering(left)
		rightID := ps.internOrdering(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test(opt.Ordering{}, opt.Ordering{}, true)
	test(opt.Ordering{1, -1, 0}, opt.Ordering{1, -1, 0}, true)
	test(opt.Ordering{1, -1, 0}, opt.Ordering{-1, 1, 0}, false)
	test(opt.Ordering{1, -1, 0}, opt.Ordering{-1, 0, 1}, false)
	test(opt.Ordering{1, 2}, opt.Ordering{1, 2, 3}, false)
}

func TestInternOrderingChoice(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right string, expected bool) {
		t.Helper()
		leftOrd := props.ParseOrderingChoice(left)
		rightOrd := props.ParseOrderingChoice(right)
		if (ps.internOrderingChoice(&leftOrd) == ps.internOrderingChoice(&rightOrd)) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	test("", "", true)
	test("+1", "+1", true)
	test("+(1|2)", "+(2|1)", true)
	test("+1 opt(2)", "+1 opt(2)", true)
	test("+1", "-1", false)
	test("+1,+2", "+1", false)
	test("+(1|2)", "+1", false)
	test("+1 opt(2)", "+1", false)
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

	tarr1 := types.TArray{Typ: types.TTuple{Types: []types.T{types.Int, types.String}}}
	tarr2 := types.TArray{Typ: types.TTuple{Types: []types.T{types.Int, types.String}}}
	test(tarr1, tarr2, true)

	test(
		types.TTuple{Types: []types.T{types.Int, types.Decimal}},
		types.TTuple{Types: []types.T{types.Decimal, types.Int}},
		false,
	)
}

func TestInternColType(t *testing.T) {
	var ps privateStorage
	ps.init()

	test := func(left, right coltypes.T, expected bool) {
		t.Helper()
		leftID := ps.internColType(left)
		rightID := ps.internColType(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	// Arithmetic types.
	test(coltypes.Bool, &coltypes.TBool{}, true)

	test(coltypes.SmallInt, &coltypes.TInt{Name: "SMALLINT", Width: 16, ImplicitWidth: true}, true)
	test(&coltypes.TInt{Name: "BIT", Width: 8}, &coltypes.TInt{Name: "BIT", Width: 12}, false)

	test(coltypes.Float4, &coltypes.TFloat{Name: "FLOAT4", Width: 32}, true)
	test(coltypes.Float8, &coltypes.TFloat{Name: "DOUBLE PRECISION", Width: 64}, false)
	test(coltypes.Float, coltypes.NewFloat(64, true), false)
	test(coltypes.NewFloat(0, true), coltypes.NewFloat(0, true), true)

	tdec := &coltypes.TDecimal{Name: "DECIMAL", Prec: 19}
	test(coltypes.Numeric, &coltypes.TDecimal{Name: "NUMERIC"}, true)
	test(coltypes.Decimal, tdec, false)
	test(tdec, &coltypes.TDecimal{Name: "DECIMAL", Prec: 19, Scale: 2}, false)

	// Miscellaneous types.
	test(coltypes.UUID, &coltypes.TUUID{}, true)
	test(coltypes.INet, &coltypes.TIPAddr{}, true)
	test(coltypes.JSON, &coltypes.TJSON{Name: "JSON"}, true)
	test(coltypes.JSONB, &coltypes.TJSON{Name: "JSONB"}, true)
	test(coltypes.JSON, coltypes.JSONB, false)
	test(coltypes.Oid, &coltypes.TOid{Name: "OID"}, true)

	// String types.
	test(coltypes.String, &coltypes.TString{Name: "STRING"}, true)
	test(coltypes.VarChar, &coltypes.TString{Name: "VARCHAR"}, true)
	test(coltypes.VarChar, coltypes.String, false)
	test(&coltypes.TString{Name: "VARCHAR", N: 9}, &coltypes.TString{Name: "VARCHAR", N: 10}, false)

	tstr1 := &coltypes.TCollatedString{Name: "STRING"}
	tstr2 := &coltypes.TCollatedString{Name: "STRING", N: 256}
	tstr3 := &coltypes.TCollatedString{Name: "STRING", N: 256, Locale: "en_US"}
	tstr4 := &coltypes.TCollatedString{Name: "STRING", Locale: "en_US"}
	test(tstr1, tstr2, false)
	test(tstr2, tstr2, true)
	test(tstr2, tstr3, false)
	test(tstr2, tstr4, false)
	test(tstr3, tstr4, false)

	test(coltypes.Name, &coltypes.TName{}, true)
	test(coltypes.Bytes, &coltypes.TBytes{}, true)

	// Time/date types.
	test(coltypes.Date, &coltypes.TDate{}, true)
	test(coltypes.Time, &coltypes.TTime{}, true)
	test(coltypes.Timestamp, &coltypes.TTimestamp{}, true)
	test(coltypes.TimestampWithTZ, &coltypes.TTimestampTZ{}, true)
	test(coltypes.Interval, &coltypes.TInterval{}, true)

	// Arrays types.
	tarr1, _ := coltypes.ArrayOf(coltypes.Int, []int32{-1})
	tarr2, _ := coltypes.ArrayOf(coltypes.Int, []int32{3, 3})
	tarr3, _ := coltypes.ArrayOf(coltypes.String, []int32{3, 3})
	tarr4, _ := coltypes.ArrayOf(tarr1, []int32{-1})
	test(tarr1, tarr2, true)
	test(tarr2, tarr3, false)
	test(tarr1, tarr4, false)
	test(tarr4, tarr4, true)
	test(coltypes.Int2vector, &coltypes.TVector{Name: "INT2VECTOR", ParamType: coltypes.Int}, true)
	test(coltypes.OidVector, &coltypes.TVector{Name: "OIDVECTOR", ParamType: coltypes.Oid}, true)
	test(coltypes.Int2vector, coltypes.OidVector, false)

	// Tuple types.
	ttup1 := coltypes.TTuple{coltypes.Int}
	ttup2 := coltypes.TTuple{coltypes.String, coltypes.Int}
	ttup3 := coltypes.TTuple{coltypes.Int, coltypes.String}
	test(ttup1, coltypes.TTuple{coltypes.Int}, true)
	test(ttup2, coltypes.TTuple{coltypes.String, coltypes.Int}, true)
	test(ttup2, ttup3, false)
	test(coltypes.TTuple{tarr1, tarr2}, coltypes.TTuple{tarr2, tarr1}, true)
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

func TestInternPhysProps(t *testing.T) {
	var ps privateStorage
	ps.init()

	presentation := props.Presentation{opt.LabeledColumn{Label: "c", ID: 1}}
	ordering := props.ParseOrderingChoice("+(1|2),+3 opt(4,5)")

	p := props.Physical{Presentation: presentation, Ordering: ordering}
	id1 := ps.internPhysProps(&p)
	id2 := ps.internPhysProps(&p)
	if id1 != id2 {
		t.Errorf("same physical property instance didn't return same private ID")
	}

	presentation2 := props.Presentation{opt.LabeledColumn{Label: "d", ID: 1}}
	ordering2 := props.ParseOrderingChoice("+(1|2),+3 opt(4,6)")

	p2 := props.Physical{Presentation: presentation2, Ordering: ordering}
	id1 = ps.internPhysProps(&p)
	id2 = ps.internPhysProps(&p2)
	if id1 == id2 {
		t.Errorf("different physical property instances didn't return different private IDs")
	}

	p3 := props.Physical{Presentation: presentation2, Ordering: ordering2}
	id1 = ps.internPhysProps(&p2)
	id2 = ps.internPhysProps(&p3)
	if id1 == id2 {
		t.Errorf("different physical property instances didn't return different private IDs")
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
	ordering := opt.Ordering{1, -2, 3}
	orderingChoice := props.ParseOrderingChoice("+1,-2 opt(3)")
	op := opt.PlusOp
	concatProps, concatOvls := builtins.GetBuiltinProperties("concat")
	funcOpDef := &FuncOpDef{
		Name:       "concat",
		Type:       types.String,
		Properties: concatProps,
		Overload:   &concatOvls[0],
	}
	projectionsOpDef := &ProjectionsOpDef{
		SynthesizedCols: colList,
		PassthroughCols: colSet,
	}
	scanOpDef := &ScanOpDef{Table: 1, Index: 2, Cols: colSet, Flags: ScanFlags{NoIndexJoin: true}}
	groupByDef := &GroupByDef{GroupingCols: colSet, Ordering: props.ParseOrderingChoice("+1")}
	mergeOnDef := &MergeOnDef{
		LeftEq:        opt.Ordering{+1, +2, +3},
		RightEq:       opt.Ordering{+4, +5, +6},
		LeftOrdering:  props.ParseOrderingChoice("+1,+2,+3"),
		RightOrdering: props.ParseOrderingChoice("+4,+5,+6"),
	}
	indexJoinDef := &IndexJoinDef{Table: 1, Cols: colSet}
	lookupJoinDef := &LookupJoinDef{Table: 1, Index: 2, KeyCols: colList, LookupCols: colSet}
	setOpColMap := &SetOpColMap{Left: colList, Right: colList, Out: colList}
	datum := tree.NewDInt(1)
	typ := types.Int
	colTyp := coltypes.Int
	presentation := props.Presentation{opt.LabeledColumn{Label: "c", ID: 1}}
	physProps := props.Physical{Presentation: presentation, Ordering: orderingChoice}

	testutils.TestNoMallocs(t, func() {
		ps.internColumnID(colID)
		ps.internColList(colList)
		ps.internOperator(op)
		ps.internOrdering(ordering)
		ps.internOrderingChoice(&orderingChoice)
		ps.internProjectionsOpDef(projectionsOpDef)
		ps.internFuncOpDef(funcOpDef)
		ps.internScanOpDef(scanOpDef)
		ps.internGroupByDef(groupByDef)
		ps.internMergeOnDef(mergeOnDef)
		ps.internIndexJoinDef(indexJoinDef)
		ps.internLookupJoinDef(lookupJoinDef)
		ps.internSetOpColMap(setOpColMap)
		ps.internDatum(datum)
		ps.internType(typ)
		ps.internColType(colTyp)
		ps.internTypedExpr(datum)
		ps.internPhysProps(&physProps)
	})
}

func BenchmarkPrivateStorage(b *testing.B) {
	var ps privateStorage
	ps.init()

	colID := opt.ColumnID(1)
	colSet := util.MakeFastIntSet(1, 2, 3)
	colList := opt.ColList{3, 2, 1}
	ordering := opt.Ordering{1, -2, 3}
	orderingChoice := props.ParseOrderingChoice("+1,-2 opt(3)")
	op := opt.PlusOp
	funcProps, overloads := builtins.GetBuiltinProperties("concat")
	funcOpDef := &FuncOpDef{
		Name:       "concat",
		Type:       types.String,
		Properties: funcProps,
		Overload:   &overloads[0],
	}
	projectionsOpDef := &ProjectionsOpDef{
		SynthesizedCols: colList,
		PassthroughCols: colSet,
	}
	scanOpDef := &ScanOpDef{Table: 1, Index: 2, Cols: colSet}
	groupByDef := &GroupByDef{GroupingCols: colSet, Ordering: props.ParseOrderingChoice("+1")}
	mergeOnDef := &MergeOnDef{
		LeftEq:        opt.Ordering{+1, +2, +3},
		RightEq:       opt.Ordering{+4, +5, +6},
		LeftOrdering:  props.ParseOrderingChoice("+1,+2,+3"),
		RightOrdering: props.ParseOrderingChoice("+4,+5,+6"),
	}
	indexJoinDef := &IndexJoinDef{Table: 1, Cols: colSet}
	lookupJoinDef := &LookupJoinDef{Table: 1, Index: 2, KeyCols: colList, LookupCols: colSet}
	setOpColMap := &SetOpColMap{Left: colList, Right: colList, Out: colList}
	datum := tree.NewDInt(1)
	typ := types.Int
	colTyp := coltypes.Int
	presentation := props.Presentation{opt.LabeledColumn{Label: "c", ID: 1}}
	physProps := props.Physical{Presentation: presentation, Ordering: orderingChoice}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.internColumnID(colID)
		ps.internColList(colList)
		ps.internOperator(op)
		ps.internOrdering(ordering)
		ps.internOrderingChoice(&orderingChoice)
		ps.internFuncOpDef(funcOpDef)
		ps.internProjectionsOpDef(projectionsOpDef)
		ps.internScanOpDef(scanOpDef)
		ps.internScanOpDef(scanOpDef)
		ps.internGroupByDef(groupByDef)
		ps.internMergeOnDef(mergeOnDef)
		ps.internIndexJoinDef(indexJoinDef)
		ps.internLookupJoinDef(lookupJoinDef)
		ps.internSetOpColMap(setOpColMap)
		ps.internDatum(datum)
		ps.internType(typ)
		ps.internColType(colTyp)
		ps.internTypedExpr(datum)
		ps.internPhysProps(&physProps)
	}
}
