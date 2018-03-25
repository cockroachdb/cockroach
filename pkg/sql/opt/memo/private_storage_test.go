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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestPrivateStorage(t *testing.T) {
	var ps privateStorage
	ps.init()
	test := func(left, right interface{}, expected bool) {
		t.Helper()
		leftID := ps.intern(left)
		rightID := ps.intern(right)
		if (leftID == rightID) != expected {
			t.Errorf("%v == %v, expected %v, got %v", left, right, expected, !expected)
		}
	}

	// SQL types
	test(types.Unknown, types.Unknown, true)
	test(types.Bool, types.Bool, true)
	test(types.Int, types.Int, true)
	test(types.Float, types.Float, true)
	test(types.Decimal, types.Decimal, true)
	test(types.String, types.String, true)
	test(types.Bytes, types.Bytes, true)
	test(types.Date, types.Date, true)
	test(types.Time, types.Time, true)
	test(types.Timestamp, types.Timestamp, true)
	test(types.TimestampTZ, types.TimestampTZ, true)
	test(types.Interval, types.Interval, true)
	test(types.JSON, types.JSON, true)
	test(types.UUID, types.UUID, true)
	test(types.INet, types.INet, true)
	test(types.Any, types.Any, true)
	test(types.AnyArray, types.TArray{Typ: types.Any}, true)
	test(types.TArray{Typ: types.Int}, types.TArray{Typ: types.Int}, true)
	test(types.TArray{Typ: types.TTuple{types.Int}}, types.TArray{Typ: types.TTuple{types.Int}}, true)
	test(types.TTuple{types.Int, types.String}, types.TTuple{types.Int, types.String}, true)
	test(types.Bool, types.Int, false)
	test(types.Timestamp, types.TimestampTZ, false)
	test(types.TArray{Typ: types.Int}, types.TArray{Typ: types.Decimal}, false)
	test(types.TTuple{types.Int}, types.TTuple{types.Decimal}, false)
	test(types.TTuple{types.Int, types.String}, types.TTuple{types.String, types.Int}, false)

	// TTable type ignores labels as part of key.
	ttuple := types.TTuple{types.Bytes, types.JSON}
	test(types.TTable{Cols: ttuple, Labels: []string{"a"}}, types.TTable{Cols: ttuple}, true)

	// Datum types
	test(tree.DNull, tree.DNull, true)
	test(tree.DBoolTrue, tree.DBoolTrue, true)
	test(tree.DBoolTrue, tree.DBoolFalse, false)
	test(tree.NewDInt(1), tree.NewDInt(1), true)
	test(tree.NewDFloat(1.0), tree.NewDFloat(1e0), true)

	dec1, _ := tree.ParseDDecimal("1.0")
	dec2, _ := tree.ParseDDecimal("1.0")
	test(dec1, dec2, true)
	dec3, _ := tree.ParseDDecimal("1")
	test(dec2, dec3, false)
	test(dec3, tree.NewDInt(1), false)

	test(tree.NewDString("foo"), tree.NewDString("foo"), true)
	test(tree.NewDString("foo"), tree.NewDString("food"), false)
	test(tree.NewDBytes(tree.DBytes([]byte{0})), tree.NewDBytes(tree.DBytes([]byte{0})), true)
	test(tree.NewDString(""), tree.NewDBytes(""), false)
	test(tree.NewDDate(1000), tree.NewDDate(1000), true)
	test(tree.MakeDTime(0), tree.MakeDTime(0), true)
	test(tree.NewDDate(100), tree.MakeDTime(100), false)
	test(&tree.DInterval{}, &tree.DInterval{}, true)
	test(&tree.DUuid{UUID: uuid.Nil}, &tree.DUuid{UUID: uuid.Nil}, true)
	test(&tree.DIPAddr{}, &tree.DIPAddr{}, true)

	arr1 := tree.NewDArray(types.Int)
	arr1.Array = tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}
	arr2 := tree.NewDArray(types.Int)
	arr2.Array = tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}
	test(arr1, arr2, true)

	// ColumnID type.
	test(opt.ColumnID(1), opt.ColumnID(1), true)
	test(opt.ColumnID(1), opt.ColumnID(2), false)

	// ColSet type.
	test(util.MakeFastIntSet(), util.MakeFastIntSet(), true)
	test(util.MakeFastIntSet(0), util.MakeFastIntSet(0), true)
	test(util.MakeFastIntSet(0, 1, 2, 10, 63), util.MakeFastIntSet(2, 63, 1, 10, 0), true)
	test(util.MakeFastIntSet(64, 100, 1000), util.MakeFastIntSet(1000, 100, 64), true)
	test(util.MakeFastIntSet(1, 10, 100, 1000), util.MakeFastIntSet(10, 100, 1, 1000), true)
	test(util.MakeFastIntSet(1), util.MakeFastIntSet(2), false)
	test(util.MakeFastIntSet(1, 2), util.MakeFastIntSet(1, 2, 1000), false)

	// ColList type.
	test(opt.ColList{}, opt.ColList{}, true)
	test(opt.ColList{1, 2, 3}, opt.ColList{1, 2, 3}, true)
	test(opt.ColList{1, 10, 100}, opt.ColList{1, 10, 100}, true)
	test(opt.ColList{1, 2, 3}, opt.ColList{1, 3, 2}, false)
	test(opt.ColList{1, 2}, opt.ColList{1, 2, 3}, false)

	// ScanOpDef type.
	scanDef1 := &ScanOpDef{Table: 0, Cols: util.MakeFastIntSet(1, 2)}
	scanDef2 := &ScanOpDef{Table: 0, Cols: util.MakeFastIntSet(2, 1)}
	test(scanDef1, scanDef2, true)
	scanDef3 := &ScanOpDef{Table: 1, Cols: util.MakeFastIntSet(1, 2)}
	test(scanDef1, scanDef3, false)
	scanDef4 := &ScanOpDef{Table: 1, Cols: util.MakeFastIntSet(1, 2, 3)}
	test(scanDef3, scanDef4, false)

	// FuncOpDef type.
	ttuple1 := types.TTuple{types.Int}
	ttuple2 := types.TTuple{types.Decimal}
	funcDef1 := &FuncOpDef{Name: "foo", Type: ttuple1, Overload: &builtins.Builtins["now"][0]}
	funcDef2 := &FuncOpDef{Name: "bar", Type: ttuple2, Overload: &builtins.Builtins["now"][0]}
	test(funcDef1, funcDef2, true)
	funcDef3 := &FuncOpDef{Name: "bar", Type: ttuple2, Overload: &builtins.Builtins["now"][1]}
	test(funcDef2, funcDef3, false)

	// SetOpColMap type.
	list1 := opt.ColList{1, 2}
	list2 := opt.ColList{3, 4}
	list3 := opt.ColList{5, 6}
	list4 := opt.ColList{1, 2}
	list5 := opt.ColList{3, 4}
	list6 := opt.ColList{5, 6}
	test(&SetOpColMap{list1, list2, list3}, &SetOpColMap{list4, list5, list6}, true)
	test(&SetOpColMap{list1, list2, list3}, &SetOpColMap{list1, list3, list2}, false)

	// Ordering type.
	test(Ordering{}, Ordering{}, true)
	test(Ordering{1, -1, 0}, Ordering{1, -1, 0}, true)
	test(Ordering{1, -1, 0}, Ordering{-1, 0, 1}, false)
	test(Ordering{1, 2}, Ordering{1, 2, 3}, false)
}
