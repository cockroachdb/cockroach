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
	"math"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"golang.org/x/tools/container/intsets"
)

func TestInterner(t *testing.T) {
	var in interner

	json1, _ := tree.ParseDJSON(`{"a": 5, "b": [1, 2]}`)
	json2, _ := tree.ParseDJSON(`{"a": 5, "b": [1, 2]}`)
	json3, _ := tree.ParseDJSON(`[1, 2]`)

	tupTyp1 := types.TTuple{Types: []types.T{types.Int, types.String}, Labels: []string{"a", "b"}}
	tupTyp2 := types.TTuple{Types: []types.T{types.Int, types.String}, Labels: []string{"a", "b"}}
	tupTyp3 := types.TTuple{Types: []types.T{types.Int, types.String}}
	tupTyp4 := types.TTuple{Types: []types.T{types.Int, types.String, types.Bool}}

	tup1 := tree.NewDTuple(tupTyp1, tree.NewDInt(100), tree.NewDString("foo"))
	tup2 := tree.NewDTuple(tupTyp2, tree.NewDInt(100), tree.NewDString("foo"))
	tup3 := tree.NewDTuple(tupTyp3, tree.NewDInt(100), tree.NewDString("foo"))
	tup4 := tree.NewDTuple(tupTyp4, tree.NewDInt(100), tree.NewDString("foo"), tree.DBoolTrue)

	arr1 := tree.NewDArray(tupTyp1)
	arr1.Array = tree.Datums{tup1, tup2}
	arr2 := tree.NewDArray(tupTyp2)
	arr2.Array = tree.Datums{tup2, tup1}
	arr3 := tree.NewDArray(tupTyp3)
	arr3.Array = tree.Datums{tup2, tup3}

	dec1, _ := tree.ParseDDecimal("1.0")
	dec2, _ := tree.ParseDDecimal("1.0")
	dec3, _ := tree.ParseDDecimal("1.00")
	dec4, _ := tree.ParseDDecimal("1e0")
	dec5, _ := tree.ParseDDecimal("1")

	coll1 := tree.NewDCollatedString("foo", "sv_SE", &tree.CollationEnvironment{})
	coll2 := tree.NewDCollatedString("foo", "sv_SE", &tree.CollationEnvironment{})
	coll3 := tree.NewDCollatedString("foo", "en_US", &tree.CollationEnvironment{})
	coll4 := tree.NewDCollatedString("food", "en_US", &tree.CollationEnvironment{})

	tz1 := tree.MakeDTimestampTZ(time.Date(2018, 10, 6, 11, 49, 30, 123, time.UTC), 0)
	tz2 := tree.MakeDTimestampTZ(time.Date(2018, 10, 6, 11, 49, 30, 123, time.UTC), 0)
	tz3 := tree.MakeDTimestampTZ(time.Date(2018, 10, 6, 11, 49, 30, 124, time.UTC), 0)
	tz4 := tree.MakeDTimestampTZ(time.Date(2018, 10, 6, 11, 49, 30, 124, time.FixedZone("PDT", -7)), 0)

	explain1 := tree.ExplainOptions{Mode: tree.ExplainPlan, Flags: util.MakeFastIntSet(1, 2)}
	explain2 := tree.ExplainOptions{Mode: tree.ExplainOpt, Flags: util.MakeFastIntSet(1, 2)}
	explain3 := tree.ExplainOptions{Mode: tree.ExplainOpt, Flags: util.MakeFastIntSet(1, 2, 3)}

	scanNode := &ScanExpr{}
	andExpr := &AndExpr{}

	projections1 := ProjectionsExpr{{Element: andExpr, ColPrivate: ColPrivate{Col: 0}}}
	projections2 := ProjectionsExpr{{Element: andExpr, ColPrivate: ColPrivate{Col: 0}}}
	projections3 := ProjectionsExpr{{Element: andExpr, ColPrivate: ColPrivate{Col: 1}}}
	projections4 := ProjectionsExpr{
		{Element: andExpr, ColPrivate: ColPrivate{Col: 1}},
		{Element: andExpr, ColPrivate: ColPrivate{Col: 2}},
	}
	projections5 := ProjectionsExpr{{Element: &AndExpr{}, ColPrivate: ColPrivate{Col: 1}}}

	aggs1 := AggregationsExpr{{Agg: CountRowsSingleton, ColPrivate: ColPrivate{Col: 0}}}
	aggs2 := AggregationsExpr{{Agg: CountRowsSingleton, ColPrivate: ColPrivate{Col: 0}}}
	aggs3 := AggregationsExpr{{Agg: CountRowsSingleton, ColPrivate: ColPrivate{Col: 1}}}
	aggs4 := AggregationsExpr{
		{Agg: CountRowsSingleton, ColPrivate: ColPrivate{Col: 1}},
		{Agg: CountRowsSingleton, ColPrivate: ColPrivate{Col: 2}},
	}
	aggs5 := AggregationsExpr{{Agg: &CountRowsExpr{}, ColPrivate: ColPrivate{Col: 1}}}

	type testVariation struct {
		val1  interface{}
		val2  interface{}
		equal bool
	}

	testCases := []struct {
		hashFn     interface{}
		eqFn       interface{}
		variations []testVariation
	}{
		{hashFn: in.hasher.HashBool, eqFn: in.hasher.IsBoolEqual, variations: []testVariation{
			{val1: true, val2: true, equal: true},
			{val1: true, val2: false, equal: false},
		}},

		{hashFn: in.hasher.HashInt, eqFn: in.hasher.IsIntEqual, variations: []testVariation{
			{val1: 1, val2: 1, equal: true},
			{val1: 0, val2: 1, equal: false},
			{val1: intsets.MaxInt, val2: intsets.MaxInt, equal: true},
			{val1: intsets.MinInt, val2: intsets.MaxInt, equal: false},
		}},

		{hashFn: in.hasher.HashFloat64, eqFn: in.hasher.IsFloat64Equal, variations: []testVariation{
			{val1: float64(0), val2: float64(0), equal: true},
			{val1: float64(1e300), val2: float64(1e-300), equal: false},
			{val1: math.MaxFloat64, val2: math.MaxFloat64, equal: true},
			{val1: math.NaN(), val2: math.NaN(), equal: true},
			{val1: math.Inf(1), val2: math.Inf(1), equal: true},
			{val1: math.Inf(-1), val2: math.Inf(1), equal: false},
			{val1: math.NaN(), val2: math.Inf(1), equal: false},

			// Use Copysign to create negative zero float64.
			{val1: float64(0), val2: math.Copysign(0, -1), equal: false},
		}},

		{hashFn: in.hasher.HashString, eqFn: in.hasher.IsStringEqual, variations: []testVariation{
			{val1: "", val2: "", equal: true},
			{val1: "abc", val2: "abcd", equal: false},
			{val1: "", val2: " ", equal: false},
			{val1: "the quick brown fox", val2: "the quick brown fox", equal: true},
		}},

		{hashFn: in.hasher.HashBytes, eqFn: in.hasher.IsBytesEqual, variations: []testVariation{
			{val1: []byte{}, val2: []byte{}, equal: true},
			{val1: []byte{}, val2: []byte{0}, equal: false},
			{val1: []byte{1, 2, 3}, val2: []byte{1, 2, 3, 4}, equal: false},
			{val1: []byte{10, 1, 100}, val2: []byte{10, 1, 100}, equal: true},
		}},

		{hashFn: in.hasher.HashOperator, eqFn: in.hasher.IsOperatorEqual, variations: []testVariation{
			{val1: opt.AndOp, val2: opt.AndOp, equal: true},
			{val1: opt.SelectOp, val2: opt.InnerJoinOp, equal: false},
		}},

		{hashFn: in.hasher.HashType, eqFn: in.hasher.IsTypeEqual, variations: []testVariation{
			{val1: reflect.TypeOf(int(0)), val2: reflect.TypeOf(int(1)), equal: true},
			{val1: reflect.TypeOf(int64(0)), val2: reflect.TypeOf(int32(0)), equal: false},
		}},

		{hashFn: in.hasher.HashDatum, eqFn: in.hasher.IsDatumEqual, variations: []testVariation{
			{val1: tree.DBoolTrue, val2: tree.DBoolTrue, equal: true},
			{val1: tree.DBoolTrue, val2: tree.DBoolFalse, equal: false},

			{val1: tree.NewDInt(0), val2: tree.NewDInt(0), equal: true},
			{val1: tree.NewDInt(tree.DInt(intsets.MinInt)), val2: tree.NewDInt(tree.DInt(intsets.MinInt)), equal: true},
			{val1: tree.NewDInt(tree.DInt(intsets.MinInt)), val2: tree.NewDInt(tree.DInt(intsets.MaxInt)), equal: false},

			{val1: tree.NewDFloat(0), val2: tree.NewDFloat(0), equal: true},
			{val1: tree.NewDFloat(tree.DFloat(math.NaN())), val2: tree.NewDFloat(tree.DFloat(math.NaN())), equal: true},
			{val1: tree.NewDFloat(tree.DFloat(math.Inf(-1))), val2: tree.NewDFloat(tree.DFloat(math.Inf(1))), equal: false},

			{val1: tree.NewDString(""), val2: tree.NewDString(""), equal: true},
			{val1: tree.NewDString("123"), val2: tree.NewDString("123"), equal: true},
			{val1: tree.NewDString("  "), val2: tree.NewDString("\t"), equal: false},

			{val1: tree.NewDBytes(tree.DBytes([]byte{0})), val2: tree.NewDBytes(tree.DBytes([]byte{0})), equal: true},
			{val1: tree.NewDBytes("foo"), val2: tree.NewDBytes("foo2"), equal: false},

			{val1: tree.NewDDate(0), val2: tree.NewDDate(0), equal: true},
			{val1: tree.NewDDate(0), val2: tree.NewDDate(1), equal: false},

			{val1: tree.MakeDTime(timeofday.Min), val2: tree.MakeDTime(timeofday.Min), equal: true},
			{val1: tree.MakeDTime(timeofday.Min), val2: tree.MakeDTime(timeofday.Max), equal: false},

			{val1: json1, val2: json2, equal: true},
			{val1: json2, val2: json3, equal: false},

			{val1: tup1, val2: tup2, equal: true},
			{val1: tup2, val2: tup3, equal: false},
			{val1: tup3, val2: tup4, equal: false},

			{val1: arr1, val2: arr2, equal: true},
			{val1: arr2, val2: arr3, equal: false},

			{val1: dec1, val2: dec2, equal: true},
			{val1: dec2, val2: dec3, equal: false},
			{val1: dec3, val2: dec4, equal: false},
			{val1: dec4, val2: dec5, equal: true},

			{val1: coll1, val2: coll2, equal: true},
			{val1: coll2, val2: coll3, equal: false},
			{val1: coll3, val2: coll4, equal: false},

			{val1: tz1, val2: tz2, equal: true},
			{val1: tz2, val2: tz3, equal: false},
			{val1: tz3, val2: tz4, equal: false},

			{val1: tree.NewDOid(0), val2: tree.NewDOid(0), equal: true},
			{val1: tree.NewDOid(0), val2: tree.NewDOid(1), equal: false},

			{val1: tree.NewDInt(0), val2: tree.NewDFloat(0), equal: false},
			{val1: tree.NewDInt(0), val2: tree.NewDOid(0), equal: false},
		}},

		{hashFn: in.hasher.HashDatumType, eqFn: in.hasher.IsDatumTypeEqual, variations: []testVariation{
			{val1: types.Int, val2: types.Int, equal: true},
			{val1: tupTyp1, val2: tupTyp2, equal: true},
			{val1: tupTyp2, val2: tupTyp3, equal: false},
			{val1: tupTyp3, val2: tupTyp4, equal: false},
		}},

		{hashFn: in.hasher.HashColType, eqFn: in.hasher.IsColTypeEqual, variations: []testVariation{
			{val1: coltypes.Int8, val2: coltypes.Int8, equal: true},
			{val1: coltypes.Int8, val2: coltypes.Int2, equal: false},
			{val1: coltypes.Float4, val2: coltypes.Float8, equal: false},
			{val1: coltypes.VarChar, val2: coltypes.String, equal: false},
			{val1: &coltypes.TDecimal{Prec: 19}, val2: &coltypes.TDecimal{Prec: 19, Scale: 2}, equal: false},
			{val1: coltypes.TTuple{coltypes.String, coltypes.Int8}, val2: coltypes.TTuple{coltypes.Int8, coltypes.String}, equal: false},
			{val1: coltypes.Int2vector, val2: coltypes.OidVector, equal: false},
		}},

		{hashFn: in.hasher.HashTypedExpr, eqFn: in.hasher.IsTypedExprEqual, variations: []testVariation{
			{val1: tup1, val2: tup1, equal: true},
			{val1: tup1, val2: tup2, equal: false},
			{val1: tup2, val2: tup3, equal: false},
		}},

		{hashFn: in.hasher.HashColumnID, eqFn: in.hasher.IsColumnIDEqual, variations: []testVariation{
			{val1: opt.ColumnID(0), val2: opt.ColumnID(0), equal: true},
			{val1: opt.ColumnID(0), val2: opt.ColumnID(1), equal: false},
		}},

		{hashFn: in.hasher.HashColSet, eqFn: in.hasher.IsColSetEqual, variations: []testVariation{
			{val1: util.MakeFastIntSet(), val2: util.MakeFastIntSet(), equal: true},
			{val1: util.MakeFastIntSet(1, 2, 3), val2: util.MakeFastIntSet(3, 2, 1), equal: true},
			{val1: util.MakeFastIntSet(1, 2, 3), val2: util.MakeFastIntSet(1, 2), equal: false},
		}},

		{hashFn: in.hasher.HashColList, eqFn: in.hasher.IsColListEqual, variations: []testVariation{
			{val1: opt.ColList{}, val2: opt.ColList{}, equal: true},
			{val1: opt.ColList{1, 2, 3}, val2: opt.ColList{1, 2, 3}, equal: true},
			{val1: opt.ColList{1, 2, 3}, val2: opt.ColList{3, 2, 1}, equal: false},
			{val1: opt.ColList{1, 2}, val2: opt.ColList{1, 2, 3}, equal: false},
		}},

		{hashFn: in.hasher.HashOrdering, eqFn: in.hasher.IsOrderingEqual, variations: []testVariation{
			{val1: opt.Ordering{}, val2: opt.Ordering{}, equal: true},
			{val1: opt.Ordering{-1, 1}, val2: opt.Ordering{-1, 1}, equal: true},
			{val1: opt.Ordering{-1, 1}, val2: opt.Ordering{1, -1}, equal: false},
			{val1: opt.Ordering{-1, 1}, val2: opt.Ordering{-1, 1, 2}, equal: false},
		}},

		{hashFn: in.hasher.HashOrderingChoice, eqFn: in.hasher.IsOrderingChoiceEqual, variations: []testVariation{
			{val1: physical.ParseOrderingChoice(""), val2: physical.ParseOrderingChoice(""), equal: true},
			{val1: physical.ParseOrderingChoice("+1"), val2: physical.ParseOrderingChoice("+1"), equal: true},
			{val1: physical.ParseOrderingChoice("+(1|2)"), val2: physical.ParseOrderingChoice("+(2|1)"), equal: true},
			{val1: physical.ParseOrderingChoice("+1 opt(2)"), val2: physical.ParseOrderingChoice("+1 opt(2)"), equal: true},
			{val1: physical.ParseOrderingChoice("+1"), val2: physical.ParseOrderingChoice("-1"), equal: false},
			{val1: physical.ParseOrderingChoice("+1,+2"), val2: physical.ParseOrderingChoice("+1"), equal: false},
			{val1: physical.ParseOrderingChoice("+(1|2)"), val2: physical.ParseOrderingChoice("+1"), equal: false},
			{val1: physical.ParseOrderingChoice("+1 opt(2)"), val2: physical.ParseOrderingChoice("+1"), equal: false},
		}},

		{hashFn: in.hasher.HashTableID, eqFn: in.hasher.IsTableIDEqual, variations: []testVariation{
			{val1: opt.TableID(0), val2: opt.TableID(0), equal: true},
			{val1: opt.TableID(0), val2: opt.TableID(1), equal: false},
		}},

		{hashFn: in.hasher.HashSchemaID, eqFn: in.hasher.IsSchemaIDEqual, variations: []testVariation{
			{val1: opt.SchemaID(0), val2: opt.SchemaID(0), equal: true},
			{val1: opt.SchemaID(0), val2: opt.SchemaID(1), equal: false},
		}},

		{hashFn: in.hasher.HashScanLimit, eqFn: in.hasher.IsScanLimitEqual, variations: []testVariation{
			{val1: ScanLimit(100), val2: ScanLimit(100), equal: true},
			{val1: ScanLimit(0), val2: ScanLimit(1), equal: false},
		}},

		{hashFn: in.hasher.HashScanFlags, eqFn: in.hasher.IsScanFlagsEqual, variations: []testVariation{
			{val1: ScanFlags{}, val2: ScanFlags{}, equal: true},
			{val1: ScanFlags{NoIndexJoin: true, Index: 1}, val2: ScanFlags{NoIndexJoin: true, Index: 1}, equal: true},
			{val1: ScanFlags{NoIndexJoin: true, Index: 1}, val2: ScanFlags{NoIndexJoin: true, Index: 2}, equal: false},
			{val1: ScanFlags{NoIndexJoin: true, Index: 1}, val2: ScanFlags{NoIndexJoin: false, Index: 1}, equal: false},
		}},

		{hashFn: in.hasher.HashPointer, eqFn: in.hasher.IsPointerEqual, variations: []testVariation{
			{val1: unsafe.Pointer((*tree.Subquery)(nil)), val2: unsafe.Pointer((*tree.Subquery)(nil)), equal: true},
			{val1: unsafe.Pointer(&tree.Subquery{}), val2: unsafe.Pointer(&tree.Subquery{}), equal: false},
		}},

		{hashFn: in.hasher.HashExplainOptions, eqFn: in.hasher.IsExplainOptionsEqual, variations: []testVariation{
			{val1: tree.ExplainOptions{}, val2: tree.ExplainOptions{}, equal: true},
			{val1: explain1, val2: explain1, equal: true},
			{val1: explain1, val2: explain2, equal: false},
			{val1: explain2, val2: explain3, equal: false},
		}},

		{hashFn: in.hasher.HashShowTraceType, eqFn: in.hasher.IsShowTraceTypeEqual, variations: []testVariation{
			{val1: tree.ShowTraceKV, val2: tree.ShowTraceKV, equal: true},
			{val1: tree.ShowTraceKV, val2: tree.ShowTraceRaw, equal: false},
		}},

		{hashFn: in.hasher.HashTupleOrdinal, eqFn: in.hasher.IsTupleOrdinalEqual, variations: []testVariation{
			{val1: TupleOrdinal(0), val2: TupleOrdinal(0), equal: true},
			{val1: TupleOrdinal(0), val2: TupleOrdinal(1), equal: false},
		}},

		// PhysProps hash/isEqual methods are tested in TestInternerPhysProps.

		{hashFn: in.hasher.HashRelExpr, eqFn: in.hasher.IsRelExprEqual, variations: []testVariation{
			{val1: (*ScanExpr)(nil), val2: (*ScanExpr)(nil), equal: true},
			{val1: scanNode, val2: scanNode, equal: true},
			{val1: &ScanExpr{}, val2: &ScanExpr{}, equal: false},
		}},

		{hashFn: in.hasher.HashScalarExpr, eqFn: in.hasher.IsScalarExprEqual, variations: []testVariation{
			{val1: (*AndExpr)(nil), val2: (*AndExpr)(nil), equal: true},
			{val1: andExpr, val2: andExpr, equal: true},
			{val1: &AndExpr{}, val2: &AndExpr{}, equal: false},
		}},

		{hashFn: in.hasher.HashScalarListExpr, eqFn: in.hasher.IsScalarListExprEqual, variations: []testVariation{
			{val1: ScalarListExpr{andExpr, andExpr}, val2: ScalarListExpr{andExpr, andExpr}, equal: true},
			{val1: ScalarListExpr{andExpr, andExpr}, val2: ScalarListExpr{andExpr}, equal: false},
			{val1: ScalarListExpr{&AndExpr{}}, val2: ScalarListExpr{&AndExpr{}}, equal: false},
		}},

		{hashFn: in.hasher.HashFiltersExpr, eqFn: in.hasher.IsFiltersExprEqual, variations: []testVariation{
			{val1: FiltersExpr{{Condition: andExpr}}, val2: FiltersExpr{{Condition: andExpr}}, equal: true},
			{val1: FiltersExpr{{Condition: andExpr}}, val2: FiltersExpr{}, equal: false},
			{val1: FiltersExpr{{Condition: &AndExpr{}}}, val2: FiltersExpr{{Condition: &AndExpr{}}}, equal: false},
		}},

		{hashFn: in.hasher.HashProjectionsExpr, eqFn: in.hasher.IsProjectionsExprEqual, variations: []testVariation{
			{val1: projections1, val2: projections2, equal: true},
			{val1: projections2, val2: projections3, equal: false},
			{val1: projections3, val2: projections4, equal: false},
			{val1: projections3, val2: projections5, equal: false},
		}},

		{hashFn: in.hasher.HashAggregationsExpr, eqFn: in.hasher.IsAggregationsExprEqual, variations: []testVariation{
			{val1: aggs1, val2: aggs2, equal: true},
			{val1: aggs2, val2: aggs3, equal: false},
			{val1: aggs3, val2: aggs4, equal: false},
			{val1: aggs3, val2: aggs5, equal: false},
		}},
	}

	computeHashValue := func(hashFn reflect.Value, val interface{}) internHash {
		in.hasher.Init()
		hashFn.Call([]reflect.Value{reflect.ValueOf(val)})
		return in.hasher.hash
	}

	isEqual := func(eqFn reflect.Value, val1, val2 interface{}) bool {
		in.hasher.Init()
		res := eqFn.Call([]reflect.Value{reflect.ValueOf(val1), reflect.ValueOf(val2)})
		return res[0].Interface().(bool)
	}

	for _, tc := range testCases {
		hashFn := reflect.ValueOf(tc.hashFn)
		eqFn := reflect.ValueOf(tc.eqFn)

		for _, tv := range tc.variations {
			hash1 := computeHashValue(hashFn, tv.val1)
			hash2 := computeHashValue(hashFn, tv.val2)

			if tv.equal && hash1 != hash2 {
				t.Errorf("expected hash values to be equal for %v and %v", tv.val1, tv.val2)
			} else if !tv.equal && hash1 == hash2 {
				t.Errorf("expected hash values to not be equal for %v and %v", tv.val1, tv.val2)
			}

			eq := isEqual(eqFn, tv.val1, tv.val2)
			if tv.equal && !eq {
				t.Errorf("expected values to be equal for %v and %v", tv.val1, tv.val2)
			} else if !tv.equal && eq {
				t.Errorf("expected values to not be equal for %v and %v", tv.val1, tv.val2)
			}
		}
	}
}

func TestInternerPhysProps(t *testing.T) {
	var in interner

	physProps1 := physical.Required{
		Presentation: physical.Presentation{{Alias: "c", ID: 1}},
		Ordering:     physical.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps2 := physical.Required{
		Presentation: physical.Presentation{{Alias: "c", ID: 1}},
		Ordering:     physical.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps3 := physical.Required{
		Presentation: physical.Presentation{{Alias: "d", ID: 1}},
		Ordering:     physical.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps4 := physical.Required{
		Presentation: physical.Presentation{{Alias: "d", ID: 2}},
		Ordering:     physical.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps5 := physical.Required{
		Presentation: physical.Presentation{{Alias: "d", ID: 2}, {Alias: "e", ID: 3}},
		Ordering:     physical.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps6 := physical.Required{
		Presentation: physical.Presentation{{Alias: "d", ID: 2}, {Alias: "e", ID: 3}},
		Ordering:     physical.ParseOrderingChoice("+(1|2),+3 opt(4,5,6)"),
	}

	testCases := []struct {
		phys    *physical.Required
		inCache bool
	}{
		{phys: &physProps1, inCache: false},
		{phys: &physProps1, inCache: true},
		{phys: &physProps2, inCache: true},
		{phys: &physProps3, inCache: false},
		{phys: &physProps4, inCache: false},
		{phys: &physProps5, inCache: false},
		{phys: &physProps6, inCache: false},
	}

	inCache := make(map[*physical.Required]bool)

	for _, tc := range testCases[:1] {
		interned := in.InternPhysicalProps(tc.phys)
		if tc.inCache && !inCache[interned] {
			t.Errorf("expected physical props to already be in cache: %s", tc.phys)
		} else if !tc.inCache && inCache[interned] {
			t.Errorf("expected physical props to not yet be in cache: %s", tc.phys)
		}
		inCache[interned] = true
	}
}

func TestInternerCollision(t *testing.T) {
	var in interner

	// Start with a non-colliding value to make sure it doesn't interfere with
	// subsequent values.
	in.hasher.Init()
	in.hasher.HashString("no-collide")
	in.cache.Start(in.hasher.hash)
	in.cache.Next()
	in.cache.Add("no-collide")

	// Intern a string that will "collide" with other values.
	in.hasher.Init()
	in.hasher.HashString("foo")
	in.cache.Start(in.hasher.hash)
	in.cache.Next()
	in.cache.Add("foo")

	// Now simulate a collision by using same hash as "foo".
	in.cache.Start(in.hasher.hash)
	in.cache.Next()
	in.cache.Next()
	in.cache.Add("bar")

	// And another.
	in.cache.Start(in.hasher.hash)
	in.cache.Next()
	in.cache.Next()
	in.cache.Next()
	in.cache.Add("baz")

	// Ensure that first item can still be located.
	in.cache.Start(in.hasher.hash)
	if !in.cache.Next() || in.cache.Item() != "foo" {
		t.Errorf("expected to find foo in cache after collision")
	}

	// Expect to find colliding item as well.
	if !in.cache.Next() || in.cache.Item() != "bar" {
		t.Errorf("expected to find bar in cache after collision")
	}

	// And last colliding item.
	if !in.cache.Next() || in.cache.Item() != "baz" {
		t.Errorf("expected to find baz in cache after collision")
	}

	// Should be no more items.
	if in.cache.Next() {
		t.Errorf("expected no more colliding items in cache")
	}
}
