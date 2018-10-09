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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"golang.org/x/tools/container/intsets"
)

func TestInterner(t *testing.T) {
	var in interner
	in.Init()

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

	aggs1 := AggregationsExpr{{Agg: &CountRowsSingleton, ColPrivate: ColPrivate{Col: 0}}}
	aggs2 := AggregationsExpr{{Agg: &CountRowsSingleton, ColPrivate: ColPrivate{Col: 0}}}
	aggs3 := AggregationsExpr{{Agg: &CountRowsSingleton, ColPrivate: ColPrivate{Col: 1}}}
	aggs4 := AggregationsExpr{
		{Agg: &CountRowsSingleton, ColPrivate: ColPrivate{Col: 1}},
		{Agg: &CountRowsSingleton, ColPrivate: ColPrivate{Col: 2}},
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
		{hashFn: in.hashBool, eqFn: in.isBoolEqual, variations: []testVariation{
			{val1: true, val2: true, equal: true},
			{val1: true, val2: false, equal: false},
		}},

		{hashFn: in.hashInt, eqFn: in.isIntEqual, variations: []testVariation{
			{val1: 1, val2: 1, equal: true},
			{val1: 0, val2: 1, equal: false},
			{val1: intsets.MaxInt, val2: intsets.MaxInt, equal: true},
			{val1: intsets.MinInt, val2: intsets.MaxInt, equal: false},
		}},

		{hashFn: in.hashFloat64, eqFn: in.isFloat64Equal, variations: []testVariation{
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

		{hashFn: in.hashString, eqFn: in.isStringEqual, variations: []testVariation{
			{val1: "", val2: "", equal: true},
			{val1: "abc", val2: "abcd", equal: false},
			{val1: "", val2: " ", equal: false},
			{val1: "the quick brown fox", val2: "the quick brown fox", equal: true},
		}},

		{hashFn: in.hashBytes, eqFn: in.isBytesEqual, variations: []testVariation{
			{val1: []byte{}, val2: []byte{}, equal: true},
			{val1: []byte{}, val2: []byte{0}, equal: false},
			{val1: []byte{1, 2, 3}, val2: []byte{1, 2, 3, 4}, equal: false},
			{val1: []byte{10, 1, 100}, val2: []byte{10, 1, 100}, equal: true},
		}},

		{hashFn: in.hashOperator, eqFn: in.isOperatorEqual, variations: []testVariation{
			{val1: opt.AndOp, val2: opt.AndOp, equal: true},
			{val1: opt.SelectOp, val2: opt.InnerJoinOp, equal: false},
		}},

		{hashFn: in.hashType, eqFn: in.isTypeEqual, variations: []testVariation{
			{val1: reflect.TypeOf(int(0)), val2: reflect.TypeOf(int(1)), equal: true},
			{val1: reflect.TypeOf(int64(0)), val2: reflect.TypeOf(int32(0)), equal: false},
		}},

		{hashFn: in.hashDatum, eqFn: in.isDatumEqual, variations: []testVariation{
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

		{hashFn: in.hashDatumType, eqFn: in.isDatumTypeEqual, variations: []testVariation{
			{val1: types.Int, val2: types.Int, equal: true},
			{val1: tupTyp1, val2: tupTyp2, equal: true},
			{val1: tupTyp2, val2: tupTyp3, equal: false},
			{val1: tupTyp3, val2: tupTyp4, equal: false},
		}},

		{hashFn: in.hashColType, eqFn: in.isColTypeEqual, variations: []testVariation{
			{val1: coltypes.Int, val2: coltypes.Int, equal: true},
			{val1: coltypes.Int, val2: coltypes.Int2, equal: false},
			{val1: coltypes.Float4, val2: coltypes.Float8, equal: false},
			{val1: coltypes.VarChar, val2: coltypes.String, equal: false},
			{val1: &coltypes.TDecimal{Prec: 19}, val2: &coltypes.TDecimal{Prec: 19, Scale: 2}, equal: false},
			{val1: coltypes.TTuple{coltypes.String, coltypes.Int}, val2: coltypes.TTuple{coltypes.Int, coltypes.String}, equal: false},
			{val1: coltypes.Int2vector, val2: coltypes.OidVector, equal: false},
		}},

		{hashFn: in.hashTypedExpr, eqFn: in.isTypedExprEqual, variations: []testVariation{
			{val1: tup1, val2: tup1, equal: true},
			{val1: tup1, val2: tup2, equal: false},
			{val1: tup2, val2: tup3, equal: false},
		}},

		{hashFn: in.hashColumnID, eqFn: in.isColumnIDEqual, variations: []testVariation{
			{val1: opt.ColumnID(0), val2: opt.ColumnID(0), equal: true},
			{val1: opt.ColumnID(0), val2: opt.ColumnID(1), equal: false},
		}},

		{hashFn: in.hashColSet, eqFn: in.isColSetEqual, variations: []testVariation{
			{val1: util.MakeFastIntSet(), val2: util.MakeFastIntSet(), equal: true},
			{val1: util.MakeFastIntSet(1, 2, 3), val2: util.MakeFastIntSet(3, 2, 1), equal: true},
			{val1: util.MakeFastIntSet(1, 2, 3), val2: util.MakeFastIntSet(1, 2), equal: false},
		}},

		{hashFn: in.hashColList, eqFn: in.isColListEqual, variations: []testVariation{
			{val1: opt.ColList{}, val2: opt.ColList{}, equal: true},
			{val1: opt.ColList{1, 2, 3}, val2: opt.ColList{1, 2, 3}, equal: true},
			{val1: opt.ColList{1, 2, 3}, val2: opt.ColList{3, 2, 1}, equal: false},
			{val1: opt.ColList{1, 2}, val2: opt.ColList{1, 2, 3}, equal: false},
		}},

		{hashFn: in.hashOrdering, eqFn: in.isOrderingEqual, variations: []testVariation{
			{val1: opt.Ordering{}, val2: opt.Ordering{}, equal: true},
			{val1: opt.Ordering{-1, 1}, val2: opt.Ordering{-1, 1}, equal: true},
			{val1: opt.Ordering{-1, 1}, val2: opt.Ordering{1, -1}, equal: false},
			{val1: opt.Ordering{-1, 1}, val2: opt.Ordering{-1, 1, 2}, equal: false},
		}},

		{hashFn: in.hashOrderingChoice, eqFn: in.isOrderingChoiceEqual, variations: []testVariation{
			{val1: props.ParseOrderingChoice(""), val2: props.ParseOrderingChoice(""), equal: true},
			{val1: props.ParseOrderingChoice("+1"), val2: props.ParseOrderingChoice("+1"), equal: true},
			{val1: props.ParseOrderingChoice("+(1|2)"), val2: props.ParseOrderingChoice("+(2|1)"), equal: true},
			{val1: props.ParseOrderingChoice("+1 opt(2)"), val2: props.ParseOrderingChoice("+1 opt(2)"), equal: true},
			{val1: props.ParseOrderingChoice("+1"), val2: props.ParseOrderingChoice("-1"), equal: false},
			{val1: props.ParseOrderingChoice("+1,+2"), val2: props.ParseOrderingChoice("+1"), equal: false},
			{val1: props.ParseOrderingChoice("+(1|2)"), val2: props.ParseOrderingChoice("+1"), equal: false},
			{val1: props.ParseOrderingChoice("+1 opt(2)"), val2: props.ParseOrderingChoice("+1"), equal: false},
		}},

		{hashFn: in.hashTableID, eqFn: in.isTableIDEqual, variations: []testVariation{
			{val1: opt.TableID(0), val2: opt.TableID(0), equal: true},
			{val1: opt.TableID(0), val2: opt.TableID(1), equal: false},
		}},

		{hashFn: in.hashConstraint, eqFn: in.isConstraintEqual, variations: []testVariation{
			{val1: (*constraint.Constraint)(nil), val2: (*constraint.Constraint)(nil), equal: true},
			{val1: &constraint.Constraint{}, val2: &constraint.Constraint{}, equal: false},
		}},

		{hashFn: in.hashScanLimit, eqFn: in.isScanLimitEqual, variations: []testVariation{
			{val1: ScanLimit(100), val2: ScanLimit(100), equal: true},
			{val1: ScanLimit(0), val2: ScanLimit(1), equal: false},
		}},

		{hashFn: in.hashScanFlags, eqFn: in.isScanFlagsEqual, variations: []testVariation{
			{val1: ScanFlags{}, val2: ScanFlags{}, equal: true},
			{val1: ScanFlags{NoIndexJoin: true, Index: 1}, val2: ScanFlags{NoIndexJoin: true, Index: 1}, equal: true},
			{val1: ScanFlags{NoIndexJoin: true, Index: 1}, val2: ScanFlags{NoIndexJoin: true, Index: 2}, equal: false},
			{val1: ScanFlags{NoIndexJoin: true, Index: 1}, val2: ScanFlags{NoIndexJoin: false, Index: 1}, equal: false},
		}},

		{hashFn: in.hashSubquery, eqFn: in.isSubqueryEqual, variations: []testVariation{
			{val1: (*tree.Subquery)(nil), val2: (*tree.Subquery)(nil), equal: true},
			{val1: &tree.Subquery{}, val2: &tree.Subquery{}, equal: false},
		}},

		{hashFn: in.hashExplainOptions, eqFn: in.isExplainOptionsEqual, variations: []testVariation{
			{val1: tree.ExplainOptions{}, val2: tree.ExplainOptions{}, equal: true},
			{val1: explain1, val2: explain1, equal: true},
			{val1: explain1, val2: explain2, equal: false},
			{val1: explain2, val2: explain3, equal: false},
		}},

		{hashFn: in.hashShowTraceType, eqFn: in.isShowTraceTypeEqual, variations: []testVariation{
			{val1: tree.ShowTraceKV, val2: tree.ShowTraceKV, equal: true},
			{val1: tree.ShowTraceKV, val2: tree.ShowTraceRaw, equal: false},
		}},

		{hashFn: in.hashFuncProps, eqFn: in.isFuncPropsEqual, variations: []testVariation{
			{val1: &tree.FunDefs["length"].FunctionProperties, val2: &tree.FunDefs["length"].FunctionProperties, equal: true},
			{val1: &tree.FunDefs["max"].FunctionProperties, val2: &tree.FunDefs["min"].FunctionProperties, equal: false},
		}},

		{hashFn: in.hashFuncOverload, eqFn: in.isFuncOverloadEqual, variations: []testVariation{
			{val1: tree.FunDefs["min"].Definition[0], val2: tree.FunDefs["min"].Definition[0], equal: true},
			{val1: tree.FunDefs["min"].Definition[0], val2: tree.FunDefs["min"].Definition[1], equal: false},
		}},

		{hashFn: in.hashTupleOrdinal, eqFn: in.isTupleOrdinalEqual, variations: []testVariation{
			{val1: TupleOrdinal(0), val2: TupleOrdinal(0), equal: true},
			{val1: TupleOrdinal(0), val2: TupleOrdinal(1), equal: false},
		}},

		// PhysProps hash/isEqual methods are tested in TestInternerPhysProps.

		{hashFn: in.hashRelExpr, eqFn: in.isRelExprEqual, variations: []testVariation{
			{val1: (*ScanExpr)(nil), val2: (*ScanExpr)(nil), equal: true},
			{val1: scanNode, val2: scanNode, equal: true},
			{val1: &ScanExpr{}, val2: &ScanExpr{}, equal: false},
		}},

		{hashFn: in.hashScalarExpr, eqFn: in.isScalarExprEqual, variations: []testVariation{
			{val1: (*AndExpr)(nil), val2: (*AndExpr)(nil), equal: true},
			{val1: andExpr, val2: andExpr, equal: true},
			{val1: &AndExpr{}, val2: &AndExpr{}, equal: false},
		}},

		{hashFn: in.hashScalarListExpr, eqFn: in.isScalarListExprEqual, variations: []testVariation{
			{val1: ScalarListExpr{andExpr, andExpr}, val2: ScalarListExpr{andExpr, andExpr}, equal: true},
			{val1: ScalarListExpr{andExpr, andExpr}, val2: ScalarListExpr{andExpr}, equal: false},
			{val1: ScalarListExpr{&AndExpr{}}, val2: ScalarListExpr{&AndExpr{}}, equal: false},
		}},

		{hashFn: in.hashFiltersExpr, eqFn: in.isFiltersExprEqual, variations: []testVariation{
			{val1: FiltersExpr{{Condition: andExpr}}, val2: FiltersExpr{{Condition: andExpr}}, equal: true},
			{val1: FiltersExpr{{Condition: andExpr}}, val2: FiltersExpr{}, equal: false},
			{val1: FiltersExpr{{Condition: &AndExpr{}}}, val2: FiltersExpr{{Condition: &AndExpr{}}}, equal: false},
		}},

		{hashFn: in.hashProjectionsExpr, eqFn: in.isProjectionsExprEqual, variations: []testVariation{
			{val1: projections1, val2: projections2, equal: true},
			{val1: projections2, val2: projections3, equal: false},
			{val1: projections3, val2: projections4, equal: false},
			{val1: projections3, val2: projections5, equal: false},
		}},

		{hashFn: in.hashAggregationsExpr, eqFn: in.isAggregationsExprEqual, variations: []testVariation{
			{val1: aggs1, val2: aggs2, equal: true},
			{val1: aggs2, val2: aggs3, equal: false},
			{val1: aggs3, val2: aggs4, equal: false},
			{val1: aggs3, val2: aggs5, equal: false},
		}},
	}

	computeHashValue := func(hashFn reflect.Value, val interface{}) internHash {
		in.hash = offset64
		hashFn.Call([]reflect.Value{reflect.ValueOf(val)})
		return in.hash
	}

	isEqual := func(eqFn reflect.Value, val1, val2 interface{}) bool {
		in.hash = offset64
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
	in.Init()

	physProps1 := props.Physical{
		Presentation: props.Presentation{{Label: "c", ID: 1}},
		Ordering:     props.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps2 := props.Physical{
		Presentation: props.Presentation{{Label: "c", ID: 1}},
		Ordering:     props.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps3 := props.Physical{
		Presentation: props.Presentation{{Label: "d", ID: 1}},
		Ordering:     props.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps4 := props.Physical{
		Presentation: props.Presentation{{Label: "d", ID: 2}},
		Ordering:     props.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps5 := props.Physical{
		Presentation: props.Presentation{{Label: "d", ID: 2}, {Label: "e", ID: 3}},
		Ordering:     props.ParseOrderingChoice("+(1|2),+3 opt(4,5)"),
	}
	physProps6 := props.Physical{
		Presentation: props.Presentation{{Label: "d", ID: 2}, {Label: "e", ID: 3}},
		Ordering:     props.ParseOrderingChoice("+(1|2),+3 opt(4,5,6)"),
	}

	testCases := []struct {
		phys    *props.Physical
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

	inCache := make(map[*props.Physical]bool)

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
	in.Init()

	// Intern string.
	in.hash = offset64
	in.hashString("foo")
	in.cache[in.hash] = internedItem{item: "foo"}

	// Now simulate a collision.
	in.item = "bar"
	in.handleCollision()
	in.cache[in.hash] = internedItem{item: "bar"}

	// And another.
	in.item = "baz"
	in.handleCollision()
	in.cache[in.hash] = internedItem{item: "baz"}

	// Ensure that first item can still be located.
	in.hash = offset64
	in.hashString("foo")
	if !in.lookupItem() || in.item != "foo" {
		t.Errorf("expected to find foo in cache after collision")
	}

	// Expect to find colliding item as well.
	in.handleCollision()
	if !in.lookupItem() || in.item != "bar" {
		t.Errorf("expected to find bar in cache after collision")
	}

	// Start over and try to find second colliding item.
	in.hash = offset64
	in.hashString("foo")
	in.lookupItem()
	in.handleCollision()
	in.lookupItem()
	in.handleCollision()
	if !in.lookupItem() || in.item != "baz" {
		t.Errorf("expected to find baz in cache after collision")
	}
}
