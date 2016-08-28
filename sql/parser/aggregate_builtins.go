// Copyright 2015 The Cockroach Authors.
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

package parser

import (
	"fmt"
	"math"
	"strings"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util/decimal"
)

func init() {
	for k, v := range Aggregates {
		for i := range v {
			v[i].impure = true
		}
		Aggregates[strings.ToUpper(k)] = v
	}
}

// AggregateFunc accumulates the result of a some function of a Datum.
type AggregateFunc interface {
	Add(Datum)
	Result() Datum
}

var _ Visitor = &IsAggregateVisitor{}

// IsAggregateVisitor checks if walked expressions contain aggregate functions.
type IsAggregateVisitor struct {
	Aggregated bool
}

// VisitPre satisfies the Visitor interface.
func (v *IsAggregateVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *FuncExpr:
		fn, err := t.Name.Normalize()
		if err != nil {
			return false, expr
		}
		if _, ok := Aggregates[strings.ToLower(fn.Function())]; ok {
			v.Aggregated = true
			return false, expr
		}
	case *Subquery:
		return false, expr
	}

	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*IsAggregateVisitor) VisitPost(expr Expr) Expr { return expr }

// Reset clear the IsAggregateVisitor's internal state.
func (v *IsAggregateVisitor) Reset() {
	v.Aggregated = false
}

// AggregateInExpr determines if an Expr contains an aggregate function.
func (p *Parser) AggregateInExpr(expr Expr) bool {
	if expr != nil {
		defer p.isAggregateVisitor.Reset()
		WalkExprConst(&p.isAggregateVisitor, expr)
		if p.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// IsAggregate determines if SelectClause contains an aggregate function.
func (p *Parser) IsAggregate(n *SelectClause) bool {
	if n.Having != nil || len(n.GroupBy) > 0 {
		return true
	}

	defer p.isAggregateVisitor.Reset()
	for _, target := range n.Exprs {
		WalkExprConst(&p.isAggregateVisitor, target.Expr)
		if p.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// Aggregates are a special class of builtin functions that are wrapped
// at execution in a bucketing layer to combine (aggregate) the result
// of the function being run over many rows.
// See `aggregateFuncHolder` in the sql
// In particular they must not be simplified during normalization
// (and thus must be marked as impure), even when they are given a
// constant argument (e.g. SUM(1)). This is because aggregate
// functions must return NULL when they are no rows in the source
// table, so their evaluation must always be delayed until query
// execution.
var Aggregates = map[string][]Builtin{
	"avg": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntAvgAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatAvgAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalAvgAggregate),
	},

	"bool_and": {
		makeAggBuiltin(TypeBool, TypeBool, newBoolAndAggregate),
	},

	"bool_or": {
		makeAggBuiltin(TypeBool, TypeBool, newBoolOrAggregate),
	},

	"count": countImpls(),

	"max": makeAggBuiltins(newMaxAggregate, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeBytes, TypeDate, TypeTimestamp, TypeInterval),
	"min": makeAggBuiltins(newMinAggregate, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeBytes, TypeDate, TypeTimestamp, TypeInterval),

	"sum": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntSumAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatSumAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalSumAggregate),
	},

	"variance": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntVarianceAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalVarianceAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatVarianceAggregate),
	},

	"stddev": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntStddevAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalStddevAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatStddevAggregate),
	},
}

func makeAggBuiltin(in, ret Datum, f func() AggregateFunc) Builtin {
	return Builtin{
		// See the comment about aggregate functions in the definitions
		// of the Builtins array above.
		impure:        true,
		Types:         ArgTypes{in},
		ReturnType:    ret,
		AggregateFunc: f,
	}
}
func makeAggBuiltins(f func() AggregateFunc, types ...Datum) []Builtin {
	ret := make([]Builtin, len(types))
	for i := range types {
		ret[i] = makeAggBuiltin(types[i], types[i], f)
	}
	return ret
}

func countImpls() []Builtin {
	types := ArgTypes{TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeBytes, TypeDate, TypeTimestamp, TypeInterval, TypeTuple}
	r := make([]Builtin, len(types))
	for i := range types {
		r[i] = makeAggBuiltin(types[i], TypeInt, newCountAggregate)
	}
	return r
}

var _ AggregateFunc = &avgAggregate{}
var _ AggregateFunc = &countAggregate{}
var _ AggregateFunc = &MaxAggregate{}
var _ AggregateFunc = &MinAggregate{}
var _ AggregateFunc = &intSumAggregate{}
var _ AggregateFunc = &decimalSumAggregate{}
var _ AggregateFunc = &floatSumAggregate{}
var _ AggregateFunc = &stddevAggregate{}
var _ AggregateFunc = &intVarianceAggregate{}
var _ AggregateFunc = &floatVarianceAggregate{}
var _ AggregateFunc = &decimalVarianceAggregate{}
var _ AggregateFunc = &identAggregate{}

// In order to render the unaggregated (i.e. grouped) fields, during aggregation,
// the values for those fields have to be stored for each bucket.
// The `identAggregate` provides an "aggregate" function that actually
// just returns the last value passed to `add`, unchanged. For accumulating
// and rendering though it behaves like the other aggregate functions,
// allowing both those steps to avoid special-casing grouped vs aggregated fields.
type identAggregate struct {
	val Datum
}

// NewIdentAggregate returns an identAggregate (see comment on struct).
func NewIdentAggregate() AggregateFunc {
	return &identAggregate{}
}

// Add sets the value to the passed datum.
func (a *identAggregate) Add(datum Datum) {
	a.val = datum
}

// Result returns the value most recently passed to Add.
func (a *identAggregate) Result() Datum {
	return a.val
}

type avgAggregate struct {
	agg   AggregateFunc
	count int
}

func newIntAvgAggregate() AggregateFunc {
	return &avgAggregate{agg: newIntSumAggregate()}
}
func newFloatAvgAggregate() AggregateFunc {
	return &avgAggregate{agg: newFloatSumAggregate()}
}
func newDecimalAvgAggregate() AggregateFunc {
	return &avgAggregate{agg: newDecimalSumAggregate()}
}

// Add accumulates the passed datum into the average.
func (a *avgAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	a.agg.Add(datum)
	a.count++
}

// Result returns the average of all datums passed to Add.
func (a *avgAggregate) Result() Datum {
	sum := a.agg.Result()
	if sum == DNull {
		return sum
	}
	switch t := sum.(type) {
	case *DFloat:
		return NewDFloat(*t / DFloat(a.count))
	case *DDecimal:
		count := inf.NewDec(int64(a.count), 0)
		t.QuoRound(&t.Dec, count, decimal.Precision, inf.RoundHalfUp)
		return t
	default:
		panic(fmt.Sprintf("unexpected SUM result type: %s", t.Type()))
	}
}

type boolAndAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolAndAggregate() AggregateFunc {
	return &boolAndAggregate{}
}

func (a *boolAndAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	if !a.sawNonNull {
		a.sawNonNull = true
		a.result = true
	}
	a.result = a.result && bool(*datum.(*DBool))
}

func (a *boolAndAggregate) Result() Datum {
	if !a.sawNonNull {
		return DNull
	}
	return MakeDBool(DBool(a.result))
}

type boolOrAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolOrAggregate() AggregateFunc {
	return &boolOrAggregate{}
}

func (a *boolOrAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	a.sawNonNull = true
	a.result = a.result || bool(*datum.(*DBool))
}

func (a *boolOrAggregate) Result() Datum {
	if !a.sawNonNull {
		return DNull
	}
	return MakeDBool(DBool(a.result))
}

type countAggregate struct {
	count int
}

func newCountAggregate() AggregateFunc {
	return &countAggregate{}
}

func (a *countAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	a.count++
	return
}

func (a *countAggregate) Result() Datum {
	return NewDInt(DInt(a.count))
}

// MaxAggregate keeps track of the largest value passed to Add.
type MaxAggregate struct {
	max Datum
}

func newMaxAggregate() AggregateFunc {
	return &MaxAggregate{}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *MaxAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	if a.max == nil {
		a.max = datum
		return
	}
	c := a.max.Compare(datum)
	if c < 0 {
		a.max = datum
	}
}

// Result returns the largest value passed to Add.
func (a *MaxAggregate) Result() Datum {
	if a.max == nil {
		return DNull
	}
	return a.max
}

// MinAggregate keeps track of the smallest value passed to Add.
type MinAggregate struct {
	min Datum
}

func newMinAggregate() AggregateFunc {
	return &MinAggregate{}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *MinAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	if a.min == nil {
		a.min = datum
		return
	}
	c := a.min.Compare(datum)
	if c > 0 {
		a.min = datum
	}
}

// Result returns the smallest value passed to Add.
func (a *MinAggregate) Result() Datum {
	if a.min == nil {
		return DNull
	}
	return a.min
}

type intSumAggregate struct {
	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	intSum      int64
	decSum      DDecimal
	tmpDec      inf.Dec
	large       bool
	seenNonNull bool
}

func newIntSumAggregate() AggregateFunc {
	return &intSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *intSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}

	t := int64(*datum.(*DInt))
	if t != 0 {
		// The sum can be computed using a single int64 as long as the
		// result of the addition does not overflow.  However since Go
		// does not provide checked addition, we have to check for the
		// overflow explicitly.
		if !a.large &&
			((t < 0 && a.intSum < math.MinInt64-t) ||
				(t > 0 && a.intSum > math.MaxInt64-t)) {
			// And overflow was detected; go to large integers, but keep the
			// sum computed so far.
			a.large = true
			a.decSum.SetUnscaled(a.intSum)
		}

		if a.large {
			a.tmpDec.SetUnscaled(t)
			a.decSum.Add(&a.decSum.Dec, &a.tmpDec)
		} else {
			a.intSum += t
		}
	}
	a.seenNonNull = true
}

// Result returns the sum.
func (a *intSumAggregate) Result() Datum {
	if !a.seenNonNull {
		return DNull
	}
	if !a.large {
		a.decSum.SetUnscaled(a.intSum)
	}
	return &a.decSum
}

type decimalSumAggregate struct {
	sum        inf.Dec
	sawNonNull bool
}

func newDecimalSumAggregate() AggregateFunc {
	return &decimalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *decimalSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	t := datum.(*DDecimal)
	a.sum.Add(&a.sum, &t.Dec)
	a.sawNonNull = true
}

// Result returns the sum.
func (a *decimalSumAggregate) Result() Datum {
	if !a.sawNonNull {
		return DNull
	}
	dd := &DDecimal{}
	dd.Set(&a.sum)
	return dd
}

type floatSumAggregate struct {
	sum        float64
	sawNonNull bool
}

func newFloatSumAggregate() AggregateFunc {
	return &floatSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *floatSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	t := datum.(*DFloat)
	a.sum += float64(*t)
	a.sawNonNull = true
}

// Result returns the sum.
func (a *floatSumAggregate) Result() Datum {
	if !a.sawNonNull {
		return DNull
	}
	return NewDFloat(DFloat(a.sum))
}

type intVarianceAggregate struct {
	agg decimalVarianceAggregate
	// Used for passing int64s as *inf.Dec values.
	tmpDec DDecimal
}

func newIntVarianceAggregate() AggregateFunc {
	return &intVarianceAggregate{}
}

func (a *intVarianceAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}

	a.tmpDec.SetUnscaled(int64(*datum.(*DInt)))
	a.agg.Add(&a.tmpDec)
}

func (a *intVarianceAggregate) Result() Datum {
	return a.agg.Result()
}

type floatVarianceAggregate struct {
	count   int
	mean    float64
	sqrDiff float64
}

func newFloatVarianceAggregate() AggregateFunc {
	return &floatVarianceAggregate{}
}

func (a *floatVarianceAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	f := float64(*datum.(*DFloat))

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count++
	delta := f - a.mean
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
}

func (a *floatVarianceAggregate) Result() Datum {
	if a.count < 2 {
		return DNull
	}
	return NewDFloat(DFloat(a.sqrDiff / (float64(a.count) - 1)))
}

type decimalVarianceAggregate struct {
	// Variables used across iterations.
	count   inf.Dec
	mean    inf.Dec
	sqrDiff inf.Dec

	// Variables used as scratch space within iterations.
	delta inf.Dec
	tmp   inf.Dec
}

func newDecimalVarianceAggregate() AggregateFunc {
	return &decimalVarianceAggregate{}
}

// Read-only constants used for compuation.
var (
	decimalOne = inf.NewDec(1, 0)
	decimalTwo = inf.NewDec(2, 0)
)

func (a *decimalVarianceAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	d := &datum.(*DDecimal).Dec

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count.Add(&a.count, decimalOne)
	a.delta.Sub(d, &a.mean)
	a.tmp.QuoRound(&a.delta, &a.count, decimal.Precision, inf.RoundHalfUp)
	a.mean.Add(&a.mean, &a.tmp)
	a.tmp.Sub(d, &a.mean)
	a.sqrDiff.Add(&a.sqrDiff, a.delta.Mul(&a.delta, &a.tmp))
}

func (a *decimalVarianceAggregate) Result() Datum {
	if a.count.Cmp(decimalTwo) < 0 {
		return DNull
	}
	a.tmp.Sub(&a.count, decimalOne)
	dd := &DDecimal{}
	dd.QuoRound(&a.sqrDiff, &a.tmp, decimal.Precision, inf.RoundHalfUp)
	return dd
}

type stddevAggregate struct {
	agg AggregateFunc
}

func newIntStddevAggregate() AggregateFunc {
	return &stddevAggregate{agg: newIntVarianceAggregate()}
}
func newFloatStddevAggregate() AggregateFunc {
	return &stddevAggregate{agg: newFloatVarianceAggregate()}
}
func newDecimalStddevAggregate() AggregateFunc {
	return &stddevAggregate{agg: newDecimalVarianceAggregate()}
}

// Add implements the AggregateFunc interface.
func (a *stddevAggregate) Add(datum Datum) {
	a.agg.Add(datum)
}

// Result computes the square root of the variance.
func (a *stddevAggregate) Result() Datum {
	variance := a.agg.Result()
	if variance == DNull {
		return variance
	}
	switch t := variance.(type) {
	case *DFloat:
		return NewDFloat(DFloat(math.Sqrt(float64(*t))))
	case *DDecimal:
		decimal.Sqrt(&t.Dec, &t.Dec, decimal.Precision)
		return t
	}
	panic(fmt.Sprintf("unexpected variance result type: %s", variance.Type()))
}
