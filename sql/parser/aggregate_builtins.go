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
	"math"
	"strings"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/pkg/errors"
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
	Add(Datum) error
	Result() (Datum, error)
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
		if _, ok := Aggregates[strings.ToLower(string(t.Name.Base))]; ok {
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
		makeAggBuiltin(TypeInt, TypeDecimal, newAvgAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newAvgAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newAvgAggregate),
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
		makeAggBuiltin(TypeInt, TypeDecimal, newSumAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newSumAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newSumAggregate),
	},

	"variance": {
		makeAggBuiltin(TypeInt, TypeDecimal, newVarianceAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newVarianceAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newVarianceAggregate),
	},

	"stddev": {
		makeAggBuiltin(TypeInt, TypeDecimal, newStddevAggregate),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newStddevAggregate),
		makeAggBuiltin(TypeFloat, TypeFloat, newStddevAggregate),
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
var _ AggregateFunc = &sumAggregate{}
var _ AggregateFunc = &stddevAggregate{}
var _ AggregateFunc = &varianceAggregate{}
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
func (a *identAggregate) Add(datum Datum) error {
	a.val = datum
	return nil
}

// Result returns the value most recently passed to Add.
func (a *identAggregate) Result() (Datum, error) {
	return a.val, nil
}

type avgAggregate struct {
	sumAggregate
	count int
}

func newAvgAggregate() AggregateFunc {
	return &avgAggregate{}
}

// Add accumulates the passed datum into the average.
func (a *avgAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	if err := a.sumAggregate.Add(datum); err != nil {
		return err
	}
	a.count++
	return nil
}

// Result returns the average of all datums passed to Add.
func (a *avgAggregate) Result() (Datum, error) {
	sum, err := a.sumAggregate.Result()
	if err != nil {
		return nil, err
	}
	if sum == DNull {
		return sum, nil
	}
	switch t := sum.(type) {
	case *DFloat:
		return NewDFloat(*t / DFloat(a.count)), nil
	case *DDecimal:
		count := inf.NewDec(int64(a.count), 0)
		t.QuoRound(&t.Dec, count, decimal.Precision, inf.RoundHalfUp)
		return t, nil
	default:
		return nil, errors.Errorf("unexpected SUM result type: %s", t.Type())
	}
}

type boolAndAggregate struct {
	sawNonNull bool
	sawFalse   bool
}

func newBoolAndAggregate() AggregateFunc {
	return &boolAndAggregate{}
}

func (a *boolAndAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	a.sawNonNull = true
	switch t := datum.(type) {
	case *DBool:
		if !a.sawFalse {
			a.sawFalse = !bool(*t)
		}
		return nil
	default:
		return errors.Errorf("unexpected BOOL_AND argument type: %s", t.Type())
	}
}

func (a *boolAndAggregate) Result() (Datum, error) {
	if !a.sawNonNull {
		return DNull, nil
	}
	return MakeDBool(DBool(!a.sawFalse)), nil
}

type boolOrAggregate struct {
	sawNonNull bool
	sawTrue    bool
}

func newBoolOrAggregate() AggregateFunc {
	return &boolOrAggregate{}
}

func (a *boolOrAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	a.sawNonNull = true
	switch t := datum.(type) {
	case *DBool:
		if !a.sawTrue {
			a.sawTrue = bool(*t)
		}
		return nil
	default:
		return errors.Errorf("unexpected BOOL_OR argument type: %s", t.Type())
	}
}

func (a *boolOrAggregate) Result() (Datum, error) {
	if !a.sawNonNull {
		return DNull, nil
	}
	return MakeDBool(DBool(a.sawTrue)), nil
}

type countAggregate struct {
	count int
}

func newCountAggregate() AggregateFunc {
	return &countAggregate{}
}

func (a *countAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	switch t := datum.(type) {
	case *DTuple:
		for _, d := range *t {
			if d != DNull {
				a.count++
				break
			}
		}
	default:
		a.count++
	}
	return nil
}

func (a *countAggregate) Result() (Datum, error) {
	return NewDInt(DInt(a.count)), nil
}

// MaxAggregate keeps track of the largest value passed to Add.
type MaxAggregate struct {
	max Datum
}

func newMaxAggregate() AggregateFunc {
	return &MaxAggregate{}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *MaxAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	if a.max == nil {
		a.max = datum
		return nil
	}
	c := a.max.Compare(datum)
	if c < 0 {
		a.max = datum
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *MaxAggregate) Result() (Datum, error) {
	if a.max == nil {
		return DNull, nil
	}
	return a.max, nil
}

// MinAggregate keeps track of the smallest value passed to Add.
type MinAggregate struct {
	min Datum
}

func newMinAggregate() AggregateFunc {
	return &MinAggregate{}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *MinAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	if a.min == nil {
		a.min = datum
		return nil
	}
	c := a.min.Compare(datum)
	if c > 0 {
		a.min = datum
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *MinAggregate) Result() (Datum, error) {
	if a.min == nil {
		return DNull, nil
	}
	return a.min, nil
}

type sumAggregate struct {
	sumType  Datum
	sumFloat DFloat
	sumDec   inf.Dec
	tmpDec   inf.Dec
}

func newSumAggregate() AggregateFunc {
	return &sumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *sumAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}
	switch t := datum.(type) {
	case *DFloat:
		a.sumFloat += *t
	case *DInt:
		a.tmpDec.SetUnscaled(int64(*t))
		a.sumDec.Add(&a.sumDec, &a.tmpDec)
	case *DDecimal:
		a.sumDec.Add(&a.sumDec, &t.Dec)
	default:
		return errors.Errorf("unexpected SUM argument type: %s", datum.Type())
	}
	if a.sumType == nil {
		a.sumType = datum
	}
	return nil
}

// Result returns the sum.
func (a *sumAggregate) Result() (Datum, error) {
	if a.sumType == nil {
		return DNull, nil
	}
	switch {
	case a.sumType.TypeEqual(TypeFloat):
		return NewDFloat(a.sumFloat), nil
	case a.sumType.TypeEqual(TypeInt), a.sumType.TypeEqual(TypeDecimal):
		dd := &DDecimal{}
		dd.Set(&a.sumDec)
		return dd, nil
	default:
		panic("unreachable")
	}
}

type varianceAggregate struct {
	typedAggregate AggregateFunc
	// Used for passing int64s as *inf.Dec values.
	tmpDec DDecimal
}

func newVarianceAggregate() AggregateFunc {
	return &varianceAggregate{}
}

func (a *varianceAggregate) Add(datum Datum) error {
	if datum == DNull {
		return nil
	}

	const unexpectedErrFormat = "unexpected VARIANCE argument type: %s"
	switch t := datum.(type) {
	case *DFloat:
		if a.typedAggregate == nil {
			a.typedAggregate = newFloatVarianceAggregate()
		} else {
			switch a.typedAggregate.(type) {
			case *floatVarianceAggregate:
			default:
				return errors.Errorf(unexpectedErrFormat, datum.Type())
			}
		}
		return a.typedAggregate.Add(t)
	case *DInt:
		if a.typedAggregate == nil {
			a.typedAggregate = newDecimalVarianceAggregate()
		} else {
			switch a.typedAggregate.(type) {
			case *decimalVarianceAggregate:
			default:
				return errors.Errorf(unexpectedErrFormat, datum.Type())
			}
		}
		a.tmpDec.SetUnscaled(int64(*t))
		return a.typedAggregate.Add(&a.tmpDec)
	case *DDecimal:
		if a.typedAggregate == nil {
			a.typedAggregate = newDecimalVarianceAggregate()
		} else {
			switch a.typedAggregate.(type) {
			case *decimalVarianceAggregate:
			default:
				return errors.Errorf(unexpectedErrFormat, datum.Type())
			}
		}
		return a.typedAggregate.Add(t)
	default:
		return errors.Errorf(unexpectedErrFormat, datum.Type())
	}
}

func (a *varianceAggregate) Result() (Datum, error) {
	if a.typedAggregate == nil {
		return DNull, nil
	}
	return a.typedAggregate.Result()
}

type floatVarianceAggregate struct {
	count   int
	mean    float64
	sqrDiff float64
}

func newFloatVarianceAggregate() AggregateFunc {
	return &floatVarianceAggregate{}
}

func (a *floatVarianceAggregate) Add(datum Datum) error {
	f := float64(*datum.(*DFloat))

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count++
	delta := f - a.mean
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
	return nil
}

func (a *floatVarianceAggregate) Result() (Datum, error) {
	if a.count < 2 {
		return DNull, nil
	}
	return NewDFloat(DFloat(a.sqrDiff / (float64(a.count) - 1))), nil
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

func (a *decimalVarianceAggregate) Add(datum Datum) error {
	d := datum.(*DDecimal).Dec

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count.Add(&a.count, decimalOne)
	a.delta.Sub(&d, &a.mean)
	a.tmp.QuoRound(&a.delta, &a.count, decimal.Precision, inf.RoundHalfUp)
	a.mean.Add(&a.mean, &a.tmp)
	a.tmp.Sub(&d, &a.mean)
	a.sqrDiff.Add(&a.sqrDiff, a.delta.Mul(&a.delta, &a.tmp))
	return nil
}

func (a *decimalVarianceAggregate) Result() (Datum, error) {
	if a.count.Cmp(decimalTwo) < 0 {
		return DNull, nil
	}
	a.tmp.Sub(&a.count, decimalOne)
	dd := &DDecimal{}
	dd.QuoRound(&a.sqrDiff, &a.tmp, decimal.Precision, inf.RoundHalfUp)
	return dd, nil
}

type stddevAggregate struct {
	varianceAggregate
}

func newStddevAggregate() AggregateFunc {
	return &stddevAggregate{varianceAggregate: *newVarianceAggregate().(*varianceAggregate)}
}

func (a *stddevAggregate) Result() (Datum, error) {
	variance, err := a.varianceAggregate.Result()
	if err != nil || variance == DNull {
		return variance, err
	}
	switch t := variance.(type) {
	case *DFloat:
		return NewDFloat(DFloat(math.Sqrt(float64(*t)))), nil
	case *DDecimal:
		decimal.Sqrt(&t.Dec, &t.Dec, decimal.Precision)
		return t, nil
	}
	return nil, errors.Errorf("unexpected variance result type: %s", variance.Type())
}
