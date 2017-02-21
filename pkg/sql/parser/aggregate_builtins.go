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
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/apd"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

func initAggregateBuiltins() {
	// Add all aggregates to the Builtins map after a few sanity checks.
	for k, v := range Aggregates {
		for i, a := range v {
			if !a.impure {
				panic(fmt.Sprintf("aggregate functions should all be impure, found %v", a))
			}
			if a.class != AggregateClass {
				panic(fmt.Sprintf("aggregate functions should be marked with the AggregateClass "+
					"function class, found %v", a))
			}
			if a.AggregateFunc == nil {
				panic(fmt.Sprintf("aggregate functions should have AggregateFunc constructors, "+
					"found %v", a))
			}
			if a.WindowFunc == nil {
				panic(fmt.Sprintf("aggregate functions should have WindowFunc constructors, "+
					"found %v", a))
			}

			// The aggregate functions are considered "row dependent". This is
			// because each aggregate function application receives the set of
			// grouped rows as implicit parameter. It may have a different
			// value in every group, so it cannot be considered constant in
			// the context of a data source.
			v[i].needsRepeatedEvaluation = true
		}

		Builtins[k] = v
	}
}

// AggregateFunc accumulates the result of a function of a Datum.
type AggregateFunc interface {
	// Add accumulates the passed datum into the AggregateFunc.
	Add(Datum)

	// Result returns the current value of the accumulation. This value
	// will be a deep copy of any AggregateFunc internal state, so that
	// it will not be mutated by additional calls to Add.
	Result() Datum
}

// Aggregates are a special class of builtin functions that are wrapped
// at execution in a bucketing layer to combine (aggregate) the result
// of the function being run over many rows.
// See `aggregateFuncHolder` in the sql package.
// In particular they must not be simplified during normalization
// (and thus must be marked as impure), even when they are given a
// constant argument (e.g. SUM(1)). This is because aggregate
// functions must return NULL when they are no rows in the source
// table, so their evaluation must always be delayed until query
// execution.
// Exported for use in documentation.
var Aggregates = map[string][]Builtin{
	"array_agg": {
		makeAggBuiltinWithReturnType(
			TypeAny,
			func(args []TypedExpr) Type {
				if len(args) == 0 {
					return unknownReturnType
				}
				return tArray{args[0].ResolvedType()}
			},
			newArrayAggregate,
			"Aggregates the selected values into an array.",
		),
	},

	"avg": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalAvgAggregate,
			"Calculates the average of the selected values."),
	},

	"bool_and": {
		makeAggBuiltin(TypeBool, TypeBool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values."),
	},

	"bool_or": {
		makeAggBuiltin(TypeBool, TypeBool, newBoolOrAggregate,
			"Calculates the boolean value of `OR`ing all selected values."),
	},

	"concat_agg": {
		// TODO(knz) When CockroachDB supports STRING_AGG, CONCAT_AGG(X)
		// should be substituted to STRING_AGG(X, '') and executed as
		// such (no need for a separate implementation).
		makeAggBuiltin(TypeString, TypeString, newStringConcatAggregate,
			"Concatenates all selected values."),
		makeAggBuiltin(TypeBytes, TypeBytes, newBytesConcatAggregate,
			"Concatenates all selected values."),
	},

	"count": {
		makeAggBuiltin(TypeAny, TypeInt, newCountAggregate,
			"Calculates the number of selected elements."),
	},

	"max": collectBuiltins(func(t Type) Builtin {
		return makeAggBuiltin(t, t, newMaxAggregate,
			"Identifies the maximum selected value.")
	}, TypesAnyNonArray...),
	"min": collectBuiltins(func(t Type) Builtin {
		return makeAggBuiltin(t, t, newMinAggregate,
			"Identifies the minimum selected value.")
	}, TypesAnyNonArray...),

	"sum_int": {
		makeAggBuiltin(TypeInt, TypeInt, newSmallIntSumAggregate,
			"Calculates the sum of the selected values."),
	},

	"sum": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggBuiltin(TypeInterval, TypeInterval, newIntervalSumAggregate,
			"Calculates the sum of the selected values."),
	},

	"variance": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatVarianceAggregate,
			"Calculates the variance of the selected values."),
	},

	"stddev": {
		makeAggBuiltin(TypeInt, TypeDecimal, newIntStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggBuiltin(TypeDecimal, TypeDecimal, newDecimalStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggBuiltin(TypeFloat, TypeFloat, newFloatStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
	},
}

func makeAggBuiltin(in, ret Type, f func([]Type) AggregateFunc, info string) Builtin {
	return makeAggBuiltinWithReturnType(in, fixedReturnType(ret), f, info)
}

func makeAggBuiltinWithReturnType(
	in Type, retType returnTyper, f func([]Type) AggregateFunc, info string,
) Builtin {
	return Builtin{
		// See the comment about aggregate functions in the definitions
		// of the Builtins array above.
		impure:        true,
		class:         AggregateClass,
		Types:         ArgTypes{{"arg", in}},
		ReturnType:    retType,
		AggregateFunc: f,
		WindowFunc: func(params []Type) WindowFunc {
			return newAggregateWindow(f(params))
		},
		Info: info,
	}
}

var _ AggregateFunc = &arrayAggregate{}
var _ AggregateFunc = &avgAggregate{}
var _ AggregateFunc = &countAggregate{}
var _ AggregateFunc = &MaxAggregate{}
var _ AggregateFunc = &MinAggregate{}
var _ AggregateFunc = &intSumAggregate{}
var _ AggregateFunc = &decimalSumAggregate{}
var _ AggregateFunc = &floatSumAggregate{}
var _ AggregateFunc = &stdDevAggregate{}
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

// IsIdentAggregate returns true for identAggregate.
func IsIdentAggregate(f AggregateFunc) bool {
	_, ok := f.(*identAggregate)
	return ok
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
	// It is significant that identAggregate returns nil, and not DNull,
	// if no result was known via Add(). See
	// sql.(*aggregateFuncHolder).Eval() for details.
	return a.val
}

type arrayAggregate struct {
	arr *DArray
}

func newArrayAggregate(params []Type) AggregateFunc {
	return &arrayAggregate{
		arr: NewDArray(params[0]),
	}
}

// Add accumulates the passed datum into the array.
func (a *arrayAggregate) Add(datum Datum) {
	if err := a.arr.Append(datum); err != nil {
		panic(fmt.Sprintf("error appending to array: %s", err))
	}
}

// Result returns an array of all datums passed to Add.
func (a *arrayAggregate) Result() Datum {
	if len(a.arr.Array) > 0 {
		return a.arr
	}
	return DNull
}

type avgAggregate struct {
	agg   AggregateFunc
	count int
}

func newIntAvgAggregate(params []Type) AggregateFunc {
	return &avgAggregate{agg: newIntSumAggregate(params)}
}
func newFloatAvgAggregate(params []Type) AggregateFunc {
	return &avgAggregate{agg: newFloatSumAggregate(params)}
}
func newDecimalAvgAggregate(params []Type) AggregateFunc {
	return &avgAggregate{agg: newDecimalSumAggregate(params)}
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
		count := apd.New(int64(a.count), 0)
		_, err := DecimalCtx.Quo(&t.Decimal, &t.Decimal, count)
		if err != nil {
			// TODO(mjibson): see #13640
			panic(err)
		}
		return t
	default:
		panic(fmt.Sprintf("unexpected SUM result type: %s", t))
	}
}

type concatAggregate struct {
	forBytes   bool
	sawNonNull bool
	result     bytes.Buffer
}

func newBytesConcatAggregate(_ []Type) AggregateFunc {
	return &concatAggregate{forBytes: true}
}
func newStringConcatAggregate(_ []Type) AggregateFunc {
	return &concatAggregate{}
}

func (a *concatAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	a.sawNonNull = true
	var arg string
	if a.forBytes {
		arg = string(*datum.(*DBytes))
	} else {
		arg = string(MustBeDString(datum))
	}
	a.result.WriteString(arg)
}

func (a *concatAggregate) Result() Datum {
	if !a.sawNonNull {
		return DNull
	}
	if a.forBytes {
		res := DBytes(a.result.String())
		return &res
	}
	res := DString(a.result.String())
	return &res
}

type boolAndAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolAndAggregate(_ []Type) AggregateFunc {
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

func newBoolOrAggregate(_ []Type) AggregateFunc {
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

func newCountAggregate(_ []Type) AggregateFunc {
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

func newMaxAggregate(_ []Type) AggregateFunc {
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

func newMinAggregate(_ []Type) AggregateFunc {
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

type smallIntSumAggregate struct {
	sum         int64
	seenNonNull bool
}

func newSmallIntSumAggregate(_ []Type) AggregateFunc {
	return &smallIntSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *smallIntSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}

	a.sum += int64(MustBeDInt(datum))
	a.seenNonNull = true
}

// Result returns the sum.
func (a *smallIntSumAggregate) Result() Datum {
	if !a.seenNonNull {
		return DNull
	}
	return NewDInt(DInt(a.sum))
}

type intSumAggregate struct {
	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	intSum      int64
	decSum      DDecimal
	tmpDec      apd.Decimal
	large       bool
	seenNonNull bool
}

func newIntSumAggregate(_ []Type) AggregateFunc {
	return &intSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *intSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}

	t := int64(MustBeDInt(datum))
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
			a.decSum.SetCoefficient(a.intSum)
		}

		if a.large {
			a.tmpDec.SetCoefficient(t)
			// TODO(mjibson): see #13640
			_, err := ExactCtx.Add(&a.decSum.Decimal, &a.decSum.Decimal, &a.tmpDec)
			if err != nil {
				panic(err)
			}
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
	dd := &DDecimal{}
	if a.large {
		dd.Set(&a.decSum.Decimal)
	} else {
		dd.SetCoefficient(a.intSum)
	}
	return dd
}

type decimalSumAggregate struct {
	sum        apd.Decimal
	sawNonNull bool
}

func newDecimalSumAggregate(_ []Type) AggregateFunc {
	return &decimalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *decimalSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	t := datum.(*DDecimal)
	// TODO(mjibson): see #13640
	_, err := ExactCtx.Add(&a.sum, &a.sum, &t.Decimal)
	if err != nil {
		panic(err)
	}
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

func newFloatSumAggregate(_ []Type) AggregateFunc {
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

type intervalSumAggregate struct {
	sum        duration.Duration
	sawNonNull bool
}

func newIntervalSumAggregate(_ []Type) AggregateFunc {
	return &intervalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *intervalSumAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	t := datum.(*DInterval).Duration
	a.sum = a.sum.Add(t)
	a.sawNonNull = true
}

// Result returns the sum.
func (a *intervalSumAggregate) Result() Datum {
	if !a.sawNonNull {
		return DNull
	}
	return &DInterval{Duration: a.sum}
}

type intVarianceAggregate struct {
	agg decimalVarianceAggregate
	// Used for passing int64s as *apd.Decimal values.
	tmpDec DDecimal
}

func newIntVarianceAggregate(_ []Type) AggregateFunc {
	return &intVarianceAggregate{}
}

func (a *intVarianceAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}

	a.tmpDec.SetCoefficient(int64(MustBeDInt(datum)))
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

func newFloatVarianceAggregate(_ []Type) AggregateFunc {
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
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	delta apd.Decimal
	tmp   apd.Decimal
}

func newDecimalVarianceAggregate(_ []Type) AggregateFunc {
	return &decimalVarianceAggregate{}
}

// Read-only constants used for compuation.
var (
	decimalOne = apd.New(1, 0)
	decimalTwo = apd.New(2, 0)
)

func (a *decimalVarianceAggregate) Add(datum Datum) {
	if datum == DNull {
		return
	}
	d := &datum.(*DDecimal).Decimal

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	ed := apd.MakeErrDecimal(DecimalCtx)
	ed.Add(&a.count, &a.count, decimalOne)
	ed.Sub(&a.delta, d, &a.mean)
	ed.Quo(&a.tmp, &a.delta, &a.count)
	ed.Add(&a.mean, &a.mean, &a.tmp)
	ed.Sub(&a.tmp, d, &a.mean)
	ed.Add(&a.sqrDiff, &a.sqrDiff, ed.Mul(&a.delta, &a.delta, &a.tmp))
	// TODO(mjibson): see #13640
	if err := ed.Err(); err != nil {
		panic(err)
	}
}

func (a *decimalVarianceAggregate) Result() Datum {
	if a.count.Cmp(decimalTwo) < 0 {
		return DNull
	}
	ed := apd.MakeErrDecimal(DecimalCtx)
	ed.Sub(&a.tmp, &a.count, decimalOne)
	dd := &DDecimal{}
	ed.Quo(&dd.Decimal, &a.sqrDiff, &a.tmp)
	// TODO(mjibson): see #13640
	if err := ed.Err(); err != nil {
		panic(err)
	}
	return dd
}

type stdDevAggregate struct {
	agg AggregateFunc
}

func newIntStdDevAggregate(params []Type) AggregateFunc {
	return &stdDevAggregate{agg: newIntVarianceAggregate(params)}
}
func newFloatStdDevAggregate(params []Type) AggregateFunc {
	return &stdDevAggregate{agg: newFloatVarianceAggregate(params)}
}
func newDecimalStdDevAggregate(params []Type) AggregateFunc {
	return &stdDevAggregate{agg: newDecimalVarianceAggregate(params)}
}

// Add implements the AggregateFunc interface.
func (a *stdDevAggregate) Add(datum Datum) {
	a.agg.Add(datum)
}

// Result computes the square root of the variance.
func (a *stdDevAggregate) Result() Datum {
	variance := a.agg.Result()
	if variance == DNull {
		return variance
	}
	switch t := variance.(type) {
	case *DFloat:
		return NewDFloat(DFloat(math.Sqrt(float64(*t))))
	case *DDecimal:
		// TODO(mjibson): see #13640
		_, err := DecimalCtx.Sqrt(&t.Decimal, &t.Decimal)
		if err != nil {
			panic(err)
		}
		return t
	}
	panic(fmt.Sprintf("unexpected variance result type: %s", variance.ResolvedType()))
}

var _ Visitor = &IsAggregateVisitor{}

// IsAggregateVisitor checks if walked expressions contain aggregate functions.
type IsAggregateVisitor struct {
	Aggregated bool
	// searchPath is used to search for unqualified function names.
	searchPath SearchPath
}

// VisitPre satisfies the Visitor interface.
func (v *IsAggregateVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *FuncExpr:
		if t.IsWindowFunctionApplication() {
			// A window function application of an aggregate builtin is not an
			// aggregate function, but it can contain aggregate functions.
			return true, expr
		}
		fd, err := t.Func.Resolve(v.searchPath)
		if err != nil {
			return false, expr
		}
		if _, ok := Aggregates[fd.Name]; ok {
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
func (p *Parser) AggregateInExpr(expr Expr, searchPath SearchPath) bool {
	if expr != nil {
		p.isAggregateVisitor.searchPath = searchPath
		defer p.isAggregateVisitor.Reset()
		WalkExprConst(&p.isAggregateVisitor, expr)
		if p.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// IsAggregate determines if SelectClause contains an aggregate function.
func (p *Parser) IsAggregate(n *SelectClause, searchPath SearchPath) bool {
	if n.Having != nil || len(n.GroupBy) > 0 {
		return true
	}

	p.isAggregateVisitor.searchPath = searchPath
	defer p.isAggregateVisitor.Reset()
	for _, target := range n.Exprs {
		WalkExprConst(&p.isAggregateVisitor, target.Expr)
		if p.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// AssertNoAggregationOrWindowing checks if the provided expression contains either
// aggregate functions or window functions, returning an error in either case.
func (p *Parser) AssertNoAggregationOrWindowing(expr Expr, op string, searchPath SearchPath) error {
	if p.AggregateInExpr(expr, searchPath) {
		return fmt.Errorf("aggregate functions are not allowed in %s", op)
	}
	if p.WindowFuncInExpr(expr) {
		return fmt.Errorf("window functions are not allowed in %s", op)
	}
	return nil
}
