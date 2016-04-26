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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package parser

import (
	"fmt"
	"go/constant"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/util"
)

var (
	// TypeBytes returns a bytes datum.
	TypeBytes = DummyBytes
	// TypeDate returns a date datum.
	TypeDate = DummyDate
	// TypeFloat returns a float datum.
	TypeFloat = DummyFloat
	// TypeDecimal returns a decimal datum.
	TypeDecimal = DummyDecimal
	// TypeInt returns an int datum.
	TypeInt = DummyInt
	// TypeInterval returns an interval datum.
	TypeInterval = DummyInterval
	// TypeString returns a string datum.
	TypeString = DummyString
	// TypeTimestamp returns a timestamp datum.
	TypeTimestamp = DummyTimestamp
)

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return typeCheckBooleanExprs(args, "AND", expr.Left, expr.Right)
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	ops := BinOps[expr.Operator]
	overloads := make([]overload, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	types, fn, err := typeCheckOverloadedExprs(args, desired, overloads, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}

	leftType, rightType := types[0], types[1]
	if leftType == DNull || rightType == DNull {
		return DNull, nil
	}

	if fn == nil {
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" = <%s>", desired.Type())
		}
		return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>%s",
			leftType.Type(), expr.Operator, rightType.Type(), desStr)
	}
	expr.fn = fn.(BinOp)
	return expr.fn.ReturnType, nil
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(args MapArgs, desired Datum) (d Datum, errrr error) {
	tmpExprs := make([]Expr, 0, len(expr.Whens)+1)
	if expr.Expr != nil {
		tmpExprs = tmpExprs[:0]
		tmpExprs = append(tmpExprs, expr.Expr)
		for _, when := range expr.Whens {
			tmpExprs = append(tmpExprs, when.Cond)
		}

		_, err := typeCheckSameTypedExprs(args, nil, tmpExprs...)
		if err != nil {
			return nil, fmt.Errorf("incompatible condition type: %v", err)
		}
	} else {
		// If expr.Expr is nil, the WHEN clauses contain boolean expressions.
		for _, when := range expr.Whens {
			condType, err := when.Cond.TypeCheck(args, DummyBool)
			if err != nil {
				return nil, err
			}
			if !(condType == DNull || condType.TypeEqual(DummyBool)) {
				return nil, fmt.Errorf("incompatible condition types %s, %s", DummyBool.Type(), condType.Type())
			}
		}
	}

	tmpExprs = tmpExprs[:0]
	for _, when := range expr.Whens {
		tmpExprs = append(tmpExprs, when.Val)
	}
	if expr.Else != nil {
		tmpExprs = append(tmpExprs, expr.Else)
	}
	typ, err := typeCheckSameTypedExprs(args, desired, tmpExprs...)
	if err != nil {
		return nil, fmt.Errorf("incompatible value type: %v", err)
	}
	return typ, nil
}

var (
	boolCastTypes      = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	intCastTypes       = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	floatCastTypes     = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	decimalCastTypes   = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	stringCastTypes    = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString, DummyBytes}
	bytesCastTypes     = []Datum{DNull, DummyBytes, DummyString}
	dateCastTypes      = []Datum{DNull, DummyString, DummyDate, DummyTimestamp}
	timestampCastTypes = []Datum{DNull, DummyString, DummyDate, DummyTimestamp, DummyTimestampTZ}
	intervalCastTypes  = []Datum{DNull, DummyString, DummyInt, DummyInterval}
)

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	dummyExpr, err := expr.Expr.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}

	var returnDatum Datum
	var validTypes []Datum
	switch expr.Type.(type) {
	case *BoolType:
		returnDatum = DummyBool
		validTypes = boolCastTypes

	case *IntType:
		returnDatum = DummyInt
		validTypes = intCastTypes

	case *FloatType:
		returnDatum = DummyFloat
		validTypes = floatCastTypes

	case *DecimalType:
		returnDatum = DummyDecimal
		validTypes = decimalCastTypes

	case *StringType:
		returnDatum = DummyString
		validTypes = stringCastTypes

	case *BytesType:
		returnDatum = DummyBytes
		validTypes = bytesCastTypes

	case *DateType:
		returnDatum = DummyDate
		validTypes = dateCastTypes

	case *TimestampType:
		returnDatum = DummyTimestamp
		validTypes = timestampCastTypes

	case *TimestampTZType:
		returnDatum = DummyTimestampTZ
		validTypes = timestampCastTypes

	case *IntervalType:
		returnDatum = DummyInterval
		validTypes = intervalCastTypes
	}

	if set, err := args.SetInferredType(dummyExpr, DummyString); err != nil {
		return nil, err
	} else if set != nil {
		dummyExpr = DummyString
	}

	for _, t := range validTypes {
		if dummyExpr.TypeEqual(t) {
			return returnDatum, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", dummyExpr.Type(), expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	exprsCopy := append([]Expr(nil), expr.Exprs...) // To avoid reordering.
	typ, err := typeCheckSameTypedExprs(args, desired, exprsCopy...)
	if err != nil {
		return nil, fmt.Errorf("incompatible %s expressions: %v", expr.Name, err)
	}
	return typ, nil
}

// TypeCheck implements the Expr interface.
func (expr *ComparisonExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	typ, cmp, err := typeCheckComparisonOp(args, expr.Operator, expr.Left, expr.Right)
	expr.fn = cmp
	return typ, err
}

// TypeCheck implements the Expr interface.
func (expr *ExistsExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	_, err := expr.Subquery.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	if len(expr.Name.Indirect) > 0 {
		// We don't support qualified function names (yet).
		return nil, fmt.Errorf("unknown function: %s", expr.Name)
	}

	name := string(expr.Name.Base)
	candidates, ok := Builtins[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("unknown function: %s", name)
	}

	overloads := make([]overload, len(candidates))
	for i := range candidates {
		overloads[i] = candidates[i]
	}
	types, fn, err := typeCheckOverloadedExprs(args, desired, overloads, expr.Exprs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", name, err)
	} else if fn == nil {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, typ := range types {
			// TODO(nvanbenschoten)
			// if typ == DNull {
			// 	return DNull, nil
			// }
			typeNames = append(typeNames, typ.Type())
		}
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" = <%s>", desired.Type())
		}
		return nil, fmt.Errorf("unknown signature for %s: %s(%s)%s",
			expr.Name, expr.Name, strings.Join(typeNames, ", "), desStr)
	}

	expr.fn = fn.(Builtin)
	returnType := fn.returnType()
	if _, ok = expr.fn.params().(AnyType); ok {
		if len(types) > 0 {
			returnType = types[0]
		} else {
			returnType = DNull
		}
	}
	return returnType, nil

	// // Function lookup succeeded and `fn` encodes its return type.
	// if expr.fn.ReturnType != nil {
	// 	datum, err := expr.fn.ReturnType(args, dummyArgs)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("%s: %v", expr.Name, err)
	// 	}
	// 	return datum, nil
	// }

	// // Function lookup succeeded but `fn` doesn't encode its return type.
	// // We need to call the function with dummy arguments.
	// res, err := expr.fn.fn(defaultContext, dummyArgs)
	// if err != nil {
	// 	return nil, fmt.Errorf("%s: %v", expr.Name, err)
	// }
	// return res, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	cond, err := expr.Cond.TypeCheck(args, DummyBool)
	if err != nil {
		return nil, err
	}
	if !(cond == DNull || cond.TypeEqual(DummyBool)) {
		return nil, fmt.Errorf("IF condition must be a boolean: %s", cond.Type())
	}
	typ, err := typeCheckSameTypedExprs(args, desired, expr.True, expr.Else)
	if err != nil {
		return nil, fmt.Errorf("incompatible IF expressions: %v", err)
	}
	return typ, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	if _, err := expr.Expr.TypeCheck(args, desired); err != nil {
		return nil, err
	}
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return typeCheckBooleanExprs(args, "NOT", expr.Expr)
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	typ, err := typeCheckSameTypedExprs(args, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, fmt.Errorf("incompatible NULLIF expressions: %v", err)
	}
	return typ, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return typeCheckBooleanExprs(args, "OR", expr.Left, expr.Right)
}

// TypeCheck implements the Expr interface.
func (expr *QualifiedName) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return nil, fmt.Errorf("qualified name \"%s\" not found", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	_, err := typeCheckSameTypedExprs(args, nil, expr.Left, expr.From, expr.To)
	if err != nil {
		return nil, err
	}
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	// Avoid type checking subqueries. We need the subquery to be expanded in
	// order to do so properly.
	return DNull, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	ops := UnaryOps[expr.Operator]
	overloads := make([]overload, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	types, fn, err := typeCheckOverloadedExprs(args, desired, overloads, expr.Expr)
	if err != nil {
		return nil, err
	}

	exprType := types[0]
	if exprType == DNull {
		return DNull, nil
	}

	if fn == nil {
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" = <%s>", desired.Type())
		}
		return nil, fmt.Errorf("unsupported unary operator: %s <%s>%s",
			expr.Operator, exprType.Type(), desStr)
	}
	expr.fn = fn.(UnaryOp)
	return expr.fn.ReturnType, nil
}

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr *ConstVal) TypeCheck(args MapArgs, desired Datum) (d Datum, err error) {
	if expr.ResolvedType != nil {
		return expr.ResolvedType, nil
	}
	defer func() {
		if err == nil {
			expr.ResolvedType = d
		}
	}()

	if desired != nil && expr.canBecomeType(desired) {
		return desired, nil
	}

	natural := expr.naturalType()
	if natural != nil {
		return natural, nil
	}
	return nil, fmt.Errorf("could not determine prefered type for ConstVal %v", expr.Value)
}

var constTypePreference = []Datum{DummyInt, DummyFloat, DummyDecimal}

var intFloatDecimalAvailable = map[Datum]struct{}{
	DummyInt:     {},
	DummyFloat:   {},
	DummyDecimal: {},
}

// var intDecimalAvailable = map[Datum]struct{}{
// 	DummyInt:     {},
// 	DummyDecimal: {},
// }
var floatDecimalAvailable = map[Datum]struct{}{
	DummyFloat:   {},
	DummyDecimal: {},
}
var decimalAvailable = map[Datum]struct{}{
	DummyDecimal: {},
}

func (expr *ConstVal) canBecomeType(desired Datum) bool {
	switch desired {
	case DummyInt:
		return expr.canBeInt64()
	case DummyFloat:
		// TODO(nvanbenschoten) float overflow?
		return true
	case DummyDecimal:
		return true
	default:
		return false
	}
}

func (expr *ConstVal) availableTypes() map[Datum]struct{} {
	switch {
	case expr.shouldBeInt64():
		return intFloatDecimalAvailable
	case expr.Kind() == constant.Float:
		return floatDecimalAvailable
	default:
		return decimalAvailable
	}
}

func (expr *ConstVal) naturalType() Datum {
	available := expr.availableTypes()
	for _, t := range constTypePreference {
		if _, ok := available[t]; ok {
			return t
		}
	}
	return nil
}

func typeCheckExprs(args MapArgs, exprs []Expr, desired Datum) (Datum, error) {
	tuple := make(DTuple, 0, len(exprs))
	for i, v := range exprs {
		var desiredElem Datum
		if t, ok := desired.(*DTuple); ok && len(*t) > i {
			desiredElem = (*t)[i]
		}
		d, err := v.TypeCheck(args, desiredElem)
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return &tuple, nil
}

// TypeCheck implements the Expr interface.
func (expr *Row) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return typeCheckExprs(args, expr.Exprs, desired)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return typeCheckExprs(args, expr.Exprs, desired)
}

// TypeCheck implements the Expr interface.
func (expr ValArg) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	if v, ok := args[expr.name]; ok {
		return v, nil
	}
	dVal := &DValArg{name: expr.name}
	if desired != nil {
		_, err := args.SetInferredType(dVal, desired)
		if err != nil {
			return nil, err
		}
		return desired, nil
	}
	return dVal, nil
}

// TypeCheck implements the Expr interface.
func (expr DValArg) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr *DBool) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *DBytes) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyBytes, nil
}

// TypeCheck implements the Expr interface.
func (expr *DDate) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyDate, nil
}

// TypeCheck implements the Expr interface.
func (expr *DFloat) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyFloat, nil
}

// TypeCheck implements the Expr interface.
func (expr *DDecimal) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyDecimal, nil
}

// TypeCheck implements the Expr interface.
func (expr *DInt) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyInt, nil
}

// TypeCheck implements the Expr interface.
func (expr *DInterval) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyInterval, nil
}

// TypeCheck implements the Expr interface.
func (expr dNull) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DNull, nil
}

// TypeCheck implements the Expr interface.
func (expr *DString) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyString, nil
}

// TypeCheck implements the Expr interface.
func (expr *DTimestamp) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyTimestamp, nil
}

// TypeCheck implements the Expr interface.
func (expr *DTimestampTZ) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	return DummyTimestampTZ, nil
}

// TypeCheck implements the Expr interface.
func (expr DTuple) TypeCheck(args MapArgs, desired Datum) (Datum, error) {
	tuple := make(DTuple, 0, len(expr))
	for i, v := range expr {
		var desiredElem Datum
		if t, ok := desired.(*DTuple); ok && len(*t) > i {
			desiredElem = (*t)[i]
		}
		d, err := v.TypeCheck(args, desiredElem)
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return &tuple, nil
}

func typeCheckBooleanExprs(args MapArgs, op string, exprs ...Expr) (Datum, error) {
	for _, expr := range exprs {
		dummyExpr, err := expr.TypeCheck(args, DummyBool)
		if err != nil {
			return nil, err
		}
		if dummyExpr == DNull {
			continue
		}
		if set, err := args.SetInferredType(dummyExpr, DummyBool); err != nil {
			return nil, err
		} else if set != nil {
			continue
		}
		if !dummyExpr.TypeEqual(DummyBool) {
			return nil, fmt.Errorf("incompatible %s argument type: %s", op, dummyExpr.Type())
		}
	}
	return DummyBool, nil
}

func typeCheckComparisonOp(args MapArgs, op ComparisonOp, left, right Expr) (Datum, CmpOp, error) {
	origOp := op
	op, left, right, _ = foldComparisonExpr(op, left, right)

	ops := CmpOps[op]
	overloads := make([]overload, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	types, fn, err := typeCheckOverloadedExprs(args, nil, overloads, left, right)
	if err != nil {
		return nil, CmpOp{}, err
	}

	leftType, rightType := types[0], types[1]
	if leftType == DNull || rightType == DNull {
		switch origOp {
		case Is, IsNot, IsDistinctFrom, IsNotDistinctFrom:
			// TODO(pmattis): For IS {UNKNOWN,TRUE,FALSE} we should be requiring that
			// dummyLeft.TypeEquals(DummyBool). We currently can't distinguish NULL from
			// UNKNOWN. Is it important to do so?
			return DummyBool, CmpOp{}, nil
		default:
			return DNull, CmpOp{}, nil
		}
	}

	if fn == nil {
		return nil, CmpOp{}, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
			leftType.Type(), origOp, rightType.Type())
	}
	cmpOp := fn.(CmpOp)

	if op == In && cmpOp.RightType.TypeEqual(dummyTuple) {
		if err := typeCheckTupleIN(args, leftType, rightType); err != nil {
			return nil, CmpOp{}, err
		}
	} else if cmpOp.LeftType.TypeEqual(dummyTuple) && cmpOp.RightType.TypeEqual(dummyTuple) {
		if err := typeCheckTupleCmp(args, leftType, rightType); err != nil {
			return nil, CmpOp{}, err
		}
	}

	return DummyBool, cmpOp, nil
}

func typeCheckTupleCmp(args MapArgs, leftTupleType, rightTupleType Datum) error {
	lTuple := *leftTupleType.(*DTuple)
	rTuple := *rightTupleType.(*DTuple)
	if len(lTuple) != len(rTuple) {
		return fmt.Errorf("unequal number of entries in tuple expressions: %d, %d", len(lTuple), len(rTuple))
	}

	for i := range lTuple {
		if _, _, err := typeCheckComparisonOp(args, EQ, lTuple[i], rTuple[i]); err != nil {
			return err
		}
	}

	return nil
}

func typeCheckTupleIN(args MapArgs, arg, values Datum) error {
	if arg == DNull {
		return nil
	}

	vtuple := *values.(*DTuple)
	for _, val := range vtuple {
		if _, _, err := typeCheckComparisonOp(args, EQ, arg, val); err != nil {
			return err
		}
	}

	return nil
}

// Order: normal Expr, ConstVal, unresolved VarArg
type exprsSorter struct {
	exprs []Expr
	args  MapArgs
}

func (es exprsSorter) Len() int      { return len(es.exprs) }
func (es exprsSorter) Swap(i, j int) { es.exprs[i], es.exprs[j] = es.exprs[j], es.exprs[i] }
func (es exprsSorter) Less(i, j int) bool {
	return es.priority(i) < es.priority(j)
}
func (es exprsSorter) priority(i int) int {
	expr := es.exprs[i]
	switch {
	case isUnresolvedValArg(es.args, expr):
		return 2
	case isUnresolvedConstVal(expr):
		return 1
	default:
		return 0
	}
}

func isUnresolvedConstVal(expr Expr) bool {
	if t, ok := expr.(*ConstVal); ok {
		return t.ResolvedType == nil
	}
	return false
}

func isUnresolvedValArg(args MapArgs, expr Expr) bool {
	if t, ok := expr.(ValArg); ok {
		if _, ok := args[t.name]; !ok {
			return true
		}
	}
	return false
}

func typeCheckSameTypedExprs(args MapArgs, desired Datum, exprs ...Expr) (Datum, error) {
	switch len(exprs) {
	case 0:
		return nil, fmt.Errorf("no exprs provided to typeCheckSameTypedExprs")
	case 1:
		return exprs[0].TypeCheck(args, desired)
	default:
		// Sort the expressions to be easier to use.
		sort.Sort(exprsSorter{
			exprs: exprs,
			args:  args,
		})
	}

	// Called when all non-NULL Exprs are ValArgs.
	typeCheckSameTypedExprsArgsOnly := func(exprs []Expr) (Datum, error) {
		if desired == nil {
			return nil, fmt.Errorf("could not determine data type of parameter %s", exprs[0])
		}
		for i := 0; i < len(exprs); i++ {
			if _, err := exprs[i].(ValArg).TypeCheck(args, desired); err != nil {
				panic(fmt.Sprintf("could not set inferred type on unresolved ValArg: %v", err))
			}
		}
		return desired, nil
	}

	// Called when all non-NULL Exprs are ConstVals and ValArgs.
	typeCheckSameTypedExprsConstsAndArgs := func(exprs []Expr) (Datum, error) {
		lastConst := sort.Search(len(exprs), func(i int) bool { return isUnresolvedValArg(args, exprs[i]) })

		setTypeForConstsAndValArgs := func(typ Datum) {
			for i := 0; i < lastConst; i++ {
				exprs[i].(*ConstVal).ResolvedType = typ
			}
			for i := lastConst; i < len(exprs); i++ {
				if _, err := exprs[i].(ValArg).TypeCheck(args, typ); err != nil {
					panic(fmt.Sprintf("could not set inferred type on unresolved ValArg: %v", err))
				}
			}
		}

		// Can we set all constants to the desired value?
		if desired != nil {
			all := true
			for i := 0; i < lastConst; i++ {
				if !exprs[i].(*ConstVal).canBecomeType(desired) {
					all = false
					break
				}
			}
			if all {
				setTypeForConstsAndValArgs(desired)
				return desired, nil
			}
		}

		constExprs := make([]*ConstVal, lastConst)
		for i := 0; i < lastConst; i++ {
			constExprs[i] = exprs[i].(*ConstVal)
		}
		bestType := commonConstantType(constExprs...)
		setTypeForConstsAndValArgs(bestType)
		return bestType, nil
	}

	switch {
	case isUnresolvedValArg(args, exprs[0]):
		return typeCheckSameTypedExprsArgsOnly(exprs)
	case isUnresolvedConstVal(exprs[0]):
		return typeCheckSameTypedExprsConstsAndArgs(exprs)
	default:
		firstValidType := DNull
		firstValidIdx := 0
		for ; firstValidIdx < len(exprs); firstValidIdx++ {
			switch {
			case isUnresolvedValArg(args, exprs[firstValidIdx]):
				return typeCheckSameTypedExprsArgsOnly(exprs[firstValidIdx:])
			case isUnresolvedConstVal(exprs[firstValidIdx]):
				return typeCheckSameTypedExprsConstsAndArgs(exprs[firstValidIdx:])
			}

			if t, ok := exprs[firstValidIdx].(*DValArg); ok {
				if typ, set := args.Arg(t.name); set {
					firstValidType = typ
				} else {
					panic("found unresolved ValArg")
				}
			} else {
				var err error
				if firstValidType, err = exprs[firstValidIdx].TypeCheck(args, desired); err != nil {
					return nil, err
				}
			}
			if firstValidType != DNull {
				break
			}
		}

		for i := firstValidIdx; i < len(exprs); i++ {
			typ, err := exprs[i].TypeCheck(args, firstValidType)
			if err != nil {
				return nil, err
			}
			if !(typ == DNull || typ.TypeEqual(firstValidType)) {
				return nil, fmt.Errorf("expected %s to be of type %s, found type %s", exprs[i], firstValidType.Type(), typ.Type())
			}
		}
		return firstValidType, nil
	}
}

type indexedExpr struct {
	Expr
	i int
}

// typeCheckOverloadedExprs determines the correct overload to use for the given set of
// expression parameters, along with an optional desired return type. The order of presidence
// in chosing the correct overload is as follows:
// 1. overloads are filtered based on argument length
// 2. overloads are filtered based on the types of resolved types
// 3. overloads are filtered based on the possible types of constants
// - if only one overload is left here, select it
// 4. if all resolved parameters are homogeneous and all constants can become this type, filter
// - if only one overload is left here, select it
// 5. overloads are filtered based on the desired return type, if one is provided
// - if only one overload is left here, select it
// 6. overloads are filtered based on the "best" homogeneous constant type
// - if only one overload is left here, select it
// 7. if all other parameters are homogeneous, overloads are filtered based on the type for ValArgs
// - if only one overload is left here, select it
// - else we can not determine the desired overload
func typeCheckOverloadedExprs(args MapArgs, desired Datum, overloads []overload, exprs ...Expr) (dadadad []Datum, dosajdaoisjd overload, errrr error) {
	// Create a slice to hold the types of each parameter, in order.
	typeChecked := make([]Datum, len(exprs))

	// Special-case the AnyType overload. We determine it's return type be checking that
	// all parameters have the same type.
	for _, overload := range overloads {
		// Only one overload can be provided if it is type
		if _, ok := overload.params().(AnyType); ok {
			if len(overloads) > 1 {
				return nil, nil, fmt.Errorf("only one overload can have parameters with AnyType")
			}
			if len(exprs) > 0 {
				exprsCopy := append([]Expr(nil), exprs...) // To avoid reordering.
				typ, err := typeCheckSameTypedExprs(args, desired, exprsCopy...)
				if err != nil {
					return nil, nil, err
				}
				for i := range typeChecked {
					typeChecked[i] = typ
				}
			}
			return typeChecked, overload, nil
		}
	}

	var resolvedExprs, constExprs, valExprs []indexedExpr
	for i, expr := range exprs {
		idxExpr := indexedExpr{Expr: expr, i: i}
		switch {
		case isUnresolvedConstVal(expr):
			constExprs = append(constExprs, idxExpr)
		case isUnresolvedValArg(args, expr):
			valExprs = append(valExprs, idxExpr)
		default:
			resolvedExprs = append(resolvedExprs, idxExpr)
		}
	}

	// defaultTypeCheck type checks the constant and valArg expressions without a preference
	// and adds them to the type checked slice.
	defaultTypeCheck := func() error {
		for _, expr := range constExprs {
			typ, err := expr.Expr.(*ConstVal).TypeCheck(args, nil)
			if err != nil {
				return fmt.Errorf("error type checking constant value: %v", err)
			}
			typeChecked[expr.i] = typ
		}
		for _, expr := range valExprs {
			typ, err := expr.Expr.(ValArg).TypeCheck(args, nil)
			if err != nil {
				return fmt.Errorf("error type checking parameter value: %v", err)
			}
			typeChecked[expr.i] = typ
		}
		return nil
	}

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, expr := range resolvedExprs {
			typ, err := expr.Expr.TypeCheck(args, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("error type checking resolved expression: %v", err)
			}
			typeChecked[expr.i] = typ
		}
		if err := defaultTypeCheck(); err != nil {
			return nil, nil, err
		}
		return typeChecked, nil, nil
	}

	// Function to filter overloads which return false from the provided closure.
	filterOverloads := func(fn func(overload) bool) {
		for i := 0; i < len(overloads); {
			if fn(overloads[i]) {
				i++
			} else {
				overloads[i], overloads[len(overloads)-1] = overloads[len(overloads)-1], overloads[i]
				overloads = overloads[:len(overloads)-1]
			}
		}
	}

	// Filter out incorrect parameter length overloads.
	filterOverloads(func(o overload) bool {
		return o.params().matchLen(len(exprs))
	})

	// Filter out overloads on resolved types.
	for _, expr := range resolvedExprs {
		typ, err := expr.TypeCheck(args, nil)
		if err != nil {
			return nil, nil, err
		}
		typeChecked[expr.i] = typ
		filterOverloads(func(o overload) bool {
			return o.params().matchAt(typ, expr.i)
		})
	}

	// Filter out overloads which constants cannot become.
	for _, expr := range constExprs {
		constExpr := expr.Expr.(*ConstVal)
		filterOverloads(func(o overload) bool {
			return constExpr.canBecomeType(o.params().getAt(expr.i))
		})
	}

	checkReturn := func() (bool, overload, error) {
		switch len(overloads) {
		case 0:
			if err := defaultTypeCheck(); err != nil {
				return true, nil, err
			}
			return true, nil, nil
		case 1:
			o := overloads[0]
			p := o.params()
			for _, expr := range constExprs {
				des := p.getAt(expr.i)
				typ, err := expr.Expr.(*ConstVal).TypeCheck(args, des)
				if err != nil {
					return true, nil, fmt.Errorf("error type checking constant value: %v", err)
				} else if des != nil && !typ.TypeEqual(des) {
					panic(fmt.Errorf("desired constant value type %s but set type %s", des.Type(), typ.Type()))
				}
				typeChecked[expr.i] = typ
			}

			for _, expr := range valExprs {
				typ := p.getAt(expr.i)
				if _, err := expr.Expr.(ValArg).TypeCheck(args, typ); err != nil {
					panic(fmt.Sprintf("could not set inferred type on unresolved ValArg: %v", err))
				}
				typeChecked[expr.i] = typ
			}
			return true, o, nil
		default:
			return false, nil, nil
		}
	}
	if ok, fn, err := checkReturn(); ok {
		return typeChecked, fn, err
	}

	// Filter out overloads which return the desired type.
	if desired != nil {
		filterOverloads(func(o overload) bool {
			return o.returnType().TypeEqual(desired)
		})
		if ok, fn, err := checkReturn(); ok {
			return typeChecked, fn, err
		}
	}

	var homogeneousTyp Datum
	if len(resolvedExprs) > 0 {
		homogeneousTyp = typeChecked[resolvedExprs[0].i]
		for _, resExprs := range resolvedExprs[1:] {
			if !homogeneousTyp.TypeEqual(typeChecked[resExprs.i]) {
				homogeneousTyp = nil
				break
			}
		}
	}

	var bestConstType Datum
	if len(constExprs) > 0 {
		constVals := make([]*ConstVal, len(constExprs))
		for i, expr := range constExprs {
			constVals[i] = expr.Expr.(*ConstVal)
		}
		before := overloads

		// Check if all constants can become the homogeneous type.
		if homogeneousTyp != nil {
			all := true
			for _, constVal := range constVals {
				if !constVal.canBecomeType(homogeneousTyp) {
					all = false
					break
				}
			}
			if all {
				for _, expr := range constExprs {
					filterOverloads(func(o overload) bool {
						return o.params().getAt(expr.i).TypeEqual(homogeneousTyp)
					})
				}
			}
		}
		if len(overloads) == 1 {
			if ok, fn, err := checkReturn(); ok {
				return typeChecked, fn, err
			}
		}
		// Restore the expressions if this did not work.
		overloads = before

		// Check if an overload fits with the natural constant types.
		for i, expr := range constExprs {
			natural := constVals[i].naturalType()
			if natural != nil {
				filterOverloads(func(o overload) bool {
					return o.params().getAt(expr.i).TypeEqual(natural)
				})
			}
		}
		if len(overloads) == 1 {
			if ok, fn, err := checkReturn(); ok {
				return typeChecked, fn, err
			}
		}
		// Restore the expressions if this did not work.
		overloads = before

		// Check if an overload fits with the "best" mutual constant types.
		bestConstType = commonConstantType(constVals...)
		for _, expr := range constExprs {
			filterOverloads(func(o overload) bool {
				return o.params().getAt(expr.i).TypeEqual(bestConstType)
			})
		}
		if ok, fn, err := checkReturn(); ok {
			return typeChecked, fn, err
		}
		if homogeneousTyp != nil {
			if !homogeneousTyp.TypeEqual(bestConstType) {
				homogeneousTyp = nil
			}
		} else {
			homogeneousTyp = bestConstType
		}
	}

	// If all other parameters are homogeneous, we favor this type for ValArgs.
	if homogeneousTyp != nil && len(valExprs) > 0 {
		for _, expr := range valExprs {
			filterOverloads(func(o overload) bool {
				return o.params().getAt(expr.i).TypeEqual(homogeneousTyp)
			})
		}
		if ok, fn, err := checkReturn(); ok {
			return typeChecked, fn, err
		}
	}

	if err := defaultTypeCheck(); err != nil {
		return nil, nil, err
	}
	return typeChecked, nil, nil
}

func commonConstantType(vals ...*ConstVal) Datum {
	bestType := 0
	for _, c := range vals {
		avail := c.availableTypes()
		for {
			// This will not work if the available types are not strictly
			// supersets of their previous types in order of preference.
			if _, ok := avail[constTypePreference[bestType]]; ok {
				break
			}
			bestType++
			if bestType == len(constTypePreference)-1 {
				return constTypePreference[bestType]
			}
		}
	}
	return constTypePreference[bestType]
}
