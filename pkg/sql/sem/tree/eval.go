// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

//go:generate go run ./evalgen *.go

var (
	// ErrIntOutOfRange is reported when integer arithmetic overflows.
	ErrIntOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "integer out of range")
	// ErrInt4OutOfRange is reported when casting to INT4 overflows.
	ErrInt4OutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "integer out of range for type int4")
	// ErrInt2OutOfRange is reported when casting to INT2 overflows.
	ErrInt2OutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "integer out of range for type int2")
	// ErrFloatOutOfRange is reported when float arithmetic overflows.
	ErrFloatOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	// ErrDecOutOfRange is reported when decimal arithmetic overflows.
	ErrDecOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "decimal out of range")
	// ErrCharOutOfRange is reported when int cast to ASCII byte overflows.
	ErrCharOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "\"char\" out of range")

	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = pgerror.New(pgcode.DivisionByZero, "division by zero")
	// ErrSqrtOfNegNumber is reported when taking the sqrt of a negative number.
	ErrSqrtOfNegNumber = pgerror.New(pgcode.InvalidArgumentForPowerFunction, "cannot take square root of a negative number")

	// ErrShiftArgOutOfRange is reported when a shift argument is out of range.
	ErrShiftArgOutOfRange = pgerror.New(pgcode.InvalidParameterValue, "shift argument out of range")
)

// NewCannotMixBitArraySizesError creates an error for the case when a bitwise
// aggregate function is called on bit arrays with different sizes.
func NewCannotMixBitArraySizesError(op string) error {
	return pgerror.Newf(pgcode.StringDataLengthMismatch,
		"cannot %s bit strings of different sizes", op)
}

// UnaryOp is a unary operator.
type UnaryOp struct {
	Typ        *types.T
	ReturnType *types.T
	EvalOp     UnaryEvalOp
	Volatility volatility.V

	types   TypeList
	retType ReturnTyper

	// OnTypeCheck is called when the op is type checked.
	OnTypeCheck func()
}

func (op *UnaryOp) params() TypeList {
	return op.types
}

func (op *UnaryOp) returnType() ReturnTyper {
	return op.retType
}

func (*UnaryOp) preference() OverloadPreference {
	return OverloadPreferenceNone
}

func (*UnaryOp) outParamInfo() (RoutineType, []int32, TypeList) {
	return BuiltinRoutine, nil, nil
}

func (op *UnaryOp) defaultExprs() Exprs {
	return nil
}

func unaryOpFixups(
	ops map[UnaryOperatorSymbol]*UnaryOpOverloads,
) map[UnaryOperatorSymbol]*UnaryOpOverloads {
	for _, overload := range ops {
		for _, impl := range overload.overloads {
			impl.types = ParamTypes{{"arg", impl.Typ}}
			impl.retType = FixedReturnType(impl.ReturnType)
		}
	}
	return ops
}

// UnaryOpOverloads is an overloaded set of unary operator implementations.
// It implements overloadSet.
type UnaryOpOverloads struct {
	overloads []*UnaryOp
}

func (u *UnaryOpOverloads) len() int               { return len(u.overloads) }
func (u *UnaryOpOverloads) get(i int) overloadImpl { return u.overloads[i] }

// ForEachUnaryOp iterates the set of overloads.
func (u *UnaryOpOverloads) ForEachUnaryOp(f func(op *UnaryOp) error) error {
	if u == nil {
		return nil
	}
	for _, op := range u.overloads {
		if err := f(op); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// UnaryOps contains the unary operations indexed by operation type.
var UnaryOps = unaryOpFixups(map[UnaryOperatorSymbol]*UnaryOpOverloads{
	UnaryPlus: {overloads: []*UnaryOp{
		{
			Typ:        types.Int,
			ReturnType: types.Int,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
	}},

	UnaryMinus: {overloads: []*UnaryOp{
		{
			Typ:        types.Int,
			ReturnType: types.Int,
			EvalOp:     &UnaryMinusIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &UnaryMinusFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &UnaryMinusDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &UnaryMinusIntervalOp{},
			Volatility: volatility.Immutable,
		},
	}},

	UnaryComplement: {overloads: []*UnaryOp{
		{
			Typ:        types.Int,
			ReturnType: types.Int,
			EvalOp:     &ComplementIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &ComplementVarBitOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.INet,
			ReturnType: types.INet,
			EvalOp:     &ComplementINetOp{},
			Volatility: volatility.Immutable,
		},
	}},

	UnarySqrt: {overloads: []*UnaryOp{
		{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &SqrtFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &SqrtDecimalOp{},
			Volatility: volatility.Immutable,
		},
	}},

	UnaryCbrt: {overloads: []*UnaryOp{
		{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &CbrtFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &CbrtDecimalOp{},
			Volatility: volatility.Immutable,
		},
	}},
})

// BinOp is a binary operator.
type BinOp struct {
	LeftType          *types.T
	RightType         *types.T
	ReturnType        *types.T
	CalledOnNullInput bool
	EvalOp            BinaryEvalOp
	Volatility        volatility.V
	OverloadPreference

	types   TypeList
	retType ReturnTyper

	OnTypeCheck func()
}

func (op *BinOp) params() TypeList {
	return op.types
}

func (op *BinOp) matchParams(l, r *types.T) bool {
	return op.params().MatchAt(l, 0) && op.params().MatchAt(r, 1)
}

func (op *BinOp) returnType() ReturnTyper {
	return op.retType
}

func (op *BinOp) preference() OverloadPreference {
	return op.OverloadPreference
}

func (op *BinOp) outParamInfo() (RoutineType, []int32, TypeList) {
	return BuiltinRoutine, nil, nil
}

func (op *BinOp) defaultExprs() Exprs {
	return nil
}

// AppendToMaybeNullArray appends an element to an array. If the first
// argument is NULL, an array of one element is created.
func AppendToMaybeNullArray(typ *types.T, left Datum, right Datum) (Datum, error) {
	result := NewDArray(typ)
	if left != DNull {
		for _, e := range MustBeDArray(left).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	if err := result.Append(right); err != nil {
		return nil, err
	}
	return result, nil
}

// PrependToMaybeNullArray prepends an element in the front of an arrray.
// If the argument is NULL, an array of one element is created.
func PrependToMaybeNullArray(typ *types.T, left Datum, right Datum) (Datum, error) {
	result := NewDArray(typ)
	if err := result.Append(left); err != nil {
		return nil, err
	}
	if right != DNull {
		for _, e := range MustBeDArray(right).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// TODO(justin): these might be improved by making arrays into an interface and
// then introducing a ConcatenatedArray implementation which just references two
// existing arrays. This would optimize the common case of appending an element
// (or array) to an array from O(n) to O(1).
func initArrayElementConcatenation() {
	for _, t := range types.Scalar {
		typ := t
		addBinOp(treebin.Concat, &BinOp{
			LeftType:          types.MakeArray(typ),
			RightType:         typ,
			ReturnType:        types.MakeArray(typ),
			CalledOnNullInput: true,
			EvalOp:            &AppendToMaybeNullArrayOp{Typ: typ},
			Volatility:        volatility.Immutable,
		}, &BinOp{
			LeftType:          typ,
			RightType:         types.MakeArray(typ),
			ReturnType:        types.MakeArray(typ),
			CalledOnNullInput: true,
			EvalOp:            &PrependToMaybeNullArrayOp{Typ: typ},
			Volatility:        volatility.Immutable,
		})
	}
}

// ConcatArrays concatenates two arrays.
func ConcatArrays(typ *types.T, left Datum, right Datum) (Datum, error) {
	if left == DNull && right == DNull {
		return DNull, nil
	}
	result := NewDArray(typ)
	if left != DNull {
		for _, e := range MustBeDArray(left).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	if right != DNull {
		for _, e := range MustBeDArray(right).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// ArrayContains return true if the haystack contains all needles.
func ArrayContains(
	ctx context.Context, cmpCtx CompareContext, haystack *DArray, needles *DArray,
) (*DBool, error) {
	if !haystack.ParamTyp.Equivalent(needles.ParamTyp) {
		return DBoolFalse, pgerror.New(pgcode.DatatypeMismatch, "cannot compare arrays with different element types")
	}
	for _, needle := range needles.Array {
		// Nulls don't compare to each other in @> syntax.
		if needle == DNull {
			return DBoolFalse, nil
		}
		var found bool
		for _, hay := range haystack.Array {
			if cmp, err := needle.Compare(ctx, cmpCtx, hay); err != nil {
				return DBoolFalse, err
			} else if cmp == 0 {
				found = true
				break
			}
		}
		if !found {
			return DBoolFalse, nil
		}
	}
	return DBoolTrue, nil
}

// ArrayOverlaps return true if there is even one element
// common between the left and right arrays.
func ArrayOverlaps(
	ctx context.Context, cmpCtx CompareContext, array, other *DArray,
) (*DBool, error) {
	if !array.ParamTyp.Equivalent(other.ParamTyp) {
		return nil, pgerror.New(pgcode.DatatypeMismatch, "cannot compare arrays with different element types")
	}
	for _, needle := range array.Array {
		// Nulls don't compare to each other in && syntax.
		if needle == DNull {
			continue
		}
		for _, hay := range other.Array {
			if cmp, err := needle.Compare(ctx, cmpCtx, hay); err != nil {
				return DBoolFalse, err
			} else if cmp == 0 {
				return DBoolTrue, nil
			}
		}
	}
	return DBoolFalse, nil
}

// JSONExistsAny return true if any value in dArray is exist in the json
func JSONExistsAny(json DJSON, dArray *DArray) (*DBool, error) {
	// TODO(justin): this can be optimized.
	for _, k := range dArray.Array {
		if k == DNull {
			continue
		}
		e, err := json.JSON.Exists(string(MustBeDString(k)))
		if err != nil {
			return nil, err
		}
		if e {
			return DBoolTrue, nil
		}
	}
	return DBoolFalse, nil
}

func initArrayToArrayConcatenation() {
	for _, t := range types.Scalar {
		typ := t
		at := types.MakeArray(typ)
		addBinOp(treebin.Concat, &BinOp{
			LeftType:          at,
			RightType:         at,
			ReturnType:        at,
			CalledOnNullInput: true,
			EvalOp:            &ConcatArraysOp{Typ: typ},
			Volatility:        volatility.Immutable,
		})
	}
}

// initNonArrayToNonArrayConcatenation initializes string + nonarrayelement
// and nonarrayelement + string concatenation.
func initNonArrayToNonArrayConcatenation() {
	addConcat := func(leftType, rightType *types.T, volatility volatility.V) {
		addBinOp(treebin.Concat, &BinOp{
			LeftType:          leftType,
			RightType:         rightType,
			ReturnType:        types.String,
			CalledOnNullInput: false,
			EvalOp: &ConcatOp{
				Left:  leftType,
				Right: rightType,
			},
			Volatility: volatility,
		})
	}
	fromTypeToVolatility := make(map[oid.Oid]volatility.V)
	cast.ForEachCast(func(src, tgt oid.Oid, _ cast.Context, _ cast.ContextOrigin, v volatility.V) {
		if tgt == oid.T_text {
			fromTypeToVolatility[src] = v
		}
	})
	// We allow tuple + string concatenation, as well as any scalar types.
	for _, t := range append([]*types.T{types.AnyTuple}, types.Scalar...) {
		// Do not re-add String+String or String+Bytes, as they already exist
		// and have predefined correct behavior.
		if !t.Identical(types.String) && !t.Identical(types.Bytes) {
			addConcat(t, types.String, fromTypeToVolatility[t.Oid()])
			addConcat(types.String, t, fromTypeToVolatility[t.Oid()])
		}
	}
}

func init() {
	initArrayElementConcatenation()
	initArrayToArrayConcatenation()
	initNonArrayToNonArrayConcatenation()
}

func init() {
	for _, overload := range BinOps {
		_ = overload.ForEachBinOp(func(impl *BinOp) error {
			impl.types = ParamTypes{{"left", impl.LeftType}, {"right", impl.RightType}}
			impl.retType = FixedReturnType(impl.ReturnType)
			return nil
		})
	}
}

// BinOpOverloads is an overloaded set of binary operator implementations.
// It implements overloadSet.
type BinOpOverloads struct {
	overloads []*BinOp
}

// ForEachBinOp iterates the BinOps in the set.
func (o *BinOpOverloads) ForEachBinOp(f func(op *BinOp) error) error {
	if o == nil {
		return nil
	}
	for _, ol := range o.overloads {
		if err := f(ol); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (o *BinOpOverloads) len() int               { return len(o.overloads) }
func (o *BinOpOverloads) get(i int) overloadImpl { return o.overloads[i] }

// LookupImpl can be used to look up the overload which match the requested types.
func (o *BinOpOverloads) LookupImpl(left, right *types.T) (*BinOp, bool) {
	if o == nil {
		return nil, false
	}
	for _, ol := range o.overloads {
		if ol.matchParams(left, right) {
			return ol, true
		}
	}
	return nil, false
}

// GetJSONPath is used for the #> and #>> operators.
func GetJSONPath(j json.JSON, ary DArray) (json.JSON, error) {
	// TODO(justin): this is slightly annoying because we have to allocate
	// a new array since the JSON package isn't aware of DArray.
	path := make([]string, len(ary.Array))
	for i, v := range ary.Array {
		if v == DNull {
			return nil, nil
		}
		path[i] = string(MustBeDString(v))
	}
	return json.FetchPath(j, path)
}

func addBinOp(symbol treebin.BinaryOperatorSymbol, ops ...*BinOp) {
	s, ok := BinOps[symbol]
	if !ok {
		s = new(BinOpOverloads)
		BinOps[symbol] = s
	}
	s.overloads = append(s.overloads, ops...)
}

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[treebin.BinaryOperatorSymbol]*BinOpOverloads{
	treebin.Bitand: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &BitAndIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &BitAndVarBitOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.INet,
			EvalOp:     &BitAndINetOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Bitor: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &BitOrIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &BitOrVarBitOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.INet,
			EvalOp:     &BitOrINetOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Bitxor: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &BitXorIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &BitXorVarBitOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Plus: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &PlusIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &PlusFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PlusDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &PlusDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PlusIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			EvalOp:     &PlusDateIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Date,
			ReturnType: types.Date,
			EvalOp:     &PlusIntDateOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusDateTimeOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Time,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusTimeDateOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.TimeTZ,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusDateTimeTZOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TimeTZ,
			RightType:  types.Date,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusTimeTZDateOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			EvalOp:     &PlusTimeIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Time,
			ReturnType: types.Time,
			EvalOp:     &PlusIntervalTimeOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TimeTZ,
			RightType:  types.Interval,
			ReturnType: types.TimeTZ,
			EvalOp:     &PlusTimeTZIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.TimeTZ,
			ReturnType: types.TimeTZ,
			EvalOp:     &PlusIntervalTimeTZOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusTimestampIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Timestamp,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusIntervalTimestampOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusTimestampTZIntervalOp{},
			Volatility: volatility.Stable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.TimestampTZ,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusIntervalTimestampTZOp{},
			Volatility: volatility.Stable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &PlusIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusDateIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusIntervalDateOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.Int,
			ReturnType: types.INet,
			EvalOp:     &PlusINetIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.INet,
			ReturnType: types.INet,
			EvalOp:     &PlusIntINetOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.PGLSN,
			ReturnType: types.PGLSN,
			EvalOp:     &PlusDecimalPGLSNOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.PGLSN,
			RightType:  types.Decimal,
			ReturnType: types.PGLSN,
			EvalOp:     &PlusPGLSNDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.PGVector,
			RightType:  types.PGVector,
			ReturnType: types.PGVector,
			EvalOp:     &PlusPGVectorOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Minus: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &MinusIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &MinusFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MinusDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &MinusDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MinusIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			EvalOp:     &MinusDateIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Date,
			ReturnType: types.Int,
			EvalOp:     &MinusDateOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			EvalOp:     &MinusDateTimeOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Time,
			RightType:  types.Time,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimeOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Timestamp,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TimestampTZ,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampTZOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Timestamp,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampTimestampTZOp{},
			Volatility: volatility.Stable,
		},
		{
			LeftType:   types.TimestampTZ,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampTZTimestampOp{},
			Volatility: volatility.Stable,
		},
		{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			EvalOp:     &MinusTimeIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TimeTZ,
			RightType:  types.Interval,
			ReturnType: types.TimeTZ,
			EvalOp:     &MinusTimeTZIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &MinusTimestampIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			EvalOp:     &MinusTimestampTZIntervalOp{},
			Volatility: volatility.Stable,
		},
		{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &MinusDateIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MinusIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			ReturnType: types.Jsonb,
			EvalOp:     &MinusJsonbStringOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.Jsonb,
			EvalOp:     &MinusJsonbIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.Jsonb,
			EvalOp:     &MinusJsonbStringArrayOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Int,
			EvalOp:     &MinusINetOp{},
			Volatility: volatility.Immutable,
		},
		{
			// Note: postgres ver 10 does NOT have Int - INet. Throws ERROR: 42883.
			LeftType:   types.INet,
			RightType:  types.Int,
			ReturnType: types.INet,
			EvalOp:     &MinusINetIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.PGLSN,
			RightType:  types.Decimal,
			ReturnType: types.PGLSN,
			EvalOp:     &MinusPGLSNDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.PGLSN,
			RightType:  types.PGLSN,
			ReturnType: types.Decimal,
			EvalOp:     &MinusPGLSNOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.PGVector,
			RightType:  types.PGVector,
			ReturnType: types.PGVector,
			EvalOp:     &MinusPGVectorOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Mult: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &MultIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &MultFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MultDecimalOp{},
			Volatility: volatility.Immutable,
		},
		// The following two overloads are needed because DInt/DInt = DDecimal. Due
		// to this operation, normalization may sometimes create a DInt * DDecimal
		// operation.
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &MultDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MultIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MultIntIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			EvalOp:     &MultIntervalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			EvalOp:     &MultIntervalFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MultFloatIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MultDecimalIntervalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Decimal,
			ReturnType: types.Interval,
			EvalOp:     &MultIntervalDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.PGVector,
			RightType:  types.PGVector,
			ReturnType: types.PGVector,
			EvalOp:     &MultPGVectorOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Div: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &DivIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &DivFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &DivDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &DivDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &DivIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			EvalOp:     &DivIntervalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			EvalOp:     &DivIntervalFloatOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.FloorDiv: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &FloorDivIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &FloorDivFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &FloorDivDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &FloorDivDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &FloorDivIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Mod: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &ModIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &ModFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &ModDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &ModDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &ModIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.String,
			RightType:  types.String,
			ReturnType: types.Bool,
			EvalOp:     &ModStringOp{},
			// This operator is only stable because its result depends on the value
			// of the pg_trgm.similarity_threshold session setting.
			Volatility: volatility.Stable,
		},
	}},

	treebin.Concat: {overloads: []*BinOp{
		{
			LeftType:   types.String,
			RightType:  types.String,
			ReturnType: types.String,
			EvalOp:     &ConcatStringOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Bytes,
			RightType:  types.Bytes,
			ReturnType: types.Bytes,
			EvalOp:     &ConcatBytesOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &ConcatVarBitOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			ReturnType: types.Jsonb,
			EvalOp:     &ConcatJsonbOp{},
			Volatility: volatility.Immutable,
		},
	}},

	// TODO(pmattis): Check that the shift is valid.
	treebin.LShift: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &LShiftIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.VarBit,
			RightType:  types.Int,
			ReturnType: types.VarBit,
			EvalOp:     &LShiftVarBitIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Bool,
			EvalOp:     &LShiftINetOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.RShift: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &RShiftIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.VarBit,
			RightType:  types.Int,
			ReturnType: types.VarBit,
			EvalOp:     &RShiftVarBitIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Bool,
			EvalOp:     &RShiftINetOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.Pow: {overloads: []*BinOp{
		{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &PowIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &PowFloatOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PowDecimalOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &PowDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PowIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.JSONFetchVal: {overloads: []*BinOp{
		{
			LeftType:           types.Jsonb,
			RightType:          types.String,
			ReturnType:         types.Jsonb,
			EvalOp:             &JSONFetchValStringOp{},
			OverloadPreference: OverloadPreferencePreferred,
			Volatility:         volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.Jsonb,
			EvalOp:     &JSONFetchValIntOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.JSONFetchValPath: {overloads: []*BinOp{
		{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.Jsonb,
			EvalOp:     &JSONFetchValPathOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.JSONFetchText: {overloads: []*BinOp{
		{
			LeftType:           types.Jsonb,
			RightType:          types.String,
			ReturnType:         types.String,
			OverloadPreference: OverloadPreferencePreferred,
			EvalOp:             &JSONFetchTextStringOp{},
			Volatility:         volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.String,
			EvalOp:     &JSONFetchTextIntOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treebin.JSONFetchTextPath: {overloads: []*BinOp{
		{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.String,
			EvalOp:     &JSONFetchTextPathOp{},
			Volatility: volatility.Immutable,
		},
	}},
	treebin.Distance: {overloads: []*BinOp{
		{
			LeftType:   types.PGVector,
			RightType:  types.PGVector,
			ReturnType: types.Float,
			EvalOp:     &DistanceVectorOp{},
			Volatility: volatility.Immutable,
		},
	}},
	treebin.CosDistance: {overloads: []*BinOp{
		{
			LeftType:   types.PGVector,
			RightType:  types.PGVector,
			ReturnType: types.Float,
			EvalOp:     &CosDistanceVectorOp{},
			Volatility: volatility.Immutable,
		},
	}},
	treebin.NegInnerProduct: {overloads: []*BinOp{
		{
			LeftType:   types.PGVector,
			RightType:  types.PGVector,
			ReturnType: types.Float,
			EvalOp:     &NegInnerProductVectorOp{},
			Volatility: volatility.Immutable,
		},
	}},
}

// CmpOp is a comparison operator.
type CmpOp struct {
	types TypeList

	LeftType  *types.T
	RightType *types.T

	// Datum return type is a union between *DBool and dNull.
	EvalOp BinaryEvalOp

	OnTypeCheck func()

	// If CalledOnNullInput is false, the operator returns NULL
	// whenever either argument is NULL.
	CalledOnNullInput bool

	Volatility volatility.V

	OverloadPreference
}

func (op *CmpOp) params() TypeList {
	return op.types
}

func (op *CmpOp) matchParams(l, r *types.T) bool {
	return op.params().MatchAt(l, 0) && op.params().MatchAt(r, 1)
}

var cmpOpReturnType = FixedReturnType(types.Bool)

func (op *CmpOp) returnType() ReturnTyper {
	return cmpOpReturnType
}

func (op *CmpOp) preference() OverloadPreference {
	return op.OverloadPreference
}

func (op *CmpOp) outParamInfo() (RoutineType, []int32, TypeList) {
	return BuiltinRoutine, nil, nil
}

func (op *CmpOp) defaultExprs() Exprs {
	return nil
}

func cmpOpFixups(
	cmpOps map[treecmp.ComparisonOperatorSymbol]*CmpOpOverloads,
) map[treecmp.ComparisonOperatorSymbol]*CmpOpOverloads {
	findVolatility := func(op treecmp.ComparisonOperatorSymbol, t *types.T) volatility.V {
		for _, o := range cmpOps[op].overloads {
			if o.LeftType.Equivalent(t) && o.RightType.Equivalent(t) {
				return o.Volatility
			}
		}
		panic(errors.AssertionFailedf(
			"could not find cmp op %s(%s,%s)",
			redact.Safe(op.String()), t.SQLStringForError(), t.SQLStringForError(),
		))
	}

	// Array equality comparisons.
	for _, t := range append(types.Scalar, types.AnyEnum, types.AnyCollatedString) {
		appendCmpOp := func(sym treecmp.ComparisonOperatorSymbol, cmpOp *CmpOp) {
			s, ok := cmpOps[sym]
			if !ok {
				s = new(CmpOpOverloads)
				cmpOps[sym] = s
			}
			s.overloads = append(s.overloads, cmpOp)
		}
		appendCmpOp(treecmp.EQ, &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			EvalOp:     &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.EQ)},
			Volatility: findVolatility(treecmp.EQ, t),
		})
		appendCmpOp(treecmp.LE, &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			EvalOp:     &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.LE)},
			Volatility: findVolatility(treecmp.LE, t),
		})
		appendCmpOp(treecmp.LT, &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			EvalOp:     &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.LT)},
			Volatility: findVolatility(treecmp.LT, t),
		})
		appendCmpOp(treecmp.IsNotDistinctFrom, &CmpOp{
			LeftType:          types.MakeArray(t),
			RightType:         types.MakeArray(t),
			EvalOp:            &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom)},
			CalledOnNullInput: true,
			Volatility:        findVolatility(treecmp.IsNotDistinctFrom, t),
		})
	}

	for _, overloads := range cmpOps {
		_ = overloads.ForEachCmpOp(func(op *CmpOp) error {
			op.types = ParamTypes{{"left", op.LeftType}, {"right", op.RightType}}
			return nil
		})
	}

	return cmpOps
}

// CmpOpOverloads is an overloaded set of comparison operator implementations.
type CmpOpOverloads struct {
	overloads []*CmpOp
}

func (o *CmpOpOverloads) len() int               { return len(o.overloads) }
func (o *CmpOpOverloads) get(i int) overloadImpl { return o.overloads[i] }

// LookupImpl is used to look up the overload for a pair of types.
func (o *CmpOpOverloads) LookupImpl(left, right *types.T) (*CmpOp, bool) {
	if o == nil {
		return nil, false
	}
	for _, fn := range o.overloads {
		if fn.matchParams(left, right) {
			return fn, true
		}
	}
	return nil, false
}

// ForEachCmpOp iterates the ops in the set.
func (o *CmpOpOverloads) ForEachCmpOp(f func(op *CmpOp) error) error {
	if o == nil {
		return nil
	}
	for _, ol := range o.overloads {
		if err := f(ol); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func makeCmpOpOverload(
	op treecmp.ComparisonOperatorSymbol, a, b *types.T, calledOnNullInput bool, v volatility.V,
) *CmpOp {
	return &CmpOp{
		LeftType:          a,
		RightType:         b,
		EvalOp:            &CompareScalarOp{ComparisonOperator: treecmp.MakeComparisonOperator(op)},
		CalledOnNullInput: calledOnNullInput,
		Volatility:        v,
	}
}

func makeEqFn(a, b *types.T, v volatility.V) *CmpOp {
	return makeCmpOpOverload(treecmp.EQ, a, b, false, v)
}
func makeLtFn(a, b *types.T, v volatility.V) *CmpOp {
	return makeCmpOpOverload(treecmp.LT, a, b, false, v)
}
func makeLeFn(a, b *types.T, v volatility.V) *CmpOp {
	return makeCmpOpOverload(treecmp.LE, a, b, false, v)
}
func makeIsFn(a, b *types.T, v volatility.V) *CmpOp {
	return makeCmpOpOverload(treecmp.IsNotDistinctFrom, a, b, true, v)
}
func unpreferred(cmp *CmpOp) *CmpOp {
	cmp.OverloadPreference = OverloadPreferenceUnpreferred
	return cmp
}

// CmpOps contains the comparison operations indexed by operation type.
var CmpOps = cmpOpFixups(map[treecmp.ComparisonOperatorSymbol]*CmpOpOverloads{
	treecmp.EQ: {overloads: []*CmpOp{
		// Single-type comparisons.
		makeEqFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeEqFn(types.Bool, types.Bool, volatility.Leakproof),
		makeEqFn(types.Bytes, types.Bytes, volatility.Leakproof),
		makeEqFn(types.Date, types.Date, volatility.Leakproof),
		makeEqFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leakproof under the assumption that these cases will be
		// detected during type checking.
		makeEqFn(types.AnyCollatedString, types.AnyCollatedString, volatility.Leakproof),
		makeEqFn(types.Float, types.Float, volatility.Leakproof),
		makeEqFn(types.Box2D, types.Box2D, volatility.Leakproof),
		makeEqFn(types.Geography, types.Geography, volatility.Leakproof),
		makeEqFn(types.Geometry, types.Geometry, volatility.Leakproof),
		makeEqFn(types.INet, types.INet, volatility.Leakproof),
		makeEqFn(types.Int, types.Int, volatility.Leakproof),
		makeEqFn(types.Interval, types.Interval, volatility.Leakproof),
		makeEqFn(types.Jsonb, types.Jsonb, volatility.Immutable),
		makeEqFn(types.Oid, types.Oid, volatility.Leakproof),
		makeEqFn(types.PGLSN, types.PGLSN, volatility.Leakproof),
		makeEqFn(types.PGVector, types.PGVector, volatility.Leakproof),
		makeEqFn(types.RefCursor, types.RefCursor, volatility.Leakproof),
		makeEqFn(types.String, types.String, volatility.Leakproof),
		// NOTE: Using unpreferred here is a hack that avoids some "ambiguous
		// comparison operator" errors. It is necessary because we do not follow
		// all of Postgres's type conversion rules. See #75101.
		unpreferred(makeEqFn(types.BPChar, types.BPChar, volatility.Leakproof)),
		makeEqFn(types.Time, types.Time, volatility.Leakproof),
		makeEqFn(types.TimeTZ, types.TimeTZ, volatility.Leakproof),
		makeEqFn(types.Timestamp, types.Timestamp, volatility.Leakproof),
		makeEqFn(types.TimestampTZ, types.TimestampTZ, volatility.Leakproof),
		makeEqFn(types.TSQuery, types.TSQuery, volatility.Immutable),
		makeEqFn(types.TSVector, types.TSVector, volatility.Immutable),
		makeEqFn(types.Uuid, types.Uuid, volatility.Leakproof),
		makeEqFn(types.VarBit, types.VarBit, volatility.Leakproof),

		// Mixed-type comparisons.
		makeEqFn(types.Date, types.Timestamp, volatility.Immutable),
		makeEqFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeEqFn(types.Decimal, types.Float, volatility.Leakproof),
		makeEqFn(types.Decimal, types.Int, volatility.Leakproof),
		makeEqFn(types.Float, types.Decimal, volatility.Leakproof),
		makeEqFn(types.Float, types.Int, volatility.Leakproof),
		makeEqFn(types.Int, types.Decimal, volatility.Leakproof),
		makeEqFn(types.Int, types.Float, volatility.Leakproof),
		makeEqFn(types.Int, types.Oid, volatility.Leakproof),
		makeEqFn(types.Oid, types.Int, volatility.Leakproof),
		makeEqFn(types.Timestamp, types.Date, volatility.Immutable),
		makeEqFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeEqFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeEqFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeEqFn(types.Time, types.TimeTZ, volatility.Stable),
		makeEqFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			EvalOp: &CompareTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.EQ),
			},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.LT: {overloads: []*CmpOp{
		// Single-type comparisons.
		makeLtFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeLtFn(types.Bool, types.Bool, volatility.Leakproof),
		makeLtFn(types.Bytes, types.Bytes, volatility.Leakproof),
		makeLtFn(types.Date, types.Date, volatility.Leakproof),
		makeLtFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leakproof under the assumption that these cases will be
		// detected during type checking.
		makeLtFn(types.AnyCollatedString, types.AnyCollatedString, volatility.Leakproof),
		makeLtFn(types.Float, types.Float, volatility.Leakproof),
		makeLtFn(types.Box2D, types.Box2D, volatility.Leakproof),
		makeLtFn(types.Geography, types.Geography, volatility.Leakproof),
		makeLtFn(types.Geometry, types.Geometry, volatility.Leakproof),
		makeLtFn(types.INet, types.INet, volatility.Leakproof),
		makeLtFn(types.Int, types.Int, volatility.Leakproof),
		makeLtFn(types.Interval, types.Interval, volatility.Leakproof),
		makeLtFn(types.Oid, types.Oid, volatility.Leakproof),
		makeLtFn(types.PGLSN, types.PGLSN, volatility.Leakproof),
		makeLtFn(types.PGVector, types.PGVector, volatility.Leakproof),
		makeLtFn(types.RefCursor, types.RefCursor, volatility.Leakproof),
		makeLtFn(types.String, types.String, volatility.Leakproof),
		// NOTE: Using unpreferred here is a hack that avoids some "ambiguous
		// comparison operator" errors. It is necessary because we do not follow
		// all of Postgres's type conversion rules. See #75101.
		unpreferred(makeLtFn(types.BPChar, types.BPChar, volatility.Leakproof)),
		makeLtFn(types.Time, types.Time, volatility.Leakproof),
		makeLtFn(types.TimeTZ, types.TimeTZ, volatility.Leakproof),
		makeLtFn(types.Timestamp, types.Timestamp, volatility.Leakproof),
		makeLtFn(types.TimestampTZ, types.TimestampTZ, volatility.Leakproof),
		makeLtFn(types.Uuid, types.Uuid, volatility.Leakproof),
		makeLtFn(types.VarBit, types.VarBit, volatility.Leakproof),
		makeLtFn(types.Jsonb, types.Jsonb, volatility.Immutable),

		// Mixed-type comparisons.
		makeLtFn(types.Date, types.Timestamp, volatility.Immutable),
		makeLtFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeLtFn(types.Decimal, types.Float, volatility.Leakproof),
		makeLtFn(types.Decimal, types.Int, volatility.Leakproof),
		makeLtFn(types.Float, types.Decimal, volatility.Leakproof),
		makeLtFn(types.Float, types.Int, volatility.Leakproof),
		makeLtFn(types.Int, types.Decimal, volatility.Leakproof),
		makeLtFn(types.Int, types.Float, volatility.Leakproof),
		makeLtFn(types.Int, types.Oid, volatility.Leakproof),
		makeLtFn(types.Oid, types.Int, volatility.Leakproof),
		makeLtFn(types.Timestamp, types.Date, volatility.Immutable),
		makeLtFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeLtFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeLtFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeLtFn(types.Time, types.TimeTZ, volatility.Stable),
		makeLtFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			EvalOp: &CompareTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.LT),
			},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.LE: {overloads: []*CmpOp{
		// Single-type comparisons.
		makeLeFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeLeFn(types.Bool, types.Bool, volatility.Leakproof),
		makeLeFn(types.Bytes, types.Bytes, volatility.Leakproof),
		makeLeFn(types.Date, types.Date, volatility.Leakproof),
		makeLeFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leakproof under the assumption that these cases will be
		// detected during type checking.
		makeLeFn(types.AnyCollatedString, types.AnyCollatedString, volatility.Leakproof),
		makeLeFn(types.Float, types.Float, volatility.Leakproof),
		makeLeFn(types.Box2D, types.Box2D, volatility.Leakproof),
		makeLeFn(types.Geography, types.Geography, volatility.Leakproof),
		makeLeFn(types.Geometry, types.Geometry, volatility.Leakproof),
		makeLeFn(types.INet, types.INet, volatility.Leakproof),
		makeLeFn(types.Int, types.Int, volatility.Leakproof),
		makeLeFn(types.Interval, types.Interval, volatility.Leakproof),
		makeLeFn(types.Oid, types.Oid, volatility.Leakproof),
		makeLeFn(types.PGLSN, types.PGLSN, volatility.Leakproof),
		makeLeFn(types.PGVector, types.PGVector, volatility.Leakproof),
		makeLeFn(types.RefCursor, types.RefCursor, volatility.Leakproof),
		makeLeFn(types.String, types.String, volatility.Leakproof),
		// NOTE: Using unpreferred here is a hack that avoids some "ambiguous
		// comparison operator" errors. It is necessary because we do not follow
		// all of Postgres's type conversion rules. See #75101.
		unpreferred(makeLeFn(types.BPChar, types.BPChar, volatility.Leakproof)),
		makeLeFn(types.Time, types.Time, volatility.Leakproof),
		makeLeFn(types.TimeTZ, types.TimeTZ, volatility.Leakproof),
		makeLeFn(types.Timestamp, types.Timestamp, volatility.Leakproof),
		makeLeFn(types.TimestampTZ, types.TimestampTZ, volatility.Leakproof),
		makeLeFn(types.Uuid, types.Uuid, volatility.Leakproof),
		makeLeFn(types.VarBit, types.VarBit, volatility.Leakproof),
		makeLeFn(types.Jsonb, types.Jsonb, volatility.Immutable),

		// Mixed-type comparisons.
		makeLeFn(types.Date, types.Timestamp, volatility.Immutable),
		makeLeFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeLeFn(types.Decimal, types.Float, volatility.Leakproof),
		makeLeFn(types.Decimal, types.Int, volatility.Leakproof),
		makeLeFn(types.Float, types.Decimal, volatility.Leakproof),
		makeLeFn(types.Float, types.Int, volatility.Leakproof),
		makeLeFn(types.Int, types.Decimal, volatility.Leakproof),
		makeLeFn(types.Int, types.Float, volatility.Leakproof),
		makeLeFn(types.Int, types.Oid, volatility.Leakproof),
		makeLeFn(types.Oid, types.Int, volatility.Leakproof),
		makeLeFn(types.Timestamp, types.Date, volatility.Immutable),
		makeLeFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeLeFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeLeFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeLeFn(types.Time, types.TimeTZ, volatility.Stable),
		makeLeFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			EvalOp: &CompareTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.LE),
			},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.IsNotDistinctFrom: {overloads: []*CmpOp{
		{
			LeftType:  types.Unknown,
			RightType: types.Unknown,
			EvalOp: &CompareScalarOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			},
			CalledOnNullInput: true,
			// Avoids ambiguous comparison error for NULL IS NOT DISTINCT FROM NULL.
			OverloadPreference: OverloadPreferencePreferred,
			Volatility:         volatility.Leakproof,
		},
		{
			LeftType:  types.AnyArray,
			RightType: types.Unknown,
			EvalOp: &CompareScalarOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			},
			CalledOnNullInput: true,
			Volatility:        volatility.Leakproof,
		},
		// Single-type comparisons.
		makeIsFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeIsFn(types.Bool, types.Bool, volatility.Leakproof),
		makeIsFn(types.Bytes, types.Bytes, volatility.Leakproof),
		makeIsFn(types.Date, types.Date, volatility.Leakproof),
		makeIsFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leakproof under the assumption that these cases will be
		// detected during type checking.
		makeIsFn(types.AnyCollatedString, types.AnyCollatedString, volatility.Leakproof),
		makeIsFn(types.Float, types.Float, volatility.Leakproof),
		makeIsFn(types.Box2D, types.Box2D, volatility.Leakproof),
		makeIsFn(types.Geography, types.Geography, volatility.Leakproof),
		makeIsFn(types.Geometry, types.Geometry, volatility.Leakproof),
		makeIsFn(types.INet, types.INet, volatility.Leakproof),
		makeIsFn(types.Int, types.Int, volatility.Leakproof),
		makeIsFn(types.Interval, types.Interval, volatility.Leakproof),
		makeIsFn(types.Jsonb, types.Jsonb, volatility.Immutable),
		makeIsFn(types.Jsonpath, types.Jsonpath, volatility.Leakproof),
		makeIsFn(types.Oid, types.Oid, volatility.Leakproof),
		makeIsFn(types.PGLSN, types.PGLSN, volatility.Leakproof),
		makeIsFn(types.PGVector, types.PGVector, volatility.Leakproof),
		makeIsFn(types.RefCursor, types.RefCursor, volatility.Leakproof),
		makeIsFn(types.String, types.String, volatility.Leakproof),
		// NOTE: Using unpreferred here is a hack that avoids some "ambiguous
		// comparison operator" errors. It is necessary because we do not follow
		// all of Postgres's type conversion rules. See #75101.
		unpreferred(makeIsFn(types.BPChar, types.BPChar, volatility.Leakproof)),
		makeIsFn(types.Time, types.Time, volatility.Leakproof),
		makeIsFn(types.TimeTZ, types.TimeTZ, volatility.Leakproof),
		makeIsFn(types.Timestamp, types.Timestamp, volatility.Leakproof),
		makeIsFn(types.TimestampTZ, types.TimestampTZ, volatility.Leakproof),
		makeIsFn(types.TSQuery, types.TSQuery, volatility.Immutable),
		makeIsFn(types.TSVector, types.TSVector, volatility.Immutable),
		makeIsFn(types.Uuid, types.Uuid, volatility.Leakproof),
		makeIsFn(types.VarBit, types.VarBit, volatility.Leakproof),

		// Mixed-type comparisons.
		makeIsFn(types.Date, types.Timestamp, volatility.Immutable),
		makeIsFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeIsFn(types.Decimal, types.Float, volatility.Leakproof),
		makeIsFn(types.Decimal, types.Int, volatility.Leakproof),
		makeIsFn(types.Float, types.Decimal, volatility.Leakproof),
		makeIsFn(types.Float, types.Int, volatility.Leakproof),
		makeIsFn(types.Int, types.Decimal, volatility.Leakproof),
		makeIsFn(types.Int, types.Float, volatility.Leakproof),
		makeIsFn(types.Int, types.Oid, volatility.Leakproof),
		makeIsFn(types.Oid, types.Int, volatility.Leakproof),
		makeIsFn(types.Timestamp, types.Date, volatility.Immutable),
		makeIsFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeIsFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeIsFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeIsFn(types.Time, types.TimeTZ, volatility.Stable),
		makeIsFn(types.TimeTZ, types.Time, volatility.Stable),

		// Void is unique in that it is not equivalent with itself, so implicit
		// equivalence with Unknown in function ArgTypes.MatchAt due to the check
		// `(typ.Family() == types.UnknownFamily || a[i].Typ.Equivalent(typ))` does
		// not occur. Therefore, to allow the comparison
		// `''::VOID IS DISTINCT FROM NULL`, an explicit equivalence with Unknown is
		// added:
		makeIsFn(types.Void, types.Unknown, volatility.Leakproof),
		makeIsFn(types.Unknown, types.Void, volatility.Leakproof),

		// Tuple comparison.
		{
			LeftType:          types.AnyTuple,
			RightType:         types.AnyTuple,
			CalledOnNullInput: true,
			EvalOp: &CompareAnyTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.In: {overloads: []*CmpOp{
		makeEvalTupleIn(types.AnyEnum, volatility.Leakproof),
		makeEvalTupleIn(types.Bool, volatility.Leakproof),
		makeEvalTupleIn(types.Bytes, volatility.Leakproof),
		makeEvalTupleIn(types.Date, volatility.Leakproof),
		makeEvalTupleIn(types.Decimal, volatility.Leakproof),
		makeEvalTupleIn(types.AnyCollatedString, volatility.Leakproof),
		makeEvalTupleIn(types.AnyTuple, volatility.Leakproof),
		makeEvalTupleIn(types.Float, volatility.Leakproof),
		makeEvalTupleIn(types.Box2D, volatility.Leakproof),
		makeEvalTupleIn(types.Geography, volatility.Leakproof),
		makeEvalTupleIn(types.Geometry, volatility.Leakproof),
		makeEvalTupleIn(types.INet, volatility.Leakproof),
		makeEvalTupleIn(types.Int, volatility.Leakproof),
		makeEvalTupleIn(types.Interval, volatility.Leakproof),
		makeEvalTupleIn(types.Jsonb, volatility.Leakproof),
		makeEvalTupleIn(types.Oid, volatility.Leakproof),
		makeEvalTupleIn(types.PGLSN, volatility.Leakproof),
		makeEvalTupleIn(types.PGVector, volatility.Leakproof),
		makeEvalTupleIn(types.RefCursor, volatility.Leakproof),
		makeEvalTupleIn(types.String, volatility.Leakproof),
		// NOTE: Using unpreferred here is a hack that avoids some "ambiguous
		// comparison operator" errors. It is necessary because we do not follow
		// all of Postgres's type conversion rules. See #75101.
		unpreferred(makeEvalTupleIn(types.BPChar, volatility.Leakproof)),
		makeEvalTupleIn(types.Time, volatility.Leakproof),
		makeEvalTupleIn(types.TimeTZ, volatility.Leakproof),
		makeEvalTupleIn(types.Timestamp, volatility.Leakproof),
		makeEvalTupleIn(types.TimestampTZ, volatility.Leakproof),
		makeEvalTupleIn(types.Uuid, volatility.Leakproof),
		makeEvalTupleIn(types.VarBit, volatility.Leakproof),
	}},

	treecmp.Like: {overloads: []*CmpOp{
		{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchLikeOp{CaseInsensitive: false},
			Volatility: volatility.Leakproof,
		},
	}},

	treecmp.ILike: {overloads: []*CmpOp{
		{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchLikeOp{CaseInsensitive: true},
			Volatility: volatility.Leakproof,
		},
	}},

	treecmp.SimilarTo: {overloads: []*CmpOp{
		{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &SimilarToOp{Escape: '\\'},
			Volatility: volatility.Leakproof,
		},
	}},

	treecmp.RegMatch: {overloads: append([]*CmpOp{
		{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchRegexpOp{},
			Volatility: volatility.Immutable,
		},
	},
		makeBox2DComparisonOperators(
			func(lhs, rhs *geo.CartesianBoundingBox) bool {
				return lhs.Covers(rhs)
			},
		)...,
	)},

	treecmp.RegIMatch: {overloads: []*CmpOp{
		{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchRegexpOp{CaseInsensitive: true},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.JSONExists: {overloads: []*CmpOp{
		{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			EvalOp:     &JSONExistsOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.JSONSomeExists: {overloads: []*CmpOp{
		{
			LeftType:   types.Jsonb,
			RightType:  types.StringArray,
			EvalOp:     &JSONSomeExistsOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.JSONAllExists: {overloads: []*CmpOp{
		{
			LeftType:   types.Jsonb,
			RightType:  types.StringArray,
			EvalOp:     &JSONAllExistsOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.Contains: {overloads: []*CmpOp{
		{
			LeftType:   types.AnyArray,
			RightType:  types.AnyArray,
			EvalOp:     &ContainsArrayOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			EvalOp:     &ContainsJsonbOp{},
			Volatility: volatility.Immutable,
		},
	}},

	treecmp.ContainedBy: {overloads: []*CmpOp{
		{
			LeftType:   types.AnyArray,
			RightType:  types.AnyArray,
			EvalOp:     &ContainedByArrayOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			EvalOp:     &ContainedByJsonbOp{},
			Volatility: volatility.Immutable,
		},
	}},
	treecmp.Overlaps: {overloads: append([]*CmpOp{
		{
			LeftType:   types.AnyArray,
			RightType:  types.AnyArray,
			EvalOp:     &OverlapsArrayOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.INet,
			RightType:  types.INet,
			EvalOp:     &OverlapsINetOp{},
			Volatility: volatility.Immutable,
		},
	}, makeBox2DComparisonOperators(
		func(lhs, rhs *geo.CartesianBoundingBox) bool {
			return lhs.Intersects(rhs)
		},
	)...),
	},
	treecmp.TSMatches: {overloads: []*CmpOp{
		{
			LeftType:   types.TSQuery,
			RightType:  types.TSVector,
			EvalOp:     &TSMatchesQueryVectorOp{},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:   types.TSVector,
			RightType:  types.TSQuery,
			EvalOp:     &TSMatchesVectorQueryOp{},
			Volatility: volatility.Immutable,
		},
	}},
})

func makeBox2DComparisonOperators(op func(lhs, rhs *geo.CartesianBoundingBox) bool) []*CmpOp {
	return []*CmpOp{
		{
			LeftType:  types.Box2D,
			RightType: types.Box2D,
			EvalOp: &CompareBox2DOp{Op: func(left, right Datum) bool {
				return op(
					&MustBeDBox2D(left).CartesianBoundingBox,
					&MustBeDBox2D(right).CartesianBoundingBox,
				)
			}},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:  types.Box2D,
			RightType: types.Geometry,
			EvalOp: &CompareBox2DOp{Op: func(left, right Datum) bool {
				return op(
					&MustBeDBox2D(left).CartesianBoundingBox,
					MustBeDGeometry(right).CartesianBoundingBox(),
				)
			}},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:  types.Geometry,
			RightType: types.Box2D,
			EvalOp: &CompareBox2DOp{Op: func(left, right Datum) bool {
				return op(
					MustBeDGeometry(left).CartesianBoundingBox(),
					&MustBeDBox2D(right).CartesianBoundingBox,
				)
			}},
			Volatility: volatility.Immutable,
		},
		{
			LeftType:  types.Geometry,
			RightType: types.Geometry,
			EvalOp: &CompareBox2DOp{Op: func(left, right Datum) bool {
				return op(
					MustBeDGeometry(left).CartesianBoundingBox(),
					MustBeDGeometry(right).CartesianBoundingBox(),
				)
			}},
			Volatility: volatility.Immutable,
		},
	}
}

// This map contains the inverses for operators in the CmpOps map that have
// inverses.
var cmpOpsInverse map[treecmp.ComparisonOperatorSymbol]treecmp.ComparisonOperatorSymbol

func init() {
	cmpOpsInverse = make(map[treecmp.ComparisonOperatorSymbol]treecmp.ComparisonOperatorSymbol)
	for cmpOp := treecmp.ComparisonOperatorSymbol(0); cmpOp < treecmp.NumComparisonOperatorSymbols; cmpOp++ {
		newOp, _, _, _, _ := FoldComparisonExpr(treecmp.MakeComparisonOperator(cmpOp), DNull, DNull)
		if newOp.Symbol != cmpOp {
			cmpOpsInverse[newOp.Symbol] = cmpOp
			cmpOpsInverse[cmpOp] = newOp.Symbol
		}
	}
}

// CmpOpInverse returns the inverse of the comparison operator if it exists. The
// second return value is true if it exists, and false otherwise.
func CmpOpInverse(i treecmp.ComparisonOperatorSymbol) (treecmp.ComparisonOperatorSymbol, bool) {
	inverse, ok := cmpOpsInverse[i]
	return inverse, ok
}

func makeEvalTupleIn(typ *types.T, v volatility.V) *CmpOp {
	return &CmpOp{
		LeftType:          typ,
		RightType:         types.AnyTuple,
		EvalOp:            &InTupleOp{},
		CalledOnNullInput: true,
		Volatility:        v,
	}
}

// MultipleResultsError is returned by QueryRow when more than one result is
// encountered.
type MultipleResultsError struct {
	SQL string // the query that produced this error
}

func (e *MultipleResultsError) Error() string {
	return fmt.Sprintf("%s: unexpected multiple results", e.SQL)
}

// MaybeWrapError updates non-nil error depending on the FuncExpr to provide
// more context.
func (expr *FuncExpr) MaybeWrapError(err error) error {
	// If we are facing an explicit error, propagate it unchanged.
	fName := expr.Func.String()
	if fName == `crdb_internal.force_error` || fName == `crdb_internal.plpgsql_raise` ||
		fName == `crdb_internal.plpgsql_close` || fName == `crdb_internal.plpgsql_fetch` {
		return err
	}
	// Otherwise, wrap it with context.
	// TODO(yuzefovich): consider removing this context in order to match
	// postgres error messages.
	newErr := errors.Wrapf(err, "%s()", errors.Safe(fName))
	// Count function errors as it flows out of the system. We need to handle
	// them this way because if we are facing a retry error, in particular those
	// generated by crdb_internal.force_retry(), Wrap() will propagate it as a
	// non-pgerror error (so that the executor can see it with the right type).
	newErr = errors.WithTelemetry(newErr, fName+"()")
	return newErr
}

// EqualComparisonFunctionExists looks up an overload of the "=" operator
// for a given pair of input operand types.
func EqualComparisonFunctionExists(leftType, rightType *types.T) bool {
	_, found := CmpOps[treecmp.EQ].LookupImpl(leftType, rightType)
	return found
}
