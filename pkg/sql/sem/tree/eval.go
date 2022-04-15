// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
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

	// counter, if non-nil, should be incremented every time the
	// operator is type checked.
	counter telemetry.Counter
}

func (op *UnaryOp) params() TypeList {
	return op.types
}

func (op *UnaryOp) returnType() ReturnTyper {
	return op.retType
}

func (*UnaryOp) preferred() bool {
	return false
}

func unaryOpFixups(
	ops map[UnaryOperatorSymbol]unaryOpOverload,
) map[UnaryOperatorSymbol]unaryOpOverload {
	for op, overload := range ops {
		for i, impl := range overload {
			casted := impl.(*UnaryOp)
			casted.types = ArgTypes{{"arg", casted.Typ}}
			casted.retType = FixedReturnType(casted.ReturnType)
			ops[op][i] = casted
		}
	}
	return ops
}

// unaryOpOverload is an overloaded set of unary operator implementations.
type unaryOpOverload []overloadImpl

// UnaryOps contains the unary operations indexed by operation type.
var UnaryOps = unaryOpFixups(map[UnaryOperatorSymbol]unaryOpOverload{
	UnaryPlus: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &UnaryNoop{},
			Volatility: volatility.Immutable,
		},
	},

	UnaryMinus: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			EvalOp:     &UnaryMinusIntOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &UnaryMinusFloatOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &UnaryMinusDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &UnaryMinusIntervalOp{},
			Volatility: volatility.Immutable,
		},
	},

	UnaryComplement: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			EvalOp:     &ComplementIntOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &ComplementVarBitOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.INet,
			ReturnType: types.INet,
			EvalOp:     &ComplementINetOp{},
			Volatility: volatility.Immutable,
		},
	},

	UnarySqrt: {
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &SqrtFloatOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &SqrtDecimalOp{},
			Volatility: volatility.Immutable,
		},
	},

	UnaryCbrt: {
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			EvalOp:     &CbrtFloatOp{},
			Volatility: volatility.Immutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &CbrtDecimalOp{},
			Volatility: volatility.Immutable,
		},
	},
})

// BinOp is a binary operator.
type BinOp struct {
	LeftType          *types.T
	RightType         *types.T
	ReturnType        *types.T
	NullableArgs      bool
	EvalOp            BinaryEvalOp
	Volatility        volatility.V
	PreferredOverload bool

	types   TypeList
	retType ReturnTyper

	// counter, if non-nil, should be incremented every time the
	// operator is type checked.
	counter telemetry.Counter
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

func (op *BinOp) preferred() bool {
	return op.PreferredOverload
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
		BinOps[treebin.Concat] = append(BinOps[treebin.Concat], &BinOp{
			LeftType:     types.MakeArray(typ),
			RightType:    typ,
			ReturnType:   types.MakeArray(typ),
			NullableArgs: true,
			EvalOp:       &AppendToMaybeNullArrayOp{Typ: typ},
			Volatility:   volatility.Immutable,
		})

		BinOps[treebin.Concat] = append(BinOps[treebin.Concat], &BinOp{
			LeftType:     typ,
			RightType:    types.MakeArray(typ),
			ReturnType:   types.MakeArray(typ),
			NullableArgs: true,
			EvalOp:       &PrependToMaybeNullArrayOp{Typ: typ},
			Volatility:   volatility.Immutable,
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
func ArrayContains(ctx CompareContext, haystack *DArray, needles *DArray) (*DBool, error) {
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
			if needle.Compare(ctx, hay) == 0 {
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
func ArrayOverlaps(ctx CompareContext, array, other *DArray) (*DBool, error) {
	if !array.ParamTyp.Equivalent(other.ParamTyp) {
		return nil, pgerror.New(pgcode.DatatypeMismatch, "cannot compare arrays with different element types")
	}
	for _, needle := range array.Array {
		// Nulls don't compare to each other in && syntax.
		if needle == DNull {
			continue
		}
		for _, hay := range other.Array {
			if needle.Compare(ctx, hay) == 0 {
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
		BinOps[treebin.Concat] = append(BinOps[treebin.Concat], &BinOp{
			LeftType:     at,
			RightType:    at,
			ReturnType:   at,
			NullableArgs: true,
			EvalOp:       &ConcatArraysOp{Typ: typ},
			Volatility:   volatility.Immutable,
		})
	}
}

// initNonArrayToNonArrayConcatenation initializes string + nonarrayelement
// and nonarrayelement + string concatenation.
func initNonArrayToNonArrayConcatenation() {
	addConcat := func(leftType, rightType *types.T, volatility volatility.V) {
		BinOps[treebin.Concat] = append(BinOps[treebin.Concat], &BinOp{
			LeftType:     leftType,
			RightType:    rightType,
			ReturnType:   types.String,
			NullableArgs: false,
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
		if t != types.String && t != types.Bytes {
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
	for op, overload := range BinOps {
		for i, impl := range overload {
			casted := impl.(*BinOp)
			casted.types = ArgTypes{{"left", casted.LeftType}, {"right", casted.RightType}}
			casted.retType = FixedReturnType(casted.ReturnType)
			BinOps[op][i] = casted
		}
	}
}

// binOpOverload is an overloaded set of binary operator implementations.
type binOpOverload []overloadImpl

func (o binOpOverload) LookupImpl(left, right *types.T) (*BinOp, bool) {
	for _, fn := range o {
		casted := fn.(*BinOp)
		if casted.matchParams(left, right) {
			return casted, true
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

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[treebin.BinaryOperatorSymbol]binOpOverload{
	treebin.Bitand: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &BitAndIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &BitAndVarBitOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.INet,
			EvalOp:     &BitAndINetOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Bitor: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &BitOrIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &BitOrVarBitOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.INet,
			EvalOp:     &BitOrINetOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Bitxor: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &BitXorIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &BitXorVarBitOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Plus: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &PlusIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &PlusFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PlusDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &PlusDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PlusIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			EvalOp:     &PlusDateIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Date,
			ReturnType: types.Date,
			EvalOp:     &PlusIntDateOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusDateTimeOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusTimeDateOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.TimeTZ,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusDateTimeTZOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.TimeTZ,
			RightType:  types.Date,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusTimeTZDateOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			EvalOp:     &PlusTimeIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Time,
			ReturnType: types.Time,
			EvalOp:     &PlusIntervalTimeOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.TimeTZ,
			RightType:  types.Interval,
			ReturnType: types.TimeTZ,
			EvalOp:     &PlusTimeTZIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.TimeTZ,
			ReturnType: types.TimeTZ,
			EvalOp:     &PlusIntervalTimeTZOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusTimestampIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Timestamp,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusIntervalTimestampOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusTimestampTZIntervalOp{},
			Volatility: volatility.Stable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.TimestampTZ,
			ReturnType: types.TimestampTZ,
			EvalOp:     &PlusIntervalTimestampTZOp{},
			Volatility: volatility.Stable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &PlusIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusDateIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			EvalOp:     &PlusIntervalDateOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.Int,
			ReturnType: types.INet,
			EvalOp:     &PlusINetIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.INet,
			ReturnType: types.INet,
			EvalOp:     &PlusIntINetOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Minus: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &MinusIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &MinusFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MinusDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &MinusDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MinusIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			EvalOp:     &MinusDateIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Date,
			ReturnType: types.Int,
			EvalOp:     &MinusDateOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			EvalOp:     &MinusDateTimeOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Time,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimeOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampTZOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampTimestampTZOp{},
			Volatility: volatility.Stable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			EvalOp:     &MinusTimestampTZTimestampOp{},
			Volatility: volatility.Stable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			EvalOp:     &MinusTimeIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.TimeTZ,
			RightType:  types.Interval,
			ReturnType: types.TimeTZ,
			EvalOp:     &MinusTimeTZIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &MinusTimestampIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			EvalOp:     &MinusTimestampTZIntervalOp{},
			Volatility: volatility.Stable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			EvalOp:     &MinusDateIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MinusIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			ReturnType: types.Jsonb,
			EvalOp:     &MinusJsonbStringOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.Jsonb,
			EvalOp:     &MinusJsonbIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.Jsonb,
			EvalOp:     &MinusJsonbStringArrayOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Int,
			EvalOp:     &MinusINetOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			// Note: postgres ver 10 does NOT have Int - INet. Throws ERROR: 42883.
			LeftType:   types.INet,
			RightType:  types.Int,
			ReturnType: types.INet,
			EvalOp:     &MinusINetIntOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Mult: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &MultIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &MultFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MultDecimalOp{},
			Volatility: volatility.Immutable,
		},
		// The following two overloads are needed because DInt/DInt = DDecimal. Due
		// to this operation, normalization may sometimes create a DInt * DDecimal
		// operation.
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &MultDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &MultIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MultIntIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			EvalOp:     &MultIntervalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			EvalOp:     &MultIntervalFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MultFloatIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			EvalOp:     &MultDecimalIntervalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Decimal,
			ReturnType: types.Interval,
			EvalOp:     &MultIntervalDecimalOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Div: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &DivIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &DivFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &DivDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &DivDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &DivIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			EvalOp:     &DivIntervalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			EvalOp:     &DivIntervalFloatOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.FloorDiv: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &FloorDivIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &FloorDivFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &FloorDivDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &FloorDivDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &FloorDivIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Mod: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &ModIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &ModFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &ModDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &ModDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &ModIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			// The string % string operator returns whether the two strings have a
			// greater or equal trigram similarity() than the threshold in
			// pg_trgm.similarity_threshold.
			// TODO (jordan): we shouldn't implement this here - we should implement
			// this as a normalization that returns the expanded predicate
			// 1 - (string <-> string) > pg_trgm.similarity_threshold. That way we
			// don't have to ship similarity_threshold across the DistSQL network.
			LeftType:   types.String,
			RightType:  types.String,
			ReturnType: types.Bool,
			EvalOp:     &ModStringOp{},
			// This operator is only stable because its result depends on the value
			// of the pg_trgm.similarity_threshold session setting.
			Volatility: volatility.Stable,
		},
	},

	treebin.Concat: {
		&BinOp{
			LeftType:   types.String,
			RightType:  types.String,
			ReturnType: types.String,
			EvalOp:     &ConcatStringOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Bytes,
			RightType:  types.Bytes,
			ReturnType: types.Bytes,
			EvalOp:     &ConcatBytesOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			EvalOp:     &ConcatVarBitOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			ReturnType: types.Jsonb,
			EvalOp:     &ConcatJsonbOp{},
			Volatility: volatility.Immutable,
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	treebin.LShift: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &LShiftIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.Int,
			ReturnType: types.VarBit,
			EvalOp:     &LShiftVarBitIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Bool,
			EvalOp:     &LShiftINetOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.RShift: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &RShiftIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.Int,
			ReturnType: types.VarBit,
			EvalOp:     &RShiftVarBitIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Bool,
			EvalOp:     &RShiftINetOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.Pow: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			EvalOp:     &PowIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			EvalOp:     &PowFloatOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PowDecimalOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			EvalOp:     &PowDecimalIntOp{},
			Volatility: volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			EvalOp:     &PowIntDecimalOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.JSONFetchVal: {
		&BinOp{
			LeftType:          types.Jsonb,
			RightType:         types.String,
			ReturnType:        types.Jsonb,
			EvalOp:            &JSONFetchValStringOp{},
			PreferredOverload: true,
			Volatility:        volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.Jsonb,
			EvalOp:     &JSONFetchValIntOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.JSONFetchValPath: {
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.Jsonb,
			EvalOp:     &JSONFetchValPathOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.JSONFetchText: {
		&BinOp{
			LeftType:          types.Jsonb,
			RightType:         types.String,
			ReturnType:        types.String,
			PreferredOverload: true,
			EvalOp:            &JSONFetchTextStringOp{},
			Volatility:        volatility.Immutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.String,
			EvalOp:     &JSONFetchTextIntOp{},
			Volatility: volatility.Immutable,
		},
	},

	treebin.JSONFetchTextPath: {
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.String,
			EvalOp:     &JSONFetchTextPathOp{},
			Volatility: volatility.Immutable,
		},
	},
}

// CmpOp is a comparison operator.
type CmpOp struct {
	types TypeList

	LeftType  *types.T
	RightType *types.T

	// Datum return type is a union between *DBool and dNull.
	EvalOp BinaryEvalOp

	// counter, if non-nil, should be incremented every time the
	// operator is type checked.
	counter telemetry.Counter

	// If NullableArgs is false, the operator returns NULL
	// whenever either argument is NULL.
	NullableArgs bool

	Volatility volatility.V

	PreferredOverload bool
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

func (op *CmpOp) preferred() bool {
	return op.PreferredOverload
}

func cmpOpFixups(
	cmpOps map[treecmp.ComparisonOperatorSymbol]cmpOpOverload,
) map[treecmp.ComparisonOperatorSymbol]cmpOpOverload {
	findVolatility := func(op treecmp.ComparisonOperatorSymbol, t *types.T) volatility.V {
		for _, impl := range cmpOps[treecmp.EQ] {
			o := impl.(*CmpOp)
			if o.LeftType.Equivalent(t) && o.RightType.Equivalent(t) {
				return o.Volatility
			}
		}
		panic(errors.AssertionFailedf("could not find cmp op %s(%s,%s)", op, t, t))
	}

	// Array equality comparisons.
	for _, t := range append(types.Scalar, types.AnyEnum) {
		cmpOps[treecmp.EQ] = append(cmpOps[treecmp.EQ], &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			EvalOp:     &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.EQ)},
			Volatility: findVolatility(treecmp.EQ, t),
		})
		cmpOps[treecmp.LE] = append(cmpOps[treecmp.LE], &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			EvalOp:     &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.LE)},
			Volatility: findVolatility(treecmp.LE, t),
		})
		cmpOps[treecmp.LT] = append(cmpOps[treecmp.LT], &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			EvalOp:     &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.LT)},
			Volatility: findVolatility(treecmp.LT, t),
		})

		cmpOps[treecmp.IsNotDistinctFrom] = append(cmpOps[treecmp.IsNotDistinctFrom], &CmpOp{
			LeftType:     types.MakeArray(t),
			RightType:    types.MakeArray(t),
			EvalOp:       &CompareScalarOp{treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom)},
			NullableArgs: true,
			Volatility:   findVolatility(treecmp.IsNotDistinctFrom, t),
		})
	}

	for op, overload := range cmpOps {
		for i, impl := range overload {
			casted := impl.(*CmpOp)
			casted.types = ArgTypes{{"left", casted.LeftType}, {"right", casted.RightType}}
			cmpOps[op][i] = casted
		}
	}

	return cmpOps
}

// cmpOpOverload is an overloaded set of comparison operator implementations.
type cmpOpOverload []overloadImpl

func (o cmpOpOverload) LookupImpl(left, right *types.T) (*CmpOp, bool) {
	for _, fn := range o {
		casted := fn.(*CmpOp)
		if casted.matchParams(left, right) {
			return casted, true
		}
	}
	return nil, false
}

func makeCmpOpOverload(
	op treecmp.ComparisonOperatorSymbol, a, b *types.T, nullableArgs bool, v volatility.V,
) *CmpOp {
	return &CmpOp{
		LeftType:     a,
		RightType:    b,
		EvalOp:       &CompareScalarOp{ComparisonOperator: treecmp.MakeComparisonOperator(op)},
		NullableArgs: nullableArgs,
		Volatility:   v,
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

// CmpOps contains the comparison operations indexed by operation type.
var CmpOps = cmpOpFixups(map[treecmp.ComparisonOperatorSymbol]cmpOpOverload{
	treecmp.EQ: {
		// Single-type comparisons.
		makeEqFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeEqFn(types.Bool, types.Bool, volatility.LeakProof),
		makeEqFn(types.Bytes, types.Bytes, volatility.LeakProof),
		makeEqFn(types.Date, types.Date, volatility.LeakProof),
		makeEqFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeEqFn(types.AnyCollatedString, types.AnyCollatedString, volatility.LeakProof),
		makeEqFn(types.Float, types.Float, volatility.LeakProof),
		makeEqFn(types.Box2D, types.Box2D, volatility.LeakProof),
		makeEqFn(types.Geography, types.Geography, volatility.LeakProof),
		makeEqFn(types.Geometry, types.Geometry, volatility.LeakProof),
		makeEqFn(types.INet, types.INet, volatility.LeakProof),
		makeEqFn(types.Int, types.Int, volatility.LeakProof),
		makeEqFn(types.Interval, types.Interval, volatility.LeakProof),
		makeEqFn(types.Jsonb, types.Jsonb, volatility.Immutable),
		makeEqFn(types.Oid, types.Oid, volatility.LeakProof),
		makeEqFn(types.String, types.String, volatility.LeakProof),
		makeEqFn(types.Time, types.Time, volatility.LeakProof),
		makeEqFn(types.TimeTZ, types.TimeTZ, volatility.LeakProof),
		makeEqFn(types.Timestamp, types.Timestamp, volatility.LeakProof),
		makeEqFn(types.TimestampTZ, types.TimestampTZ, volatility.LeakProof),
		makeEqFn(types.Uuid, types.Uuid, volatility.LeakProof),
		makeEqFn(types.VarBit, types.VarBit, volatility.LeakProof),

		// Mixed-type comparisons.
		makeEqFn(types.Date, types.Timestamp, volatility.Immutable),
		makeEqFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeEqFn(types.Decimal, types.Float, volatility.LeakProof),
		makeEqFn(types.Decimal, types.Int, volatility.LeakProof),
		makeEqFn(types.Float, types.Decimal, volatility.LeakProof),
		makeEqFn(types.Float, types.Int, volatility.LeakProof),
		makeEqFn(types.Int, types.Decimal, volatility.LeakProof),
		makeEqFn(types.Int, types.Float, volatility.LeakProof),
		makeEqFn(types.Int, types.Oid, volatility.LeakProof),
		makeEqFn(types.Oid, types.Int, volatility.LeakProof),
		makeEqFn(types.Timestamp, types.Date, volatility.Immutable),
		makeEqFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeEqFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeEqFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeEqFn(types.Time, types.TimeTZ, volatility.Stable),
		makeEqFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			EvalOp: &CompareTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.EQ),
			},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.LT: {
		// Single-type comparisons.
		makeLtFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeLtFn(types.Bool, types.Bool, volatility.LeakProof),
		makeLtFn(types.Bytes, types.Bytes, volatility.LeakProof),
		makeLtFn(types.Date, types.Date, volatility.LeakProof),
		makeLtFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeLtFn(types.AnyCollatedString, types.AnyCollatedString, volatility.LeakProof),
		makeLtFn(types.Float, types.Float, volatility.LeakProof),
		makeLtFn(types.Box2D, types.Box2D, volatility.LeakProof),
		makeLtFn(types.Geography, types.Geography, volatility.LeakProof),
		makeLtFn(types.Geometry, types.Geometry, volatility.LeakProof),
		makeLtFn(types.INet, types.INet, volatility.LeakProof),
		makeLtFn(types.Int, types.Int, volatility.LeakProof),
		makeLtFn(types.Interval, types.Interval, volatility.LeakProof),
		makeLtFn(types.Oid, types.Oid, volatility.LeakProof),
		makeLtFn(types.String, types.String, volatility.LeakProof),
		makeLtFn(types.Time, types.Time, volatility.LeakProof),
		makeLtFn(types.TimeTZ, types.TimeTZ, volatility.LeakProof),
		makeLtFn(types.Timestamp, types.Timestamp, volatility.LeakProof),
		makeLtFn(types.TimestampTZ, types.TimestampTZ, volatility.LeakProof),
		makeLtFn(types.Uuid, types.Uuid, volatility.LeakProof),
		makeLtFn(types.VarBit, types.VarBit, volatility.LeakProof),

		// Mixed-type comparisons.
		makeLtFn(types.Date, types.Timestamp, volatility.Immutable),
		makeLtFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeLtFn(types.Decimal, types.Float, volatility.LeakProof),
		makeLtFn(types.Decimal, types.Int, volatility.LeakProof),
		makeLtFn(types.Float, types.Decimal, volatility.LeakProof),
		makeLtFn(types.Float, types.Int, volatility.LeakProof),
		makeLtFn(types.Int, types.Decimal, volatility.LeakProof),
		makeLtFn(types.Int, types.Float, volatility.LeakProof),
		makeLtFn(types.Int, types.Oid, volatility.LeakProof),
		makeLtFn(types.Oid, types.Int, volatility.LeakProof),
		makeLtFn(types.Timestamp, types.Date, volatility.Immutable),
		makeLtFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeLtFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeLtFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeLtFn(types.Time, types.TimeTZ, volatility.Stable),
		makeLtFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			EvalOp: &CompareTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.LT),
			},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.LE: {
		// Single-type comparisons.
		makeLeFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeLeFn(types.Bool, types.Bool, volatility.LeakProof),
		makeLeFn(types.Bytes, types.Bytes, volatility.LeakProof),
		makeLeFn(types.Date, types.Date, volatility.LeakProof),
		makeLeFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeLeFn(types.AnyCollatedString, types.AnyCollatedString, volatility.LeakProof),
		makeLeFn(types.Float, types.Float, volatility.LeakProof),
		makeLeFn(types.Box2D, types.Box2D, volatility.LeakProof),
		makeLeFn(types.Geography, types.Geography, volatility.LeakProof),
		makeLeFn(types.Geometry, types.Geometry, volatility.LeakProof),
		makeLeFn(types.INet, types.INet, volatility.LeakProof),
		makeLeFn(types.Int, types.Int, volatility.LeakProof),
		makeLeFn(types.Interval, types.Interval, volatility.LeakProof),
		makeLeFn(types.Oid, types.Oid, volatility.LeakProof),
		makeLeFn(types.String, types.String, volatility.LeakProof),
		makeLeFn(types.Time, types.Time, volatility.LeakProof),
		makeLeFn(types.TimeTZ, types.TimeTZ, volatility.LeakProof),
		makeLeFn(types.Timestamp, types.Timestamp, volatility.LeakProof),
		makeLeFn(types.TimestampTZ, types.TimestampTZ, volatility.LeakProof),
		makeLeFn(types.Uuid, types.Uuid, volatility.LeakProof),
		makeLeFn(types.VarBit, types.VarBit, volatility.LeakProof),

		// Mixed-type comparisons.
		makeLeFn(types.Date, types.Timestamp, volatility.Immutable),
		makeLeFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeLeFn(types.Decimal, types.Float, volatility.LeakProof),
		makeLeFn(types.Decimal, types.Int, volatility.LeakProof),
		makeLeFn(types.Float, types.Decimal, volatility.LeakProof),
		makeLeFn(types.Float, types.Int, volatility.LeakProof),
		makeLeFn(types.Int, types.Decimal, volatility.LeakProof),
		makeLeFn(types.Int, types.Float, volatility.LeakProof),
		makeLeFn(types.Int, types.Oid, volatility.LeakProof),
		makeLeFn(types.Oid, types.Int, volatility.LeakProof),
		makeLeFn(types.Timestamp, types.Date, volatility.Immutable),
		makeLeFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeLeFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeLeFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeLeFn(types.Time, types.TimeTZ, volatility.Stable),
		makeLeFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			EvalOp: &CompareTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.LE),
			},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.IsNotDistinctFrom: {
		&CmpOp{
			LeftType:  types.Unknown,
			RightType: types.Unknown,
			EvalOp: &CompareScalarOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			},
			NullableArgs: true,
			// Avoids ambiguous comparison error for NULL IS NOT DISTINCT FROM NULL.
			PreferredOverload: true,
			Volatility:        volatility.LeakProof,
		},
		&CmpOp{
			LeftType:  types.AnyArray,
			RightType: types.Unknown,
			EvalOp: &CompareScalarOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			},
			NullableArgs: true,
			Volatility:   volatility.LeakProof,
		},
		// Single-type comparisons.
		makeIsFn(types.AnyEnum, types.AnyEnum, volatility.Immutable),
		makeIsFn(types.Bool, types.Bool, volatility.LeakProof),
		makeIsFn(types.Bytes, types.Bytes, volatility.LeakProof),
		makeIsFn(types.Date, types.Date, volatility.LeakProof),
		makeIsFn(types.Decimal, types.Decimal, volatility.Immutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeIsFn(types.AnyCollatedString, types.AnyCollatedString, volatility.LeakProof),
		makeIsFn(types.Float, types.Float, volatility.LeakProof),
		makeIsFn(types.Box2D, types.Box2D, volatility.LeakProof),
		makeIsFn(types.Geography, types.Geography, volatility.LeakProof),
		makeIsFn(types.Geometry, types.Geometry, volatility.LeakProof),
		makeIsFn(types.INet, types.INet, volatility.LeakProof),
		makeIsFn(types.Int, types.Int, volatility.LeakProof),
		makeIsFn(types.Interval, types.Interval, volatility.LeakProof),
		makeIsFn(types.Jsonb, types.Jsonb, volatility.Immutable),
		makeIsFn(types.Oid, types.Oid, volatility.LeakProof),
		makeIsFn(types.String, types.String, volatility.LeakProof),
		makeIsFn(types.Time, types.Time, volatility.LeakProof),
		makeIsFn(types.TimeTZ, types.TimeTZ, volatility.LeakProof),
		makeIsFn(types.Timestamp, types.Timestamp, volatility.LeakProof),
		makeIsFn(types.TimestampTZ, types.TimestampTZ, volatility.LeakProof),
		makeIsFn(types.Uuid, types.Uuid, volatility.LeakProof),
		makeIsFn(types.VarBit, types.VarBit, volatility.LeakProof),

		// Mixed-type comparisons.
		makeIsFn(types.Date, types.Timestamp, volatility.Immutable),
		makeIsFn(types.Date, types.TimestampTZ, volatility.Stable),
		makeIsFn(types.Decimal, types.Float, volatility.LeakProof),
		makeIsFn(types.Decimal, types.Int, volatility.LeakProof),
		makeIsFn(types.Float, types.Decimal, volatility.LeakProof),
		makeIsFn(types.Float, types.Int, volatility.LeakProof),
		makeIsFn(types.Int, types.Decimal, volatility.LeakProof),
		makeIsFn(types.Int, types.Float, volatility.LeakProof),
		makeIsFn(types.Int, types.Oid, volatility.LeakProof),
		makeIsFn(types.Oid, types.Int, volatility.LeakProof),
		makeIsFn(types.Timestamp, types.Date, volatility.Immutable),
		makeIsFn(types.Timestamp, types.TimestampTZ, volatility.Stable),
		makeIsFn(types.TimestampTZ, types.Date, volatility.Stable),
		makeIsFn(types.TimestampTZ, types.Timestamp, volatility.Stable),
		makeIsFn(types.Time, types.TimeTZ, volatility.Stable),
		makeIsFn(types.TimeTZ, types.Time, volatility.Stable),

		// Tuple comparison.
		&CmpOp{
			LeftType:     types.AnyTuple,
			RightType:    types.AnyTuple,
			NullableArgs: true,
			EvalOp: &CompareAnyTupleOp{
				ComparisonOperator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.In: {
		makeEvalTupleIn(types.AnyEnum, volatility.LeakProof),
		makeEvalTupleIn(types.Bool, volatility.LeakProof),
		makeEvalTupleIn(types.Bytes, volatility.LeakProof),
		makeEvalTupleIn(types.Date, volatility.LeakProof),
		makeEvalTupleIn(types.Decimal, volatility.LeakProof),
		makeEvalTupleIn(types.AnyCollatedString, volatility.LeakProof),
		makeEvalTupleIn(types.AnyTuple, volatility.LeakProof),
		makeEvalTupleIn(types.Float, volatility.LeakProof),
		makeEvalTupleIn(types.Box2D, volatility.LeakProof),
		makeEvalTupleIn(types.Geography, volatility.LeakProof),
		makeEvalTupleIn(types.Geometry, volatility.LeakProof),
		makeEvalTupleIn(types.INet, volatility.LeakProof),
		makeEvalTupleIn(types.Int, volatility.LeakProof),
		makeEvalTupleIn(types.Interval, volatility.LeakProof),
		makeEvalTupleIn(types.Jsonb, volatility.LeakProof),
		makeEvalTupleIn(types.Oid, volatility.LeakProof),
		makeEvalTupleIn(types.String, volatility.LeakProof),
		makeEvalTupleIn(types.Time, volatility.LeakProof),
		makeEvalTupleIn(types.TimeTZ, volatility.LeakProof),
		makeEvalTupleIn(types.Timestamp, volatility.LeakProof),
		makeEvalTupleIn(types.TimestampTZ, volatility.LeakProof),
		makeEvalTupleIn(types.Uuid, volatility.LeakProof),
		makeEvalTupleIn(types.VarBit, volatility.LeakProof),
	},

	treecmp.Like: {
		&CmpOp{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchLikeOp{CaseInsensitive: false},
			Volatility: volatility.LeakProof,
		},
	},

	treecmp.ILike: {
		&CmpOp{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchLikeOp{CaseInsensitive: true},
			Volatility: volatility.LeakProof,
		},
	},

	treecmp.SimilarTo: {
		&CmpOp{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &SimilarToOp{Escape: '\\'},
			Volatility: volatility.LeakProof,
		},
	},

	treecmp.RegMatch: append(
		cmpOpOverload{
			&CmpOp{
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
	),

	treecmp.RegIMatch: {
		&CmpOp{
			LeftType:   types.String,
			RightType:  types.String,
			EvalOp:     &MatchRegexpOp{CaseInsensitive: true},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.JSONExists: {
		&CmpOp{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			EvalOp:     &JSONExistsOp{},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.JSONSomeExists: {
		&CmpOp{
			LeftType:   types.Jsonb,
			RightType:  types.StringArray,
			EvalOp:     &JSONSomeExistsOp{},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.JSONAllExists: {
		&CmpOp{
			LeftType:   types.Jsonb,
			RightType:  types.StringArray,
			EvalOp:     &JSONAllExistsOp{},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.Contains: {
		&CmpOp{
			LeftType:   types.AnyArray,
			RightType:  types.AnyArray,
			EvalOp:     &ContainsArrayOp{},
			Volatility: volatility.Immutable,
		},
		&CmpOp{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			EvalOp:     &ContainsJsonbOp{},
			Volatility: volatility.Immutable,
		},
	},

	treecmp.ContainedBy: {
		&CmpOp{
			LeftType:   types.AnyArray,
			RightType:  types.AnyArray,
			EvalOp:     &ContainedByArrayOp{},
			Volatility: volatility.Immutable,
		},
		&CmpOp{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			EvalOp:     &ContainedByJsonbOp{},
			Volatility: volatility.Immutable,
		},
	},
	treecmp.Overlaps: append(
		cmpOpOverload{
			&CmpOp{
				LeftType:   types.AnyArray,
				RightType:  types.AnyArray,
				EvalOp:     &OverlapsArrayOp{},
				Volatility: volatility.Immutable,
			},
			&CmpOp{
				LeftType:   types.INet,
				RightType:  types.INet,
				EvalOp:     &OverlapsINetOp{},
				Volatility: volatility.Immutable,
			},
		},
		makeBox2DComparisonOperators(
			func(lhs, rhs *geo.CartesianBoundingBox) bool {
				return lhs.Intersects(rhs)
			},
		)...,
	),
})

func makeBox2DComparisonOperators(op func(lhs, rhs *geo.CartesianBoundingBox) bool) cmpOpOverload {
	return cmpOpOverload{
		&CmpOp{
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
		&CmpOp{
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
		&CmpOp{
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
		&CmpOp{
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
		LeftType:     typ,
		RightType:    types.AnyTuple,
		EvalOp:       &InTupleOp{},
		NullableArgs: true,
		Volatility:   v,
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
	if fName == `crdb_internal.force_error` {
		return err
	}
	// Otherwise, wrap it with context.
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
