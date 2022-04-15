// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"math"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
	"github.com/cockroachdb/errors"
)

// BinaryOp evaluates a tree.BinaryEvalOp.
func BinaryOp(ctx *Context, op tree.BinaryEvalOp, left, right tree.Datum) (tree.Datum, error) {
	return op.Eval((*evaluator)(ctx), left, right)
}

func (e *evaluator) EvalAppendToMaybeNullArrayOp(
	op *tree.AppendToMaybeNullArrayOp, a, b tree.Datum,
) (tree.Datum, error) {
	return tree.AppendToMaybeNullArray(op.Typ, a, b)
}

func (e *evaluator) EvalArrayOverlapsOp(
	_ *tree.OverlapsArrayOp, a, b tree.Datum,
) (tree.Datum, error) {
	return tree.ArrayOverlaps(e.ctx(), tree.MustBeDArray(a), tree.MustBeDArray(b))
}

func (e *evaluator) EvalBitAndINetOp(_ *tree.BitAndINetOp, a, b tree.Datum) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(a).IPAddr
	other := tree.MustBeDIPAddr(b).IPAddr
	newIPAddr, err := ipAddr.And(&other)
	return tree.NewDIPAddr(tree.DIPAddr{
		IPAddr: newIPAddr,
	}), err
}

func (e *evaluator) EvalBitAndIntOp(_ *tree.BitAndIntOp, a, b tree.Datum) (tree.Datum, error) {
	return tree.NewDInt(tree.MustBeDInt(a) & tree.MustBeDInt(b)), nil
}

func (e *evaluator) EvalBitAndVarBitOp(
	_ *tree.BitAndVarBitOp, a, b tree.Datum,
) (tree.Datum, error) {
	lhs := tree.MustBeDBitArray(a)
	rhs := tree.MustBeDBitArray(b)
	if lhs.BitLen() != rhs.BitLen() {
		return nil, tree.NewCannotMixBitArraySizesError("AND")
	}
	return &tree.DBitArray{
		BitArray: bitarray.And(lhs.BitArray, rhs.BitArray),
	}, nil
}

func (e *evaluator) EvalBitOrINetOp(_ *tree.BitOrINetOp, a, b tree.Datum) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(a).IPAddr
	other := tree.MustBeDIPAddr(b).IPAddr
	newIPAddr, err := ipAddr.Or(&other)
	return tree.NewDIPAddr(tree.DIPAddr{
		IPAddr: newIPAddr,
	}), err
}

func (e *evaluator) EvalBitOrIntOp(_ *tree.BitOrIntOp, a, b tree.Datum) (tree.Datum, error) {
	return tree.NewDInt(tree.MustBeDInt(a) | tree.MustBeDInt(b)), nil
}

func (e *evaluator) EvalBitOrVarBitOp(_ *tree.BitOrVarBitOp, a, b tree.Datum) (tree.Datum, error) {
	lhs := tree.MustBeDBitArray(a)
	rhs := tree.MustBeDBitArray(b)
	if lhs.BitLen() != rhs.BitLen() {
		return nil, tree.NewCannotMixBitArraySizesError("OR")
	}
	return &tree.DBitArray{
		BitArray: bitarray.Or(lhs.BitArray, rhs.BitArray),
	}, nil
}

func (e *evaluator) EvalBitXorIntOp(_ *tree.BitXorIntOp, a, b tree.Datum) (tree.Datum, error) {
	return tree.NewDInt(tree.MustBeDInt(a) ^ tree.MustBeDInt(b)), nil
}

func (e *evaluator) EvalBitXorVarBitOp(
	_ *tree.BitXorVarBitOp, a, b tree.Datum,
) (tree.Datum, error) {
	lhs := tree.MustBeDBitArray(a)
	rhs := tree.MustBeDBitArray(b)
	if lhs.BitLen() != rhs.BitLen() {
		return nil, tree.NewCannotMixBitArraySizesError("XOR")
	}
	return &tree.DBitArray{
		BitArray: bitarray.Xor(lhs.BitArray, rhs.BitArray),
	}, nil
}

func (e *evaluator) EvalCompareBox2DOp(
	op *tree.CompareBox2DOp, left, right tree.Datum,
) (tree.Datum, error) {
	if err := checkExperimentalBox2DComparisonOperatorEnabled(e.Settings); err != nil {
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(op.Op(left, right))), nil
}

func (e *evaluator) EvalCompareScalarOp(
	op *tree.CompareScalarOp, left, right tree.Datum,
) (tree.Datum, error) {
	// Before deferring to the Datum.Compare method, check for values that should
	// be handled differently during SQL comparison evaluation than they should when
	// ordering Datum values.
	if left == tree.DNull || right == tree.DNull {
		switch op.Symbol {
		case treecmp.IsNotDistinctFrom:
			return tree.MakeDBool((left == tree.DNull) == (right == tree.DNull)), nil

		default:
			// If either Datum is NULL, the result of the comparison is NULL.
			return tree.DNull, nil
		}
	}
	cmp := left.Compare(e.ctx(), right)
	return boolFromCmp(cmp, op.ComparisonOperator), nil
}

func (e *evaluator) EvalCompareTupleOp(
	op *tree.CompareTupleOp, leftDatum, rightDatum tree.Datum,
) (tree.Datum, error) {
	left, right := leftDatum.(*tree.DTuple), rightDatum.(*tree.DTuple)
	cmp := 0
	sawNull := false
	for i, leftElem := range left.D {
		rightElem := right.D[i]
		// Like with cmpOpScalarFn, check for values that need to be handled
		// differently than when ordering Datums.
		if leftElem == tree.DNull || rightElem == tree.DNull {
			switch op.Symbol {
			case treecmp.EQ:
				// If either Datum is NULL and the op is EQ, we continue the
				// comparison and the result is only NULL if the other (non-NULL)
				// elements are equal. This is because NULL is thought of as "unknown",
				// so a NULL equality comparison does not prevent the equality from
				// being proven false, but does prevent it from being proven true.
				sawNull = true

			case treecmp.IsNotDistinctFrom:
				// For IS NOT DISTINCT FROM, NULLs are "equal".
				if leftElem != tree.DNull || rightElem != tree.DNull {
					return tree.DBoolFalse, nil
				}

			default:
				// If either Datum is NULL and the op is not EQ or IS NOT DISTINCT FROM,
				// we short-circuit the evaluation and the result of the comparison is
				// NULL. This is because NULL is thought of as "unknown" and tuple
				// inequality is defined lexicographically, so once a NULL comparison is
				// seen, the result of the entire tuple comparison is unknown.
				return tree.DNull, nil
			}
		} else {
			cmp = leftElem.Compare(e.ctx(), rightElem)
			if cmp != 0 {
				break
			}
		}
	}
	b := boolFromCmp(cmp, op.ComparisonOperator)
	if b == tree.DBoolTrue && sawNull {
		// The op is EQ and all non-NULL elements are equal, but we saw at least
		// one NULL element. Since NULL comparisons are treated as unknown, the
		// result of the comparison becomes unknown (NULL).
		return tree.DNull, nil
	}
	return b, nil
}

func (e *evaluator) EvalConcatArraysOp(
	op *tree.ConcatArraysOp, a, b tree.Datum,
) (tree.Datum, error) {
	return tree.ConcatArrays(op.Typ, a, b)
}

func (e *evaluator) EvalConcatOp(op *tree.ConcatOp, left, right tree.Datum) (tree.Datum, error) {
	if op.Left == types.String {
		casted, err := PerformCast(e.ctx(), right, types.String)
		if err != nil {
			return nil, err
		}
		return tree.NewDString(
			string(tree.MustBeDString(left)) + string(tree.MustBeDString(casted)),
		), nil
	}
	if op.Right == types.String {
		casted, err := PerformCast(e.ctx(), left, types.String)
		if err != nil {
			return nil, err
		}
		return tree.NewDString(
			string(tree.MustBeDString(casted)) + string(tree.MustBeDString(right)),
		), nil
	}
	return nil, errors.New("neither LHS or RHS matched DString")
}

func (e *evaluator) EvalConcatBytesOp(
	_ *tree.ConcatBytesOp, left tree.Datum, right tree.Datum,
) (tree.Datum, error) {
	return tree.NewDBytes(*left.(*tree.DBytes) + *right.(*tree.DBytes)), nil
}

func (e *evaluator) EvalConcatJsonbOp(
	_ *tree.ConcatJsonbOp, left tree.Datum, right tree.Datum,
) (tree.Datum, error) {
	j, err := tree.MustBeDJSON(left).JSON.Concat(
		tree.MustBeDJSON(right).JSON,
	)
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

func (e *evaluator) EvalConcatStringOp(
	_ *tree.ConcatStringOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.NewDString(
		string(tree.MustBeDString(left) + tree.MustBeDString(right)),
	), nil

}

func (e *evaluator) EvalConcatVarBitOp(
	_ *tree.ConcatVarBitOp, left, right tree.Datum,
) (tree.Datum, error) {
	lhs := tree.MustBeDBitArray(left)
	rhs := tree.MustBeDBitArray(right)
	return &tree.DBitArray{
		BitArray: bitarray.Concat(lhs.BitArray, rhs.BitArray),
	}, nil
}

func (e *evaluator) EvalContainedByArrayOp(
	_ *tree.ContainedByArrayOp, a, b tree.Datum,
) (tree.Datum, error) {
	needles := tree.MustBeDArray(a)
	haystack := tree.MustBeDArray(b)
	return tree.ArrayContains(e.ctx(), haystack, needles)
}

func (e *evaluator) EvalContainedByJsonbOp(
	_ *tree.ContainedByJsonbOp, a, b tree.Datum,
) (tree.Datum, error) {
	c, err := json.Contains(b.(*tree.DJSON).JSON, a.(*tree.DJSON).JSON)
	if err != nil {
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(c)), nil
}

func (e *evaluator) EvalContainsArrayOp(
	_ *tree.ContainsArrayOp, a, b tree.Datum,
) (tree.Datum, error) {
	haystack := tree.MustBeDArray(a)
	needles := tree.MustBeDArray(b)
	return tree.ArrayContains(e.ctx(), haystack, needles)
}

func (e *evaluator) EvalContainsJsonbOp(
	_ *tree.ContainsJsonbOp, a, b tree.Datum,
) (tree.Datum, error) {
	c, err := json.Contains(a.(*tree.DJSON).JSON, b.(*tree.DJSON).JSON)
	if err != nil {
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(c)), nil
}

func (e *evaluator) EvalDivDecimalIntOp(
	_ *tree.DivDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	if r == 0 {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.DecimalCtx.Quo(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalDivDecimalOp(
	_ *tree.DivDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	if r.IsZero() {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	_, err := tree.DecimalCtx.Quo(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalDivFloatOp(_ *tree.DivFloatOp, left, right tree.Datum) (tree.Datum, error) {
	r := *right.(*tree.DFloat)
	if r == 0.0 {
		return nil, tree.ErrDivByZero
	}
	return tree.NewDFloat(*left.(*tree.DFloat) / r), nil
}

func (e *evaluator) EvalDivIntDecimalOp(
	_ *tree.DivIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	if r.IsZero() {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalDivIntOp(_ *tree.DivIntOp, left, right tree.Datum) (tree.Datum, error) {
	rInt := tree.MustBeDInt(right)
	if rInt == 0 {
		return nil, tree.ErrDivByZero
	}
	var div apd.Decimal
	div.SetInt64(int64(rInt))
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(tree.MustBeDInt(left)))
	_, err := tree.DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, &div)
	return dd, err
}

func (e *evaluator) EvalDivIntervalFloatOp(
	_ *tree.DivIntervalFloatOp, left, right tree.Datum,
) (tree.Datum, error) {
	r := float64(*right.(*tree.DFloat))
	if r == 0.0 {
		return nil, tree.ErrDivByZero
	}
	return &tree.DInterval{
		Duration: left.(*tree.DInterval).Duration.DivFloat(r),
	}, nil
}

func (e *evaluator) EvalDivIntervalIntOp(
	_ *tree.DivIntervalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	rInt := tree.MustBeDInt(right)
	if rInt == 0 {
		return nil, tree.ErrDivByZero
	}
	return &tree.DInterval{
		Duration: left.(*tree.DInterval).Duration.Div(int64(rInt)),
	}, nil
}

func (e *evaluator) EvalFloorDivDecimalIntOp(
	_ *tree.FloorDivDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	if r == 0 {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalFloorDivDecimalOp(
	_ *tree.FloorDivDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	if r.IsZero() {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalFloorDivFloatOp(
	_ *tree.FloorDivFloatOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := float64(*left.(*tree.DFloat))
	r := float64(*right.(*tree.DFloat))
	if r == 0.0 {
		return nil, tree.ErrDivByZero
	}
	return tree.NewDFloat(tree.DFloat(math.Trunc(l / r))), nil
}

func (e *evaluator) EvalFloorDivIntDecimalOp(
	_ *tree.FloorDivIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	if r.IsZero() {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalFloorDivIntOp(
	_ *tree.FloorDivIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	rInt := tree.MustBeDInt(right)
	if rInt == 0 {
		return nil, tree.ErrDivByZero
	}
	return tree.NewDInt(tree.MustBeDInt(left) / rInt), nil
}

func (e *evaluator) EvalInTupleOp(_ *tree.InTupleOp, arg, values tree.Datum) (tree.Datum, error) {
	vtuple := values.(*tree.DTuple)
	// If the tuple was sorted during normalization, we can perform an
	// efficient binary search to find if the arg is in the tuple (as
	// long as the arg doesn't contain any NULLs).
	if len(vtuple.D) == 0 {
		// If the rhs tuple is empty, the result is always false (even if arg is
		// or contains NULL).
		return tree.DBoolFalse, nil
	}
	if arg == tree.DNull {
		return tree.DNull, nil
	}
	argTuple, argIsTuple := arg.(*tree.DTuple)
	if vtuple.Sorted() && !(argIsTuple && argTuple.ContainsNull()) {
		// The right-hand tuple is already sorted and contains no NULLs, and the
		// left side is not NULL (e.g. `NULL IN (1, 2)`) or a tuple that
		// contains NULL (e.g. `(1, NULL) IN ((1, 2), (3, 4))`).
		//
		// We can use binary search to make a determination in this case. This
		// is the common case when tuples don't contain NULLs.
		_, result := vtuple.SearchSorted(e.ctx(), arg)
		return tree.MakeDBool(tree.DBool(result)), nil
	}

	sawNull := false
	if !argIsTuple {
		// The left-hand side is not a tuple, e.g. `1 IN (1, 2)`.
		for _, val := range vtuple.D {
			if val == tree.DNull {
				sawNull = true
			} else if val.Compare(e.ctx(), arg) == 0 {
				return tree.DBoolTrue, nil
			}
		}
	} else {
		// The left-hand side is a tuple, e.g. `(1, 2) IN ((1, 2), (3, 4))`.
		for _, val := range vtuple.D {
			if val == tree.DNull {
				// We allow for a null value to be in the list of tuples, so we
				// need to check that upfront.
				sawNull = true
			} else {
				// Use the EQ function which properly handles NULLs.
				if res := cmpOpTupleFn(
					e.ctx(), *argTuple, *val.(*tree.DTuple),
					treecmp.MakeComparisonOperator(treecmp.EQ),
				); res == tree.DNull {
					sawNull = true
				} else if res == tree.DBoolTrue {
					return tree.DBoolTrue, nil
				}
			}
		}
	}
	if sawNull {
		return tree.DNull, nil
	}
	return tree.DBoolFalse, nil
}

func (e *evaluator) EvalJSONAllExistsOp(
	_ *tree.JSONAllExistsOp, a, b tree.Datum,
) (tree.Datum, error) {
	// TODO(justin): this can be optimized.
	for _, k := range tree.MustBeDArray(b).Array {
		if k == tree.DNull {
			continue
		}
		e, err := a.(*tree.DJSON).JSON.Exists(string(tree.MustBeDString(k)))
		if err != nil {
			return nil, err
		}
		if !e {
			return tree.DBoolFalse, nil
		}
	}
	return tree.DBoolTrue, nil
}

func (e *evaluator) EvalJSONExistsOp(_ *tree.JSONExistsOp, a, b tree.Datum) (tree.Datum, error) {
	exists, err := a.(*tree.DJSON).JSON.Exists(string(tree.MustBeDString(b)))
	if err != nil {
		return nil, err
	}
	if exists {
		return tree.DBoolTrue, nil
	}
	return tree.DBoolFalse, nil
}

func (e *evaluator) EvalJSONFetchTextIntOp(
	_ *tree.JSONFetchTextIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	res, err := left.(*tree.DJSON).JSON.FetchValIdx(int(tree.MustBeDInt(right)))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return tree.DNull, nil
	}
	text, err := res.AsText()
	if err != nil {
		return nil, err
	}
	if text == nil {
		return tree.DNull, nil
	}
	return tree.NewDString(*text), nil
}

func (e *evaluator) EvalJSONFetchTextPathOp(
	_ *tree.JSONFetchTextPathOp, left, right tree.Datum,
) (tree.Datum, error) {
	res, err := tree.GetJSONPath(
		left.(*tree.DJSON).JSON, *tree.MustBeDArray(right),
	)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return tree.DNull, nil
	}
	text, err := res.AsText()
	if err != nil {
		return nil, err
	}
	if text == nil {
		return tree.DNull, nil
	}
	return tree.NewDString(*text), nil
}

func (e *evaluator) EvalJSONFetchTextStringOp(
	_ *tree.JSONFetchTextStringOp, left, right tree.Datum,
) (tree.Datum, error) {
	res, err := left.(*tree.DJSON).JSON.FetchValKey(
		string(tree.MustBeDString(right)),
	)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return tree.DNull, nil
	}
	text, err := res.AsText()
	if err != nil {
		return nil, err
	}
	if text == nil {
		return tree.DNull, nil
	}
	return tree.NewDString(*text), nil
}

func (e *evaluator) EvalJSONFetchValIntOp(
	_ *tree.JSONFetchValIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	j, err := left.(*tree.DJSON).JSON.FetchValIdx(int(tree.MustBeDInt(right)))
	if err != nil {
		return nil, err
	}
	if j == nil {
		return tree.DNull, nil
	}
	return &tree.DJSON{JSON: j}, nil
}

func (e *evaluator) EvalJSONFetchValPathOp(
	_ *tree.JSONFetchValPathOp, left, right tree.Datum,
) (tree.Datum, error) {
	path, err := tree.GetJSONPath(
		left.(*tree.DJSON).JSON, *tree.MustBeDArray(right),
	)
	if err != nil {
		return nil, err
	}
	if path == nil {
		return tree.DNull, nil
	}
	return &tree.DJSON{JSON: path}, nil
}

func (e *evaluator) EvalJSONFetchValStringOp(
	_ *tree.JSONFetchValStringOp, left, right tree.Datum,
) (tree.Datum, error) {
	j, err := left.(*tree.DJSON).JSON.FetchValKey(
		string(tree.MustBeDString(right)),
	)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return tree.DNull, nil
	}
	return &tree.DJSON{JSON: j}, nil
}

func (e *evaluator) EvalJSONSomeExistsOp(
	_ *tree.JSONSomeExistsOp, a, b tree.Datum,
) (tree.Datum, error) {
	return tree.JSONExistsAny(tree.MustBeDJSON(a), tree.MustBeDArray(b))
}

func (e *evaluator) EvalLShiftINetOp(
	_ *tree.LShiftINetOp, left, right tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(left).IPAddr
	other := tree.MustBeDIPAddr(right).IPAddr
	return tree.MakeDBool(tree.DBool(ipAddr.ContainedBy(&other))), nil
}

func (e *evaluator) EvalLShiftIntOp(
	_ *tree.LShiftIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	rval := tree.MustBeDInt(right)
	if rval < 0 || rval >= 64 {
		telemetry.Inc(sqltelemetry.LargeLShiftArgumentCounter)
		return nil, tree.ErrShiftArgOutOfRange
	}
	return tree.NewDInt(tree.MustBeDInt(left) << uint(rval)), nil
}

func (e *evaluator) EvalLShiftVarBitIntOp(
	_ *tree.LShiftVarBitIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	lhs := tree.MustBeDBitArray(left)
	rhs := tree.MustBeDInt(right)
	return &tree.DBitArray{
		BitArray: lhs.BitArray.LeftShiftAny(int64(rhs)),
	}, nil
}

func (e *evaluator) EvalMatchLikeOp(op *tree.MatchLikeOp, a, b tree.Datum) (tree.Datum, error) {
	return matchLike(e.ctx(), a, b, op.CaseInsensitive)
}

func (e *evaluator) EvalMatchRegexpOp(op *tree.MatchRegexpOp, a, b tree.Datum) (tree.Datum, error) {
	key := regexpKey{s: string(tree.MustBeDString(b)), caseInsensitive: op.CaseInsensitive}
	return matchRegexpWithKey(e.ctx(), a, key)
}

func (e *evaluator) EvalMinusDateIntOp(
	_ *tree.MinusDateIntOp, a, b tree.Datum,
) (tree.Datum, error) {
	d, err := a.(*tree.DDate).SubDays(int64(tree.MustBeDInt(b)))
	if err != nil {
		return nil, err
	}
	return tree.NewDDate(d), nil
}

func (e *evaluator) EvalMinusDateIntervalOp(
	_ *tree.MinusDateIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	leftTime, err := left.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := duration.Add(leftTime, right.(*tree.DInterval).Duration.Mul(-1))
	return tree.MakeDTimestamp(t, time.Microsecond)
}

func (e *evaluator) EvalMinusDateOp(_ *tree.MinusDateOp, a, b tree.Datum) (tree.Datum, error) {
	l, r := a.(*tree.DDate).Date, b.(*tree.DDate).Date
	if !l.IsFinite() || !r.IsFinite() {
		return nil, pgerror.New(pgcode.DatetimeFieldOverflow, "cannot subtract infinite dates")
	}
	ad := l.PGEpochDays()
	bd := r.PGEpochDays()
	// This can't overflow because they are upconverted from int32 to int64.
	return tree.NewDInt(tree.DInt(int64(ad) - int64(bd))), nil
}

func (e *evaluator) EvalMinusDateTimeOp(
	_ *tree.MinusDateTimeOp, left, right tree.Datum,
) (tree.Datum, error) {
	leftTime, err := left.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := time.Duration(*right.(*tree.DTime)) * time.Microsecond
	return tree.MakeDTimestamp(leftTime.Add(-1*t), time.Microsecond)
}

func (e *evaluator) EvalMinusDecimalIntOp(
	_ *tree.MinusDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.ExactCtx.Sub(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalMinusDecimalOp(
	_ *tree.MinusDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	_, err := tree.ExactCtx.Sub(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalMinusFloatOp(
	_ *tree.MinusFloatOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.NewDFloat(*left.(*tree.DFloat) - *right.(*tree.DFloat)), nil
}

func (e *evaluator) EvalMinusINetIntOp(
	_ *tree.MinusINetIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(left).IPAddr
	i := tree.MustBeDInt(right)
	newIPAddr, err := ipAddr.Sub(int64(i))
	return tree.NewDIPAddr(tree.DIPAddr{IPAddr: newIPAddr}), err
}

func (e *evaluator) EvalMinusINetOp(
	_ *tree.MinusINetOp, left, right tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(left).IPAddr
	other := tree.MustBeDIPAddr(right).IPAddr
	diff, err := ipAddr.SubIPAddr(&other)
	return tree.NewDInt(tree.DInt(diff)), err
}

func (e *evaluator) EvalMinusIntDecimalOp(
	_ *tree.MinusIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.ExactCtx.Sub(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalMinusIntOp(_ *tree.MinusIntOp, left, right tree.Datum) (tree.Datum, error) {
	a, b := tree.MustBeDInt(left), tree.MustBeDInt(right)
	r, ok := arith.SubWithOverflow(int64(a), int64(b))
	if !ok {
		return nil, tree.ErrIntOutOfRange
	}
	return tree.NewDInt(tree.DInt(r)), nil
}

func (e *evaluator) EvalMinusIntervalOp(
	_ *tree.MinusIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l, r := left.(*tree.DInterval), right.(*tree.DInterval)
	return &tree.DInterval{
		Duration: l.Duration.Sub(r.Duration),
	}, nil
}

func (e *evaluator) EvalMinusJsonbIntOp(
	_ *tree.MinusJsonbIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	j, _, err := left.(*tree.DJSON).JSON.RemoveIndex(int(tree.MustBeDInt(right)))
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

func (e *evaluator) EvalMinusJsonbStringArrayOp(
	_ *tree.MinusJsonbStringArrayOp, left, right tree.Datum,
) (tree.Datum, error) {
	j := left.(*tree.DJSON).JSON
	arr := *tree.MustBeDArray(right)

	for _, str := range arr.Array {
		if str == tree.DNull {
			continue
		}
		var err error
		j, _, err = j.RemoveString(string(tree.MustBeDString(str)))
		if err != nil {
			return nil, err
		}
	}
	return &tree.DJSON{JSON: j}, nil
}

func (e *evaluator) EvalMinusJsonbStringOp(
	_ *tree.MinusJsonbStringOp, left, right tree.Datum,
) (tree.Datum, error) {
	j, _, err := left.(*tree.DJSON).JSON.RemoveString(
		string(tree.MustBeDString(right)),
	)
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

func (e *evaluator) EvalMinusTimeIntervalOp(
	_ *tree.MinusTimeIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := timeofday.TimeOfDay(*left.(*tree.DTime))
	return tree.MakeDTime(t.Add(right.(*tree.DInterval).Duration.Mul(-1))), nil
}

func (e *evaluator) EvalMinusTimeOp(
	_ *tree.MinusTimeOp, left, right tree.Datum,
) (tree.Datum, error) {
	t1 := timeofday.TimeOfDay(*left.(*tree.DTime))
	t2 := timeofday.TimeOfDay(*right.(*tree.DTime))
	diff := timeofday.Difference(t1, t2)
	return &tree.DInterval{Duration: diff}, nil
}

func (e *evaluator) EvalMinusTimeTZIntervalOp(
	_ *tree.MinusTimeTZIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := left.(*tree.DTimeTZ)
	d := right.(*tree.DInterval).Duration
	return tree.NewDTimeTZFromOffset(t.Add(d.Mul(-1)), t.OffsetSecs), nil
}

func (e *evaluator) EvalMinusTimestampIntervalOp(
	_ *tree.MinusTimestampIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.MakeDTimestamp(
		duration.Add(
			left.(*tree.DTimestamp).Time,
			right.(*tree.DInterval).Duration.Mul(-1),
		),
		time.Microsecond,
	)
}

func (e *evaluator) EvalMinusTimestampOp(
	_ *tree.MinusTimestampOp, left, right tree.Datum,
) (tree.Datum, error) {
	nanos := left.(*tree.DTimestamp).Sub(
		right.(*tree.DTimestamp).Time,
	).Nanoseconds()
	return &tree.DInterval{
		Duration: duration.MakeDurationJustifyHours(nanos, 0, 0),
	}, nil
}

func (e *evaluator) EvalMinusTimestampTZIntervalOp(
	_ *tree.MinusTimestampTZIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := duration.Add(
		left.(*tree.DTimestampTZ).Time.In(e.ctx().GetLocation()),
		right.(*tree.DInterval).Duration.Mul(-1),
	)
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func (e *evaluator) EvalMinusTimestampTZOp(
	_ *tree.MinusTimestampTZOp, left, right tree.Datum,
) (tree.Datum, error) {
	nanos := left.(*tree.DTimestampTZ).Sub(
		right.(*tree.DTimestampTZ).Time,
	).Nanoseconds()
	return &tree.DInterval{
		Duration: duration.MakeDurationJustifyHours(nanos, 0, 0),
	}, nil
}

func (e *evaluator) EvalMinusTimestampTZTimestampOp(
	_ *tree.MinusTimestampTZTimestampOp, left, right tree.Datum,
) (tree.Datum, error) {
	// These two quantities aren't directly comparable. Convert the
	// TimestampTZ to a timestamp first.
	stripped, err := left.(*tree.DTimestampTZ).
		EvalAtTimeZone(e.ctx().GetLocation())
	if err != nil {
		return nil, err
	}
	nanos := stripped.Sub(right.(*tree.DTimestamp).Time).Nanoseconds()
	return &tree.DInterval{
		Duration: duration.MakeDurationJustifyHours(nanos, 0, 0),
	}, nil
}

func (e *evaluator) EvalMinusTimestampTimestampTZOp(
	_ *tree.MinusTimestampTimestampTZOp, left, right tree.Datum,
) (tree.Datum, error) {
	// These two quantities aren't directly comparable. Convert the
	// TimestampTZ to a timestamp first.
	stripped, err := right.(*tree.DTimestampTZ).
		EvalAtTimeZone(e.ctx().GetLocation())
	if err != nil {
		return nil, err
	}
	nanos := left.(*tree.DTimestamp).Sub(stripped.Time).Nanoseconds()
	return &tree.DInterval{
		Duration: duration.MakeDurationJustifyHours(nanos, 0, 0),
	}, nil
}

func (e *evaluator) EvalModDecimalIntOp(
	_ *tree.ModDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	if r == 0 {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalModDecimalOp(
	_ *tree.ModDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	if r.IsZero() {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalModFloatOp(_ *tree.ModFloatOp, left, right tree.Datum) (tree.Datum, error) {
	l := float64(*left.(*tree.DFloat))
	r := float64(*right.(*tree.DFloat))
	if r == 0.0 {
		return nil, tree.ErrDivByZero
	}
	return tree.NewDFloat(tree.DFloat(math.Mod(l, r))), nil
}

func (e *evaluator) EvalModIntDecimalOp(
	_ *tree.ModIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	if r.IsZero() {
		return nil, tree.ErrDivByZero
	}
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalModIntOp(_ *tree.ModIntOp, left, right tree.Datum) (tree.Datum, error) {
	r := tree.MustBeDInt(right)
	if r == 0 {
		return nil, tree.ErrDivByZero
	}
	return tree.NewDInt(tree.MustBeDInt(left) % r), nil
}

func (e *evaluator) EvalModStringOp(
	_ *tree.ModStringOp, left, right tree.Datum,
) (tree.Datum, error) {
	l, r := tree.MustBeDString(left), tree.MustBeDString(right)
	f := trigram.Similarity(string(l), string(r))
	return tree.MakeDBool(f >= e.ctx().SessionData().TrigramSimilarityThreshold), nil
}

func (e *evaluator) EvalMultDecimalIntOp(
	_ *tree.MultDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.ExactCtx.Mul(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalMultDecimalIntervalOp(
	_ *tree.MultDecimalIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	t, err := l.Float64()
	if err != nil {
		return nil, err
	}
	return &tree.DInterval{
		Duration: right.(*tree.DInterval).Duration.MulFloat(t),
	}, nil
}

func (e *evaluator) EvalMultDecimalOp(
	_ *tree.MultDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	_, err := tree.ExactCtx.Mul(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalMultFloatIntervalOp(
	_ *tree.MultFloatIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := float64(*left.(*tree.DFloat))
	return &tree.DInterval{
		Duration: right.(*tree.DInterval).Duration.MulFloat(l),
	}, nil
}

func (e *evaluator) EvalMultFloatOp(
	_ *tree.MultFloatOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.NewDFloat(*left.(*tree.DFloat) * *right.(*tree.DFloat)), nil
}

func (e *evaluator) EvalMultIntDecimalOp(
	_ *tree.MultIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.ExactCtx.Mul(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalMultIntIntervalOp(
	_ *tree.MultIntIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	return &tree.DInterval{
		Duration: right.(*tree.DInterval).Duration.
			Mul(int64(tree.MustBeDInt(left))),
	}, nil
}

func (e *evaluator) EvalMultIntOp(_ *tree.MultIntOp, left, right tree.Datum) (tree.Datum, error) {
	// See Rob Pike's implementation from
	// https://groups.google.com/d/msg/golang-nuts/h5oSN5t3Au4/KaNQREhZh0QJ

	a, b := tree.MustBeDInt(left), tree.MustBeDInt(right)
	c := a * b
	if a == 0 || b == 0 || a == 1 || b == 1 {
		// ignore
	} else if a == math.MinInt64 || b == math.MinInt64 {
		// This test is required to detect math.MinInt64 * -1.
		return nil, tree.ErrIntOutOfRange
	} else if c/b != a {
		return nil, tree.ErrIntOutOfRange
	}
	return tree.NewDInt(c), nil
}

func (e *evaluator) EvalMultIntervalDecimalOp(
	_ *tree.MultIntervalDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	r := &right.(*tree.DDecimal).Decimal
	t, err := r.Float64()
	if err != nil {
		return nil, err
	}
	return &tree.DInterval{
		Duration: left.(*tree.DInterval).Duration.MulFloat(t),
	}, nil
}

func (e *evaluator) EvalMultIntervalFloatOp(
	_ *tree.MultIntervalFloatOp, left, right tree.Datum,
) (tree.Datum, error) {
	r := float64(*right.(*tree.DFloat))
	return &tree.DInterval{
		Duration: left.(*tree.DInterval).Duration.MulFloat(r),
	}, nil
}

func (e *evaluator) EvalMultIntervalIntOp(
	_ *tree.MultIntervalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	return &tree.DInterval{
		Duration: left.(*tree.DInterval).Duration.
			Mul(int64(tree.MustBeDInt(right))),
	}, nil
}

func (e *evaluator) EvalOverlapsArrayOp(
	_ *tree.OverlapsArrayOp, left, right tree.Datum,
) (tree.Datum, error) {
	array := tree.MustBeDArray(left)
	other := tree.MustBeDArray(right)
	return tree.ArrayOverlaps(e.ctx(), array, other)
}

func (e *evaluator) EvalOverlapsINetOp(
	_ *tree.OverlapsINetOp, left, right tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(left).IPAddr
	other := tree.MustBeDIPAddr(right).IPAddr
	return tree.MakeDBool(tree.DBool(ipAddr.ContainsOrContainedBy(&other))), nil
}

func (e *evaluator) EvalPlusDateIntOp(
	_ *tree.PlusDateIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	d, err := left.(*tree.DDate).AddDays(int64(tree.MustBeDInt(right)))
	if err != nil {
		return nil, err
	}
	return tree.NewDDate(d), nil
}

func (e *evaluator) EvalPlusDateIntervalOp(
	_ *tree.PlusDateIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	leftTime, err := left.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := duration.Add(leftTime, right.(*tree.DInterval).Duration)
	return tree.MakeDTimestamp(t, time.Microsecond)
}

func (e *evaluator) EvalPlusDateTimeOp(
	_ *tree.PlusDateTimeOp, left, right tree.Datum,
) (tree.Datum, error) {
	leftTime, err := left.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := time.Duration(*right.(*tree.DTime)) * time.Microsecond
	return tree.MakeDTimestamp(leftTime.Add(t), time.Microsecond)
}

func (e *evaluator) EvalPlusDateTimeTZOp(
	_ *tree.PlusDateTimeTZOp, left, right tree.Datum,
) (tree.Datum, error) {
	leftTime, err := left.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := leftTime.Add(right.(*tree.DTimeTZ).ToDuration())
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func (e *evaluator) EvalPlusDecimalIntOp(
	_ *tree.PlusDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.ExactCtx.Add(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalPlusDecimalOp(
	_ *tree.PlusDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	_, err := tree.ExactCtx.Add(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalPlusFloatOp(
	_ *tree.PlusFloatOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.NewDFloat(*left.(*tree.DFloat) + *right.(*tree.DFloat)), nil
}

func (e *evaluator) EvalPlusINetIntOp(
	_ *tree.PlusINetIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(left).IPAddr
	i := tree.MustBeDInt(right)
	newIPAddr, err := ipAddr.Add(int64(i))
	return tree.NewDIPAddr(tree.DIPAddr{IPAddr: newIPAddr}), err
}

func (e *evaluator) EvalPlusIntDecimalOp(
	_ *tree.PlusIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.ExactCtx.Add(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalPlusIntDateOp(
	_ *tree.PlusIntDateOp, left, right tree.Datum,
) (tree.Datum, error) {
	d, err := right.(*tree.DDate).AddDays(int64(tree.MustBeDInt(left)))
	if err != nil {
		return nil, err
	}
	return tree.NewDDate(d), nil
}

func (e *evaluator) EvalPlusIntINetOp(
	_ *tree.PlusIntINetOp, left, right tree.Datum,
) (tree.Datum, error) {
	i := tree.MustBeDInt(left)
	ipAddr := tree.MustBeDIPAddr(right).IPAddr
	newIPAddr, err := ipAddr.Add(int64(i))
	return tree.NewDIPAddr(tree.DIPAddr{IPAddr: newIPAddr}), err
}

func (e *evaluator) EvalPlusIntOp(_ *tree.PlusIntOp, left, right tree.Datum) (tree.Datum, error) {
	a, b := tree.MustBeDInt(left), tree.MustBeDInt(right)
	r, ok := arith.AddWithOverflow(int64(a), int64(b))
	if !ok {
		return nil, tree.ErrIntOutOfRange
	}
	return tree.NewDInt(tree.DInt(r)), nil
}

func (e *evaluator) EvalPlusIntervalDateOp(
	_ *tree.PlusIntervalDateOp, left, right tree.Datum,
) (tree.Datum, error) {
	rightTime, err := right.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := duration.Add(rightTime, left.(*tree.DInterval).Duration)
	return tree.MakeDTimestamp(t, time.Microsecond)
}

func (e *evaluator) EvalPlusIntervalOp(
	_ *tree.PlusIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	return &tree.DInterval{
		Duration: left.(*tree.DInterval).Duration.Add(
			right.(*tree.DInterval).Duration,
		),
	}, nil
}

func (e *evaluator) EvalPlusIntervalTimeOp(
	_ *tree.PlusIntervalTimeOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := timeofday.TimeOfDay(*right.(*tree.DTime))
	return tree.MakeDTime(t.Add(left.(*tree.DInterval).Duration)), nil
}

func (e *evaluator) EvalPlusIntervalTimeTZOp(
	_ *tree.PlusIntervalTimeTZOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := right.(*tree.DTimeTZ)
	d := left.(*tree.DInterval).Duration
	return tree.NewDTimeTZFromOffset(t.Add(d), t.OffsetSecs), nil
}

func (e *evaluator) EvalPlusIntervalTimestampOp(
	_ *tree.PlusIntervalTimestampOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.MakeDTimestamp(duration.Add(
		right.(*tree.DTimestamp).Time, left.(*tree.DInterval).Duration,
	), time.Microsecond)
}

func (e *evaluator) EvalPlusIntervalTimestampTZOp(
	_ *tree.PlusIntervalTimestampTZOp, left, right tree.Datum,
) (tree.Datum, error) {
	// Convert time to be in the given timezone, as math relies on matching timezones..
	t := duration.Add(
		right.(*tree.DTimestampTZ).Time.In(e.ctx().GetLocation()),
		left.(*tree.DInterval).Duration,
	)
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func (e *evaluator) EvalPlusTimeDateOp(
	_ *tree.PlusTimeDateOp, left, right tree.Datum,
) (tree.Datum, error) {
	rightTime, err := right.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := time.Duration(*left.(*tree.DTime)) * time.Microsecond
	return tree.MakeDTimestamp(rightTime.Add(t), time.Microsecond)
}

func (e *evaluator) EvalPlusTimeIntervalOp(
	_ *tree.PlusTimeIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := timeofday.TimeOfDay(*left.(*tree.DTime))
	return tree.MakeDTime(t.Add(right.(*tree.DInterval).Duration)), nil
}

func (e *evaluator) EvalPlusTimeTZDateOp(
	_ *tree.PlusTimeTZDateOp, left, right tree.Datum,
) (tree.Datum, error) {
	rightTime, err := right.(*tree.DDate).ToTime()
	if err != nil {
		return nil, err
	}
	t := rightTime.Add(left.(*tree.DTimeTZ).ToDuration())
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func (e *evaluator) EvalPlusTimeTZIntervalOp(
	_ *tree.PlusTimeTZIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	t := left.(*tree.DTimeTZ)
	d := right.(*tree.DInterval).Duration
	return tree.NewDTimeTZFromOffset(t.Add(d), t.OffsetSecs), nil
}

func (e *evaluator) EvalPlusTimestampIntervalOp(
	_ *tree.PlusTimestampIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.MakeDTimestamp(
		duration.Add(
			left.(*tree.DTimestamp).Time, right.(*tree.DInterval).Duration,
		),
		time.Microsecond,
	)
}

func (e *evaluator) EvalPlusTimestampTZIntervalOp(
	_ *tree.PlusTimestampTZIntervalOp, left, right tree.Datum,
) (tree.Datum, error) {
	// Convert time to be in the given timezone, as math relies on matching timezones..
	t := duration.Add(
		left.(*tree.DTimestampTZ).Time.In(e.ctx().GetLocation()),
		right.(*tree.DInterval).Duration,
	)
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func (e *evaluator) EvalPowDecimalIntOp(
	_ *tree.PowDecimalIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := tree.MustBeDInt(right)
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(r))
	_, err := tree.DecimalCtx.Pow(&dd.Decimal, l, &dd.Decimal)
	return dd, err
}

func (e *evaluator) EvalPowDecimalOp(
	_ *tree.PowDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := &left.(*tree.DDecimal).Decimal
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	_, err := tree.DecimalCtx.Pow(&dd.Decimal, l, r)
	return dd, err
}

func (e *evaluator) EvalPowFloatOp(_ *tree.PowFloatOp, left, right tree.Datum) (tree.Datum, error) {
	f := math.Pow(float64(*left.(*tree.DFloat)), float64(*right.(*tree.DFloat)))
	return tree.NewDFloat(tree.DFloat(f)), nil
}

func (e *evaluator) EvalPowIntDecimalOp(
	_ *tree.PowIntDecimalOp, left, right tree.Datum,
) (tree.Datum, error) {
	l := tree.MustBeDInt(left)
	r := &right.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	dd.SetInt64(int64(l))
	_, err := tree.DecimalCtx.Pow(&dd.Decimal, &dd.Decimal, r)
	return dd, err
}

func (e *evaluator) EvalPowIntOp(_ *tree.PowIntOp, left, right tree.Datum) (tree.Datum, error) {
	return IntPow(tree.MustBeDInt(left), tree.MustBeDInt(right))
}

func (e *evaluator) EvalPrependToMaybeNullArrayOp(
	op *tree.PrependToMaybeNullArrayOp, left, right tree.Datum,
) (tree.Datum, error) {
	return tree.PrependToMaybeNullArray(op.Typ, left, right)
}

func (e *evaluator) EvalRShiftINetOp(
	_ *tree.RShiftINetOp, left, right tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(left).IPAddr
	other := tree.MustBeDIPAddr(right).IPAddr
	return tree.MakeDBool(tree.DBool(ipAddr.Contains(&other))), nil
}

func (e *evaluator) EvalRShiftIntOp(
	_ *tree.RShiftIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	rval := tree.MustBeDInt(right)
	if rval < 0 || rval >= 64 {
		telemetry.Inc(sqltelemetry.LargeRShiftArgumentCounter)
		return nil, tree.ErrShiftArgOutOfRange
	}
	return tree.NewDInt(tree.MustBeDInt(left) >> uint(rval)), nil
}

func (e *evaluator) EvalRShiftVarBitIntOp(
	_ *tree.RShiftVarBitIntOp, left, right tree.Datum,
) (tree.Datum, error) {
	lhs := tree.MustBeDBitArray(left)
	rhs := tree.MustBeDInt(right)
	return &tree.DBitArray{
		BitArray: lhs.BitArray.LeftShiftAny(-int64(rhs)),
	}, nil
}

func (e *evaluator) EvalSimilarToOp(
	op *tree.SimilarToOp, left, right tree.Datum,
) (tree.Datum, error) {
	key := similarToKey{s: string(tree.MustBeDString(right)), escape: '\\'}
	return matchRegexpWithKey(e.ctx(), left, key)
}
