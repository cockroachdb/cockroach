// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"math"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/errors"
)

var (
	errWildcardOnNonArray     = pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath wildcard array accessor can only be applied to an array")
	errIndexOnNonArray        = pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath array accessor can only be applied to an array")
	errIndexOutOfBounds       = pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is out of bounds")
	errIndexNotSingleNumValue = pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is not a single numeric value")
	errInvalidSubscript       = pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is out of integer range")
)

func (ctx *jsonpathCtx) evalArrayWildcard(jsonValue json.JSON) ([]json.JSON, error) {
	if jsonValue.Type() == json.ArrayJSONType {
		// Do not evaluate any paths, just unwrap the current target.
		return ctx.unwrapCurrentTargetAndEval(nil /* jsonPath */, jsonValue, !ctx.strict /* unwrapNext */)
	} else if !ctx.strict {
		return []json.JSON{jsonValue}, nil
	}
	return nil, maybeThrowError(ctx, errWildcardOnNonArray)
}

func (ctx *jsonpathCtx) evalArrayList(
	arrayList jsonpath.ArrayList, jsonValue json.JSON,
) ([]json.JSON, error) {
	if ctx.strict && jsonValue.Type() != json.ArrayJSONType {
		return nil, maybeThrowError(ctx, errIndexOnNonArray)
	}

	length := jsonValue.Len()
	if jsonValue.Type() != json.ArrayJSONType {
		length = 1
	}
	// Store the current length of the innermost array, and restore it after
	// the evaluation is done.
	innermostArrayLength := ctx.innermostArrayLength
	ctx.innermostArrayLength = length
	defer func() {
		ctx.innermostArrayLength = innermostArrayLength
	}()

	var agg []json.JSON
	for _, idxAccessor := range arrayList {
		var from, to int
		var err error
		if idxRange, ok := idxAccessor.(jsonpath.ArrayIndexRange); ok {
			from, err = ctx.resolveArrayIndex(idxRange.Start, jsonValue)
			if err != nil {
				return nil, maybeThrowError(ctx, err)
			}
			to, err = ctx.resolveArrayIndex(idxRange.End, jsonValue)
			if err != nil {
				return nil, maybeThrowError(ctx, err)
			}
		} else {
			from, err = ctx.resolveArrayIndex(idxAccessor, jsonValue)
			if err != nil {
				return nil, maybeThrowError(ctx, err)
			}
			to = from
		}

		if ctx.strict && (from < 0 || from > to || to >= length) {
			return nil, maybeThrowError(ctx, errIndexOutOfBounds)
		}
		for i := max(from, 0); i <= min(to, length-1); i++ {
			v, err := jsonArrayValueAtIndex(ctx, jsonValue, i)
			if err != nil {
				return nil, err
			}
			if v == nil {
				continue
			}
			agg = append(agg, v)
		}
	}
	return agg, nil
}

func (ctx *jsonpathCtx) evalLast() ([]json.JSON, error) {
	if ctx.innermostArrayLength == -1 {
		// This invariant is checked during jsonpath parsing.
		return nil, errors.AssertionFailedf("LAST is allowed only in array subscripts")
	}

	lastIndex := ctx.innermostArrayLength - 1
	return []json.JSON{json.FromInt(lastIndex)}, nil
}

// resolveArrayIndex resolves the index of the array. It returns the resolved index,
// and an error for errors. resolveArrayIndex does not suppress errors even if
// ctx.silent is set. The caller is responsible for suppressing errors if needed.
func (ctx *jsonpathCtx) resolveArrayIndex(
	jsonPath jsonpath.Path, jsonValue json.JSON,
) (int, error) {
	evalResults, err := ctx.eval(jsonPath, jsonValue, !ctx.strict /* unwrap */)
	if err != nil {
		return 0, err
	}
	if len(evalResults) != 1 || evalResults[0].Type() != json.NumberJSONType {
		return -1, errIndexNotSingleNumValue
	}
	i, err := asInt(evalResults[0])
	if err != nil {
		return -1, err
	}
	return i, nil
}

func asInt(j json.JSON) (int, error) {
	d, ok := j.AsDecimal()
	if !ok {
		return 0, errIndexNotSingleNumValue
	}

	// First, try direct conversion to int64.
	if i64, err := d.Int64(); err == nil {
		return validateInt32Range(i64)
	}

	// If direct conversion fails, truncate the non-integer towards zero.
	var err error
	dec := &apd.Decimal{}
	if d.Sign() == 1 {
		_, err = tree.ExactCtx.Floor(dec, d)
	} else {
		_, err = tree.ExactCtx.Ceil(dec, d)
	}
	if err != nil {
		return 0, errIndexNotSingleNumValue
	}

	i64, err := dec.Int64()
	if err != nil {
		return 0, errIndexNotSingleNumValue
	}
	return validateInt32Range(i64)
}

// validateInt32Range checks if the int64 value is within int32 range.
func validateInt32Range(i64 int64) (int, error) {
	if i64 < math.MinInt32 || i64 > math.MaxInt32 {
		return 0, errInvalidSubscript
	}
	return int(i64), nil
}

func jsonArrayValueAtIndex(ctx *jsonpathCtx, jsonValue json.JSON, index int) (json.JSON, error) {
	if ctx.strict && jsonValue.Type() != json.ArrayJSONType {
		return nil, errors.AssertionFailedf("jsonpath array accessor can only be applied to an array")
	} else if jsonValue.Type() != json.ArrayJSONType {
		if index == 0 {
			return jsonValue, nil
		}
		return nil, nil
	}

	if ctx.strict && index >= jsonValue.Len() {
		return nil, errors.AssertionFailedf("jsonpath array subscript is out of bounds")
	}
	if index < 0 {
		return nil, errors.AssertionFailedf("negative array index")
	}
	return jsonValue.FetchValIdx(index)
}
