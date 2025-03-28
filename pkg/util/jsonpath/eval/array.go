// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/errors"
)

func (ctx *jsonpathCtx) evalArrayWildcard(jsonValue json.JSON) ([]json.JSON, error) {
	if jsonValue.Type() == json.ArrayJSONType {
		// Do not evaluate any paths, just unwrap the current target.
		return ctx.unwrapCurrentTargetAndEval(nil /* jsonPath */, jsonValue, !ctx.strict /* unwrapNext */)
	} else if !ctx.strict {
		return []json.JSON{jsonValue}, nil
	} else {
		return nil, pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath wildcard array accessor can only be applied to an array")
	}
}

func (ctx *jsonpathCtx) evalArrayList(
	arrayList jsonpath.ArrayList, jsonValue json.JSON,
) ([]json.JSON, error) {
	if ctx.strict && jsonValue.Type() != json.ArrayJSONType {
		return nil, pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath array accessor can only be applied to an array")
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
				return nil, err
			}
			to, err = ctx.resolveArrayIndex(idxRange.End, jsonValue)
			if err != nil {
				return nil, err
			}
		} else {
			from, err = ctx.resolveArrayIndex(idxAccessor, jsonValue)
			if err != nil {
				return nil, err
			}
			to = from
		}

		if ctx.strict && (from < 0 || from > to || to >= length) {
			return nil, pgerror.Newf(pgcode.InvalidSQLJSONSubscript,
				"jsonpath array subscript is out of bounds")
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
		// TODO(normanchenn): this check should be done during jsonpath parsing.
		return nil, pgerror.Newf(pgcode.Syntax, "LAST is allowed only in array subscripts")
	}

	lastIndex := ctx.innermostArrayLength - 1
	return []json.JSON{json.FromInt(lastIndex)}, nil
}

func (ctx *jsonpathCtx) resolveArrayIndex(
	jsonPath jsonpath.Path, jsonValue json.JSON,
) (int, error) {
	evalResults, err := ctx.eval(jsonPath, jsonValue, !ctx.strict /* unwrap */)
	if err != nil {
		return 0, err
	}
	if len(evalResults) != 1 || evalResults[0].Type() != json.NumberJSONType {
		return -1, pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is not a single numeric value")
	}
	i, err := asInt(evalResults[0])
	if err != nil {
		return -1, pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is not a single numeric value")
	}
	return i, nil
}

func asInt(j json.JSON) (int, error) {
	d, ok := j.AsDecimal()
	if !ok {
		return 0, errInternal
	}
	i64, err := d.Int64()
	if err != nil {
		return 0, err
	}
	return int(i64), nil
}

func jsonArrayValueAtIndex(ctx *jsonpathCtx, jsonValue json.JSON, index int) (json.JSON, error) {
	if ctx.strict && jsonValue.Type() != json.ArrayJSONType {
		return nil, pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath array accessor can only be applied to an array")
	} else if jsonValue.Type() != json.ArrayJSONType {
		if index == 0 {
			return jsonValue, nil
		}
		return nil, nil
	}

	if ctx.strict && index >= jsonValue.Len() {
		return nil, pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is out of bounds")
	}
	if index < 0 {
		// Shouldn't happen, would have been caught above.
		return nil, errors.AssertionFailedf("negative array index")
	}
	return jsonValue.FetchValIdx(index)
}
