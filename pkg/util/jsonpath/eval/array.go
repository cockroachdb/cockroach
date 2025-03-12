// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/errors"
)

func (ctx *jsonpathCtx) evalArrayWildcard(current []tree.DJSON) ([]tree.DJSON, error) {
	var agg []tree.DJSON
	for _, res := range current {
		if res.JSON.Type() == json.ArrayJSONType {
			paths, err := json.AllPathsWithDepth(res.JSON, 1)
			if err != nil {
				return nil, err
			}
			for _, path := range paths {
				if path.Len() != 1 {
					return nil, errors.AssertionFailedf("unexpected path length")
				}
				unwrapped, err := path.FetchValIdx(0)
				if err != nil {
					return nil, err
				}
				if unwrapped == nil {
					return nil, errors.AssertionFailedf("unwrapping json element")
				}
				agg = append(agg, *ctx.a.NewDJSON(tree.DJSON{JSON: unwrapped}))
			}
		} else if !ctx.strict {
			agg = append(agg, res)
		} else {
			return nil, pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath wildcard array accessor can only be applied to an array")
		}
	}
	return agg, nil
}

func (ctx *jsonpathCtx) evalArrayList(
	a jsonpath.ArrayList, current []tree.DJSON,
) ([]tree.DJSON, error) {
	var agg []tree.DJSON
	for _, path := range a {
		var from, to int
		var err error
		if idxRange, ok := path.(jsonpath.ArrayIndexRange); ok {
			from, err = ctx.resolveArrayIndex(idxRange.Start, current)
			if err != nil {
				return nil, err
			}
			to, err = ctx.resolveArrayIndex(idxRange.End, current)
			if err != nil {
				return nil, err
			}
		} else {
			from, err = ctx.resolveArrayIndex(path, current)
			if err != nil {
				return nil, err
			}
			to = from
		}

		for _, res := range current {
			if ctx.strict && res.JSON.Type() != json.ArrayJSONType {
				return nil, pgerror.Newf(pgcode.SQLJSONArrayNotFound,
					"jsonpath array accessor can only be applied to an array")
			}
			length := res.JSON.Len()
			if res.JSON.Type() != json.ArrayJSONType {
				length = 1
			}
			if ctx.strict && (from < 0 || from > to || to >= length) {
				return nil, pgerror.Newf(pgcode.InvalidSQLJSONSubscript,
					"jsonpath array subscript is out of bounds")
			}
			for i := max(from, 0); i <= min(to, length-1); i++ {
				j, err := jsonArrayValueAtIndex(ctx, res.JSON, i)
				if err != nil {
					return nil, err
				}
				if j == nil {
					continue
				}
				agg = append(agg, *ctx.a.NewDJSON(tree.DJSON{JSON: j}))
			}
		}
	}
	return agg, nil
}

func (ctx *jsonpathCtx) resolveArrayIndex(p jsonpath.Path, current []tree.DJSON) (int, error) {
	results, err := ctx.eval(p, current)
	if err != nil {
		return 0, err
	}
	if len(results) != 1 || results[0].JSON.Type() != json.NumberJSONType {
		return -1, pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is not a single numeric value")
	}
	i, err := asInt(results[0].JSON)
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

func jsonArrayValueAtIndex(ctx *jsonpathCtx, j json.JSON, index int) (json.JSON, error) {
	if ctx.strict && j.Type() != json.ArrayJSONType {
		return nil, pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath array accessor can only be applied to an array")
	} else if j.Type() != json.ArrayJSONType {
		if index == 0 {
			return j, nil
		}
		return nil, nil
	}

	if ctx.strict && index >= j.Len() {
		return nil, pgerror.Newf(pgcode.InvalidSQLJSONSubscript, "jsonpath array subscript is out of bounds")
	}
	if index < 0 {
		// Shouldn't happen, not supported in parser.
		return nil, errors.AssertionFailedf("negative array index")
	}
	return j.FetchValIdx(index)
}
