// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser"
	"github.com/cockroachdb/errors"
)

var errUnimplemented = unimplemented.NewWithIssue(22513, "unimplemented")
var errInternal = errors.New("internal error")

type jsonpathCtx struct {
	// Root of the given JSON object ($). We store this because we will need to
	// support queries with multiple root elements (ex. $.a ? ($.b == "hello").
	root   tree.DJSON
	vars   tree.DJSON
	strict bool
	a      tree.DatumAlloc
}

func JsonpathQuery(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) ([]tree.DJSON, error) {
	parsedPath, err := parser.Parse(string(path))
	if err != nil {
		return []tree.DJSON{}, err
	}
	expr := parsedPath.AST

	ctx := &jsonpathCtx{
		root:   target,
		vars:   vars,
		strict: expr.Strict,
	}

	// When silent is true, overwrite the strict mode.
	if bool(silent) {
		ctx.strict = false
	}

	res, err := ctx.eval(expr.Path, []tree.DJSON{target})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (ctx *jsonpathCtx) eval(jp jsonpath.Path, current []tree.DJSON) ([]tree.DJSON, error) {
	switch p := jp.(type) {
	case jsonpath.Paths:
		// Evaluate each path within the path list, update the current JSON
		// object after each evaluation.
		for _, path := range p {
			results, err := ctx.eval(path, current)
			if err != nil {
				return nil, err
			}
			current = results
		}
		return current, nil
	case jsonpath.Root:
		return []tree.DJSON{ctx.root}, nil
	case jsonpath.Key:
		var agg []tree.DJSON
		for _, res := range current {
			if res.JSON.Type() == json.ObjectJSONType {
				val, err := res.JSON.FetchValKey(string(p))
				if err != nil {
					return nil, err
				}
				if val == nil {
					if ctx.strict {
						return nil, pgerror.Newf(pgcode.KeyNotInJSON, "JSON object does not contain key %q", string(p))
					}
					continue
				}
				agg = append(agg, *ctx.a.NewDJSON(tree.DJSON{JSON: val}))
			} else if !ctx.strict && res.JSON.Type() == json.ArrayJSONType {
				arr, ok := res.JSON.AsArray()
				if !ok {
					return nil, errors.AssertionFailedf("array expected")
				}
				for _, elem := range arr {
					results, err := ctx.eval(p, []tree.DJSON{*ctx.a.NewDJSON(tree.DJSON{JSON: elem})})
					if err != nil {
						return nil, err
					}
					agg = append(agg, results...)
				}
			} else if ctx.strict {
				return nil, pgerror.Newf(pgcode.KeyNotInJSON, "jsonpath member accessor can only be applied to an object")
			}
		}
		return agg, nil
	case jsonpath.Wildcard:
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
				return nil, pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath wildcard array accessor can only be applied to an array")
			}
		}
		return agg, nil
	case jsonpath.ArrayList:
		var agg []tree.DJSON
		for _, path := range p {
			// path should be ArrayIndex or ArrayIndexRange.
			results, err := ctx.eval(path, current)
			if err != nil {
				return nil, err
			}
			// Aggregate results of each array index accessor.
			agg = append(agg, results...)
		}
		return agg, nil
	case jsonpath.ArrayIndex:
		var agg []tree.DJSON
		idx, err := ctx.resolveArrayIndex(jsonpath.Scalar(p))
		if err != nil {
			return nil, err
		}
		for _, res := range current {
			if ctx.strict && res.JSON.Type() != json.ArrayJSONType {
				return nil, pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath array accessor can only be applied to an array")
			}
			djson, err := ctx.getJSONArrayValueAtIndex(res, idx)
			if err != nil {
				return nil, err
			}
			if djson == nil {
				continue
			}
			agg = append(agg, *djson)
		}
		return agg, nil
	case jsonpath.ArrayIndexRange:
		var agg []tree.DJSON
		start, err := ctx.resolveArrayIndex(jsonpath.Scalar(p.Start))
		if err != nil {
			return nil, err
		}
		end, err := ctx.resolveArrayIndex(jsonpath.Scalar(p.End))
		if err != nil {
			return nil, err
		}
		for _, res := range current {
			// Return array errors here before checking range bounds.
			if ctx.strict && res.JSON.Type() != json.ArrayJSONType {
				return nil, pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath array accessor can only be applied to an array")
			}
			if ctx.strict && start > end {
				return nil, pgerror.Newf(pgcode.ArraySubscriptOutOfBounds, "jsonpath array subscript is out of bounds")
			}
			for i := start; i <= end; i++ {
				djson, err := ctx.getJSONArrayValueAtIndex(res, i)
				if err != nil {
					return nil, err
				}
				if djson == nil {
					continue
				}
				agg = append(agg, *djson)
			}
		}
		return agg, nil
	case jsonpath.Scalar:
		return nil, errors.AssertionFailedf("scalar evaluation shouldn't occur")
	default:
		return nil, errUnimplemented
	}
}

func (ctx *jsonpathCtx) resolveArrayIndex(s jsonpath.Scalar) (int, error) {
	// TODO(normanchenn): support floats.
	if s.Type == jsonpath.ScalarFloat {
		return 0, errors.AssertionFailedf("floats not supported yet")
	}
	resolved, err := ctx.resolveScalar(s)
	if err != nil {
		return 0, err
	}
	i, err := asInt(resolved)
	if err != nil {
		return 0, pgerror.Newf(pgcode.ArraySubscriptOutOfBounds, "jsonpath array subscript is not a single numeric value")
	}
	return i, nil
}

func (ctx *jsonpathCtx) resolveScalar(s jsonpath.Scalar) (tree.DJSON, error) {
	if s.Type == jsonpath.ScalarVariable {
		val, err := ctx.vars.FetchValKey(s.Variable)
		if err != nil {
			return tree.DJSON{}, err
		}
		if val == nil {
			return tree.DJSON{}, pgerror.Newf(pgcode.UndefinedObject, "could not find jsonpath variable %q", s.Variable)
		}
		return *ctx.a.NewDJSON(tree.DJSON{JSON: val}), nil
	}
	return *ctx.a.NewDJSON(tree.DJSON{JSON: s.Value}), nil
}

func asInt(j tree.DJSON) (int, error) {
	d, ok := j.JSON.AsDecimal()
	if !ok {
		return 0, errInternal
	}
	i64, err := d.Int64()
	if err != nil {
		return 0, err
	}
	return int(i64), nil
}

func (ctx *jsonpathCtx) getJSONArrayValueAtIndex(res tree.DJSON, index int) (*tree.DJSON, error) {
	if ctx.strict && res.JSON.Type() != json.ArrayJSONType {
		return nil, pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath array accessor can only be applied to an array")
	} else if res.JSON.Type() != json.ArrayJSONType {
		if index == 0 {
			return &res, nil
		}
		return nil, nil
	}

	if ctx.strict && index >= res.JSON.Len() {
		return nil, pgerror.Newf(pgcode.ArraySubscriptOutOfBounds, "jsonpath array subscript is out of bounds")
	}
	if index < 0 {
		// Shouldn't happen, not supported in parser.
		return nil, errors.AssertionFailedf("negative array index")
	}

	val, err := res.JSON.FetchValIdx(index)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	return ctx.a.NewDJSON(tree.DJSON{JSON: val}), nil
}
