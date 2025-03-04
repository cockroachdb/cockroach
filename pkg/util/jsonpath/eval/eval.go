// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser"
	"github.com/cockroachdb/errors"
)

var ErrUnimplemented = errors.New("unimplemented")
var ErrInternal = errors.New("internal error")

type JsonpathCtx struct {
	// Root of the given JSON object ($). We store this because we will need to
	// support queries with multiple root elements (ex. $.a ? ($.b == "hello").
	root   tree.DJSON
	vars   tree.DJSON
	strict bool

	// We need two fields because some Path nodes update the current JSON object
	// being worked on, while others don't.

	// Current JSON objects being evaluated. This will get updated for Path nodes
	// such as key accessors, wildcards, etc. For individual array index
	// accessors, we don't update this field because we need to aggregate results
	// for each index accessor before updating the current JSON object with all
	// the results.
	current []tree.DJSON
	// Intermediate results.
	results []tree.DJSON
}

func JsonpathQuery(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) ([]tree.DJSON, error) {
	parsedPath, err := parser.Parse(string(path))
	if err != nil {
		return []tree.DJSON{}, err
	}
	expr := parsedPath.AST

	// When silent is true, overwrite the strict mode.
	if bool(silent) {
		expr.Strict = false
	}

	ctx := &JsonpathCtx{
		vars:    vars,
		root:    target,
		current: []tree.DJSON{target},
		strict:  expr.Strict,
	}

	err = ctx.Eval(expr.Path)
	if err != nil {
		return nil, err
	}
	return ctx.current, nil
}

func (ctx *JsonpathCtx) Eval(jp jsonpath.Path) error {
	switch p := jp.(type) {
	case jsonpath.Paths:
		// Evaluate each path within the path list, update the current JSON
		// object after each evaluation.
		for _, path := range p {
			err := ctx.Eval(path)
			if err != nil {
				return err
			}
			ctx.current = ctx.results
		}
		return nil
	case jsonpath.Root:
		ctx.results = []tree.DJSON{ctx.root}
		return nil
	case jsonpath.Key:
		var agg []tree.DJSON
		for _, res := range ctx.current {
			if res.JSON.Type() != json.ObjectJSONType {
				if ctx.strict {
					return pgerror.Newf(pgcode.KeyNotInJSON, "jsonpath member accessor can only be applied to an object")
				}
				continue
			}
			val, err := res.JSON.FetchValKey(string(p))
			if err != nil {
				return err
			}
			if val == nil {
				if ctx.strict {
					return pgerror.Newf(pgcode.KeyNotInJSON, "JSON object does not contain key %q", string(p))
				}
				continue
			}
			agg = append(agg, tree.DJSON{JSON: val})
		}
		ctx.results = agg
		return nil
	case jsonpath.Wildcard:
		var agg []tree.DJSON
		for _, res := range ctx.current {
			if res.JSON.Type() != json.ArrayJSONType {
				if ctx.strict {
					return pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath wildcard array accessor can only be applied to an array")
				}
				// Wildcard treated as no-op on non-array.
				agg = append(agg, res)
				continue
			}
			paths, err := json.AllPathsWithDepth(res.JSON, 1)
			if err != nil {
				return err
			}
			for _, path := range paths {
				// All paths are wrapped in an array due to the implementation of
				// json and AllPathsWithDepth.
				unwrapped, err := path.FetchValIdx(0)
				if err != nil {
					return err
				}
				if unwrapped == nil {
					// Should never happen.
					return fmt.Errorf("unwrapping json element: %w", ErrInternal)
				}
				agg = append(agg, tree.DJSON{JSON: unwrapped})
			}
		}
		ctx.results = agg
		return nil
	case jsonpath.ArrayList:
		var agg []tree.DJSON
		for _, path := range p {
			// path should be ArrayIndex or ArrayIndexRange.
			err := ctx.Eval(path)
			if err != nil {
				return err
			}
			// Aggregate results of each array index accessor.
			agg = append(agg, ctx.results...)
		}
		ctx.results = agg
		return nil
	case jsonpath.ArrayIndex:
		var agg []tree.DJSON
		for _, res := range ctx.current {
			djson, err := ctx.getJSONIndex(res, int(p.IntValue))
			if err != nil {
				return err
			}
			if djson == nil {
				continue
			}
			agg = append(agg, *djson)
		}
		ctx.results = agg
		return nil
	case jsonpath.ArrayIndexRange:
		var agg []tree.DJSON
		for _, res := range ctx.current {
			if ctx.strict && res.JSON.Type() != json.ArrayJSONType {
				return pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath array accessor can only be applied to an array")
			}
			// Custom non-postgres error message for start > end.
			if ctx.strict && int(p.Start.IntValue) > int(p.End.IntValue) {
				return pgerror.Newf(pgcode.ArraySubscriptOutOfBounds, "jsonpath array subscript start is greater than end")
			}
			if ctx.strict && (int(p.Start.IntValue) >= res.JSON.Len() || int(p.End.IntValue) >= res.JSON.Len()) {
				return pgerror.Newf(pgcode.ArraySubscriptOutOfBounds, "jsonpath array subscript is out of bounds")
			}
			if p.Start.IntValue < 0 || p.End.IntValue < 0 {
				// Shouldn't happen, not supported in parser.
				return fmt.Errorf("negative array index: %w", ErrInternal)
			}

			for i := int(p.Start.IntValue); i <= int(p.End.IntValue); i++ {
				djson, err := ctx.getJSONIndex(res, i)
				if err != nil {
					return err
				}
				if djson == nil {
					continue
				}
				agg = append(agg, *djson)
			}
		}
		ctx.results = agg
		return nil
	case jsonpath.Variable:
		return fmt.Errorf("variable evaluation not implemented: %w", ErrUnimplemented)
	case jsonpath.Numeric:
		return fmt.Errorf("numeric evaluation shouldn't occur: %w", ErrInternal)
	default:
		return ErrUnimplemented
	}
}

func (ctx *JsonpathCtx) getJSONIndex(res tree.DJSON, index int) (*tree.DJSON, error) {
	if ctx.strict && res.JSON.Type() != json.ArrayJSONType {
		return nil, pgerror.Newf(pgcode.WildcardOnNonArray, "jsonpath array accessor can only be applied to an array")
	}
	if ctx.strict && index >= res.JSON.Len() {
		return nil, pgerror.Newf(pgcode.ArraySubscriptOutOfBounds, "jsonpath array subscript is out of bounds")
	}
	if !ctx.strict && res.JSON.Type() != json.ArrayJSONType {
		// If not strict, not an array and index is 0, return the original.
		if index == 0 {
			return &res, nil
		}
		return nil, nil
	}
	if !ctx.strict && index >= res.JSON.Len() {
		return nil, nil
	}
	if index < 0 {
		// Shouldn't happen, not supported in parser.
		return nil, fmt.Errorf("negative array index: %w", ErrInternal)
	}

	val, err := res.JSON.FetchValIdx(index)
	if err != nil {
		return nil, err
	}
	if val == nil {
		// Shouldn't happen, bounds and type checked above.
		return nil, nil
	}
	return &tree.DJSON{JSON: val}, nil
}
