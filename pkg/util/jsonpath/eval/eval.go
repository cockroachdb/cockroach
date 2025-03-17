// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
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
	root   json.JSON
	vars   json.JSON
	strict bool
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
		root:   target.JSON,
		vars:   vars.JSON,
		strict: expr.Strict,
	}

	// When silent is true, overwrite the strict mode.
	if bool(silent) {
		ctx.strict = false
	}

	j, err := ctx.eval(expr.Path, []json.JSON{ctx.root})
	if err != nil {
		return nil, err
	}
	res := make([]tree.DJSON, len(j))
	for i, j := range j {
		res[i] = tree.DJSON{JSON: j}
	}
	return res, nil
}

func (ctx *jsonpathCtx) eval(jp jsonpath.Path, current []json.JSON) ([]json.JSON, error) {
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
		return []json.JSON{ctx.root}, nil
	case jsonpath.Key:
		return ctx.evalKey(p, current)
	case jsonpath.Wildcard:
		return ctx.evalArrayWildcard(current)
	case jsonpath.ArrayList:
		return ctx.evalArrayList(p, current)
	case jsonpath.Scalar:
		resolved, err := ctx.resolveScalar(p)
		if err != nil {
			return nil, err
		}
		return []json.JSON{resolved}, nil
	case jsonpath.Operation:
		return ctx.evalOperation(p, current)
	default:
		return nil, errUnimplemented
	}
}

func (ctx *jsonpathCtx) evalAndUnwrap(path jsonpath.Path, inputs []json.JSON) ([]json.JSON, error) {
	results, err := ctx.eval(path, inputs)
	if err != nil {
		return nil, err
	}
	if ctx.strict {
		return results, nil
	}
	var unwrapped []json.JSON
	for _, result := range results {
		if result.Type() == json.ArrayJSONType {
			array, _ := result.AsArray()
			unwrapped = append(unwrapped, array...)
		} else {
			unwrapped = append(unwrapped, result)
		}
	}
	return unwrapped, nil
}
