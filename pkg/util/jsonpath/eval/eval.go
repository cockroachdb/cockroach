// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
		return []tree.DJSON{resolved}, nil
	default:
		return nil, errUnimplemented
	}
}
