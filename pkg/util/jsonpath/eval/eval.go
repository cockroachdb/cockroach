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

var (
	errUnimplemented = unimplemented.NewWithIssue(22513, "unimplemented")
	errInternal      = errors.New("internal error")
)

type jsonpathCtx struct {
	// Root of the given JSON object ($). We store this because we will need to
	// support queries with multiple root elements (ex. $.a ? ($.b == "hello").
	root   json.JSON
	vars   json.JSON
	strict bool

	// innermostArrayLength stores the length of the innermost array. If the current
	// evaluation context is not evaluating on an array, this value is -1.
	innermostArrayLength int
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
		root:                 target.JSON,
		vars:                 vars.JSON,
		strict:               expr.Strict,
		innermostArrayLength: -1,
	}
	// When silent is true, overwrite the strict mode.
	if bool(silent) {
		ctx.strict = false
	}

	j, err := ctx.eval(expr.Path, ctx.root, !ctx.strict /* unwrap */)
	if err != nil {
		return nil, err
	}
	res := make([]tree.DJSON, len(j))
	for i, j := range j {
		res[i] = tree.DJSON{JSON: j}
	}
	return res, nil
}

func JsonpathExists(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) (tree.DBool, error) {
	j, err := JsonpathQuery(target, path, vars, silent)
	if err != nil {
		return false, err
	}
	return len(j) > 0, nil
}

func (ctx *jsonpathCtx) eval(
	jsonPath jsonpath.Path, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	switch path := jsonPath.(type) {
	case jsonpath.Paths:
		results := []json.JSON{jsonValue}
		var err error
		for _, p := range path {
			results, err = ctx.evalArray(p, results, unwrap)
			if err != nil {
				return nil, err
			}
		}
		return results, nil
	case jsonpath.Root:
		return []json.JSON{ctx.root}, nil
	case jsonpath.Current:
		return []json.JSON{jsonValue}, nil
	case jsonpath.Key:
		return ctx.evalKey(path, jsonValue, unwrap)
	case jsonpath.AnyKey:
		return ctx.evalAnyKey(path, jsonValue, unwrap)
	case jsonpath.Wildcard:
		return ctx.evalArrayWildcard(jsonValue)
	case jsonpath.ArrayList:
		return ctx.evalArrayList(path, jsonValue)
	case jsonpath.Scalar:
		resolved, err := ctx.resolveScalar(path)
		if err != nil {
			return nil, err
		}
		return []json.JSON{resolved}, nil
	case jsonpath.Operation:
		return ctx.evalOperation(path, jsonValue)
	case jsonpath.Filter:
		return ctx.evalFilter(path, jsonValue, unwrap)
	case jsonpath.Last:
		return ctx.evalLast()
	default:
		return nil, errUnimplemented
	}
}

func (ctx *jsonpathCtx) evalArray(
	jsonPath jsonpath.Path, jsonValue []json.JSON, unwrap bool,
) ([]json.JSON, error) {
	var agg []json.JSON
	for _, j := range jsonValue {
		arr, err := ctx.eval(jsonPath, j, unwrap)
		if err != nil {
			return nil, err
		}
		agg = append(agg, arr...)
	}
	return agg, nil
}

// unwrapCurrentTargetAndEval is used to unwrap the current json array and evaluate
// the jsonpath query on each element. It is similar to executeItemUnwrapTargetArray
// in postgres/src/backend/utils/adt/jsonpath_exec.c.
func (ctx *jsonpathCtx) unwrapCurrentTargetAndEval(
	jsonPath jsonpath.Path, jsonValue json.JSON, unwrapNext bool,
) ([]json.JSON, error) {
	if jsonValue.Type() != json.ArrayJSONType {
		return nil, errors.AssertionFailedf("unwrapCurrentTargetAndEval can only be applied to an array")
	}
	return ctx.executeAnyItem(jsonPath, jsonValue, unwrapNext)
}

func (ctx *jsonpathCtx) executeAnyItem(
	jsonPath jsonpath.Path, jsonValue json.JSON, unwrapNext bool,
) ([]json.JSON, error) {
	childItems, err := json.AllPathsWithDepth(jsonValue, 1 /* depth */)
	if err != nil {
		return nil, err
	}
	var agg []json.JSON
	for _, item := range childItems {
		// The case when this will happen is if jsonValue is an empty array or empty
		// object, in which case we just skip the evaluation.
		if item.Len() == 0 {
			continue
		}
		if item.Len() != 1 {
			return nil, errors.AssertionFailedf("unexpected path length")
		}

		var unwrappedItem json.JSON
		switch item.Type() {
		case json.ArrayJSONType:
			unwrappedItem, err = item.FetchValIdx(0 /* idx */)
			if err != nil {
				return nil, err
			}
			if unwrappedItem == nil {
				return nil, errors.AssertionFailedf("unwrapping json element")
			}
		case json.ObjectJSONType:
			iter, _ := item.ObjectIter()
			// Guaranteed to have one item.
			ok := iter.Next()
			if !ok {
				return nil, errors.AssertionFailedf("unexpected empty json object")
			}
			unwrappedItem = iter.Value()
		default:
			panic(errors.AssertionFailedf("unexpected json type"))
		}
		if jsonPath == nil {
			agg = append(agg, unwrappedItem)
		} else {
			evalResults, err := ctx.eval(jsonPath, unwrappedItem, unwrapNext)
			if err != nil {
				return nil, err
			}
			agg = append(agg, evalResults...)
		}
	}
	return agg, nil
}

// evalAndUnwrapResult is used to evaluate the jsonpath query and unwrap the result
// if the unwrap flag is true. It is similar to executeItemOptUnwrapResult
// in postgres/src/backend/utils/adt/jsonpath_exec.c.
func (ctx *jsonpathCtx) evalAndUnwrapResult(
	jsonPath jsonpath.Path, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	evalResults, err := ctx.eval(jsonPath, jsonValue, !ctx.strict /* unwrap */)
	if err != nil {
		return nil, err
	}
	if unwrap && !ctx.strict {
		var agg []json.JSON
		for _, j := range evalResults {
			if j.Type() == json.ArrayJSONType {
				// Pass in nil to just unwrap the array.
				arr, err := ctx.unwrapCurrentTargetAndEval(nil /* jsonPath */, j, false /* unwrapNext */)
				if err != nil {
					return nil, err
				}
				agg = append(agg, arr...)
			} else {
				agg = append(agg, j)
			}
		}
		return agg, nil
	}
	return evalResults, nil
}
