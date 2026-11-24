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
	"github.com/cockroachdb/errors"
)

var (
	errUnimplemented         = unimplemented.NewWithIssue(22513, "unimplemented")
	errSingleBooleanRequired = pgerror.Newf(pgcode.SingletonSQLJSONItemRequired, "single boolean result is expected")
)

type jsonpathCtx struct {
	// Root of the given JSON object ($). We store this because we will need to
	// support queries with multiple root elements (ex. $.a ? ($.b == "hello").
	root json.JSON
	// vars is the JSON object that contains the variables that may be used in
	// the JSONPath query. It is a JSON object that contains key-value pairs of
	// variable names to their corresponding values.
	vars json.JSON
	// strict variable is used to determine how structural errors within the
	// JSON objects are handled. If strict is true, the query will error out on
	// structural errors (ex. key accessors on arrays, key accessors on invalid
	// keys, etc.). Otherwise, the query will attempt to continue execution.
	// This is controlled by the strict or lax keywords at the start of the
	// JSONPath query.
	strict bool
	// silent variable is used to determine how errors should be thrown during
	// evaluation. If silent is true, the query will not throw most errors. If
	// silent is false, the query will throw errors such as key accessors in
	// strict mode on invalid keys. However, if silent is true, the query will
	// return nothing. This is controlled by the optional silent variable in
	// jsonb_path_* builtin functions.
	silent bool

	// innermostArrayLength stores the length of the innermost array. If the current
	// evaluation context is not evaluating on an array, this value is -1.
	innermostArrayLength int
}

// maybeThrowError should only be called for suppresible errors via ctx.silent.
func maybeThrowError(ctx *jsonpathCtx, err error) error {
	if ctx.silent {
		return nil
	}
	return err
}

func JsonpathQuery(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) ([]tree.DJSON, error) {
	j, err := jsonpathQuery(target, path, vars, silent)
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
	j, err := jsonpathQuery(target, path, vars, silent)
	if err != nil {
		return false, err
	}
	return len(j) > 0, nil
}

func JsonpathQueryArray(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) (tree.DJSON, error) {
	j, err := jsonpathQuery(target, path, vars, silent)
	if err != nil {
		return tree.DJSON{}, err
	}

	b := json.NewArrayBuilder(len(j))
	for _, j := range j {
		b.Add(j)
	}
	return tree.DJSON{JSON: b.Build()}, nil
}

func JsonpathQueryFirst(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) (tree.Datum, error) {
	j, err := jsonpathQuery(target, path, vars, silent)
	if err != nil {
		return nil, err
	}
	if len(j) == 0 {
		return tree.DNull, nil
	}
	return &tree.DJSON{JSON: j[0]}, nil
}

func JsonpathMatch(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) (tree.Datum, error) {
	j, err := jsonpathQuery(target, path, vars, silent)
	if err != nil {
		return nil, err
	}

	if len(j) == 1 {
		if b, ok := j[0].AsBool(); ok {
			return tree.MakeDBool(tree.DBool(b)), nil
		}
		if j[0].Type() == json.NullJSONType {
			return tree.DNull, nil
		}
	}
	if !silent {
		return nil, errSingleBooleanRequired
	}
	return tree.DNull, nil
}

func jsonpathQuery(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) ([]json.JSON, error) {
	expr := path.Jsonpath
	ctx := &jsonpathCtx{
		root:                 target.JSON,
		vars:                 vars.JSON,
		strict:               expr.Strict,
		silent:               bool(silent),
		innermostArrayLength: -1,
	}
	return ctx.eval(expr.Path, ctx.root, !ctx.strict /* unwrap */)
}

// eval evaluates a JSONPath expression against a JSON value and returns a
// slice of results.
//
// Return value semantics are critical for proper JSONPath behavior:
//   - nil slice: Path evaluation failed or path does not exist (e.g., $.nonexistent)
//     In comparisons: returns unknown/null in strict mode, false in lax mode
//   - Empty slice ([]json.JSON{}): Path exists but contains no items (e.g., empty array [])
//     In comparisons: returns false in lax mode (no items to compare).
//   - Non-empty slice: Path found one or more matching items.
//
// This distinction is essential for JSONPath comparison operations to match
// PostgreSQL behavior.
//
// Many of jsonpath operations require automatic unwrapping of arrays in lax
// mode. If the input value is an array the operation is performed not on the
// array itself, but on all of its members one by one. The unwrap parameter
// indicates whether array unwrapping is needed.
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
	case jsonpath.Method:
		return ctx.evalMethod(path, jsonValue, unwrap)
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
	if jsonValue.Len() == 0 {
		// Return empty slice (not nil) to indicate "empty array found" vs "path not found".
		return []json.JSON{}, nil
	}
	var agg []json.JSON
	processItem := func(item json.JSON) error {
		if jsonPath == nil {
			agg = append(agg, item)
			return nil
		}
		evalResults, err := ctx.eval(jsonPath, item, unwrapNext)
		if err != nil {
			return err
		}
		agg = append(agg, evalResults...)
		return nil
	}
	switch jsonValue.Type() {
	case json.ArrayJSONType:
		for i := 0; i < jsonValue.Len(); i++ {
			item, err := jsonValue.FetchValIdx(i)
			if err != nil {
				return nil, err
			}
			if item == nil {
				return nil, errors.AssertionFailedf("fetching json array element at index %d", i)
			}
			if err = processItem(item); err != nil {
				return nil, err
			}
		}
	case json.ObjectJSONType:
		iter, err := jsonValue.ObjectIter()
		if err != nil {
			return nil, errors.Wrapf(err, "getting iterator for json object")
		}
		for iter.Next() {
			if err := processItem(iter.Value()); err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.AssertionFailedf("executeAnyItem called with type: %s", jsonValue.Type())
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
		// If evalResults is nil, preserve nil to indicate path evaluation
		// failure.
		if evalResults == nil {
			return nil, nil
		}
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
		// If agg is nil, return an empty slice to distinguish empty arrays
		// from missing paths. Note that agg can be nil even if evalResults is
		// non-nil if unwrapping an argument produces an empty array.
		if agg == nil {
			return []json.JSON{}, nil
		}
		return agg, nil
	}
	return evalResults, nil
}
