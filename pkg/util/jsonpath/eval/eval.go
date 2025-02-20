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

func JsonpathQuery(
	target tree.DJSON, path tree.DJsonpath, vars tree.DJSON, silent tree.DBool,
) ([]tree.DJSON, error) {
	// vars is not supported yet, as it's not implemented in the parser ($variable).
	_ = vars
	parsedPath, err := parser.Parse(string(path))
	if err != nil {
		return []tree.DJSON{}, err
	}
	expr := parsedPath.AST

	// When silent is true, overwrite the strict mode.
	if bool(silent) {
		expr.Strict = false
	}

	if len(expr.Query.Accessors) == 0 {
		return []tree.DJSON{}, fmt.Errorf("at least one accessor is guaranteed: %w", ErrInternal)
	}
	_, ok := expr.Query.Accessors[0].(jsonpath.Root)
	if !ok {
		return []tree.DJSON{}, fmt.Errorf("the first accessor is the root: %w", ErrInternal)
	}

	results := []tree.DJSON{target}
	for _, accessor := range expr.Query.Accessors[1:] {
		var nextResults []tree.DJSON
		switch a := accessor.(type) {
		case jsonpath.Key:
			for _, result := range results {
				value, err := result.JSON.FetchValKey(a.Key)
				if err != nil {
					return []tree.DJSON{}, err
				}
				// Key is not found in the JSON object.
				if value == nil {
					if expr.Strict {
						return []tree.DJSON{}, pgerror.Newf(pgcode.KeyNotInJSON,
							"JSON object does not contain key %q", a.Key)
					}
					// Do not add current value to results.
					continue
				}
				nextResults = append(nextResults, tree.DJSON{JSON: value})
			}
		case jsonpath.Wildcard:
			for _, result := range results {
				if result.JSON.Type() != json.ArrayJSONType {
					if expr.Strict {
						return []tree.DJSON{}, pgerror.Newf(pgcode.WildcardOnNonArray,
							"jsonpath wildcard array accessor can only be applied to an array")
					}
					// When not strict and non array, the wildcard is treated
					// as a no-op.
					nextResults = append(nextResults, result)
					continue
				}
				arrayElements, err := json.AllPathsWithDepth(result.JSON, 1)
				if err != nil {
					return []tree.DJSON{}, err
				}
				for _, element := range arrayElements {
					// Each element returned is wrapped in an array, so we unwrap
					// the array and add the element to the results.
					unwrapped, err := element.FetchValIdx(0)
					if err != nil {
						return []tree.DJSON{}, err
					}
					if unwrapped == nil {
						// This should never happen, as we check for array type above.
						return []tree.DJSON{}, fmt.Errorf("unwrapping json element: %w", ErrInternal)
					}
					nextResults = append(nextResults, tree.DJSON{JSON: unwrapped})
				}
			}
		default:
			return []tree.DJSON{}, ErrUnimplemented
		}
		results = nextResults
	}
	return results, nil
}
