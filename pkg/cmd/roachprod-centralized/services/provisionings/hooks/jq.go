// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"github.com/cockroachdb/errors"
	"github.com/itchyny/gojq"
)

// evaluateJQ parses and evaluates a jq expression against data, returning
// all string results. Non-string values are silently skipped. Returns an
// error if the expression is invalid, evaluation fails, or no string
// results are produced.
func evaluateJQ(expression string, data map[string]interface{}) ([]string, error) {
	query, err := gojq.Parse(expression)
	if err != nil {
		return nil, errors.Wrapf(err, "parse jq expression %q", expression)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, errors.Wrapf(err, "compile jq expression %q", expression)
	}

	var results []string
	iter := code.Run(data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			return nil, errors.Wrapf(
				err, "evaluate jq expression %q", expression,
			)
		}
		s, isString := v.(string)
		if !isString {
			continue // skip non-string values
		}
		results = append(results, s)
	}
	if len(results) == 0 {
		return nil, errors.Newf(
			"jq expression %q returned no results", expression,
		)
	}
	return results, nil
}
