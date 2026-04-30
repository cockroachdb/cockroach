// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"github.com/cockroachdb/errors"
	"github.com/itchyny/gojq"
)

// compileJQ parses and compiles a jq expression into executable code.
func compileJQ(expression string) (*gojq.Code, error) {
	query, err := gojq.Parse(expression)
	if err != nil {
		return nil, errors.Wrapf(err, "parse jq expression %q", expression)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, errors.Wrapf(err, "compile jq expression %q", expression)
	}
	return code, nil
}

// evaluateJQ parses and evaluates a jq expression against data, returning
// all string results. Non-string values are silently skipped. Returns an
// error if the expression is invalid, evaluation fails, or no string
// results are produced.
func evaluateJQ(expression string, data map[string]interface{}) ([]string, error) {
	code, err := compileJQ(expression)
	if err != nil {
		return nil, err
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

// evaluateJQSingle evaluates a jq expression and requires exactly one string
// result. Returning multiple values is treated as an error to avoid ambiguity.
func evaluateJQSingle(expression string, data map[string]interface{}) (string, error) {
	code, err := compileJQ(expression)
	if err != nil {
		return "", err
	}

	iter := code.Run(data)
	v, ok := iter.Next()
	if !ok {
		return "", errors.Newf("jq expression %q returned no results", expression)
	}
	if err, isErr := v.(error); isErr {
		return "", errors.Wrapf(err, "evaluate jq expression %q", expression)
	}
	s, ok := v.(string)
	if !ok {
		return "", errors.Newf(
			"jq expression %q returned %T, expected string", expression, v,
		)
	}

	// Ensure there isn't a second result.
	if next, hasNext := iter.Next(); hasNext {
		if err, isErr := next.(error); isErr {
			return "", errors.Wrapf(err, "evaluate jq expression %q", expression)
		}
		return "", errors.Newf(
			"jq expression %q returned multiple results; expected exactly one",
			expression,
		)
	}
	return s, nil
}

// evaluateJQObjects evaluates a jq expression and returns all results as
// []map[string]interface{}. Each result must be an object (map).
func evaluateJQObjects(
	expression string, data map[string]interface{},
) ([]map[string]interface{}, error) {
	code, err := compileJQ(expression)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	iter := code.Run(data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			return nil, errors.Wrapf(err, "evaluate jq expression %q", expression)
		}
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, errors.Newf(
				"jq expression %q returned %T at index %d, expected object",
				expression, v, len(results),
			)
		}
		results = append(results, m)
	}
	return results, nil
}
