// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/errors"
)

func (ctx *jsonpathCtx) evalFilter(
	filter jsonpath.Filter, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	if unwrap && jsonValue.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(filter, jsonValue, false /* unwrapNext */)
	}

	op, ok := filter.Condition.(jsonpath.Operation)
	if !ok {
		return nil, errors.AssertionFailedf("filter condition is not an operation")
	}
	b, err := ctx.evalBoolean(op, jsonValue)
	if err != nil {
		// Postgres doesn't error when there's a structure error within filter
		// conditions, and will return nothing instead.
		return nil, nil //nolint:returnerrcheck
	}

	if b == jsonpathBoolTrue {
		return []json.JSON{jsonValue}, nil
	}
	return nil, nil
}
