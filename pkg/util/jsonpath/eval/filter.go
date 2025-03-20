// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

func (ctx *jsonpathCtx) evalFilter(
	p jsonpath.Filter, current json.JSON, unwrap bool,
) ([]json.JSON, error) {
	if unwrap && current.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(p, current, false /* unwrapNext */)
	}
	results, err := ctx.eval(p.Condition, current, !ctx.strict /* unwrap */)
	if err != nil || len(results) != 1 || !isBool(results[0]) {
		// Postgres doesn't error when there's a structure error within filter
		// conditions, and will return nothing instead.
		return []json.JSON{}, nil //nolint:returnerrcheck
	}
	condition, _ := results[0].AsBool()
	if condition {
		return []json.JSON{current}, nil
	}
	return []json.JSON{}, nil
}
