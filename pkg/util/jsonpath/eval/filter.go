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

func (ctx *jsonpathCtx) evalFilter(p jsonpath.Filter, current []json.JSON) ([]json.JSON, error) {
	// TODO(normanchenn): clean up.
	var unwrapped []json.JSON
	for _, j := range current {
		unwrapped = append(unwrapped, ctx.unwrap(j)...)
	}
	current = unwrapped
	var filtered []json.JSON
	// unwrap before
	for _, j := range current {
		results, err := ctx.eval(p.Condition, []json.JSON{j})
		if err != nil {
			// Postgres doesn't error when there's a structural error within
			// filter condition, and will return nothing instead.
			return []json.JSON{}, nil //nolint:returnerrcheck
		}
		if len(results) != 1 || !isBool(results[0]) {
			return nil, errors.New("filter condition must evaluate to a boolean")
		}

		condition, _ := results[0].AsBool()
		if condition {
			filtered = append(filtered, j)
		}
	}
	return filtered, nil
}
