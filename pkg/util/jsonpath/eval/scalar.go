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

func (ctx *jsonpathCtx) resolveScalar(scalar jsonpath.Scalar) (json.JSON, error) {
	if scalar.Type == jsonpath.ScalarVariable {
		val, err := ctx.vars.FetchValKey(scalar.Variable)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, errors.AssertionFailedf("variable %q not found in jsonpath context", scalar.Variable)
		}
		return val, nil
	}
	return scalar.Value, nil
}
