// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

func (ctx *jsonpathCtx) resolveScalar(scalar jsonpath.Scalar) (json.JSON, error) {
	if scalar.Type == jsonpath.ScalarVariable {
		val, err := ctx.vars.FetchValKey(scalar.Variable)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "could not find jsonpath variable %q", scalar.Variable)
		}
		return val, nil
	}
	return scalar.Value, nil
}
