// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

func (ctx *jsonpathCtx) resolveScalar(s jsonpath.Scalar) (tree.DJSON, error) {
	if s.Type == jsonpath.ScalarVariable {
		val, err := ctx.vars.FetchValKey(s.Variable)
		if err != nil {
			return tree.DJSON{}, err
		}
		if val == nil {
			return tree.DJSON{}, pgerror.Newf(pgcode.UndefinedObject, "could not find jsonpath variable %q", s.Variable)
		}
		return *ctx.a.NewDJSON(tree.DJSON{JSON: val}), nil
	}
	return *ctx.a.NewDJSON(tree.DJSON{JSON: s.Value}), nil
}
