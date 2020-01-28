// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ParseDatumStringAs parses s as type t. This function is guaranteed to
// round-trip when printing a Datum with FmtExport.
func ParseDatumStringAs(t *types.T, s string, evalCtx *tree.EvalContext) (tree.Datum, error) {
	switch t.Family() {
	case types.BytesFamily:
		return tree.ParseDByte(s)
	case types.ArrayFamily:
		return parseArray(evalCtx, t, s)
	default:
		return tree.ParseStringAs(t, s, evalCtx)
	}
}

func parseArray(evalCtx *tree.EvalContext, typ *types.T, s string) (tree.Datum, error) {
	expr, err := parser.ParseExpr(s)
	if err != nil {
		return nil, err
	}
	typedExpr, err := tree.TypeCheck(expr, nil, typ)
	if err != nil {
		return nil, err
	}
	darr, err := typedExpr.Eval(evalCtx)
	return darr, err
}
