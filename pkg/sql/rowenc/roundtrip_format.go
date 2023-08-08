// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ParseDatumStringAs parses s as type t. This function is guaranteed to
// round-trip when printing a Datum with FmtExport.
func ParseDatumStringAs(
	ctx context.Context, t *types.T, s string, evalCtx *eval.Context,
) (tree.Datum, error) {
	switch t.Family() {
	// We use a different parser for array types because ParseAndRequireString only parses
	// the internal postgres string representation of arrays.
	case types.ArrayFamily, types.CollatedStringFamily:
		return parseAsTyp(ctx, evalCtx, t, s)
	default:
		res, _, err := tree.ParseAndRequireString(t, s, evalCtx)
		return res, err
	}
}

func parseAsTyp(
	ctx context.Context, evalCtx *eval.Context, typ *types.T, s string,
) (tree.Datum, error) {
	expr, err := parser.ParseExpr(s)
	if err != nil {
		return nil, err
	}
	semaCtx := tree.MakeSemaContext()
	typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, typ)
	if err != nil {
		return nil, err
	}
	datum, err := eval.Expr(ctx, evalCtx, typedExpr)
	return datum, err
}
