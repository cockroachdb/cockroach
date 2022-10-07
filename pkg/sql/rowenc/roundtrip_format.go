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

// ParseDatumStringAsWithRawBytes parses s as type t. However, if the requested type is Bytes
// then the string is returned unchanged. This function is used when the input string might be
// unescaped raw bytes, so we don't want to run a bytes parsing routine on the input. Other
// than the bytes case, this function does the same as ParseDatumStringAs but is not
// guaranteed to round-trip.
func ParseDatumStringAsWithRawBytes(
	ctx context.Context, t *types.T, s string, evalCtx *eval.Context,
) (tree.Datum, error) {
	switch t.Family() {
	case types.BytesFamily:
		return tree.NewDBytes(tree.DBytes(s)), nil
	default:
		return ParseDatumStringAs(ctx, t, s, evalCtx)
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
