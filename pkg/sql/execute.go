// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/errors"
)

// fillInPlaceholder helps with the EXECUTE foo(args) SQL statement: it takes in
// a prepared statement returning
// the referenced prepared statement and correctly updated placeholder info.
// See https://www.postgresql.org/docs/current/static/sql-execute.html for details.
func (p *planner) fillInPlaceholders(
	ctx context.Context, ps *PreparedStatement, name string, params tree.Exprs,
) (*tree.PlaceholderInfo, error) {
	if len(ps.Types) != len(params) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"wrong number of parameters for prepared statement %q: expected %d, got %d",
			name, len(ps.Types), len(params))
	}

	qArgs := make(tree.QueryArguments, len(params))
	for i, e := range params {
		idx := tree.PlaceholderIdx(i)

		typ, ok := ps.ValueType(idx)
		if !ok {
			return nil, errors.AssertionFailedf("no type for placeholder %s", idx)
		}

		// For user-defined types, we need to resolve the type to make sure we get
		// the latest changes to the type.
		if typ.UserDefined() {
			var err error
			typ, err = p.ResolveTypeByOID(ctx, typ.Oid())
			if err != nil {
				return nil, err
			}
		}
		typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
			ctx, e, typ, "EXECUTE parameter" /* context */, p.SemaCtx(), volatility.Volatile, true, /*allowAssignmentCast*/
		)
		if err != nil {
			return nil, pgerror.WithCandidateCode(err, pgcode.WrongObjectType)
		}

		qArgs[idx] = typedExpr
	}
	return &tree.PlaceholderInfo{
		Values: qArgs,
		PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
			TypeHints: ps.TypeHints,
			Types:     ps.Types,
		},
	}, nil
}
