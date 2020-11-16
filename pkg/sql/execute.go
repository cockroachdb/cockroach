// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// fillInPlaceholder helps with the EXECUTE foo(args) SQL statement: it takes in
// a prepared statement returning
// the referenced prepared statement and correctly updated placeholder info.
// See https://www.postgresql.org/docs/current/static/sql-execute.html for details.
func fillInPlaceholders(
	ctx context.Context,
	ps *PreparedStatement,
	name string,
	params tree.Exprs,
	searchPath sessiondata.SearchPath,
) (*tree.PlaceholderInfo, error) {
	if len(ps.Types) != len(params) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"wrong number of parameters for prepared statement %q: expected %d, got %d",
			name, len(ps.Types), len(params))
	}

	qArgs := make(tree.QueryArguments, len(params))
	var semaCtx tree.SemaContext
	for i, e := range params {
		idx := tree.PlaceholderIdx(i)

		typ, ok := ps.ValueType(idx)
		if !ok {
			return nil, errors.AssertionFailedf("no type for placeholder %s", idx)
		}
		typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
			ctx, e, typ, "EXECUTE parameter" /* context */, &semaCtx, tree.VolatilityVolatile,
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
