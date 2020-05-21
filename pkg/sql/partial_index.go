// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// validateIndexPredicate validates that an expression is a valid partial index
// predicate. If the expression is valid, it returns an expression with the
// columns replaced with typed dummies and constants folded.
//
// A predicate expression is valid if all of the follow are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include mutable, aggregate, window, or set returning
//     functions.
//
func validateIndexPredicate(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	expr tree.Expr,
	semaCtx *tree.SemaContext,
	tableName tree.TableName,
) (tree.Expr, error) {
	expr, _, err := replaceVars(desc, expr)
	if err != nil {
		return nil, err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(expr, types.Bool, "index predicate", semaCtx, false); err != nil {
		return nil, err
	}

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		tableName, sqlbase.ResultColumnsFromColDescs(
			desc.GetID(),
			desc.TableDesc().AllNonDropColumns(),
		),
	)

	return dequalifyColumnRefs(ctx, sourceInfo, expr)
}
