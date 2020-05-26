// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// IndexPredicateValidator validates that an expression is a valid partial index
// predicate. See Validate for more details.
type IndexPredicateValidator struct {
	ctx       context.Context
	tableName tree.TableName
	desc      *sqlbase.MutableTableDescriptor
	semaCtx   *tree.SemaContext
}

// NewIndexPredicateValidator returns an IndexPredicateValidator struct that can
// be used to validate partial index predicates. See Validate for more details.
func NewIndexPredicateValidator(
	ctx context.Context,
	tableName tree.TableName,
	desc *sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
) IndexPredicateValidator {
	return IndexPredicateValidator{
		ctx:       ctx,
		tableName: tableName,
		desc:      desc,
		semaCtx:   semaCtx,
	}
}

// Validate verifies that an expression is a valid partial index predicate. If
// the expression is valid, it returns an expression with the columns
// dequalified.
//
// A predicate expression is valid if all of the following are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include subqueries.
//   - It does not include mutable, aggregate, window, or set returning
//     functions.
//
func (v *IndexPredicateValidator) Validate(expr tree.Expr) (tree.Expr, error) {
	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, _, err := v.desc.ReplaceColumnVarsInExprWithDummies(expr)
	if err != nil {
		return nil, err
	}

	// Check that the type of the expression is a types.Bool and that there are
	// no variable expressions (besides dummyColumnItems) and no impure
	// functions.
	_, err = sqlbase.SanitizeVarFreeExpr(
		v.ctx,
		replacedExpr,
		types.Bool,
		"index predicate",
		v.semaCtx,
		false, /* allowImpure */
	)
	if err != nil {
		return nil, err
	}

	source := sqlbase.NewSourceInfoForSingleTable(
		v.tableName, sqlbase.ResultColumnsFromColDescs(
			v.desc.GetID(),
			v.desc.TableDesc().AllNonDropColumns(),
		),
	)

	// Dequalify column references so that they do not contain database or
	// table names.
	return DequalifyColumnRefs(v.ctx, source, expr)
}
