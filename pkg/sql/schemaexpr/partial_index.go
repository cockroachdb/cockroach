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

func init() {
	// This is a work-around for an import cycle between sqlbase and schemaexpr.
	sqlbase.SchemaExprPartialIndexPredicateValidateHook =
		func(ctx context.Context, desc sqlbase.TableDescriptorInterface, expr tree.Expr, semaCtx *tree.SemaContext) (tree.Expr, error) {
			idxValidator := NewIndexPredicateValidator(ctx, desc, semaCtx)

			// Validate is used instead of ValidateAndDequalify because there is
			// no need to dequalify the columns. They are dequalified when the
			// index is created.
			return idxValidator.Validate(expr)
		}
}

// IndexPredicateValidator validates that an expression is a valid partial index
// predicate. See Validate for more details.
type IndexPredicateValidator struct {
	ctx     context.Context
	desc    sqlbase.TableDescriptorInterface
	semaCtx *tree.SemaContext
}

// NewIndexPredicateValidator returns an IndexPredicateValidator struct that can
// be used to validate partial index predicates. See Validate for more details.
func NewIndexPredicateValidator(
	ctx context.Context, desc sqlbase.TableDescriptorInterface, semaCtx *tree.SemaContext,
) IndexPredicateValidator {
	return IndexPredicateValidator{
		ctx:     ctx,
		desc:    desc,
		semaCtx: semaCtx,
	}
}

// ValidateAndDequalify verifies that an expression is a valid partial index
// predicate. If the expression is valid, it returns an expression with the
// columns dequalified so that database and table prefixes are not part of the
// column names.
//
// See Validate for more details on what qualifies as a "valid" partial index
// predicate expression.
func (v *IndexPredicateValidator) ValidateAndDequalify(
	expr tree.Expr, tn *tree.TableName,
) (tree.Expr, error) {
	expr, err := DequalifyExpr(v.ctx, v.desc, expr, tn)
	if err != nil {
		return nil, err
	}

	return v.Validate(expr)
}

// Validate verifies that an expression is a valid partial index predicate. If
// the expression is valid, it returns the type-check expression.
//
// A predicate expression is valid if all of the following are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include subqueries.
//   - It does not include non-immutable, aggregate, window, or set returning
//     functions.
//
func (v *IndexPredicateValidator) Validate(expr tree.Expr) (tree.Expr, error) {
	expr, _, err := ValidateExprTypeAndVolatility(
		v.ctx,
		v.desc,
		expr,
		types.Bool,
		"index predicate",
		v.semaCtx,
		tree.VolatilityImmutable,
	)
	if err != nil {
		return nil, err
	}

	return expr, nil
}
