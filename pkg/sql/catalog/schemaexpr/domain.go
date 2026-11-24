// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// IsDomainValueRef reports whether e is a reference to the keyword `VALUE` used
// inside a domain CHECK expression.
func IsDomainValueRef(e tree.Expr) bool {
	n, ok := e.(*tree.UnresolvedName)
	return ok && n.NumParts == 1 && strings.EqualFold(n.Parts[0], "value")
}

// TypeCheckDomainCheckExpr validates an expression by substituting VALUE with a
// typed null of the base type and type-checking as boolean. This catches
// invalid expressions (e.g., referencing nonexistent functions) at CREATE
// DOMAIN time rather than at INSERT/UPDATE time.
func TypeCheckDomainCheckExpr(
	ctx context.Context, semaCtx *tree.SemaContext, expr tree.Expr, baseType *types.T,
) (tree.TypedExpr, error) {
	substituted, err := tree.SimpleVisit(expr, func(e tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if !IsDomainValueRef(e) {
			return true, e, nil
		}
		return false, tree.NewTypedCastExpr(tree.DNull, baseType), nil
	})
	if err != nil {
		return nil, err
	}
	return tree.TypeCheck(ctx, substituted, semaCtx, types.Bool)
}
