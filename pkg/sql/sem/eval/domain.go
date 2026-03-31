// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ValidateDomainConstraints checks that the given datum satisfies the
// constraints defined on a domain type: NOT NULL and CHECK constraints.
func ValidateDomainConstraints(
	ctx context.Context, evalCtx *Context, d tree.Datum, domainType *types.T,
) error {
	dd := domainType.TypeMeta.DomainData
	if dd == nil {
		return nil
	}

	// Check NOT NULL constraint.
	if dd.NotNull && d == tree.DNull {
		return pgerror.Newf(
			pgcode.NotNullViolation,
			"domain %s does not allow null values",
			domainType.TypeMeta.Name.Basename(),
		)
	}

	// Check each CHECK constraint.
	for i := range dd.CheckConstraints {
		chk := &dd.CheckConstraints[i]
		if err := evalDomainCheckConstraint(ctx, evalCtx, d, domainType, chk); err != nil {
			return err
		}
	}
	return nil
}

// evalDomainCheckConstraint evaluates a single CHECK constraint expression
// against the given datum. VALUE references are substituted with the datum,
// and the result is evaluated as a boolean. The parsed expression is cached
// during type hydration to avoid re-parsing on every call.
func evalDomainCheckConstraint(
	ctx context.Context,
	evalCtx *Context,
	d tree.Datum,
	domainType *types.T,
	chk *types.DomainCheckConstraint,
) error {
	var expr tree.Expr
	if cached, ok := chk.ParsedExpr.(tree.Expr); ok {
		expr = cached
	} else {
		var err error
		expr, err = parserutils.ParseExpr(chk.Expr)
		if err != nil {
			return err
		}
	}

	// Replace VALUE references with the actual datum. In the parsed expression,
	// VALUE appears as an UnresolvedName with a single part named "value"
	// (case-insensitive).
	expr, err := tree.SimpleVisit(expr, func(e tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if n, ok := e.(*tree.UnresolvedName); ok {
			if n.NumParts == 1 && strings.EqualFold(n.Parts[0], "value") {
				return false, d, nil
			}
		}
		return true, e, nil
	})
	if err != nil {
		return err
	}

	// Type-check the expression as a boolean. This happens on every row, which
	// is more expensive than table-level CHECK constraints (compiled into the
	// query plan). Caching the typed expression is not straightforward because
	// the VALUE substitution produces a new tree each time. A future
	// optimization could pre-type-check with a placeholder and substitute at
	// eval time.
	//
	// The nil resolver means UDT references in CHECK expressions would fail
	// here, but this is acceptable because CHECK expressions are validated with
	// the real resolver at CREATE DOMAIN time — any UDT references are resolved
	// eagerly during that type-check, so they won't appear as unresolved names
	// at eval time.
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Bool)
	if err != nil {
		return err
	}

	// Evaluate the expression.
	result, err := Expr(ctx, evalCtx, typedExpr)
	if err != nil {
		return err
	}

	// Per the SQL standard, a CHECK constraint is satisfied if the expression
	// evaluates to TRUE or NULL. Only FALSE is a violation.
	if result == tree.DBoolFalse {
		constraintName := chk.Name
		if constraintName == "" {
			constraintName = chk.Expr
		}
		return pgerror.Newf(
			pgcode.CheckViolation,
			"value for domain %s violates check constraint %q",
			domainType.TypeMeta.Name.Basename(),
			constraintName,
		)
	}
	return nil
}
