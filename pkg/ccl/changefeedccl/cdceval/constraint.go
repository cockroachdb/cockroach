// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ConstrainPrimaryIndexSpanByFilter attempts to constrain table primary
// index span if changefeed expression (select clause) is specified.
// Returns possibly constrained spans, and a possibly modified (optimized)
// select clause.
func ConstrainPrimaryIndexSpanByFilter(
	ctx context.Context,
	execCtx sql.JobExecContext,
	selectClause string,
	descr catalog.TableDescriptor,
) ([]roachpb.Span, string, error) {
	if selectClause == "" {
		return nil, "", errors.AssertionFailedf("unexpected empty filter")
	}
	sc, err := ParseChangefeedExpression(selectClause)
	if err != nil {
		return nil, "", pgerror.Wrap(err, pgcode.InvalidParameterValue,
			"could not parse changefeed expression")
	}

	// Predicate changefeed currently work on a single table only.
	// Verify this assumption.
	if len(sc.From.Tables) != 1 {
		return nil, "", errors.AssertionFailedf(
			"expected 1 table expression, found %d", len(sc.From.Tables))
	}

	if sc.Where == nil {
		// Nothing to constrain.
		return []roachpb.Span{descr.PrimaryIndexSpan(execCtx.ExecCfg().Codec)}, selectClause, nil
	}

	tableName := func() *tree.TableName {
		tblExpr := sc.From.Tables[0]
		if alias, isAlias := tblExpr.(*tree.AliasedTableExpr); isAlias {
			return tree.NewUnqualifiedTableName(alias.As.Alias)
		}
		return tree.NewUnqualifiedTableName(tree.Name(descr.GetName()))
	}()

	semaCtx := newSemaCtx()
	spans, remainingFilter, err := execCtx.ConstrainPrimaryIndexSpanByExpr(
		ctx, sql.BestEffortConstrain, tableName, descr,
		&execCtx.ExtendedEvalContext().Context, semaCtx, sc.Where.Expr)
	if err != nil {
		return nil, "", err
	}
	if remainingFilter == tree.DBoolTrue {
		sc.Where = nil
	} else {
		sc.Where.Expr = remainingFilter
	}

	return spans, AsStringUnredacted(sc), nil
}
