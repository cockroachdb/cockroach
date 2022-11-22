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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	target jobspb.ChangefeedTargetSpecification,
	includeVirtual bool,
	keyOnly bool,
) (_ []roachpb.Span, updatedSelect string, _ error) {
	if selectClause == "" {
		return nil, "", errors.AssertionFailedf("unexpected empty filter")
	}
	sc, err := ParseChangefeedExpression(selectClause)
	if err != nil {
		return nil, "", pgerror.Wrap(err, pgcode.InvalidParameterValue,
			"could not parse changefeed expression")
	}

	ed, err := newEventDescriptorForTarget(descr, target, schemaTS(execCtx), includeVirtual, keyOnly)
	if err != nil {
		return nil, "", err
	}

	evalCtx := &execCtx.ExtendedEvalContext().Context
	spans, remainingFilter, err := constrainSpansBySelectClause(
		ctx, execCtx, evalCtx, execCtx.ExecCfg().Codec, sc, ed)
	if err != nil {
		return nil, "", err
	}

	if remainingFilter != nil {
		// non-nil remainingFilter implies we had sc.Where clause.
		if remainingFilter == tree.DBoolTrue {
			sc.Where = nil
		} else {
			sc.Where.Expr = remainingFilter
		}
	}

	return spans, AsStringUnredacted(sc), nil
}

// constrainSpansBySelectClause is a helper that attempts to constrain primary
// index spans by the filter in the select clause.  Returns constrained spans,
// along with the remaining filter.
func constrainSpansBySelectClause(
	ctx context.Context,
	sc sql.SpanConstrainer,
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	selectClause *tree.SelectClause,
	ed *cdcevent.EventDescriptor,
) ([]roachpb.Span, tree.Expr, error) {
	// Predicate changefeed currently works on a single table only.
	// Verify this assumption.
	if len(selectClause.From.Tables) != 1 {
		return nil, nil, errors.AssertionFailedf(
			"expected 1 table expression, found %d", len(selectClause.From.Tables))
	}

	if selectClause.Where == nil {
		// Nothing to constrain.
		return []roachpb.Span{ed.TableDescriptor().PrimaryIndexSpan(codec)}, nil, nil
	}

	tableName := tableNameOrAlias(ed.TableName, selectClause.From.Tables[0])
	semaCtx := newSemaCtxWithTypeResolver(ed)
	return sc.ConstrainPrimaryIndexSpanByExpr(
		ctx, sql.BestEffortConstrain, tableName, ed.TableDescriptor(),
		evalCtx, semaCtx, selectClause.Where.Expr)
}

func schemaTS(execCtx sql.JobExecContext) hlc.Timestamp {
	return execCtx.ExecCfg().Clock.Now()
}
