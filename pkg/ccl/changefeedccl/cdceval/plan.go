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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// NormalizeExpression normalizes select clause.  Returns normalized (and rewritten)
// expression which can be serialized into job record.
// Returns boolean indicating if expression requires access to the previous
// state of the row (diff).
func NormalizeExpression(
	ctx context.Context,
	execCtx sql.JobExecContext,
	descr catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	splitFams bool,
) (norm *NormalizedSelectClause, withDiff bool, _ error) {
	// Even though we have a job exec context, we shouldn't muck with it.
	// Make our own copy of the planner instead.
	if err := withPlanner(
		ctx, execCtx.ExecCfg(), execCtx.User(), schemaTS, execCtx.SessionData().SessionData,
		func(ctx context.Context, execCtx sql.JobExecContext, cleanup func()) (err error) {
			defer cleanup()
			norm, withDiff, err = normalizeExpression(ctx, execCtx, descr, schemaTS, target, sc, splitFams)
			return err
		},
	); err != nil {
		return nil, false, withErrorHint(err, target.FamilyName, descr.NumFamilies() > 1)
	}
	return
}

func normalizeExpression(
	ctx context.Context,
	execCtx sql.JobExecContext,
	descr catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	splitFams bool,
) (*NormalizedSelectClause, bool, error) {
	norm, err := normalizeAndValidateSelectForTarget(
		ctx, execCtx.ExecCfg(), descr, schemaTS, target, sc, false /* keyOnly */, splitFams, execCtx.SemaCtx())
	if err != nil {
		return nil, false, changefeedbase.WithTerminalError(err)
	}

	defer configSemaForCDC(execCtx.SemaCtx())()

	// Add cdc_prev column; we may or may not need it, but we'll check below.
	prevCol, err := newPrevColumnForDesc(norm.desc)
	if err != nil {
		return nil, false, err
	}

	// Plan execution; this steps triggers optimizer, which
	// performs various validation steps.
	plan, err := sql.PlanCDCExpression(ctx, execCtx,
		norm.SelectStatementForFamily(), sql.WithExtraColumn(prevCol))
	if err != nil {
		return nil, false, err
	}

	// Determine if we need diff option.
	var withDiff bool
	plan.CollectPlanColumns(func(column colinfo.ResultColumn) bool {
		if uint32(prevCol.GetID()) == column.PGAttributeNum {
			withDiff = true
			return true // stop.
		}
		return false // keep going.
	})
	return norm, withDiff, nil
}

// SpansForExpression returns spans that must be scanned in order to evaluate
// changefeed expression.  Select clause expression assumed to be normalized.
func SpansForExpression(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	sd sessiondatapb.SessionData,
	descr catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
) (_ roachpb.Spans, err error) {
	d, err := newEventDescriptorForTarget(descr, target, schemaTS, false, false)
	if err != nil {
		return nil, err
	}

	var plan sql.CDCExpressionPlan
	if err := withPlanner(ctx, execCfg, user, schemaTS, sd,
		func(ctx context.Context, execCtx sql.JobExecContext, cleanup func()) error {
			defer cleanup()
			defer configSemaForCDC(execCtx.SemaCtx())()
			norm := &NormalizedSelectClause{SelectClause: sc, desc: d}

			// Add cdc_prev column; we may or may not need it, add it just in case
			// expression uses it.
			prevCol, err := newPrevColumnForDesc(norm.desc)
			if err != nil {
				return err
			}

			plan, err = sql.PlanCDCExpression(ctx, execCtx,
				norm.SelectStatementForFamily(), sql.WithExtraColumn(prevCol))
			return err

		},
	); err != nil {
		return nil, withErrorHint(err, d.FamilyName, d.HasOtherFamilies)
	}

	return plan.Spans, nil
}

// withErrorHint wraps error with error hints.
func withErrorHint(err error, targetFamily string, multiFamily bool) error {
	// Wrap error with some additional information.
	if multiFamily && pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
		return errors.WithHintf(err,
			"column may not exist in the target column family %q", targetFamily)
	}
	if pgerror.GetPGCode(err) == pgcode.UndefinedTable && strings.Contains(err.Error(), "cdc_prev") {
		return errors.WithHint(err,
			"cdc_prev is a tuple; access tuple content with (cdc_prev).x")
	}
	return err
}

// withPlanner is a helper which invokes provided function inside
// a DescsTxn transaction to ensure that descriptors get acquired
// as of correct schema timestamp.
func withPlanner(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	schemaTS hlc.Timestamp,
	sd sessiondatapb.SessionData,
	fn func(ctx context.Context, execCtx sql.JobExecContext, cleanup func()) error,
) error {
	return sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		if err := txn.KV().SetFixedTimestamp(ctx, schemaTS); err != nil {
			return err
		}

		// Current implementation relies on row-by-row evaluation;
		// so, ensure vectorized engine is off.
		sd.VectorizeMode = sessiondatapb.VectorizeOff
		planner, cleanup := sql.NewInternalPlanner(
			"cdc-expr", txn.KV(),
			user,
			&sql.MemoryMetrics{}, // TODO(yevgeniy): Use appropriate metrics.
			execCfg,
			sd,
			sql.WithDescCollection(col),
		)
		return fn(ctx, planner.(sql.JobExecContext), cleanup)
	})
}
