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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// NormalizeExpression normalizes select clause.  Returns normalized (and rewritten)
// expression which can be serialized into job record.
func NormalizeExpression(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	sd sessiondatapb.SessionData,
	descr catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	splitFams bool,
) (norm *NormalizedSelectClause, err error) {
	if err := withPlanner(ctx, execCfg, user, schemaTS, sd,
		func(ctx context.Context, execCtx sql.JobExecContext) error {
			norm, err = normalizeAndValidateSelectForTarget(
				ctx, execCfg, descr, schemaTS, target, sc, false /* keyOnly */, splitFams, execCtx.SemaCtx())
			if err != nil {
				return changefeedbase.WithTerminalError(err)
			}

			defer configSemaForCDC(execCtx.SemaCtx(), norm.desc)()
			// Plan execution; this steps triggers optimizer, which
			// performs various validation steps.
			_, err := sql.PlanCDCExpression(ctx, execCtx, norm.SelectStatementForFamily(norm.desc.FamilyID))
			if err == nil {
				return nil
			}

			// Wrap error with some additional information.
			if descr.NumFamilies() > 1 && pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
				err = errors.WithHintf(err,
					"column may not exist in the target column family %q", target.FamilyName)
			}
			return err
		},
	); err != nil {
		return nil, err
	}

	return norm, nil
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
		func(ctx context.Context, execCtx sql.JobExecContext) error {
			defer configSemaForCDC(execCtx.SemaCtx(), d)()
			norm := &NormalizedSelectClause{SelectClause: sc, desc: d}
			plan, err = sql.PlanCDCExpression(ctx, execCtx, norm.SelectStatementForFamily(d.FamilyID))
			if err == nil {
				return nil
			}

			// Wrap error with some additional information.
			if descr.NumFamilies() > 1 && pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
				err = errors.WithHintf(err,
					"column may not exist in the target column family %q", target.FamilyName)
			}
			return err
		},
	); err != nil {
		return nil, err
	}

	return plan.Spans, nil
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
	fn func(ctx context.Context, execCtx sql.JobExecContext) error,
) error {
	return sql.DescsTxn(ctx, execCfg,
		func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
			if err := txn.SetFixedTimestamp(ctx, schemaTS); err != nil {
				return err
			}

			// Current implementation relies on row-by-row evaluation;
			// so, ensure vectorized engine is off.
			sd.VectorizeMode = sessiondatapb.VectorizeOff
			planner, cleanup := sql.NewInternalPlanner(
				"cdc-expr", txn,
				user,
				&sql.MemoryMetrics{},
				execCfg,
				sd,
				sql.WithDescCollection(col),
			)
			defer cleanup()
			return fn(ctx, planner.(sql.JobExecContext))
		})
}
