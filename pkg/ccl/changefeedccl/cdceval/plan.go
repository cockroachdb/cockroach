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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
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

// NormalizationControl determines if expression should be normalized.
type NormalizationControl bool

// MustNormalize indicates that normalization step must be performed.
const MustNormalize NormalizationControl = true

// AlreadyNormalized indicates that normalization step was done before.
const AlreadyNormalized NormalizationControl = false

// NormalizeAndPlan normalizes select clause, and plans CDC expression
// execution (but does not execute).  Returns the plan along with normalized expression.
func NormalizeAndPlan(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	sd sessiondatapb.SessionData,
	descr catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	splitFams bool,
	normCtrl NormalizationControl,
) (norm *NormalizedSelectClause, plan sql.CDCExpressionPlan, err error) {
	d, err := newEventDescriptorForTarget(descr, target, schemaTS, false, false)
	if err != nil {
		return nil, sql.CDCExpressionPlan{}, err
	}

	if err := plannerExec(ctx, execCfg, user, schemaTS, sd,
		func(ctx context.Context, execCtx sql.JobExecContext) error {
			defer configSemaForCDC(execCtx.SemaCtx(), d)()

			if normCtrl == MustNormalize {
				norm, _, err = normalizeAndValidateSelectForTarget(
					ctx, execCfg, descr, schemaTS, target, sc, false /* keyOnly */, splitFams, execCtx.SemaCtx())
				if err != nil {
					return changefeedbase.WithTerminalError(err)
				}
			} else {
				// sc was normalized before.
				norm = &NormalizedSelectClause{
					SelectClause: sc,
					desc:         d,
					requiresPrev: requiresPrev(sc),
				}
			}

			cdcExpr, err := newCDCPlanExpr(norm)
			if err != nil {
				return err
			}

			plan, err = sql.PlanCDCExpression(ctx, execCtx, cdcExpr)
			if err == nil {
				return nil
			}

			// Wrap error with some additional information.
			switch pgerror.GetPGCode(err) {
			case pgcode.UndefinedColumn:
				return errors.WithHintf(err,
					"column nay not exist in the target column family %q", target.FamilyName)
			default:
				return err
			}
		},
	); err != nil {
		return nil, sql.CDCExpressionPlan{}, err
	}

	return norm, plan, nil
}

// newCDCPlanExpr construct sql.CDCExpression from the specified NormalizedSelectClause.
func newCDCPlanExpr(norm *NormalizedSelectClause) (*tree.Select, error) {
	tables := append(tree.TableExprs(nil), norm.From.Tables...)
	if len(tables) != 1 {
		return nil, errors.AssertionFailedf("expected single table")
	}

	table := tables[0]
	if aliased, ok := table.(*tree.AliasedTableExpr); ok {
		table = aliased.Expr
	}
	tableRef, ok := table.(*tree.TableRef)
	if !ok {
		return nil, errors.AssertionFailedf("expected table reference, found %T", tables[0])
	}

	tables[0] = maybeScopeTable(norm.desc, tableRef)

	if norm.requiresPrev {
		// That's a bit of a mouthful, but all we're doing here is adding
		// another table sub-select to the query to produce cdc_prev tuple:
		// SELECT ... FROM tbl, (SELECT ((crdb_internal.cdc_prev_row()).*)) AS cdc_prev
		tables = append(tables,
			&tree.AliasedTableExpr{
				As: tree.AliasClause{Alias: prevTupleName},
				Expr: &tree.Subquery{
					Select: &tree.ParenSelect{
						Select: &tree.Select{
							Select: &tree.SelectClause{
								Exprs: tree.SelectExprs{
									tree.SelectExpr{
										Expr: &tree.TupleStar{
											Expr: &tree.FuncExpr{
												Func: tree.ResolvableFunctionReference{FunctionReference: &prevRowFnName},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		)
	}

	return &tree.Select{
		Select: &tree.SelectClause{
			From:  tree.From{Tables: tables},
			Exprs: norm.Exprs,
			Where: norm.Where,
		},
	}, nil
}

// maybeScopeTable returns possibly "scoped" table expression.
// If event descriptor targets all columns, then table expression returned
// unmodified. However, if the event descriptor targets a subset of columns,
// then returns table expression restricted to targeted columns.
func maybeScopeTable(ed *cdcevent.EventDescriptor, tableRef *tree.TableRef) tree.TableExpr {
	// If the event descriptor targets all columns in the table, we can use
	// table as is; otherwise, we need to scope expression to select only
	// the columns in the event descriptor.
	if ed.FamilyID == 0 && !ed.HasVirtual && !ed.HasOtherFamilies {
		return tableRef
	}

	scopedTable := &tree.SelectClause{
		From: tree.From{
			Tables: tree.TableExprs{&tree.TableRef{
				TableID: tableRef.TableID,
				As:      tree.AliasClause{Alias: "t"},
			}},
		},
		Exprs: func() (exprs tree.SelectExprs) {
			exprs = make(tree.SelectExprs, len(ed.ResultColumns()))
			for i, c := range ed.ResultColumns() {
				exprs[i] = tree.SelectExpr{Expr: &tree.ColumnItem{ColumnName: tree.Name(c.Name)}}
			}
			return exprs
		}(),
	}

	return &tree.AliasedTableExpr{
		Expr: &tree.Subquery{
			Select: &tree.ParenSelect{
				Select: &tree.Select{
					Select: scopedTable,
				},
			},
		},
		As: tableRef.As,
	}
}

// plannerExec is a helper which invokes provided function inside
// a DescTxn transaction to ensure that descriptors get acquired
// as of correct schema timestamp.
func plannerExec(
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
