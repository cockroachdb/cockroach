// Copyright 2017 The Cockroach Authors.
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
	"go/constant"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// sqlCheckConstraintCheckOperation is a check which validates a SQL
// CHECK constraint on a table.
type sqlCheckConstraintCheckOperation struct {
	tableName *tree.TableName
	tableDesc *sqlbase.ImmutableTableDescriptor
	checkDesc *sqlbase.TableDescriptor_CheckConstraint
	asOf      hlc.Timestamp

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []*sqlbase.ColumnDescriptor
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run sqlCheckConstraintCheckRun
}

// sqlCheckConstraintCheckRun contains the run-time state for
// sqlCheckConstraintCheckOperation during local execution.
type sqlCheckConstraintCheckRun struct {
	started  bool
	rows     *rowcontainer.RowContainer
	rowIndex int
}

func newSQLCheckConstraintCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	checkDesc *sqlbase.TableDescriptor_CheckConstraint,
	asOf hlc.Timestamp,
) *sqlCheckConstraintCheckOperation {
	return &sqlCheckConstraintCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		checkDesc: checkDesc,
		asOf:      asOf,
	}
}

// Start implements the checkOperation interface.
// It creates a SELECT expression and generates a plan from it, which
// then runs in the distSQL execution engine.
func (o *sqlCheckConstraintCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	expr, err := parser.ParseExpr(o.checkDesc.Expr)
	if err != nil {
		return err
	}
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(o.tableDesc.Columns, false /* forUpdateOrDelete */),
		From: tree.From{
			Tables: tree.TableExprs{o.tableName},
		},
		Where: &tree.Where{Expr: &tree.NotExpr{Expr: expr}},
	}
	if o.asOf != hlc.MaxTimestamp {
		sel.From.AsOf = tree.AsOfClause{Expr: &tree.NumVal{Value: constant.MakeInt64(o.asOf.WallTime)}}
	}

	// This could potentially use a variant of planner.SelectClause that could
	// use the tableDesc we have, but this is a rare operation and the benefit
	// would be marginal compared to the work of the actual query, so the added
	// complexity seems unjustified.
	plan, err := params.p.SelectClause(ctx, sel, nil /* orderBy */, nil, /* limit */
		nil /* with */, nil /* desiredTypes */, publicColumns)
	if err != nil {
		return err
	}
	plan, err = params.p.optimizePlan(ctx, plan, allColumns(plan))
	if err != nil {
		return err
	}
	planCtx := params.extendedEvalCtx.DistSQLPlanner.NewPlanningCtx(ctx, params.extendedEvalCtx, params.p.txn)
	physPlan, err := scrubPlanDistSQL(ctx, planCtx, plan)
	if err != nil {
		return err
	}
	columns := planColumns(plan)
	columnTypes := make([]types.T, len(columns))
	for i := range planColumns(plan) {
		columnTypes[i] = *columns[i].Typ
	}
	rows, err := scrubRunDistSQL(ctx, planCtx, params.p, physPlan, columnTypes)
	if err != nil {
		rows.Close(ctx)
		return err
	}

	o.run.started = true
	o.run.rows = rows

	// Collect all the columns.
	for i := range o.tableDesc.Columns {
		o.columns = append(o.columns, &o.tableDesc.Columns[i])
	}
	// Find the row indexes for all of the primary index columns.
	o.primaryColIdxs, err = getPrimaryColIdxs(o.tableDesc, o.columns)
	return err
}

// Next implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows.At(o.run.rowIndex)
	o.run.rowIndex++
	timestamp := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)

	var primaryKeyDatums tree.Datums
	for _, rowIdx := range o.primaryColIdxs {
		primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
	}

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["constraint_name"] = o.checkDesc.Name
	for rowIdx, col := range o.columns {
		// TODO(joey): We should maybe try to get the underlying type.
		rowDetails[col.Name] = row[rowIdx].String()
	}
	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		tree.NewDString(scrub.CheckConstraintViolation),
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		tree.NewDString(primaryKeyDatums.String()),
		timestamp,
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

// Started implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= o.run.rows.Len()
}

// Close implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Close(ctx context.Context) {
	if o.run.rows != nil {
		o.run.rows.Close(ctx)
	}
}
