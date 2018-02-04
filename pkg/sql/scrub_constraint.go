// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"go/constant"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// sqlCheckConstraintCheckOperation is a check which validates a SQL
// CHECK constraint on a table.
type sqlCheckConstraintCheckOperation struct {
	tableName *tree.TableName
	tableDesc *sqlbase.TableDescriptor
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
	started     bool
	rows        planNode
	hasRowsLeft bool
}

func newSQLCheckConstraintCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.TableDescriptor,
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
	normalizableTableName := &tree.NormalizableTableName{TableNameReference: o.tableName}
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(o.tableDesc.Columns, false /* forUpdateOrDelete */),
		From: &tree.From{
			Tables: tree.TableExprs{normalizableTableName},
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
	rows, err := params.p.SelectClause(ctx, sel, nil /* orderBy */, nil, /* limit */
		nil /* with */, nil /* desiredTypes */, publicColumns)
	if err != nil {
		return err
	}
	rows, err = params.p.optimizePlan(ctx, rows, allColumns(rows))
	if err != nil {
		return err
	}
	if err := startPlan(params, rows); err != nil {
		return err
	}

	// Collect all the columns.
	for i := range o.tableDesc.Columns {
		o.columns = append(o.columns, &o.tableDesc.Columns[i])
	}
	// Find the row indexes for all of the primary index columns.
	if o.primaryColIdxs, err = getPrimaryColIdxs(o.tableDesc, o.columns); err != nil {
		return err
	}

	o.run.started = true
	o.run.rows = rows
	// Begin the first unit of work. This prepares the hasRowsLeft flag
	// for the first iteration.
	o.run.hasRowsLeft, err = o.run.rows.Next(params)
	return err
}

// Next implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows.Values()
	timestamp := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)

	// Start the next unit of work. This is required so during the next
	// call to Done() it is known whether there are any rows left.
	var err error
	if o.run.hasRowsLeft, err = o.run.rows.Next(params); err != nil {
		return nil, err
	}

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
	return o.run.rows == nil || !o.run.hasRowsLeft
}

// Close implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Close(ctx context.Context) {
	if o.run.rows != nil {
		o.run.rows.Close(ctx)
	}
}
