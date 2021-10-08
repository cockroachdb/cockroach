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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// sqlCheckConstraintCheckOperation is a check which validates a SQL
// CHECK constraint on a table.
type sqlCheckConstraintCheckOperation struct {
	tableName *tree.TableName
	tableDesc catalog.TableDescriptor
	checkDesc *descpb.TableDescriptor_CheckConstraint
	asOf      hlc.Timestamp

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []catalog.Column
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run sqlCheckConstraintCheckRun
}

// sqlCheckConstraintCheckRun contains the run-time state for
// sqlCheckConstraintCheckOperation during local execution.
type sqlCheckConstraintCheckRun struct {
	started  bool
	rows     []tree.Datums
	rowIndex int
}

func newSQLCheckConstraintCheckOperation(
	tableName *tree.TableName,
	tableDesc catalog.TableDescriptor,
	checkDesc *descpb.TableDescriptor_CheckConstraint,
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
	// Generate a query of the form:
	//    SELECT a,b,c FROM db.t WHERE NOT (condition)
	// We always fully qualify the table in the query.
	tn := *o.tableName
	tn.ExplicitCatalog = true
	tn.ExplicitSchema = true
	sel := &tree.SelectClause{
		Exprs: tabledesc.ColumnsSelectors(o.tableDesc.PublicColumns()),
		From: tree.From{
			Tables: tree.TableExprs{&tn},
		},
		Where: &tree.Where{
			Type: tree.AstWhere,
			Expr: &tree.NotExpr{Expr: expr},
		},
	}
	if o.asOf != hlc.MaxTimestamp {
		sel.From.AsOf = tree.AsOfClause{
			Expr: tree.NewNumVal(
				constant.MakeInt64(o.asOf.WallTime),
				"", /* origString */
				false /* negative */),
		}
	}

	rows, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryBuffered(
		ctx, "check-constraint", params.p.txn, tree.AsStringWithFlags(sel, tree.FmtParsable),
	)
	if err != nil {
		return err
	}

	o.run.started = true
	o.run.rows = rows
	// Collect all the columns.
	o.columns = o.tableDesc.PublicColumns()
	// Find the row indexes for all of the primary index columns.
	o.primaryColIdxs, err = getPrimaryColIdxs(o.tableDesc, o.columns)
	return err
}

// Next implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows[o.run.rowIndex]
	o.run.rowIndex++
	timestamp, err := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(),
		time.Nanosecond,
	)
	if err != nil {
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
		rowDetails[col.GetName()] = row[rowIdx].String()
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
	return o.run.rows == nil || o.run.rowIndex >= len(o.run.rows)
}

// Close implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Close(ctx context.Context) {
	o.run.rows = nil
}
