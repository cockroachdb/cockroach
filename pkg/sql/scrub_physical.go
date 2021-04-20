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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/errors"
)

var _ checkOperation = &physicalCheckOperation{}

// physicalCheckOperation is a check on an indexes physical data.
type physicalCheckOperation struct {
	tableName *tree.TableName
	tableDesc catalog.TableDescriptor
	indexDesc *descpb.IndexDescriptor

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []*descpb.ColumnDescriptor
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run physicalCheckRun
}

// physicalCheckRun contains the run-time state for
// physicalCheckOperation during local execution.
type physicalCheckRun struct {
	started  bool
	rows     *rowcontainer.RowContainer
	rowIndex int
}

func newPhysicalCheckOperation(
	tableName *tree.TableName, tableDesc catalog.TableDescriptor, indexDesc *descpb.IndexDescriptor,
) *physicalCheckOperation {
	return &physicalCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		indexDesc: indexDesc,
	}
}

// Start implements the checkOperation interface.
// It will plan and run the physical data check using the distSQL
// execution engine.
func (o *physicalCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	// Collect all of the columns, their types, and their IDs.
	var columnIDs []tree.ColumnID
	colIDToIdx := catalog.ColumnIDToOrdinalMap(o.tableDesc.PublicColumns())
	columns := make([]*descpb.ColumnDescriptor, len(columnIDs))

	// Collect all of the columns being scanned.
	if o.indexDesc.ID == o.tableDesc.GetPrimaryIndexID() {
		for _, c := range o.tableDesc.PublicColumns() {
			columnIDs = append(columnIDs, tree.ColumnID(c.GetID()))
		}
	} else {
		for _, id := range o.indexDesc.ColumnIDs {
			columnIDs = append(columnIDs, tree.ColumnID(id))
		}
		for _, id := range o.indexDesc.ExtraColumnIDs {
			columnIDs = append(columnIDs, tree.ColumnID(id))
		}
		for _, id := range o.indexDesc.StoreColumnIDs {
			columnIDs = append(columnIDs, tree.ColumnID(id))
		}
	}

	for i := range columnIDs {
		idx := colIDToIdx.GetDefault(descpb.ColumnID(columnIDs[i]))
		columns = append(columns, o.tableDesc.PublicColumns()[idx].ColumnDesc())
	}

	// Find the row indexes for all of the primary index columns.
	primaryColIdxs, err := getPrimaryColIdxs(o.tableDesc, columns)
	if err != nil {
		return err
	}

	indexFlags := &tree.IndexFlags{
		IndexID:     tree.IndexID(o.indexDesc.ID),
		NoIndexJoin: true,
	}
	scan := params.p.Scan()
	scan.isCheck = true
	colCfg := scanColumnsConfig{wantedColumns: columnIDs, addUnwantedAsHidden: true}
	if err := scan.initTable(ctx, params.p, o.tableDesc, indexFlags, colCfg); err != nil {
		return err
	}
	scan.index = scan.specifiedIndex
	sb := span.MakeBuilder(params.EvalContext(), params.ExecCfg().Codec, o.tableDesc, o.indexDesc)
	scan.spans, err = sb.UnconstrainedSpans()
	if err != nil {
		return err
	}
	scan.isFull = true

	planCtx := params.extendedEvalCtx.DistSQLPlanner.NewPlanningCtx(ctx, params.extendedEvalCtx, params.p, params.p.txn, true /* distribute */)
	// Since physicalCheckOperation might be only one of many check operations
	// that scrubNode needs to perform, we need to make sure that scrubNode
	// is not closed when this physical check operation is being cleaned up.
	planCtx.ignoreClose = true
	physPlan, err := params.extendedEvalCtx.DistSQLPlanner.createScrubPhysicalCheck(planCtx, scan)
	if err != nil {
		return err
	}

	o.primaryColIdxs = primaryColIdxs
	o.columns = columns
	o.run.started = true
	rows, err := scrubRunDistSQL(ctx, planCtx, params.p, physPlan, rowexec.ScrubTypes)
	if err != nil {
		rows.Close(ctx)
		return err
	}
	o.run.rows = rows
	return nil
}

// Next implements the checkOperation interface.
func (o *physicalCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows.At(o.run.rowIndex)
	o.run.rowIndex++

	timestamp, err := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)
	if err != nil {
		return nil, err
	}

	details, ok := row[2].(*tree.DJSON)
	if !ok {
		return nil, errors.Errorf("expected row value 3 to be DJSON, got: %T", row[2])
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		row[0],     /* errorType */
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		row[1], /* primaryKey */
		timestamp,
		tree.DBoolFalse,
		details,
	}, nil
}

// Started implements the checkOperation interface.
func (o *physicalCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *physicalCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= o.run.rows.Len()
}

// Close implements the checkOperation interface.
func (o *physicalCheckOperation) Close(ctx context.Context) {
	if o.run.rows != nil {
		o.run.rows.Close(ctx)
	}
}
