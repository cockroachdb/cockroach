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
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// physicalCheckOperation is a check on an indexes physical data.
type physicalCheckOperation struct {
	started   bool
	tableName *tree.TableName
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor

	// Intermediate values.
	rows     *sqlbase.RowContainer
	rowIndex int

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []*sqlbase.ColumnDescriptor
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int
}

func newPhysicalCheckOperation(
	tableName *tree.TableName, tableDesc *sqlbase.TableDescriptor, indexDesc *sqlbase.IndexDescriptor,
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
func (o *physicalCheckOperation) Start(ctx context.Context, p *planner) error {
	// Collect all of the columns, their types, and their IDs.
	var columnIDs []tree.ColumnID
	colIDToIdx := make(map[sqlbase.ColumnID]int, len(o.tableDesc.Columns))
	columns := make([]*sqlbase.ColumnDescriptor, len(columnIDs))
	for i := range o.tableDesc.Columns {
		colIDToIdx[o.tableDesc.Columns[i].ID] = i
	}

	// Collect all of the columns being scanned.
	if o.indexDesc.ID == o.tableDesc.PrimaryIndex.ID {
		for i := range o.tableDesc.Columns {
			columnIDs = append(columnIDs, tree.ColumnID(o.tableDesc.Columns[i].ID))
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
		idx := colIDToIdx[sqlbase.ColumnID(columnIDs[i])]
		columns = append(columns, &o.tableDesc.Columns[idx])
	}

	// Find the row indexes for all of the primary index columns.
	primaryColIdxs, err := getPrimaryColIdxs(o.tableDesc, columns)
	if err != nil {
		return err
	}

	indexHints := &tree.IndexHints{
		IndexID:     tree.IndexID(o.indexDesc.ID),
		NoIndexJoin: true,
	}
	scan := p.Scan()
	scan.isCheck = true
	if err := scan.initTable(p, o.tableDesc, indexHints, publicColumns, columnIDs); err != nil {
		return err
	}
	plan := planNode(scan)

	neededColumns := make([]bool, len(o.tableDesc.Columns))
	for _, id := range columnIDs {
		neededColumns[colIDToIdx[sqlbase.ColumnID(id)]] = true
	}

	// Optimize the plan. This is required in order to populate scanNode
	// spans.
	plan, err = p.optimizePlan(ctx, plan, neededColumns)
	if err != nil {
		plan.Close(ctx)
		return err
	}
	defer plan.Close(ctx)

	scan = plan.(*scanNode)

	span := o.tableDesc.IndexSpan(o.indexDesc.ID)
	spans := []roachpb.Span{span}

	planCtx := p.session.distSQLPlanner.newPlanningCtx(ctx, &p.evalCtx, p.txn)
	physPlan, err := p.session.distSQLPlanner.createScrubPhysicalCheck(
		&planCtx, scan, *o.tableDesc, *o.indexDesc, spans, p.ExecCfg().Clock.Now())
	if err != nil {
		return err
	}

	rows, err := scrubRunDistSQL(ctx, &planCtx, p, &physPlan)
	if err != nil {
		rows.Close(ctx)
		return err
	}

	o.started = true
	o.rows = rows
	o.primaryColIdxs = primaryColIdxs
	o.columns = columns
	return nil
}

// Next implements the checkOperation interface.
func (o *physicalCheckOperation) Next(ctx context.Context, p *planner) (tree.Datums, error) {
	row := o.rows.At(o.rowIndex)
	o.rowIndex++

	timestamp := tree.MakeDTimestamp(
		p.evalCtx.GetStmtTimestamp(), time.Nanosecond)

	details, ok := row[2].(*tree.DJSON)
	if !ok {
		return nil, errors.Errorf("expected row value 3 to be DJSON, got: %T", row[2])
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		row[0],     /* errorType */
		tree.NewDString(o.tableName.Database()),
		tree.NewDString(o.tableName.Table()),
		row[1], /* primaryKey */
		timestamp,
		tree.DBoolFalse,
		details,
	}, nil
}

// Started implements the checkOperation interface.
func (o *physicalCheckOperation) Started() bool {
	return o.started
}

// Done implements the checkOperation interface.
func (o *physicalCheckOperation) Done(ctx context.Context) bool {
	return o.rows == nil || o.rowIndex >= o.rows.Len()
}

// Close implements the checkOperation interface.
func (o *physicalCheckOperation) Close(ctx context.Context) {
	if o.rows != nil {
		o.rows.Close(ctx)
	}
}
