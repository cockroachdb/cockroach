// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showCreateTableColumns = colinfo.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "create_statement", Typ: types.String},
}

const (
	scheduleID = iota
	createStmt
)

func (p *planner) ShowCreateSchedule(
	ctx context.Context, n *tree.ShowCreateSchedules,
) (planNode, error) {
	return &delayedNode{
		name: fmt.Sprintf("SHOW CREATE SCHEDULE %d", n.ScheduleID),
		columns: showCreateTableColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			sjID, err := strconv.Atoi(n.ScheduleID.String())
			if err != nil {
				return nil, err
			}

			sjIdDatum := tree.NewDInt(tree.DInt(sjID))
			sj, err := loadSchedule(runParams{ctx: ctx, p: p,
				extendedEvalCtx: &p.extendedEvalCtx}, sjIdDatum)
			if err != nil {
				return nil, err
			}

			ex, _, err := jobs.GetScheduledJobExecutor(sj.ExecutorType())
			if err != nil {
				return nil, err
			}

			createStmtStr, err := ex.CreateString(sj)
			if err != nil {
				return nil, err
			}

			row := tree.Datums{
				scheduleID: sjIdDatum,
				createStmt: tree.NewDString(createStmtStr),
			}

			v := p.newContainerValuesNode(showCreateTableColumns, len(row))
			if _, err := v.rows.AddRow(ctx, row); err != nil {
				v.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}
