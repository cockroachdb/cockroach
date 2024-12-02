// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showCreateScheduleColumns = colinfo.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "create_statement", Typ: types.String},
}

const (
	scheduleIDIdx = iota
	createStmtIdx
)

func loadSchedules(params runParams, n *tree.ShowCreateSchedules) ([]*jobs.ScheduledJob, error) {
	env := JobSchedulerEnv(params.ExecCfg().JobsKnobs())
	var schedules []*jobs.ScheduledJob
	var rows []tree.Datums
	var cols colinfo.ResultColumns

	if n.ScheduleID != nil {
		sjID, err := strconv.Atoi(n.ScheduleID.String())
		if err != nil {
			return nil, err
		}

		datums, columns, err := params.p.InternalSQLTxn().QueryRowExWithCols(
			params.ctx,
			"load-schedules",
			params.p.Txn(), sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf("SELECT * FROM %s WHERE schedule_id = $1", env.ScheduledJobsTableName()),
			tree.NewDInt(tree.DInt(sjID)))
		if err != nil {
			return nil, err
		}
		rows = append(rows, datums)
		cols = columns
	} else {
		datums, columns, err := params.p.InternalSQLTxn().QueryBufferedExWithCols(
			params.ctx,
			"load-schedules",
			params.p.Txn(), sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf("SELECT * FROM %s", env.ScheduledJobsTableName()))
		if err != nil {
			return nil, err
		}
		rows = append(rows, datums...)
		cols = columns
	}

	for _, row := range rows {
		schedule := jobs.NewScheduledJob(env)
		if err := schedule.InitFromDatums(row, cols); err != nil {
			return nil, err
		}
		schedules = append(schedules, schedule)
	}
	return schedules, nil
}

func (p *planner) ShowCreateSchedule(
	ctx context.Context, n *tree.ShowCreateSchedules,
) (planNode, error) {
	// Only privileged users can execute SHOW CREATE SCHEDULE
	if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
		return nil, err
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.CreateSchedule)

	return &delayedNode{
		name:    fmt.Sprintf("SHOW CREATE SCHEDULE %d", n.ScheduleID),
		columns: showCreateScheduleColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			scheduledJobs, err := loadSchedules(
				runParams{ctx: ctx, p: p, extendedEvalCtx: &p.extendedEvalCtx}, n)
			if err != nil {
				return nil, err
			}

			var rows []tree.Datums
			for _, sj := range scheduledJobs {
				ex, err := jobs.GetScheduledJobExecutor(sj.ExecutorType())
				if err != nil {
					return nil, err
				}

				createStmtStr, err := ex.GetCreateScheduleStatement(ctx, p.InternalSQLTxn(), scheduledjobs.ProdJobSchedulerEnv, sj)
				if err != nil {
					return nil, err
				}

				row := tree.Datums{
					scheduleIDIdx: tree.NewDInt(tree.DInt(sj.ScheduleID())),
					createStmtIdx: tree.NewDString(createStmtStr),
				}
				rows = append(rows, row)
			}

			v := p.newContainerValuesNode(showCreateScheduleColumns, len(rows))
			for _, row := range rows {
				if _, err := v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
