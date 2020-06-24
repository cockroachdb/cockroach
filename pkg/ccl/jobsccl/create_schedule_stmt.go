// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// createScheduleHeader is the header for "CREATE SCHEDULE..." statements results.
var createScheduleHeader = sqlbase.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "next_run", Typ: types.TimestampTZ},
	{Name: "description", Typ: types.String},
}

func doCreateBackupSchedule(
	ctx context.Context, schedule *tree.ScheduledBackup, resultsCh chan<- tree.Datums,
) error {
	// TODO(yevgeniy): Implement backup schedule.
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	schedule.Format(fmtCtx)
	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(0)),
		tree.DNull,
		tree.NewDString(fmtCtx.String()),
	}
	return nil
}

func createBackupScheduleHook(schedule *tree.ScheduledBackup) sql.PlanHookRowFn {
	return func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		return doCreateBackupSchedule(ctx, schedule, resultsCh)
	}
}

// createSchedulePlanHook implements sql.PlanHookFn.
func createSchedulePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	log.Infof(ctx, "createSchedulePlanHook: stmt: %v+", stmt)
	switch schedule := stmt.(type) {
	case *tree.ScheduledBackup:
		return createBackupScheduleHook(schedule), createScheduleHeader, nil, false, nil
	default:
		return nil, nil, nil, false, nil
	}
}

func init() {
	sql.AddPlanHook(createSchedulePlanHook)
}
