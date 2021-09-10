// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schedulebase

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/gorhill/cronexpr"
)

// CheckScheduleAlreadyExists returns true if a schedule with the same label already exists.
func CheckScheduleAlreadyExists(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	ie *sql.InternalExecutor,
	scheduleLabel string,
	txn *kv.Txn,
) (bool, error) {

	row, err := ie.QueryRowEx(ctx, "check-sched", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf("SELECT count(schedule_name) FROM %s WHERE schedule_name = '%s'",
			env.ScheduledJobsTableName(), scheduleLabel))

	if err != nil {
		return false, err
	}
	return int64(tree.MustBeDInt(row[0])) != 0, nil
}

// ScheduleRecurrence represents schedule crontab.
type ScheduleRecurrence struct {
	Cron      string
	Frequency time.Duration
}

// IsZero returns true if this schedule recurrence is a nil recurrence.
func (r ScheduleRecurrence) IsZero() bool {
	return r == ScheduleRecurrence{}
}

// EvalScheduleRecurrence evaluates schedule cron entry.
func EvalScheduleRecurrence(
	now time.Time, evalFn func() (string, error),
) (ScheduleRecurrence, error) {
	cron, err := evalFn()
	if err != nil {
		return ScheduleRecurrence{}, err
	}
	expr, err := cronexpr.Parse(cron)
	if err != nil {
		return ScheduleRecurrence{}, errors.Newf(
			`error parsing schedule expression: %q; it must be a valid cron expression`,
			cron)
	}

	nextRun := expr.Next(now)
	frequency := expr.Next(nextRun).Sub(nextRun)
	return ScheduleRecurrence{cron, frequency}, nil
}

// CreateScheduleHeader is the header for "CREATE SCHEDULE..." statements results.
var CreateScheduleHeader = colinfo.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "label", Typ: types.String},
	{Name: "status", Typ: types.String},
	{Name: "first_run", Typ: types.TimestampTZ},
	{Name: "schedule", Typ: types.String},
	{Name: "stmt", Typ: types.String},
}

// EmitSchedule writes schedule information to the specified
func EmitSchedule(
	sj *jobs.ScheduledJob, node tree.NodeFormatter, resultsCh chan<- tree.Datums,
) error {
	var nextRun tree.Datum
	status := "ACTIVE"
	if sj.IsPaused() {
		nextRun = tree.DNull
		status = "PAUSED"
		if s := sj.ScheduleStatus(); s != "" {
			status += ": " + s
		}
	} else {
		next, err := tree.MakeDTimestampTZ(sj.NextRun(), time.Microsecond)
		if err != nil {
			return err
		}
		nextRun = next
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(sj.ScheduleID())),
		tree.NewDString(sj.ScheduleLabel()),
		tree.NewDString(status),
		nextRun,
		tree.NewDString(sj.ScheduleExpr()),
		tree.NewDString(tree.AsString(node)),
	}
	return nil
}
