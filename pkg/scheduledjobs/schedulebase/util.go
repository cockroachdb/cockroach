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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	cron "github.com/robfig/cron/v3"
)

// Revisit: I created this file to hold functions that i assume would be common
// to all types of "schedule". this is to avoid code repitation. to that end,
// create_schedule_for_backup should be refactored to use these functions,
// because most of these functions are copied from there (right now these
// functions are used only to create scheduled chaangefeeds). for now, i am
// thinking to refactor backup code in a different commit. is that okay?

// ScheduleRecurrence is a helper struct used to set the schedule recurrence.
type ScheduleRecurrence struct {
	Cron      string
	Frequency time.Duration
}

// NeverRecurs is a sentinel value indicating the schedule never recurs.
var NeverRecurs *ScheduleRecurrence

func frequencyFromCron(now time.Time, cronStr string) (time.Duration, error) {
	expr, err := cron.ParseStandard(cronStr)
	if err != nil {
		return 0, errors.Newf(
			`error parsing schedule expression: %q; it must be a valid cron expression`,
			cronStr)
	}
	nextRun := expr.Next(now)
	return expr.Next(nextRun).Sub(nextRun), nil
}

// ComputeScheduleRecurrence creates ScheduleRecurrence struct from crontab notation
func ComputeScheduleRecurrence(now time.Time, rec *string) (*ScheduleRecurrence, error) {
	if rec == nil {
		return NeverRecurs, nil
	}
	cronStr := *rec
	frequency, err := frequencyFromCron(now, cronStr)
	if err != nil {
		return nil, err
	}

	return &ScheduleRecurrence{cronStr, frequency}, nil
}

// CheckScheduleAlreadyExists returns true if a schedule with the same label already exists.
func CheckScheduleAlreadyExists(
	ctx context.Context, p sql.PlanHookState, scheduleLabel string,
) (bool, error) {

	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(ctx, "check-sched",
		p.Txn(), sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		fmt.Sprintf("SELECT count(schedule_name) FROM %s WHERE schedule_name = '%s'",
			scheduledjobs.ProdJobSchedulerEnv.ScheduledJobsTableName(), scheduleLabel))

	if err != nil {
		return false, err
	}
	return int64(tree.MustBeDInt(row[0])) != 0, nil
}

// ParseOnError parses schedule option optOnExecFailure into jobspb.ScheduleDetails
func ParseOnError(onError string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(onError) {
	case "retry":
		details.OnError = jobspb.ScheduleDetails_RETRY_SOON
	case "reschedule":
		details.OnError = jobspb.ScheduleDetails_RETRY_SCHED
	case "pause":
		details.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
	default:
		return errors.Newf(
			"%q is not a valid on_execution_error; valid values are [retry|reschedule|pause]",
			onError)
	}
	return nil
}

// ParseWaitBehavior parses schedule option optOnPreviousRunning into jobspb.ScheduleDetails
func ParseWaitBehavior(wait string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(wait) {
	case "start":
		details.Wait = jobspb.ScheduleDetails_NO_WAIT
	case "skip":
		details.Wait = jobspb.ScheduleDetails_SKIP
	case "wait":
		details.Wait = jobspb.ScheduleDetails_WAIT
	default:
		return errors.Newf(
			"%q is not a valid on_previous_running; valid values are [start|skip|wait]",
			wait)
	}
	return nil
}

// ParseOnPreviousRunningOption parses optOnPreviousRunning from jobspb.ScheduleDetails_WaitBehavior
func ParseOnPreviousRunningOption(
	onPreviousRunning jobspb.ScheduleDetails_WaitBehavior,
) (string, error) {
	var onPreviousRunningOption string
	switch onPreviousRunning {
	case jobspb.ScheduleDetails_WAIT:
		onPreviousRunningOption = "WAIT"
	case jobspb.ScheduleDetails_NO_WAIT:
		onPreviousRunningOption = "START"
	case jobspb.ScheduleDetails_SKIP:
		onPreviousRunningOption = "SKIP"
	default:
		return onPreviousRunningOption, errors.Newf("%s is an invalid onPreviousRunning option", onPreviousRunning.String())
	}
	return onPreviousRunningOption, nil
}

// ParseOnErrorOption parses optOnExecFailure from jobspb.ScheduleDetails_ErrorHandlingBehavior
func ParseOnErrorOption(onError jobspb.ScheduleDetails_ErrorHandlingBehavior) (string, error) {
	var onErrorOption string
	switch onError {
	case jobspb.ScheduleDetails_RETRY_SCHED:
		onErrorOption = "RESCHEDULE"
	case jobspb.ScheduleDetails_RETRY_SOON:
		onErrorOption = "RETRY"
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		onErrorOption = "PAUSE"
	default:
		return onErrorOption, errors.Newf("%s is an invalid onError option", onError.String())
	}
	return onErrorOption, nil
}
