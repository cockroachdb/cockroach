// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// commandColumn converts executor execution arguments into jsonb representation.
const commandColumn = `crdb_internal.pb_to_json('cockroach.jobs.jobspb.ExecutionArguments', execution_args)->'args'`

func (d *delegator) delegateShowSchedules(n *tree.ShowSchedules) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Schedules)

	columnExprs := []string{
		"schedule_id as id",
		"schedule_name as label",
		"(CASE WHEN next_run IS NULL THEN 'PAUSED' ELSE 'ACTIVE' END) AS schedule_status",
		"next_run",
		"crdb_internal.pb_to_json('cockroach.jobs.jobspb.ScheduleState', schedule_state)->>'status' as state",
		"(CASE WHEN schedule_expr IS NULL THEN 'NEVER' ELSE schedule_expr END) as recurrence",
		fmt.Sprintf(`(
SELECT count(*) FROM system.jobs
WHERE status='%s' AND created_by_type='%s' AND created_by_id=schedule_id
) AS jobsRunning`, jobs.StatusRunning, jobs.CreatedByScheduledJobs),
		"owner",
		"created",
	}

	var whereExprs []string

	switch n.WhichSchedules {
	case tree.PausedSchedules:
		whereExprs = append(whereExprs, "next_run IS NULL")
	case tree.ActiveSchedules:
		whereExprs = append(whereExprs, "next_run IS NOT NULL")
	}

	switch n.ExecutorType {
	case tree.ScheduledBackupExecutor:
		whereExprs = append(whereExprs, fmt.Sprintf(
			"executor_type = '%s'", tree.ScheduledBackupExecutor.InternalName()))
		columnExprs = append(columnExprs, fmt.Sprintf(
			"%s->>'backup_statement' AS command", commandColumn))
	default:
		// Strip out '@type' tag from the ExecutionArgs.args, and display what's left.
		columnExprs = append(columnExprs, fmt.Sprintf("%s #-'{@type}' AS command", commandColumn))
	}

	if n.ScheduleID != nil {
		whereExprs = append(whereExprs,
			fmt.Sprintf("schedule_id=(%s)", tree.AsString(n.ScheduleID)))
	}

	var whereClause string
	if len(whereExprs) > 0 {
		whereClause = fmt.Sprintf("WHERE (%s)", strings.Join(whereExprs, " AND "))
	}

	return parse(fmt.Sprintf(
		"SELECT %s FROM system.scheduled_jobs %s",
		strings.Join(columnExprs, ","),
		whereClause,
	))
}
