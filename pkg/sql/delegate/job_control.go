// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ControlJobsDelegate is a struct with options for delegate.delegateJobControl
type ControlJobsDelegate struct {
	Command tree.JobCommand

	// One and only one of these should be non-empty
	Schedules *tree.Select
	Type      string
}

// protoNameForType maps job types to the matching protobuf names for Payload.details in jobs.proto
// This is also used to enumerate the types of jobs that we actually handle
var protobufNameForType = map[string]string{
	"changefeed": "changefeed",
	"backup":     "backup",
	"import":     "import",
	"restore":    "restore",
}

func (d *delegator) delegateJobControl(stmt ControlJobsDelegate) (tree.Statement, error) {
	// We filter out the jobs which have a status on which it is invalid to apply
	// the specified Command. This prevents the whole ControlJobsForSchedules
	// query from erring out if any job is in such a state that cannot be
	// controlled.
	// The valid statuses prior to application of the Command have been derived
	// from the control specific methods in pkg/jobs/jobs.go. All other states are
	// either invalid starting states or result in no-ops.

	validStartStatusForCommand := map[tree.JobCommand][]jobs.State{
		tree.PauseJob:  {jobs.StatePending, jobs.StateRunning, jobs.StateReverting},
		tree.ResumeJob: {jobs.StatePaused},
		tree.CancelJob: {jobs.StatePending, jobs.StateRunning, jobs.StatePaused},
	}

	var filterExprs []string
	var filterClause string
	if statuses, ok := validStartStatusForCommand[stmt.Command]; ok {
		for _, status := range statuses {
			filterExprs = append(filterExprs, fmt.Sprintf("'%s'", status))
		}
		filterClause = fmt.Sprint(strings.Join(filterExprs, ", "))
	} else {
		return nil, errors.New("unexpected Command encountered in schedule job control")
	}

	// TODO(yevgeniy): it is very unfortunate that we have to use the IN() clause
	// in order to select matching jobs.
	// It would be better if the job control statement had a better (planNode)
	// implementation, so that we can rely on the optimizer to select relevant
	// nodes.

	if stmt.Schedules != nil {
		return d.parse(fmt.Sprintf(`%s JOBS SELECT id FROM system.jobs WHERE jobs.created_by_type = '%s' 
AND jobs.status IN (%s) AND jobs.created_by_id IN (%s)`,
			tree.JobCommandToStatement[stmt.Command], jobs.CreatedByScheduledJobs, filterClause,
			stmt.Schedules))
	}

	if stmt.Type != "" {
		if _, ok := protobufNameForType[stmt.Type]; !ok {
			return nil, errors.New("unsupported job type")
		}
		queryStrFormat := `%s JOBS (
  SELECT id
  FROM (
        SELECT id,
               status,
               (
                crdb_internal.pb_to_json(
                  'cockroach.sql.jobs.jobspb.Payload',
                  payload, false, true
                )->'%s'
               ) IS NOT NULL AS correct_type
          FROM crdb_internal.system_jobs
         WHERE status IN (%s)
       )
  WHERE correct_type
);`
		return d.parse(fmt.Sprintf(queryStrFormat, tree.JobCommandToStatement[stmt.Command], protobufNameForType[stmt.Type], filterClause))
	}

	return nil, errors.AssertionFailedf("Missing Schedules or Type clause in delegate parameters")
}
