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
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
)

func (d *delegator) delegateJobControl(stmt *tree.ControlJobsForSchedules) (tree.Statement, error) {
	var filterByStatus []jobs.Status
	// We filter out the jobs which have a status on which it is invalid to apply
	// the specified Command. This prevents the whole ControlJobsForSchedules
	// query from erring out if any job is in such a state that cannot be
	// controlled.
	// The valid statuses prior to application of the Command have been derived
	// from the control specific methods in pkg/jobs/jobs.go. All other states are
	// either invalid starting states or result in no-ops.
	switch tree.JobCommandToStatement[stmt.Command] {
	case "PAUSE":
		filterByStatus = []jobs.Status{jobs.StatusPending, jobs.StatusRunning, jobs.StatusReverting}
	case "RESUME":
		filterByStatus = []jobs.Status{jobs.StatusPaused}
	case "CANCEL":
		filterByStatus = []jobs.Status{jobs.StatusPending, jobs.StatusRunning, jobs.StatusPaused}
	default:
		return nil, errors.New("unidentified job control command")
	}

	var filterExprs []string
	for _, status := range filterByStatus {
		filterExprs = append(filterExprs, fmt.Sprintf("'%s'", status))
	}

	var filterClause string
	if len(filterExprs) > 0 {
		filterClause = fmt.Sprint(strings.Join(filterExprs, ", "))
	}

	// TODO(yevgeniy): it is very unfortunate that we have to use the IN() clause
	// in order to select matching jobs.
	// It would be better if the job control statement had a better (planNode)
	// implementation, so that we can rely on the optimizer to select relevant
	// nodes.
	return parse(fmt.Sprintf(`%s JOBS SELECT id FROM system.jobs WHERE jobs.created_by_type = '%s' 
AND jobs.status IN (%s) AND jobs.created_by_id IN (%s)`,
		tree.JobCommandToStatement[stmt.Command], jobs.CreatedByScheduledJobs, filterClause,
		stmt.Schedules))
}
