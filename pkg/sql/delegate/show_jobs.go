// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowJobs(n *tree.ShowJobs) (tree.Statement, error) {
	if n.Schedules != nil {
		// Limit the jobs displayed to the ones started by specified schedules.
		return parse(fmt.Sprintf(`
SHOW JOBS SELECT id FROM system.jobs WHERE created_by_type='%s' and created_by_id IN (%s)
`, jobs.CreatedByScheduledJobs, n.Schedules.String()),
		)
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)

	const (
		selectClause = `
SELECT job_id, job_type, description, statement, user_name, status,
       running_status, created, started, finished, modified,
       fraction_completed, error, coordinator_id, trace_id, last_run,
       next_run, num_runs, execution_errors
  FROM crdb_internal.jobs`
	)
	var typePredicate, whereClause, orderbyClause string
	if n.Jobs == nil {
		// Display all [only automatic] jobs without selecting specific jobs.
		{
			// Build the typePredicate.
			var predicate strings.Builder
			if n.Automatic {
				predicate.WriteString("job_type IN (")
			} else {
				predicate.WriteString("job_type IS NULL OR job_type NOT IN (")
			}
			for i, jobType := range jobspb.AutomaticJobTypes {
				if i != 0 {
					predicate.WriteString(", ")
				}
				predicate.WriteByte('\'')
				predicate.WriteString(jobType.String())
				predicate.WriteByte('\'')
			}
			predicate.WriteByte(')')
			typePredicate = predicate.String()
		}

		// The query intends to present:
		// - first all the running jobs sorted in order of start time,
		// - then all completed jobs sorted in order of completion time.
		whereClause = fmt.Sprintf(
			`WHERE %s AND (finished IS NULL OR finished > now() - '12h':::interval)`, typePredicate)
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderbyClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
	} else {
		// Limit the jobs displayed to the select statement in n.Jobs.
		whereClause = fmt.Sprintf(`WHERE job_id in (%s)`, n.Jobs.String())
	}

	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)
	if n.Block {
		sqlStmt = fmt.Sprintf(
			`
    WITH jobs AS (SELECT * FROM [%s]),
       sleep_and_restart_if_unfinished AS (
              SELECT IF(pg_sleep(1), crdb_internal.force_retry('24h'), 1)
                     = 0 AS timed_out
                FROM (SELECT job_id FROM jobs WHERE finished IS NULL LIMIT 1)
             ),
       fail_if_slept_too_long AS (
                SELECT crdb_internal.force_error('55000', 'timed out waiting for jobs')
                  FROM sleep_and_restart_if_unfinished
                 WHERE timed_out
              )
SELECT *
  FROM jobs
 WHERE NOT EXISTS(SELECT * FROM fail_if_slept_too_long)`, sqlStmt)
	}
	return parse(sqlStmt)
}
