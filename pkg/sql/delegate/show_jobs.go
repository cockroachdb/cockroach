// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

const ageFilter = `(finished IS NULL OR finished > now() - '12h':::interval)`

func constructSelectQuery(n *tree.ShowJobs) string {
	var baseQuery strings.Builder
	baseQuery.WriteString(`SELECT job_id, job_type, `)
	if n.Jobs == nil {
		baseQuery.WriteString(`CASE WHEN length(description) > 70 THEN `)
		baseQuery.WriteString(`concat(substr(description, 0, 60)||' … '||right(description, 7)) `)
		baseQuery.WriteString(`ELSE description END as description, `)
	} else {
		baseQuery.WriteString(`description, statement, `)
	}
	baseQuery.WriteString(`user_name, status, running_status, `)
	baseQuery.WriteString(`date_trunc('second', created) as created, date_trunc('second', started) as started, `)
	baseQuery.WriteString(`date_trunc('second', finished) as finished, date_trunc('second', modified) as modified, `)
	baseQuery.WriteString(`fraction_completed, error, coordinator_id`)

	if n.Jobs != nil {
		baseQuery.WriteString(`, trace_id, execution_errors`)
	}

	// Check if there are any SHOW JOBS options that we need to add columns for.
	if n.Options != nil {
		if n.Options.ExecutionDetails {
			baseQuery.WriteString(`, NULLIF(crdb_internal.job_execution_details(job_id)->>'plan_diagram'::STRING, '') AS plan_diagram`)
			baseQuery.WriteString(`, NULLIF(crdb_internal.job_execution_details(job_id)->>'per_component_fraction_progressed'::STRING, '') AS component_fraction_progressed`)
		}
	}

	baseQuery.WriteString("\nFROM crdb_internal.jobs")

	// Now add the predicates and ORDER BY clauses.
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
		// - then all completed jobs sorted in order of completion time (no more than 12 hours).
		whereClause = fmt.Sprintf(
			`WHERE %s AND %s`, typePredicate, ageFilter)
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderbyClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
	} else {
		// Limit the jobs displayed to the select statement in n.Jobs.
		whereClause = fmt.Sprintf(`WHERE job_id in (%s)`, n.Jobs.String())
	}
	return fmt.Sprintf("%s %s %s", baseQuery.String(), whereClause, orderbyClause)
}

func (d *delegator) delegateShowJobs(n *tree.ShowJobs) (tree.Statement, error) {
	if n.Schedules != nil {
		// Limit the jobs displayed to the ones started by specified schedules.
		return d.parse(fmt.Sprintf(`
SHOW JOBS SELECT id FROM system.jobs WHERE created_by_type='%s' and created_by_id IN (%s)
`, jobs.CreatedByScheduledJobs, n.Schedules.String()),
		)
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)

	stmt := constructSelectQuery(n)
	if n.Block {
		stmt = fmt.Sprintf(
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
 WHERE NOT EXISTS(SELECT * FROM fail_if_slept_too_long)`, stmt)
	}
	return d.parse(stmt)
}
