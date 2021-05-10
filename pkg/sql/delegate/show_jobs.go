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
		selectClause = `SELECT job_id, job_type, description, statement, user_name, status,
				       running_status, created, started, finished, modified,
				       fraction_completed, error, coordinator_id
				FROM crdb_internal.jobs`
		selectClauseChangefeed = `WITH payload AS (SELECT id, crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', payload) AS payload_json FROM system.jobs)
               SELECT job_id, job_type, description, statement, user_name, status,
               running_status, created, started, finished, modified,
               fraction_completed, high_water_timestamp, error, coordinator_id, 
               btrim(jsonb_pretty(json_extract_path(payload_json, 'changefeed', 'sinkUri')), '"') AS sink_uri,
               ARRAY(SELECT concat(database_name, '.', schema_name, '.', name) from crdb_internal.tables WHERE table_id = any(descriptor_ids)) AS full_table_names,
               btrim(jsonb_pretty(json_extract_path(payload_json, 'changefeed', 'opts', 'format')), '"') AS format
				FROM crdb_internal.jobs INNER JOIN payload ON id = job_id`
	)
	var typePredicate, whereClause, orderbyClause string
	if n.Jobs == nil {
		// Display all [only automatic] jobs without selecting specific jobs.
		if n.WhichJobs == tree.AutomaticJobs {
			typePredicate = fmt.Sprintf("job_type = '%s'", jobspb.TypeAutoCreateStats)
		} else if n.WhichJobs == tree.ChangefeedJobs {
			typePredicate = fmt.Sprintf("job_type = '%s'", jobspb.TypeChangefeed)
		} else {
			typePredicate = fmt.Sprintf(
				"(job_type IS NULL OR job_type != '%s')", jobspb.TypeAutoCreateStats,
			)
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

	var sqlStmt string
	if n.WhichJobs == tree.ChangefeedJobs {
		sqlStmt = fmt.Sprintf("%s %s %s", selectClauseChangefeed, whereClause, orderbyClause)
	} else {
		sqlStmt = fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)
	}

	if n.Block {
		sqlStmt = fmt.Sprintf(
			`SELECT * FROM [%s]
			 WHERE
			    IF(finished IS NULL,
			      IF(pg_sleep(1), crdb_internal.force_retry('24h'), 0),
			      0
			    ) = 0`, sqlStmt)
	}
	return parse(sqlStmt)
}
