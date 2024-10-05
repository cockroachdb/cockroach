// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowChangefeedJobs(n *tree.ShowChangefeedJobs) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)

	// Note: changefeed_details may contain sensitive credentials in sink_uri. This information is redacted when marshaling
	// to JSON in ChangefeedDetails.MarshalJSONPB.
	const (
		// In 23.1, we can use the job_type column to filter jobs.
		queryTarget23_1 = `
			crdb_internal.system_jobs
  			WHERE job_type = 'CHANGEFEED'
		`
		queryTargetPre23_1 = `
			system.jobs
		`
		baseSelectClause0 = `
WITH payload AS (
  SELECT
    id,
    crdb_internal.pb_to_json(
      'cockroach.sql.jobs.jobspb.Payload',
      payload, false, true
    )->'changefeed' AS changefeed_details
  FROM
  %s%%s
)
SELECT
  job_id,
  description,
  user_name,
  status,
  running_status,
  created,
  started,
  finished,
  modified,
  high_water_timestamp,
  error,
  replace(
    changefeed_details->>'sink_uri',
    '\u0026', '&'
  ) AS sink_uri,
  ARRAY (
    SELECT
      concat(
        database_name, '.', schema_name, '.',
        name
      )
    FROM
      crdb_internal.tables
    WHERE
      table_id = ANY (descriptor_ids)
  ) AS full_table_names,
  changefeed_details->'opts'->>'topics' AS topics,
  COALESCE(changefeed_details->'opts'->>'format','json') AS format
FROM
  crdb_internal.jobs
  INNER JOIN payload ON id = job_id`
	)

	use23_1 := d.evalCtx.Settings.Version.IsActive(d.ctx, clusterversion.V23_1BackfillTypeColumnInJobsTable)

	var whereClause, innerWhereClause, orderbyClause string

	if n.Jobs == nil {
		// The query intends to present:
		// - first all the running jobs sorted in order of start time,
		// - then all completed jobs sorted in order of completion time.
		//
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderbyClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
		if !use23_1 {
			whereClause = fmt.Sprintf("WHERE job_type = '%s'", jobspb.TypeChangefeed)
		}
	} else {
		// Limit the jobs displayed to the select statement in n.Jobs.
		if use23_1 {
			whereClause = fmt.Sprintf(`WHERE job_id in (%s)`, n.Jobs.String())
			innerWhereClause = fmt.Sprintf(` AND id in (%s)`, n.Jobs.String())
		} else {
			whereClause = fmt.Sprintf("WHERE job_type = '%s' AND job_id in (%s)",
				jobspb.TypeChangefeed, n.Jobs.String())
			innerWhereClause = fmt.Sprintf(` WHERE id in (%s)`, n.Jobs.String())
		}
	}

	var baseSelectClause string
	if use23_1 {
		baseSelectClause = fmt.Sprintf(baseSelectClause0, queryTarget23_1)
	} else {
		baseSelectClause = fmt.Sprintf(baseSelectClause0, queryTargetPre23_1)
	}
	selectClause := fmt.Sprintf(baseSelectClause, innerWhereClause)
	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)

	return d.parse(sqlStmt)
}
