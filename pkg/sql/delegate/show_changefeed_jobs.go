// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowChangefeedJobs(n *tree.ShowChangefeedJobs) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)

	// Note: changefeed_details may contain sensitive credentials in sink_uri. This information is redacted when marshaling
	// to JSON in ChangefeedDetails.MarshalJSONPB.
	const (
		baseSelectClause = `
WITH payload AS (
  SELECT
    id,
    crdb_internal.pb_to_json(
      'cockroach.sql.jobs.jobspb.Payload',
      payload, false, true
    )->'changefeed' AS changefeed_details
  FROM
  crdb_internal.system_jobs
  WHERE job_type = 'CHANGEFEED'%s
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
  hlc_to_timestamp(high_water_timestamp) as readable_high_water_timestamptz,
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
      table_id = ANY (SELECT key::INT FROM json_each(changefeed_details->'tables'))
  ) AS full_table_names,
  changefeed_details->'opts'->>'topics' AS topics,
  COALESCE(changefeed_details->'opts'->>'format','json') AS format
FROM
  crdb_internal.jobs
  INNER JOIN payload ON id = job_id`
	)

	var whereClause, innerWhereClause, orderbyClause string
	if n.Jobs == nil {
		// The query intends to present:
		// - first all the running jobs sorted in order of start time,
		// - then all completed jobs sorted in order of completion time (no more than 12 hours).
		whereClause = fmt.Sprintf(`WHERE %s`, ageFilter)
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderbyClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
	} else {
		// Limit the jobs displayed to the select statement in n.Jobs.
		whereClause = fmt.Sprintf(`WHERE job_id in (%s)`, n.Jobs.String())
		innerWhereClause = fmt.Sprintf(` AND id in (%s)`, n.Jobs.String())
	}

	selectClause := fmt.Sprintf(baseSelectClause, innerWhereClause)
	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)

	return d.parse(sqlStmt)
}
