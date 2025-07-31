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
),
spec_details AS (
  SELECT
    id,
    json_array_elements(changefeed_details->'target_specifications') ? 'type' AND (json_array_elements(changefeed_details->'target_specifications')->'type')::int = 3 AS is_db, -- database level changefeed
    json_array_elements(changefeed_details->'target_specifications')->>'descriptor_id' AS descriptor_id,
    changefeed_details->'tables' AS tables
  FROM payload
),
db_targets AS (
  SELECT
    sd.id,
    CASE
      WHEN %t
      THEN array_agg(concat(t.database_name,'.',t.schema_name,'.',t.name))
      ELSE
      array_agg(distinct t.database_name)
    END AS names
  FROM spec_details sd
  INNER JOIN crdb_internal.tables t ON sd.descriptor_id::int = t.parent_id
  WHERE sd.is_db
  GROUP BY sd.id
),
table_targets AS (
  SELECT 
    sd.id,
    array_agg(distinct concat(t.database_name,'.',t.schema_name, '.', t.name)) AS names
  FROM spec_details sd
  CROSS JOIN LATERAL json_each(sd.tables) AS j(table_id, val)
  INNER JOIN crdb_internal.tables t ON t.table_id = j.table_id::INT
  WHERE NOT sd.is_db
  GROUP BY sd.id
),
targets AS (
  SELECT
    id,
    names
  FROM db_targets
  UNION ALL
  SELECT
    id,
    names
  FROM table_targets
)
SELECT
  job_id,
  description,
  user_name,
  status,
  running_status,
  created,
  created AS started,
  finished,
  modified,
  high_water_timestamp,
  hlc_to_timestamp(high_water_timestamp) AS readable_high_water_timestamptz,
  error,
  replace(
    changefeed_details->>'sink_uri',
    '\u0026', '&'
  ) AS sink_uri,
  targets.names AS full_table_names,
  changefeed_details->'opts'->>'topics' AS topics,
  COALESCE(changefeed_details->'opts'->>'format','json') AS format
FROM
  crdb_internal.jobs
  INNER JOIN targets ON job_id = targets.id
  INNER JOIN payload ON job_id = payload.id
`
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

	selectClause := fmt.Sprintf(baseSelectClause, innerWhereClause, n.WatchedTables)
	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)

	return d.parse(sqlStmt)
}
