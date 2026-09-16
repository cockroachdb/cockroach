// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
    json_array_elements(changefeed_details->'target_specifications') ? 'type' AND (json_array_elements(changefeed_details->'target_specifications')->'type')::int = %d AS is_db, -- database level changefeed
    json_array_elements(changefeed_details->'target_specifications')->>'descriptor_id' AS descriptor_id,
    changefeed_details->'tables' AS tables,
    json_array_elements(changefeed_details->'target_specifications')->>'filter_list' AS filter_specification
  FROM payload
),
db_targets AS (
  SELECT
    sd.id,
    CASE
      WHEN %t
      THEN array_agg(concat(t.database_name,'.',t.schema_name,'.',t.name))
      ELSE
      ARRAY[]::STRING[]
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
),
database as (
  SELECT
    sd.id, 
    name
  FROM spec_details sd
  INNER JOIN crdb_internal.databases d ON sd.descriptor_id::int = d.id
),
filter_targets_map AS (
  SELECT
    sd.id,
    sd.filter_specification::json AS filter_specification_json,
    (sd.filter_specification::json)->'filter_type' AS filter_type,
    (sd.filter_specification::json)->'tables' AS filter_tables_map
  FROM spec_details sd
),
filter_targets_array AS (
  SELECT
    ftm.id,
    ftm.filter_type,
    array_agg(k) AS filter_tables
  FROM filter_targets_map ftm
  CROSS JOIN LATERAL (
    SELECT key
    FROM json_object_keys(ftm.filter_tables_map) AS key
    WHERE json_typeof(ftm.filter_tables_map) = 'object'
  ) AS j(k)
  GROUP BY ftm.id, ftm.filter_type
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
  CASE
    -- Include filter is enumerated to 1.
    WHEN COALESCE((fta.filter_type::string)::INT, 0) = 1 THEN
      (
        SELECT COALESCE(array_agg(x), ARRAY[]::STRING[])
        -- targets.names is the list, before filtering, of fully qualified table names.
        FROM unnest(targets.names) AS x
        WHERE x = ANY(COALESCE(fta.filter_tables, ARRAY[]::STRING[]))
      )
    -- Exclude filter is enumerated to 0. This is the default value in the 
    -- protobuf, so it may be omitted from the serialization. 
    -- We treat exclude or no filter as the same, where if there is no filter,
    -- in this subquery treat it as an empty exclude filter array.
    WHEN COALESCE((fta.filter_type::string)::INT, 0) = 0 THEN
      (
        SELECT COALESCE(array_agg(x), ARRAY[]::STRING[])
        FROM unnest(targets.names) AS x
        WHERE NOT (x = ANY(COALESCE(fta.filter_tables, ARRAY[]::STRING[])))
      )
  END AS full_table_names,
  changefeed_details->'opts'->>'topics' AS topics,
  COALESCE(changefeed_details->'opts'->>'format','json') AS format,
  database.name AS database_name
FROM
  crdb_internal.jobs
  LEFT JOIN targets ON job_id = targets.id
  INNER JOIN payload ON job_id = payload.id
  LEFT JOIN database ON job_id = database.id
  LEFT JOIN filter_targets_array fta ON job_id = fta.id
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

	selectClause := fmt.Sprintf(baseSelectClause, innerWhereClause, jobspb.ChangefeedTargetSpecification_DATABASE, n.IncludeWatchedTables)
	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)

	return d.parse(sqlStmt)
}
