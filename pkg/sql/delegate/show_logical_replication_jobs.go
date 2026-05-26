// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

const (
	baseSelectClause = `
WITH table_names AS (
	SELECT
		t.job_id,
		array_agg(t.table_name) AS targets
	FROM (
		SELECT
			id AS job_id,
			crdb_internal.get_fully_qualified_table_name(jsonb_array_elements(crdb_internal.pb_to_json(
			 	 			'cockroach.sql.jobs.jobspb.Payload', 
			 	 			payload)->'logicalReplicationDetails'->'replicationPairs')['dstDescriptorId']::INT) AS table_name
		FROM crdb_internal.system_jobs 
		WHERE job_type = 'LOGICAL REPLICATION'
	) AS t
	GROUP BY t.job_id
)

SELECT
	job_info.id AS job_id, 
	job_info.status, 
	table_names.targets AS tables,
	hlc_to_timestamp((crdb_internal.pb_to_json(
	  	'cockroach.sql.jobs.jobspb.Progress',
	  	job_info.progress)->'LogicalReplication'->'replicatedTime'->>'wallTime')::DECIMAL) AS replicated_time%s
FROM crdb_internal.system_jobs AS job_info
LEFT JOIN table_names
ON job_info.id = table_names.job_id
WHERE job_type = 'LOGICAL REPLICATION'
`

	withDetailsClause = `
		, 
	hlc_to_timestamp((crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		payload)->'logicalReplicationDetails'->'replicationStartTime'->>'wallTime')::DECIMAL) AS replication_start_time,
	IFNULL(crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		payload)->'logicalReplicationDetails'->'defaultConflictResolution'->>'conflictResolutionType', 'LWW') AS conflict_resolution_type,
	crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		payload)->'logicalReplicationDetails'->>'command' AS command`
)

func (d *delegator) delegateShowLogicalReplicationJobs(
	n *tree.ShowLogicalReplicationJobs,
) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.LogicalReplicationJobs)
	var sqlStmt string
	if n.WithDetails {
		sqlStmt = fmt.Sprintf(baseSelectClause, withDetailsClause)
	} else {
		sqlStmt = fmt.Sprintf(baseSelectClause, "")
	}
	return d.parse(sqlStmt)
}
