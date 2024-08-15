// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

const (
	baseSelectClause = `
SELECT
	id AS job_id, 
	status, 
	jsonb_array_to_string_array(crdb_internal.pb_to_json(
			'cockroach.sql.jobs.jobspb.Payload', 
			payload)->'logicalReplicationDetails'->'tableNames') AS targets, 
	hlc_to_timestamp((crdb_internal.pb_to_json(
	  	'cockroach.sql.jobs.jobspb.Progress',
	  	progress)->'LogicalReplication'->'replicatedTime'->>'wallTime')::DECIMAL) AS replicated_time%s
FROM crdb_internal.system_jobs 
WHERE job_type = 'LOGICAL REPLICATION'
`

	withDetailsClause = `
		, 
	hlc_to_timestamp((crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		payload)->'logicalReplicationDetails'->'replicationStartTime'->>'wallTime')::DECIMAL) AS replication_start_time
`
)

func (d *delegator) delegateShowLogicalReplicationJobs(
	n *tree.ShowLogicalReplicationJobs,
) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)
	var sqlStmt string
	if n.WithDetails {
		sqlStmt = fmt.Sprintf(baseSelectClause, withDetailsClause)
	} else {
		sqlStmt = fmt.Sprintf(baseSelectClause, "")
	}
	return d.parse(sqlStmt)
}
