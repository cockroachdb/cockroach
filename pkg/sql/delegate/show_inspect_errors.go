// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

func (d *delegator) delegateShowInspectErrors(n *tree.ShowInspectErrors) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.InspectErrors)

	if err := d.catalog.CheckPrivilege(d.ctx, syntheticprivilege.GlobalPrivilegeObject,
		d.catalog.GetCurrentUser(), privilege.INSPECT); err != nil {
		return nil, err
	}

	var tableID = catid.InvalidDescID
	if n.TableName != nil {
		resolvedTableID, err := d.evalCtx.Planner.ResolveTableName(d.ctx, n.TableName)
		if err != nil {
			return nil, err
		}

		tableID = catid.DescID(resolvedTableID)
	}

	var query strings.Builder

	query.WriteString(`WITH
	completed_inspect_jobs AS (
		SELECT id
		FROM system.jobs
		WHERE job_type = 'INSPECT'
	`)
	if n.JobID == nil {
		query.WriteString(fmt.Sprintf(` AND status IN ('%s', '%s', '%s', '%s')`,
			jobs.StateFailed, jobs.StateSucceeded, jobs.StateCanceled, jobs.StateRevertFailed)) // in terminal state
	} else {
		query.WriteString(fmt.Sprintf(" AND id = %d", *n.JobID))
	}

	// TODO(148287): query the inspect job payload to figure out if a job touches a particular table or database ID
	// If a table was specified, only consider jobs that reported errors on it.
	// If a job ID was specified, only consider that job. The records from the
	// most recent completed job that satisfies those criteria is used.
	query.WriteString(`),
	job_id AS (
		SELECT max(cij.id) as id
		FROM completed_inspect_jobs cij
		JOIN system.inspect_errors ie ON cij.id = ie.job_id
		WHERE 1=1
	`)
	if tableID != catid.InvalidDescID {
		query.WriteString(fmt.Sprintf(" AND ie.id = %d", tableID))
	}
	if n.JobID != nil {
		query.WriteString(fmt.Sprintf(" AND ie.job_id = %d", *n.JobID))
	}

	query.WriteString(`)
	SELECT 
		ie.error_type,
		COALESCE(d.name, '<unknown>') AS database_name,
		COALESCE(s.name, '<unknown>') AS schema_name,
		COALESCE(t.name, '<unknown>') AS table_name,
		primary_key,
		ie.job_id, 
		to_char(aost, 'YYYY-MM-DD HH24:MI:SS.US') as aost`)
	if n.WithDetails {
		query.WriteString(", jsonb_pretty(ie.details) as details")
	}
	query.WriteString(`
		FROM system.inspect_errors ie
		LEFT JOIN system.namespace d ON ie.database_id = d.id AND d."parentID" = 0
		LEFT JOIN system.namespace s ON ie.schema_id = s.id AND s."parentID" = ie.database_id
		LEFT JOIN system.namespace t ON ie.id = t.id AND t."parentSchemaID" = ie.schema_id
		WHERE ie.job_id IN (SELECT id FROM job_id)`)
	if tableID != catid.InvalidDescID {
		query.WriteString(fmt.Sprintf(" AND ie.id = %d", tableID))
	}
	query.WriteString(` ORDER BY ie.error_type, ie.error_id`)

	return d.parse(query.String())
}
