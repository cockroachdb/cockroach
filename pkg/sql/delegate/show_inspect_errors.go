// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowInspectErrors(n *tree.ShowInspectErrors) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.InspectErrors)

	if n.TableName != nil && n.JobID != nil {
		return nil, errors.New("specifying both TABLE and JOB is not supported")
	}

	var query strings.Builder

	query.WriteString(`WITH
	successful_inspect_jobs AS (
		SELECT id
		FROM system.jobs
		WHERE job_type = 'INSPECT' AND status = 'succeeded'
	),

	payloads AS (
		SELECT job_id, value
		FROM system.job_info AS payload
		WHERE info_key = 'legacy_payload'
		AND job_id IN (SELECT id FROM successful_inspect_jobs)
	)
	`)

	query.WriteString(`SELECT 
		ie.job_id, 
		ie.error_type, 
		COALESCE(d.name, '<unknown>') AS database_name,
		COALESCE(s.name, '<unknown>') AS schema_name,
		COALESCE(t.name, '<unknown>') AS table_name`)
	if n.WithDetails {
		query.WriteString(", ie.details")
	}
	query.WriteString(`
		FROM system.inspect_errors ie
		LEFT JOIN system.namespace d ON ie.database_id = d.id AND d."parentID" = 0
		LEFT JOIN system.namespace s ON ie.schema_id = s.id AND s."parentID" = ie.database_id
		LEFT JOIN system.namespace t ON ie.id = t.id AND t."parentSchemaID" = ie.schema_id`)

	if n.JobID != nil {
		query.WriteString(fmt.Sprintf(" WHERE ie.job_id = %d", *n.JobID))
	} else if n.TableName == nil {
		query.WriteString(`
				WHERE ie.job_id IN (SELECT id FROM successful_inspect_jobs)
				ORDER BY ie.job_id DESC 
				LIMIT 1`)
	} else {
		dataSource, _, err := d.catalog.ResolveDataSource(d.ctx, resolveFlags, n.TableName)
		if err != nil {
			return nil, err
		}

		tableID := dataSource.ID()

		query.WriteString(fmt.Sprintf(`
		WITH table_specific_jobs AS (
			SELECT j.id
			FROM successful_inspect_jobs j
			INNER JOIN payloads p ON j.id = p.job_id
			WHERE crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', p.value)->'inspectDetails'->'check'->>'tableId' = '%d'
			ORDER BY j.id DESC
			LIMIT 1
		)
		WHERE ie.job_id IN (SELECT id FROM table_specific_jobs)`, tableID))
	}

	return d.parse(query.String())
}
