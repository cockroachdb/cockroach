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
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
)

const (
	useOldStyle = iota
	useDirectStyle
	useDirectV2Style
)

var showJobsUseDirectQuery = settings.RegisterIntSetting(
	"sql.jobs.experimental_use_direct_show_jobs",
	"experimental feature: query system.jobs table directly",
	useOldStyle,
)

func (d *delegator) delegateShowJobs(n *tree.ShowJobs) (tree.Statement, error) {
	switch showJobsUseDirectQuery.Get(&d.evalCtx.Settings.SV) {
	case useOldStyle:
		return d.delegateShowJobsVirtualTable(n)
	case useDirectStyle:
		return d.delegateShowJobsDirectQuery(n)
	default:
		return d.delegateShowJobsDirectQueryWithIdx(n)
	}
}

// SHOW JOBS implementation which uses crdb_intenral.jobs virtual table.
// TODO(yevgeniy): deprecate and remove.
func (d *delegator) delegateShowJobsVirtualTable(n *tree.ShowJobs) (tree.Statement, error) {
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
	)
	var typePredicate, whereClause, orderbyClause string
	if n.Jobs == nil {
		// Display all [only automatic] jobs without selecting specific jobs.
		if n.Automatic {
			typePredicate = fmt.Sprintf("job_type = '%s'", jobspb.TypeAutoCreateStats)
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

	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)
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

// jobTypeMap maps a json field for the payload detail to its job type description.
var jobTypeMap = map[string]string{
	"backup":           jobspb.TypeBackup.String(),
	"restore":          jobspb.TypeRestore.String(),
	"schemaChange":     jobspb.TypeSchemaChange.String(),
	"import":           jobspb.TypeImport.String(),
	"changefeed":       jobspb.TypeChangefeed.String(),
	"createStats":      jobspb.TypeCreateStats.String(),
	"schemaChangeGC":   jobspb.TypeSchemaChangeGC.String(),
	"typeSchemaChange": jobspb.TypeTypeSchemaChange.String(),
}

// jobTypeSecoltor returns an SQL string which produces the job type
// given job payload json representation.
var jobTypeSelector = func() string {
	jobType := &bytes.Buffer{}
	jobType.WriteString("CASE")
	for selector, value := range jobTypeMap {
		fmt.Fprintf(jobType, " WHEN payload ? '%s' THEN '%s'", selector, value)
	}

	jobType.WriteString(" ELSE NULL END")
	return jobType.String()
}()

const noAlias = ""

// Returns "selector" from jsonb column, optionally aliases "AS"
func jsonbCol(jsonb, selector, as string) string {
	if len(as) == 0 {
		as = selector
	}
	return fmt.Sprintf("%s->>'%s' AS %s", jsonb, selector, as)
}

// Returns "selector" from jsonb column, cast as a timestamp,
// and optionally aliased "AS"
func jsonbMicrosToTime(jsonb, selector, as string) string {
	if len(as) == 0 {
		as = selector
	}
	jsonCol := fmt.Sprintf("%s->>'%s'", jsonb, selector)

	return fmt.Sprintf(`
IF (cast(%s AS int) > 0,
    experimental_strptime((cast(%s AS float) / 1000000)::string, '%%s.%%f'),
		NULL
) AS %s
`, jsonCol, jsonCol, as)
}

// SHOW JOBS implementation which queries system.jobs table directly.
func (d *delegator) delegateShowJobsDirectQuery(n *tree.ShowJobs) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)

	// decodedJobs is a query to retrieve and decode system.jobs.
	const decodedJobs = `
SELECT 
   id, status, created, 
   crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', payload) as payload,
   crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', progress) as progress
FROM system.jobs
`

	// Apply whatever limits we can to filter out as many rows as we can
	// from the underlying system.jobs table.  At this stage, we can only filter
	// out based on "regular" (non-protobuf) columns.
	var jobsFilter string
	if n.Jobs != nil {
		// Limit the jobs displayed to the select statement in n.Jobs.
		jobsFilter = fmt.Sprintf(`WHERE id in (%s)`, n.Jobs.String())
	} else if n.Schedules != nil {
		// Limit the jobs displayed to the ones started by specified schedules.
		jobsFilter = fmt.Sprintf(
			"WHERE created_by_type='%s' AND created_by_id IN (%s)",
			jobs.CreatedByScheduledJobs, n.Schedules)
	}

	// jobsCTE defines a "virtual" jobs table which yields same rows as system.jobs,
	// but with the protocol buffers columns converted to their JSONB representation.
	// We also add few computed columns that can be used by WHERE clauses.
	// This CTE further restricts the rows returned only to those that can be managed
	// by the current user.
	jobsCTEComputedCols := []string{
		jsonbMicrosToTime("payload", "startedMicros", "started"),
		jsonbMicrosToTime("payload", "finishedMicros", "finished"),
		jsonbMicrosToTime("progress", "modifiedMicros", "modified"),
	}

	jobsCTE := fmt.Sprintf(`
  permissions AS (
    SELECT
      crdb_internal.is_admin() as isAdmin, 
      crdb_internal.has_role_option('CONTROLJOB') as hasJobControl
  ),
  jobs AS (
    SELECT *, %s
    FROM (WITH decodedJobs AS (%s %s) SELECT * FROM decodedJobs), permissions
    WHERE permissions.isAdmin OR permissions.hasJobControl OR payload->>'username' = current_user()
  )
`, strings.Join(jobsCTEComputedCols, ", "), decodedJobs, jobsFilter)

	// On top of jobsCTE, we will build a human readable view into payload/progress.
	// The following list of columns will be returned.
	jobCols := []string{
		"id AS job_id",
		fmt.Sprintf(`(%s) AS job_type`, jobTypeSelector),
		jsonbCol("payload", "description", noAlias),
		jsonbCol("payload", "statement", noAlias),
		jsonbCol("payload", "username", "user_name"),
		"status",
		jsonbCol("progress", "running_status", noAlias),
		"created",
		"started",
		"finished",
		"modified",
		jsonbCol("progress", "fraction_completed", noAlias),
		jsonbCol("payload", "error", noAlias),
		jsonbCol("payload->'lease'", "nodeId", "coordinator_id"),
	}

	sqlStmt := fmt.Sprintf(
		"WITH %s SELECT %s FROM jobs",
		jobsCTE, strings.Join(jobCols, ", "))

	// whereExprs are joined by ANDing all expressions.
	var whereExprs []string
	var orderByClause string

	if n.Jobs == nil && n.Schedules == nil {
		var wantAutomatic string
		if !n.Automatic {
			wantAutomatic = "NOT "
		}

		whereExprs = append(whereExprs,
			// Use fancy jsonb selector to determine if the job is the auto create stats.
			fmt.Sprintf(
				`%s payload @> '{"createStats": {"name": "%s"}}'::jsonb`,
				wantAutomatic, stats.AutoStatsName),

			// The query intends to present:
			// - first all the running jobs sorted in order of start time,
			// - then all completed jobs sorted in order of completion time.
			`(finished IS NULL OR finished > now() - '12h':::interval)`,
		)
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderByClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
	}

	if n.Block {
		whereExprs = append(whereExprs, `
			    IF(finished IS NULL,
			      IF(pg_sleep(1), crdb_internal.force_retry('24h'), 0),
			      0
			    ) = 0`)
	}

	var whereClause string
	if len(whereExprs) > 0 {
		whereClause = fmt.Sprintf("WHERE (%s) ", strings.Join(whereExprs, " AND "))
	}

	stmt := fmt.Sprintf("%s %s %s", sqlStmt, whereClause, orderByClause)
	return parse(stmt)
}

type whereClauseBuilder struct {
	clauses []string
}

func (w *whereClauseBuilder) add(clauses ...string) {
	w.clauses = append(w.clauses, clauses...)
}

func (w *whereClauseBuilder) String() string {
	var b strings.Builder
	for _, c := range w.clauses {
		if b.Len() > 0 {
			b.WriteString(" AND ")
		}
		b.WriteByte('(')
		b.WriteString(c)
		b.WriteByte(')')
	}
	return b.String()
}

func jsonbSecsToTime(jsonb, selector string) string {
	jsonCol := fmt.Sprintf("%s->>'%s'", jsonb, selector)

	return fmt.Sprintf(`IF (cast(%s as float) > 0, experimental_strptime(%s, '%%s.%%f'), NULL)`,
		jsonCol, jsonCol)
}

func jsonbSecsToTimeAs(jsonb, selector, as string) string {
	if len(as) == 0 {
		as = selector
	}
	return fmt.Sprintf("%s AS %s", jsonbSecsToTime(jsonb, selector), as)
}

// SHOW JOBS implementation which queries system.jobs table directly.
func (d *delegator) delegateShowJobsDirectQueryWithIdx(n *tree.ShowJobs) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)

	// decodedJobs is a query to retrieve and decode system.jobs.
	const decodedJobs = `
WITH permissions AS(
  SELECT	
   crdb_internal.is_admin() as isAdmin, 
   crdb_internal.has_role_option('CONTROLJOB') as hasJobControl
)
SELECT 
  id AS job_id, 
  status,
  props->>'job_type' AS job_type,
  created, 
  crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', payload) AS payload,
  crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', progress) AS progress,
  'epoch'::timestamptz + '1 microsecond'::interval * nullif((props->>'started_micros')::float, 0) AS started,
  'epoch'::timestamptz + '1 microsecond'::interval * nullif((props->>'finished_micros')::float, 0) AS finished
FROM system.jobs, permissions
`
	// Restrict decoded jobs to those that can be viewed by this user.
	// Be careful as to the type of JSONB operators used.  Use operators
	// that allow us to leverage inverted index (i.e. use "@>" or "->" as opposed to "->>")
	var whereClause whereClauseBuilder
	whereClause.add(
		`permissions.isAdmin OR
     permissions.hasJobControl OR 
     props->'username' = to_json(current_user())`)

	var orderByClause string

	// Apply additional limits to reduce the number of payload/progress we need to decode.
	if n.Jobs != nil {
		// Limit the jobs displayed to the select statement in n.Jobs.
		whereClause.add(fmt.Sprintf(`id in (%s)`, n.Jobs.String()))
	} else if n.Schedules != nil {
		// Limit the jobs displayed to the ones started by specified schedules.
		whereClause.add(fmt.Sprintf(
			"WHERE created_by_type='%s' AND created_by_id IN (%s)",
			jobs.CreatedByScheduledJobs, n.Schedules))
	} else {
		jobTypePredicate := fmt.Sprintf(`props @> '{"job_type": "%s"}'`,
			jobspb.TypeAutoCreateStats.String())
		if n.Automatic {
			whereClause.add(jobTypePredicate)
		} else {
			whereClause.add("NOT " + jobTypePredicate)
		}

		whereClause.add(
			// The query intends to present:
			// - first all the running jobs sorted in order of start time,
			// - then all completed jobs sorted in order of completion time.
			`
props @> '{"finished_micros": 0}' OR 
'epoch'::timestamptz + '1 microsecond'::interval * nullif((props->>'finished_micros')::float, 0) > now() - '12 hour'::interval`,
		)
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderByClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
	}

	if n.Block {
		whereClause.add(`
			    IF(props->'finished' IS NULL,
			      IF(pg_sleep(1), crdb_internal.force_retry('24h'), 0),
			      0
			    ) = 0`)
	}

	jobsCTE := fmt.Sprintf("%s WHERE %s", decodedJobs, whereClause.String())

	// On top of jobsCTE, we will build a human readable view into payload/progress.
	// The following list of columns will be returned.
	sqlStmt := fmt.Sprintf(`
WITH jobs AS (%s)
SELECT
  job_id,
  job_type,
  payload->>'description' AS description,
  payload->>'statement' AS statement,
  payload->>'username' AS user_name,
  status,
  progress->>'running_status' AS running_status,
  created,
  started,
  finished,
  'epoch'::timestamptz + '1 microsecond'::interval * nullif(cast(progress->>'modifiedMicros' as float), 0) AS modified,
  progress->>'fraction_completed' AS fraction_completed,
  payload->>'error' AS error,
  payload->'lease'->>'nodeId' AS coordinator_id
FROM jobs
%s
`, jobsCTE, orderByClause)

	// d.evalCtx.ClientNoticeSender.SendClientNotice(d.ctx, errors.New(sqlStmt))
	return parse(sqlStmt)
}
