// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func buildArgsForSelectPlaceholders(
	ctx context.Context, p *planner, sel *tree.Select,
) ([]interface{}, error) {
	used := make(map[int]struct{})
	_, _ = tree.SimpleStmtVisit(sel, func(e tree.Expr) (bool, tree.Expr, error) {
		if ph, ok := e.(*tree.Placeholder); ok {
			used[int(ph.Idx)] = struct{}{} // 0-based indices
		}
		return true, e, nil
	})

	if len(used) == 0 {
		return nil, nil
	}

	maxIdx := -1
	for i := range used {
		if i > maxIdx {
			maxIdx = i
		}
	}
	args := make([]interface{}, maxIdx+1)

	ph := p.ExtendedEvalContext().Placeholders
	for i := 0; i <= maxIdx; i++ {
		var te tree.TypedExpr
		if i < len(ph.Values) {
			te = ph.Values[i]
		}
		if te == nil {
			args[i] = tree.DNull
			continue
		}
		if d, ok := te.(tree.Datum); ok {
			args[i] = d
			continue
		}
		d, err := eval.Expr(ctx, p.EvalContext(), te)
		if err != nil {
			return nil, err
		}
		args[i] = d
	}
	return args, nil
}

// todo (log-head): should these be named plan?

// Retrives recent (12h or less) and running changefeed jobs, then defers to buildRowsForJobIDs to produce multiple rows.
func planRecentChangefeedJobs(
	ctx context.Context, p *planner, cols colinfo.ResultColumns, includeWatched bool,
) (planNode, error) {
	const baseSelect = `SELECT job_id, finished, created FROM crdb_internal.jobs 
	WHERE job_type = 'CHANGEFEED' AND (finished IS NULL OR finished > now() - '12h':::interval)
	ORDER BY COALESCE(finished, NOW()) DESC, created DESC`
	rows, err := p.InternalSQLTxn().QueryBufferedEx(
		ctx, "changefeed-all-recent-jobs", p.Txn(), sessiondata.NodeUserSessionDataOverride, baseSelect,
	)
	if err != nil {
		return nil, err
	}
	var ids []jobspb.JobID
	for _, r := range rows {
		ids = append(ids, jobspb.JobID(int64(tree.MustBeDInt(r[0]))))
	}
	return buildRowsForJobIDs(ctx, p, cols, ids, includeWatched)
}

// Extracts multiple JobIDs, then defers to buildRowsForJobIDs to produce multiple rows.
func planExplicitChangefeedJobs(
	ctx context.Context, p *planner, cols colinfo.ResultColumns, rows []tree.Exprs, includeWatched bool,
) (planNode, error) {
	var ids []jobspb.JobID
	for _, r := range rows {
		id, err := p.ExprEvaluator("SHOW CHANGEFEED JOBS").Int(ctx, r[0])
		if err != nil {
			return nil, errors.Wrap(err, "invalid job ID")
		}
		ids = append(ids, jobspb.JobID(id))
	}
	return buildRowsForJobIDs(ctx, p, cols, ids, includeWatched)
}

// Extracts multiple JobIDs from a subquery, then defers to buildRowsForJobIDs to produce multiple rows.
func planSubqueryChangefeedJobs(
	ctx context.Context, p *planner, cols colinfo.ResultColumns, jobs *tree.Select, includeWatched bool,
) (planNode, error) {
	args, err := buildArgsForSelectPlaceholders(ctx, p, jobs)
	if err != nil {
		return nil, err
	}
	rows, err := p.InternalSQLTxn().QueryBufferedEx(
		ctx, "cf-jobs-subselect", p.Txn(), sessiondata.NodeUserSessionDataOverride,
		jobs.String(),
		args...,
	)
	if err != nil {
		return nil, err
	}
	ids := make([]jobspb.JobID, 0, len(rows))
	for _, r := range rows {
		if len(r) != 1 {
			return nil, errors.New("SHOW CHANGEFEED JOBS subquery must produce a single column")
		}
		di, ok := r[0].(*tree.DInt)
		if !ok {
			return nil, errors.New("SHOW CHANGEFEED JOBS requires integer job ID(s)")
		}
		ids = append(ids, jobspb.JobID(int64(*di)))
	}
	return buildRowsForJobIDs(ctx, p, cols, ids, includeWatched)
}

// Extracts one JobID, then defers to buildRowsForJobIDs to produce one row.
func planSingleChangefeedJob(
	ctx context.Context, p *planner, cols colinfo.ResultColumns, expr tree.Expr, includeWatched bool,
) (planNode, error) {
	id, err := p.ExprEvaluator("SHOW CHANGEFEED JOB").Int(ctx, expr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid job ID")
	}

	return buildRowsForJobIDs(ctx, p, cols, []jobspb.JobID{jobspb.JobID(id)}, includeWatched)
}

func buildRowsForJobIDs(
	ctx context.Context, p *planner, cols colinfo.ResultColumns, jobIDs []jobspb.JobID, includeWatched bool,
) (planNode, error) {
	v := p.newContainerValuesNode(cols, 0)
	if len(jobIDs) == 0 {
		return v, nil
	}
	var querySB strings.Builder
	var args []interface{}
	querySB.WriteString(`SELECT job_id, description, user_name, status, running_status,
		created, created AS started, finished, modified,
		high_water_timestamp, 
		hlc_to_timestamp(high_water_timestamp) AS readable_high_water_timestamptz, 
		error
	FROM crdb_internal.jobs
	WHERE job_id IN (`)
	for i, jobID := range jobIDs {
		if i > 0 {
			querySB.WriteString(", ")
		}
		querySB.WriteString(fmt.Sprintf("$%d", i+1))
		args = append(args, jobID)
	}
	querySB.WriteString(")")
	jobRows, err := p.InternalSQLTxn().QueryBufferedEx(
		ctx, "changefeed-jobs-batch", p.Txn(), sessiondata.NodeUserSessionDataOverride,
		querySB.String(), args...,
	)
	if err != nil {
		return nil, err
	}
	jobByID := make(map[jobspb.JobID]tree.Datums, len(jobRows))
	for _, r := range jobRows {
		jobID := jobspb.JobID(int64(tree.MustBeDInt((r[0]))))
		jobByID[jobID] = r
	}

	for _, jobID := range jobIDs {
		jobInfo, ok := jobByID[jobID]
		if !ok {
			continue
		}

		job, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.InternalSQLTxn())
		if err != nil {
			return nil, err
		}
		payload := job.Payload()
		details := payload.GetDetails()
		cd, ok := details.(*jobspb.Payload_Changefeed)
		if !ok {
			continue
		}

		j, err := protoreflect.MessageToJSON(&payload, protoreflect.FmtFlags{EmitRedacted: true})
		if err != nil {
			return nil, err
		}
		var pj struct {
			Changefeed struct {
				SinkURI string `json:"sink_uri"`
			} `json:"changefeed"`
		}
		if err := json.Unmarshal([]byte(j.String()), &pj); err != nil {
			return nil, err
		}
		redactedSinkURI := strings.ReplaceAll(pj.Changefeed.SinkURI, `\u0026`, `&`)

		var databaseName string
		var fullTableNames []string
		for _, spec := range cd.Changefeed.TargetSpecifications {
			if spec.Type == jobspb.ChangefeedTargetSpecification_DATABASE {
				// Resolve DB name.
				dbDesc, err := p.Descriptors().ByIDWithLeased(p.InternalSQLTxn().KV()).WithoutNonPublic().Get().Database(ctx, spec.DescID)
				if err != nil {
					return nil, err
				}
				if dbDesc != nil {
					databaseName = dbDesc.GetName()
				}
				if !includeWatched {
					continue
				}
				// List tables in the DB. TODO: apply include/exclude filters if needed, same as single-job.
				rows, err := p.InternalSQLTxn().QueryBufferedEx(
					ctx, "cf-db-tables", p.Txn(), sessiondata.NodeUserSessionDataOverride,
					`SELECT concat(database_name,'.',schema_name,'.',name)
					   FROM crdb_internal.tables
					  WHERE parent_id = $1`, spec.DescID,
				)
				if err != nil {
					return nil, err
				}
				for _, tr := range rows {
					fullTableNames = append(fullTableNames, string(tree.MustBeDString(tr[0])))
				}
			} else {
				// TODO(log-head): don't use SQL to resolve fully qualified name.
				nameRow, err := p.InternalSQLTxn().QueryRowEx(
					ctx, "cf-table-name", p.Txn(), sessiondata.NodeUserSessionDataOverride,
					`SELECT concat(database_name,'.',schema_name,'.',name)
					   FROM crdb_internal.tables
					  WHERE table_id = $1`, spec.DescID,
				)
				if err != nil {
					return nil, err
				}
				fullTableNames = append(fullTableNames, string(tree.MustBeDString(nameRow[0])))
			}
		}
		fullTableNamesDatum := tree.NewDArray(types.String)
		for _, t := range fullTableNames {
			if err := fullTableNamesDatum.Append(tree.NewDString(t)); err != nil {
				return nil, err
			}
		}

		topics := cd.Changefeed.Opts["topics"]
		var topicsDatum tree.Datum = tree.DNull
		if topics != "" {
			topicsDatum = tree.NewDString(topics)
		}
		format := cd.Changefeed.Opts["format"]
		var formatDatum tree.Datum = tree.DNull
		if format != "" {
			formatDatum = tree.NewDString(format)
		}
		var dbNameDatum tree.Datum = tree.DNull
		if databaseName != "" {
			dbNameDatum = tree.NewDString(databaseName)
		}

		out := append(append(tree.Datums{}, jobInfo...),
			tree.NewDString(redactedSinkURI),
			fullTableNamesDatum,
			topicsDatum,
			formatDatum,
			dbNameDatum,
		)
		if _, err := v.rows.AddRow(ctx, out); err != nil {
			v.rows.Close(ctx)
			return nil, err
		}
	}
	return v, nil
}

func (p *planner) ShowChangefeedJob(
	ctx context.Context, n *tree.ShowChangefeedJobs,
) (planNode, error) {
	fmt.Printf("n.Jobs: %+v\n", n.Jobs)
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)
	// This type checks the job ID at planning time so placeholders are supported.
	if sel := n.Jobs; sel != nil {
		if vc, ok := sel.Select.(*tree.ValuesClause); ok {
			for _, row := range vc.Rows {
				for _, expr := range row {
					if _, err := tree.TypeCheck(ctx, expr, p.SemaCtx(), types.Int); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	// todo apply permissions check.

	columns := colinfo.ResultColumns{
		// {Name: "job_type", Typ: types.String},
		{Name: "job_id", Typ: types.Int},
		{Name: "description", Typ: types.String},
		{Name: "user_name", Typ: types.String},
		{Name: "status", Typ: types.String},
		{Name: "running_status", Typ: types.String},
		{Name: "created", Typ: types.TimestampTZ},
		{Name: "started", Typ: types.TimestampTZ},
		{Name: "finished", Typ: types.TimestampTZ},
		{Name: "modified", Typ: types.TimestampTZ},
		{Name: "high_water_timestamp", Typ: types.Decimal},
		{Name: "readable_high_water_timestamptz", Typ: types.TimestampTZ},
		{Name: "error", Typ: types.String},
		{Name: "sink_uri", Typ: types.String},
		{Name: "full_table_names", Typ: types.StringArray},
		{Name: "topics", Typ: types.String},
		{Name: "format", Typ: types.String},
		{Name: "database_name", Typ: types.String},
	}

	return &delayedNode{
		name:    "SHOW CHANGEFEED JOB",
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {

			if n.Jobs == nil {
				return planRecentChangefeedJobs(ctx, p, columns, n.IncludeWatchedTables)
			}

			if vc, ok := n.Jobs.Select.(*tree.ValuesClause); ok && len(vc.Rows) >= 1 && len(vc.Rows[0]) == 1 {
				if len(vc.Rows) == 1 {
					return planSingleChangefeedJob(ctx, p, columns, vc.Rows[0][0], n.IncludeWatchedTables)
				}
				return planExplicitChangefeedJobs(ctx, p, columns, vc.Rows, n.IncludeWatchedTables)
			}

			return planSubqueryChangefeedJobs(ctx, p, columns, n.Jobs, n.IncludeWatchedTables)
		},
	}, nil
}
