// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createStatsPostEvents controls the cluster setting for logging
// automatic table statistics collection to the event log.
var createStatsPostEvents = settings.RegisterPublicBoolSetting(
	"sql.stats.post_events.enabled",
	"if set, an event is logged for every CREATE STATISTICS job",
	false,
)

func (p *planner) CreateStatistics(ctx context.Context, n *tree.CreateStats) (planNode, error) {
	return &createStatsNode{
		CreateStats: *n,
		p:           p,
	}, nil
}

// createStatsNode is a planNode implemented in terms of a function. The
// startJob function starts a Job during Start, and the remainder of the
// CREATE STATISTICS planning and execution is performed within the jobs
// framework.
type createStatsNode struct {
	tree.CreateStats
	p *planner

	run createStatsRun
}

// createStatsRun contains the run-time state of createStatsNode during local
// execution.
type createStatsRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

func (n *createStatsNode) startExec(params runParams) error {
	n.run.resultsCh = make(chan tree.Datums)
	n.run.errCh = make(chan error)
	go func() {
		err := n.startJob(params.ctx, n.run.resultsCh)
		select {
		case <-params.ctx.Done():
		case n.run.errCh <- err:
		}
		close(n.run.errCh)
		close(n.run.resultsCh)
	}()
	return nil
}

func (n *createStatsNode) Next(params runParams) (bool, error) {
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case err := <-n.run.errCh:
		return false, err
	case <-n.run.resultsCh:
		return true, nil
	}
}

func (*createStatsNode) Close(context.Context) {}
func (*createStatsNode) Values() tree.Datums   { return nil }

// startJob starts a CreateStats job to plan and execute statistics creation.
func (n *createStatsNode) startJob(ctx context.Context, resultsCh chan<- tree.Datums) error {
	record, err := n.makeJobRecord(ctx)
	if err != nil {
		return err
	}

	if n.Name == stats.AutoStatsName {
		// Don't start the job if there is already a CREATE STATISTICS job running.
		// (To handle race conditions we check this again after the job starts,
		// but this check is used to prevent creating a large number of jobs that
		// immediately fail).
		if err := checkRunningJobs(ctx, nil /* job */, n.p); err != nil {
			return err
		}
	} else {
		telemetry.Inc(sqltelemetry.CreateStatisticsUseCounter)
	}

	job, errCh, err := n.p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, resultsCh, *record)
	if err != nil {
		return err
	}

	if err = <-errCh; err != nil {
		if errors.Is(err, stats.ConcurrentCreateStatsError) {
			// Delete the job so users don't see it and get confused by the error.
			const stmt = `DELETE FROM system.jobs WHERE id = $1`
			if _ /* cols */, delErr := n.p.ExecCfg().InternalExecutor.Exec(
				ctx, "delete-job", nil /* txn */, stmt, *job.ID(),
			); delErr != nil {
				log.Warningf(ctx, "failed to delete job: %v", delErr)
			}
		}
	}
	return err
}

// makeJobRecord creates a CreateStats job record which can be used to plan and
// execute statistics creation.
func (n *createStatsNode) makeJobRecord(ctx context.Context) (*jobs.Record, error) {
	var tableDesc *ImmutableTableDescriptor
	var fqTableName string
	var err error
	switch t := n.Table.(type) {
	case *tree.UnresolvedObjectName:
		tableDesc, err = n.p.ResolveExistingObjectEx(ctx, t, true /*required*/, ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}
		fqTableName = n.p.ResolvedName(t).FQString()

	case *tree.TableRef:
		flags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{
			AvoidCached: n.p.avoidCachedDescriptors,
		}}
		tableDesc, err = n.p.Tables().getTableVersionByID(ctx, n.p.txn, sqlbase.ID(t.TableID), flags)
		if err != nil {
			return nil, err
		}
		fqTableName, err = n.p.getQualifiedTableName(ctx, &tableDesc.TableDescriptor)
		if err != nil {
			return nil, err
		}
	}

	if tableDesc.IsVirtualTable() {
		return nil, pgerror.New(
			pgcode.WrongObjectType, "cannot create statistics on virtual tables",
		)
	}

	if tableDesc.IsView() {
		return nil, pgerror.New(
			pgcode.WrongObjectType, "cannot create statistics on views",
		)
	}

	if err := n.p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	// Identify which columns we should create statistics for.
	var colStats []jobspb.CreateStatsDetails_ColStat
	if len(n.ColumnNames) == 0 {
		if colStats, err = createStatsDefaultColumns(tableDesc); err != nil {
			return nil, err
		}
	} else {
		columns, err := tableDesc.FindActiveColumnsByNames(n.ColumnNames)
		if err != nil {
			return nil, err
		}

		columnIDs := make([]sqlbase.ColumnID, len(columns))
		for i := range columns {
			if columns[i].Type.Family() == types.JsonFamily {
				return nil, unimplemented.NewWithIssuef(35844,
					"CREATE STATISTICS is not supported for JSON columns")
			}
			columnIDs[i] = columns[i].ID
		}
		colStats = []jobspb.CreateStatsDetails_ColStat{{ColumnIDs: columnIDs, HasHistogram: false}}
		if len(columnIDs) == 1 {
			// By default, create histograms on all explicitly requested column stats
			// with a single column.
			colStats[0].HasHistogram = true
		}
	}

	// Evaluate the AS OF time, if any.
	var asOf *hlc.Timestamp
	if n.Options.AsOf.Expr != nil {
		asOfTs, err := n.p.EvalAsOfTimestamp(n.Options.AsOf)
		if err != nil {
			return nil, err
		}
		asOf = &asOfTs
	}

	// Create a job to run statistics creation.
	statement := tree.AsStringWithFQNames(n, n.p.EvalContext().Annotations)
	var description string
	if n.Name == stats.AutoStatsName {
		// Use a user-friendly description for automatic statistics.
		description = fmt.Sprintf("Table statistics refresh for %s", fqTableName)
	} else {
		// This must be a user query, so use the statement (for consistency with
		// other jobs triggered by statements).
		description = statement
		statement = ""
	}
	return &jobs.Record{
		Description: description,
		Statement:   statement,
		Username:    n.p.User(),
		Details: jobspb.CreateStatsDetails{
			Name:            string(n.Name),
			FQTableName:     fqTableName,
			Table:           tableDesc.TableDescriptor,
			ColumnStats:     colStats,
			Statement:       n.String(),
			AsOf:            asOf,
			MaxFractionIdle: n.Options.Throttling,
		},
		Progress: jobspb.CreateStatsProgress{},
	}, nil
}

// maxNonIndexCols is the maximum number of non-index columns that we will use
// when choosing a default set of column statistics.
const maxNonIndexCols = 100

// createStatsDefaultColumns creates column statistics on a default set of
// column lists when no columns were specified by the caller.
//
// To determine a useful set of default column statistics, we rely on
// information provided by the schema. In particular, the presence of an index
// on a particular set of columns indicates that the workload likely contains
// queries that involve those columns (e.g., for filters), and it would be
// useful to have statistics on prefixes of those columns. For example, if a
// table abc contains indexes on (a ASC, b ASC) and (b ASC, c ASC), we will
// collect statistics on a, {a, b}, b, and {b, c}.
//
// In addition to the index columns, we collect stats on up to maxNonIndexCols
// other columns from the table. We only collect histograms for index columns,
// plus any other boolean columns (where the "histogram" is tiny).
//
// TODO(rytaft): This currently only generates one single-column stat per
// index. Add code to collect multi-column stats once they are supported.
func createStatsDefaultColumns(
	desc *ImmutableTableDescriptor,
) ([]jobspb.CreateStatsDetails_ColStat, error) {
	colStats := make([]jobspb.CreateStatsDetails_ColStat, 0, len(desc.Indexes)+1)

	var requestedCols util.FastIntSet

	// Add a column for the primary key.
	pkCol := desc.PrimaryIndex.ColumnIDs[0]
	colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
		ColumnIDs:    []sqlbase.ColumnID{pkCol},
		HasHistogram: true,
	})
	requestedCols.Add(int(pkCol))

	// Add columns for each secondary index.
	for i := range desc.Indexes {
		if desc.Indexes[i].Type == sqlbase.IndexDescriptor_INVERTED {
			// We don't yet support stats on inverted indexes.
			continue
		}
		idxCol := desc.Indexes[i].ColumnIDs[0]
		if !requestedCols.Contains(int(idxCol)) {
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:    []sqlbase.ColumnID{idxCol},
				HasHistogram: true,
			})
			requestedCols.Add(int(idxCol))
		}
	}

	// Add all remaining non-json columns in the table, up to maxNonIndexCols.
	nonIdxCols := 0
	for i := 0; i < len(desc.Columns) && nonIdxCols < maxNonIndexCols; i++ {
		col := &desc.Columns[i]
		if col.Type.Family() != types.JsonFamily && !requestedCols.Contains(int(col.ID)) {
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:    []sqlbase.ColumnID{col.ID},
				HasHistogram: col.Type.Family() == types.BoolFamily,
			})
			nonIdxCols++
		}
	}

	return colStats, nil
}

// makePlanForExplainDistSQL is part of the distSQLExplainable interface.
func (n *createStatsNode) makePlanForExplainDistSQL(
	planCtx *PlanningCtx, distSQLPlanner *DistSQLPlanner,
) (PhysicalPlan, error) {
	// Create a job record but don't actually start the job.
	record, err := n.makeJobRecord(planCtx.ctx)
	if err != nil {
		return PhysicalPlan{}, err
	}
	job := n.p.ExecCfg().JobRegistry.NewJob(*record)

	return distSQLPlanner.createPlanForCreateStats(planCtx, job)
}

// createStatsResumer implements the jobs.Resumer interface for CreateStats
// jobs. A new instance is created for each job. evalCtx is populated inside
// createStatsResumer.Resume so it can be used in createStatsResumer.OnSuccess
// (if the job is successful).
type createStatsResumer struct {
	job     *jobs.Job
	tableID sqlbase.ID
	evalCtx *extendedEvalContext
}

var _ jobs.Resumer = &createStatsResumer{}

// Resume is part of the jobs.Resumer interface.
func (r *createStatsResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(*planner)
	details := r.job.Details().(jobspb.CreateStatsDetails)
	if details.Name == stats.AutoStatsName {
		// We want to make sure there is only one automatic CREATE STATISTICS job
		// running at a time.
		if err := checkRunningJobs(ctx, r.job, p); err != nil {
			return err
		}
	}

	r.tableID = details.Table.ID
	r.evalCtx = p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]types.T{})
	rows := rowcontainer.NewRowContainer(r.evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	dsp := p.DistSQLPlanner()
	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if details.AsOf != nil {
			p.semaCtx.AsOfTimestamp = details.AsOf
			p.extendedEvalCtx.SetTxnTimestamp(details.AsOf.GoTime())
			txn.SetFixedTimestamp(ctx, *details.AsOf)
		}

		planCtx := dsp.NewPlanningCtx(ctx, r.evalCtx, txn)
		planCtx.planner = p
		if err := dsp.planAndRunCreateStats(
			ctx, r.evalCtx, planCtx, txn, r.job, NewRowResultWriter(rows),
		); err != nil {
			// Check if this was a context canceled error and restart if it was.
			if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
				if s.Code() == codes.Canceled && s.Message() == context.Canceled.Error() {
					return jobs.NewRetryJobError("node failure")
				}
			}

			// If the job was canceled, any of the distsql processors could have been
			// the first to encounter the .Progress error. This error's string is sent
			// through distsql back here, so we can't examine the err type in this case
			// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
			// job progress to coerce out the correct error type. If the update succeeds
			// then return the original error, otherwise return this error instead so
			// it can be cleaned up at a higher level.
			if jobErr := r.job.FractionProgressed(
				ctx,
				func(ctx context.Context, _ jobspb.ProgressDetails) float32 {
					// The job failed so the progress value here doesn't really matter.
					return 0
				},
			); jobErr != nil {
				return jobErr
			}
			return err
		}

		return nil
	})
}

// checkRunningJobs checks whether there are any other CreateStats jobs in the
// pending, running, or paused status that started earlier than this one. If
// there are, checkRunningJobs returns an error. If job is nil, checkRunningJobs
// just checks if there are any pending, running, or paused CreateStats jobs.
func checkRunningJobs(ctx context.Context, job *jobs.Job, p *planner) error {
	var jobID int64
	if job != nil {
		jobID = *job.ID()
	}
	const stmt = `SELECT id, payload FROM system.jobs WHERE status IN ($1, $2, $3) ORDER BY created`

	rows, err := p.ExecCfg().InternalExecutor.Query(
		ctx,
		"get-jobs",
		nil, /* txn */
		stmt,
		jobs.StatusPending,
		jobs.StatusRunning,
		jobs.StatusPaused,
	)
	if err != nil {
		return err
	}

	for _, row := range rows {
		payload, err := jobs.UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		if payload.Type() == jobspb.TypeCreateStats || payload.Type() == jobspb.TypeAutoCreateStats {
			id := (*int64)(row[0].(*tree.DInt))
			if *id == jobID {
				break
			}

			// This is not the first CreateStats job running. This job should fail
			// so that the earlier job can succeed.
			return stats.ConcurrentCreateStatsError
		}
	}
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnFailOrCancel(context.Context, interface{}) error { return nil }

// OnSuccess is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnSuccess(ctx context.Context, _ *client.Txn) error {
	details := r.job.Details().(jobspb.CreateStatsDetails)

	// Invalidate the local cache synchronously; this guarantees that the next
	// statement in the same session won't use a stale cache (whereas the gossip
	// update is handled asynchronously).
	r.evalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(ctx, r.tableID)

	// Record this statistics creation in the event log.
	if !createStatsPostEvents.Get(&r.evalCtx.Settings.SV) {
		return nil
	}

	// TODO(rytaft): This creates a new transaction for the CREATE STATISTICS
	// event. It must be different from the CREATE STATISTICS transaction,
	// because that transaction must be read-only. In the future we may want
	// to use the transaction that inserted the new stats into the
	// system.table_statistics table, but that would require calling
	// MakeEventLogger from the distsqlrun package.
	return r.evalCtx.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return MakeEventLogger(r.evalCtx.ExecCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogCreateStatistics,
			int32(details.Table.ID),
			int32(r.evalCtx.NodeID),
			struct {
				TableName string
				Statement string
			}{details.FQTableName, details.Statement},
		)
	})
}

// OnTerminal is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnTerminal(
	ctx context.Context, status jobs.Status, resultsCh chan<- tree.Datums,
) {
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &createStatsResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeCreateStats, createResumerFn)
	jobs.RegisterConstructor(jobspb.TypeAutoCreateStats, createResumerFn)
}
