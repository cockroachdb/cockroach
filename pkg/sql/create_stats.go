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

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// createStatsPostEvents controls the cluster setting for logging
// automatic table statistics collection to the event log.
var createStatsPostEvents = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.post_events.enabled",
	"if set, an event is logged for every CREATE STATISTICS job",
	false,
).WithPublic()

// featureStatsEnabled is used to enable and disable the CREATE STATISTICS and
// ANALYZE features.
var featureStatsEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"feature.stats.enabled",
	"set to true to enable CREATE STATISTICS/ANALYZE, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

const nonIndexColHistogramBuckets = 2

// StubTableStats generates "stub" statistics for a table which are missing
// histograms and have 0 for all values.
func StubTableStats(
	desc catalog.TableDescriptor, name string, multiColEnabled bool,
) ([]*stats.TableStatisticProto, error) {
	colStats, err := createStatsDefaultColumns(desc, multiColEnabled)
	if err != nil {
		return nil, err
	}
	statistics := make([]*stats.TableStatisticProto, len(colStats))
	for i, colStat := range colStats {
		statistics[i] = &stats.TableStatisticProto{
			TableID:   desc.GetID(),
			Name:      name,
			ColumnIDs: colStat.ColumnIDs,
		}
	}
	return statistics, nil
}

// createStatsNode is a planNode implemented in terms of a function. The
// startJob function starts a Job during Start, and the remainder of the
// CREATE STATISTICS planning and execution is performed within the jobs
// framework.
type createStatsNode struct {
	tree.CreateStats
	p *planner

	// runAsJob is true by default, and causes the code below to be executed,
	// which sets up a job and waits for it.
	//
	// If it is false, the flow for create statistics is planned directly; this
	// is used when the statement is under EXPLAIN or EXPLAIN ANALYZE.
	runAsJob bool

	run createStatsRun
}

// createStatsRun contains the run-time state of createStatsNode during local
// execution.
type createStatsRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

func (n *createStatsNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("stats"))
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

	if n.Name == jobspb.AutoStatsName {
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

	var job *jobs.StartableJob
	jobID := n.p.ExecCfg().JobRegistry.MakeJobID()
	if err := n.p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		return n.p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &job, jobID, txn, *record)
	}); err != nil {
		if job != nil {
			if cleanupErr := job.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
			}
		}
		return err
	}
	if err := job.Start(ctx); err != nil {
		return err
	}

	if err := job.AwaitCompletion(ctx); err != nil {
		if errors.Is(err, stats.ConcurrentCreateStatsError) {
			// Delete the job so users don't see it and get confused by the error.
			const stmt = `DELETE FROM system.jobs WHERE id = $1`
			if _ /* cols */, delErr := n.p.ExecCfg().InternalExecutor.Exec(
				ctx, "delete-job", nil /* txn */, stmt, jobID,
			); delErr != nil {
				log.Warningf(ctx, "failed to delete job: %v", delErr)
			}
		}
		return err
	}
	return nil
}

// makeJobRecord creates a CreateStats job record which can be used to plan and
// execute statistics creation.
func (n *createStatsNode) makeJobRecord(ctx context.Context) (*jobs.Record, error) {
	var tableDesc catalog.TableDescriptor
	var fqTableName string
	var err error
	switch t := n.Table.(type) {
	case *tree.UnresolvedObjectName:
		tableDesc, err = n.p.ResolveExistingObjectEx(ctx, t, true /*required*/, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}
		fqTableName = n.p.ResolvedName(t).FQString()

	case *tree.TableRef:
		flags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{
			AvoidLeased: n.p.avoidLeasedDescriptors,
		}}
		tableDesc, err = n.p.Descriptors().GetImmutableTableByID(ctx, n.p.txn, descpb.ID(t.TableID), flags)
		if err != nil {
			return nil, err
		}
		fqName, err := n.p.getQualifiedTableName(ctx, tableDesc)
		if err != nil {
			return nil, err
		}
		fqTableName = fqName.FQString()
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
		multiColEnabled := stats.MultiColumnStatisticsClusterMode.Get(&n.p.ExecCfg().Settings.SV)
		if colStats, err = createStatsDefaultColumns(tableDesc, multiColEnabled); err != nil {
			return nil, err
		}
	} else {
		columns, err := tabledesc.FindPublicColumnsWithNames(tableDesc, n.ColumnNames)
		if err != nil {
			return nil, err
		}

		columnIDs := make([]descpb.ColumnID, len(columns))
		for i := range columns {
			if columns[i].IsVirtual() {
				return nil, pgerror.Newf(
					pgcode.InvalidColumnReference,
					"cannot create statistics on virtual column %q",
					columns[i].ColName(),
				)
			}
			columnIDs[i] = columns[i].GetID()
		}
		col, err := tableDesc.FindColumnWithID(columnIDs[0])
		if err != nil {
			return nil, err
		}
		isInvIndex := colinfo.ColumnTypeIsInvertedIndexable(col.GetType())
		colStats = []jobspb.CreateStatsDetails_ColStat{{
			ColumnIDs: columnIDs,
			// By default, create histograms on all explicitly requested column stats
			// with a single column that doesn't use an inverted index.
			HasHistogram:        len(columnIDs) == 1 && !isInvIndex,
			HistogramMaxBuckets: stats.DefaultHistogramBuckets,
		}}
		// Make histograms for inverted index column types.
		if len(columnIDs) == 1 && isInvIndex {
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:           columnIDs,
				HasHistogram:        true,
				Inverted:            true,
				HistogramMaxBuckets: stats.DefaultHistogramBuckets,
			})
		}
	}

	// Evaluate the AS OF time, if any.
	var asOfTimestamp *hlc.Timestamp
	if n.Options.AsOf.Expr != nil {
		asOf, err := n.p.EvalAsOfTimestamp(ctx, n.Options.AsOf)
		if err != nil {
			return nil, err
		}
		asOfTimestamp = &asOf.Timestamp
	}

	// Create a job to run statistics creation.
	statement := tree.AsStringWithFQNames(n, n.p.EvalContext().Annotations)
	eventLogStatement := statement
	var description string
	if n.Name == jobspb.AutoStatsName {
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
		Statements:  []string{statement},
		Username:    n.p.User(),
		Details: jobspb.CreateStatsDetails{
			Name:            string(n.Name),
			FQTableName:     fqTableName,
			Table:           *tableDesc.TableDesc(),
			ColumnStats:     colStats,
			Statement:       eventLogStatement,
			AsOf:            asOfTimestamp,
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
// collect statistics on a, {a, b}, b, and {b, c}. (But if multiColEnabled is
// false, we will only collect stats on a and b). Columns in partial index
// predicate expressions are also likely to appear in query filters, so stats
// are collected for those columns as well.
//
// In addition to the index columns, we collect stats on up to maxNonIndexCols
// other columns from the table. We only collect histograms for index columns,
// plus any other boolean or enum columns (where the "histogram" is tiny).
func createStatsDefaultColumns(
	desc catalog.TableDescriptor, multiColEnabled bool,
) ([]jobspb.CreateStatsDetails_ColStat, error) {
	colStats := make([]jobspb.CreateStatsDetails_ColStat, 0, len(desc.ActiveIndexes()))

	requestedStats := make(map[string]struct{})

	// trackStatsIfNotExists adds the given column IDs as a set to the
	// requestedStats set. If the columnIDs were not already in the set, it
	// returns true.
	trackStatsIfNotExists := func(colIDs []descpb.ColumnID) bool {
		key := makeColStatKey(colIDs)
		if _, ok := requestedStats[key]; ok {
			return false
		}
		requestedStats[key] = struct{}{}
		return true
	}

	// addIndexColumnStatsIfNotExists appends column stats for the given column
	// ID if they have not already been added. Histogram stats are collected for
	// every indexed column.
	addIndexColumnStatsIfNotExists := func(colID descpb.ColumnID, isInverted bool) error {
		col, err := desc.FindColumnWithID(colID)
		if err != nil {
			return err
		}

		// Do not collect stats for virtual computed columns. DistSQLPlanner
		// cannot currently collect stats for these columns because it plans
		// table readers on the table's primary index which does not include
		// virtual computed columns.
		if col.IsVirtual() {
			return nil
		}

		colList := []descpb.ColumnID{colID}

		// Check for existing stats and remember the requested stats.
		if !trackStatsIfNotExists(colList) {
			return nil
		}

		colStat := jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:           colList,
			HasHistogram:        !isInverted,
			HistogramMaxBuckets: stats.DefaultHistogramBuckets,
		}
		colStats = append(colStats, colStat)

		// Generate histograms for inverted indexes. The above colStat append is
		// still needed for a basic sketch of the column. The following colStat
		// is needed for the sampling and sketch of the inverted index keys of
		// the column.
		if isInverted {
			colStat.Inverted = true
			colStat.HasHistogram = true
			colStats = append(colStats, colStat)
		}

		return nil
	}

	// Add column stats for the primary key.
	primaryIdx := desc.GetPrimaryIndex()
	for i := 0; i < primaryIdx.NumKeyColumns(); i++ {
		// Generate stats for each column in the primary key.
		err := addIndexColumnStatsIfNotExists(primaryIdx.GetKeyColumnID(i), false /* isInverted */)
		if err != nil {
			return nil, err
		}

		// Only collect multi-column stats if enabled.
		if i == 0 || !multiColEnabled {
			continue
		}

		colIDs := make([]descpb.ColumnID, i+1)
		for j := 0; j <= i; j++ {
			colIDs[j] = desc.GetPrimaryIndex().GetKeyColumnID(j)
		}

		// Remember the requested stats so we don't request duplicates.
		trackStatsIfNotExists(colIDs)

		// Only generate non-histogram multi-column stats.
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:    colIDs,
			HasHistogram: false,
		})
	}

	// Add column stats for each secondary index.
	for _, idx := range desc.PublicNonPrimaryIndexes() {
		for j, n := 0, idx.NumKeyColumns(); j < n; j++ {
			colID := idx.GetKeyColumnID(j)
			isInverted := idx.GetType() == descpb.IndexDescriptor_INVERTED && colID == idx.InvertedColumnID()

			// Generate stats for each indexed column.
			if err := addIndexColumnStatsIfNotExists(colID, isInverted); err != nil {
				return nil, err
			}

			// Only collect multi-column stats if enabled.
			if j == 0 || !multiColEnabled {
				continue
			}

			colIDs := make([]descpb.ColumnID, 0, j+1)
			for k := 0; k <= j; k++ {
				col, err := desc.FindColumnWithID(idx.GetKeyColumnID(k))
				if err != nil {
					return nil, err
				}
				if col.IsVirtual() {
					continue
				}
				colIDs = append(colIDs, col.GetID())
			}

			// Check for existing stats and remember the requested stats.
			if !trackStatsIfNotExists(colIDs) {
				continue
			}

			// Only generate non-histogram multi-column stats.
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:    colIDs,
				HasHistogram: false,
			})
		}

		// Add columns referenced in partial index predicate expressions.
		if idx.IsPartial() {
			expr, err := parser.ParseExpr(idx.GetPredicate())
			if err != nil {
				return nil, err
			}

			// Extract the IDs of columns referenced in the predicate.
			colIDs, err := schemaexpr.ExtractColumnIDs(desc, expr)
			if err != nil {
				return nil, err
			}

			// Generate stats for each column individually.
			for _, colID := range colIDs.Ordered() {
				col, err := desc.FindColumnWithID(colID)
				if err != nil {
					return nil, err
				}
				isInverted := colinfo.ColumnTypeIsInvertedIndexable(col.GetType())
				if err := addIndexColumnStatsIfNotExists(colID, isInverted); err != nil {
					return nil, err
				}
			}
		}
	}

	// Add all remaining columns in the table, up to maxNonIndexCols.
	nonIdxCols := 0
	for i := 0; i < len(desc.PublicColumns()) && nonIdxCols < maxNonIndexCols; i++ {
		col := desc.PublicColumns()[i]

		// Do not collect stats for virtual computed columns.
		if col.IsVirtual() {
			continue
		}

		colList := []descpb.ColumnID{col.GetID()}

		if !trackStatsIfNotExists(colList) {
			continue
		}

		// Non-index columns have very small histograms since it's not worth the
		// overhead of storing large histograms for these columns. Since bool and
		// enum types only have a few values anyway, include all possible values
		// for those types, up to DefaultHistogramBuckets.
		maxHistBuckets := uint32(nonIndexColHistogramBuckets)
		if col.GetType().Family() == types.BoolFamily || col.GetType().Family() == types.EnumFamily {
			maxHistBuckets = stats.DefaultHistogramBuckets
		}
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:           colList,
			HasHistogram:        !colinfo.ColumnTypeIsInvertedIndexable(col.GetType()),
			HistogramMaxBuckets: maxHistBuckets,
		})
		nonIdxCols++
	}

	return colStats, nil
}

// makeColStatKey constructs a unique key representing cols that can be used
// as the key in a map.
func makeColStatKey(cols []descpb.ColumnID) string {
	var colSet util.FastIntSet
	for _, c := range cols {
		colSet.Add(int(c))
	}
	return colSet.String()
}

// createStatsResumer implements the jobs.Resumer interface for CreateStats
// jobs. A new instance is created for each job.
type createStatsResumer struct {
	job     *jobs.Job
	tableID descpb.ID
}

var _ jobs.Resumer = &createStatsResumer{}

// Resume is part of the jobs.Resumer interface.
func (r *createStatsResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	details := r.job.Details().(jobspb.CreateStatsDetails)
	if details.Name == jobspb.AutoStatsName {
		// We want to make sure that an automatic CREATE STATISTICS job only runs if
		// there are no other CREATE STATISTICS jobs running, automatic or manual.
		if err := checkRunningJobs(ctx, r.job, p); err != nil {
			return err
		}
	}

	r.tableID = details.Table.ID
	evalCtx := p.ExtendedEvalContext()

	dsp := p.DistSQLPlanner()
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction on the EvalContext to this txn. This allows for
		// use of the txn during processor setup during the execution of the flow.
		evalCtx.Txn = txn

		if details.AsOf != nil {
			p.ExtendedEvalContext().AsOfSystemTime = &tree.AsOfSystemTime{Timestamp: *details.AsOf}
			p.ExtendedEvalContext().SetTxnTimestamp(details.AsOf.GoTime())
			if err := txn.SetFixedTimestamp(ctx, *details.AsOf); err != nil {
				return err
			}
		}

		planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* planner */, txn,
			DistributionTypeSystemTenantOnly)
		// CREATE STATS flow doesn't produce any rows and only emits the
		// metadata, so we can use a nil rowContainerHelper.
		resultWriter := NewRowResultWriter(nil /* rowContainer */)
		if err := dsp.planAndRunCreateStats(
			ctx, evalCtx, planCtx, txn, r.job, resultWriter,
		); err != nil {
			// Check if this was a context canceled error and restart if it was.
			if grpcutil.IsContextCanceled(err) {
				return jobs.MarkAsRetryJobError(err)
			}

			// We can't re-use the txn from above since it has a fixed timestamp set on
			// it, and our write will be into the behind.
			txnForJobProgress := txn
			if details.AsOf != nil {
				txnForJobProgress = nil
			}

			// If the job was canceled, any of the distsql processors could have been
			// the first to encounter the .Progress error. This error's string is sent
			// through distsql back here, so we can't examine the err type in this case
			// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
			// job progress to coerce out the correct error type. If the update succeeds
			// then return the original error, otherwise return this error instead so
			// it can be cleaned up at a higher level.
			if jobErr := r.job.FractionProgressed(
				ctx, txnForJobProgress,
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
	}); err != nil {
		return err
	}
	// Record this statistics creation in the event log.
	if !createStatsPostEvents.Get(&evalCtx.Settings.SV) {
		return nil
	}

	// TODO(rytaft): This creates a new transaction for the CREATE STATISTICS
	// event. It must be different from the CREATE STATISTICS transaction,
	// because that transaction must be read-only. In the future we may want
	// to use the transaction that inserted the new stats into the
	// system.table_statistics table, but that would require calling
	// logEvent() from the distsqlrun package.
	//
	// TODO(knz): figure out why this is not triggered for a regular
	// CREATE STATISTICS statement.
	// See: https://github.com/cockroachdb/cockroach/issues/57739
	return evalCtx.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return logEventInternalForSQLStatements(ctx,
			evalCtx.ExecCfg, txn,
			0, /* depth: use event_log=2 for vmodule filtering */
			eventLogOptions{dst: LogEverywhere},
			eventpb.CommonSQLEventDetails{
				Statement:         redact.Sprint(details.Statement),
				Tag:               "CREATE STATISTICS",
				User:              evalCtx.SessionData().User().Normalized(),
				ApplicationName:   evalCtx.SessionData().ApplicationName,
				PlaceholderValues: []string{}, /* no placeholders known at this point */
			},
			eventLogEntry{
				targetID: int32(details.Table.ID),
				event: &eventpb.CreateStatistics{
					TableName: details.FQTableName,
				},
			},
		)
	})
}

// checkRunningJobs checks whether there are any other CreateStats jobs in the
// pending, running, or paused status that started earlier than this one. If
// there are, checkRunningJobs returns an error. If job is nil, checkRunningJobs
// just checks if there are any pending, running, or paused CreateStats jobs.
func checkRunningJobs(ctx context.Context, job *jobs.Job, p JobExecContext) error {
	jobID := jobspb.InvalidJobID
	if job != nil {
		jobID = job.ID()
	}
	exists, err := jobs.RunningJobExists(ctx, jobID, p.ExecCfg().InternalExecutor, nil /* txn */, func(payload *jobspb.Payload) bool {
		return payload.Type() == jobspb.TypeCreateStats || payload.Type() == jobspb.TypeAutoCreateStats
	})

	if err != nil {
		return err
	}

	if exists {
		return stats.ConcurrentCreateStatsError
	}

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnFailOrCancel(context.Context, interface{}) error { return nil }

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &createStatsResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeCreateStats, createResumerFn)
	jobs.RegisterConstructor(jobspb.TypeAutoCreateStats, createResumerFn)
}
