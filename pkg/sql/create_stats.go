// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// createStatsPostEvents controls the cluster setting for logging
// automatic table statistics collection to the event log.
var createStatsPostEvents = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.post_events.enabled",
	"if set, an event is logged for every CREATE STATISTICS job",
	false,
	settings.WithPublic)

// featureStatsEnabled is used to enable and disable the CREATE STATISTICS and
// ANALYZE features.
var featureStatsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.stats.enabled",
	"set to true to enable CREATE STATISTICS/ANALYZE, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
	settings.WithPublic)

var statsOnVirtualCols = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.virtual_computed_columns.enabled",
	"set to true to collect table statistics on virtual computed columns",
	true,
	settings.WithPublic)

// Collecting histograms on non-indexed JSON columns can require a lot of memory
// when the JSON values are large. This is true even when only two histogram
// buckets are generated because we still sample many JSON values which exist in
// memory for the duration of the stats collection job. By default, we do not
// collect histograms for non-indexed JSON columns.
var nonIndexJSONHistograms = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.non_indexed_json_histograms.enabled",
	"set to true to collect table statistics histograms on non-indexed JSON columns",
	false,
	settings.WithPublic)

const nonIndexColHistogramBuckets = 2

// StubTableStats generates "stub" statistics for a table which are missing
// statistics on virtual computed columns, multi-column stats, and histograms,
// and have 0 for all values.
func StubTableStats(
	desc catalog.TableDescriptor, name string,
) ([]*stats.TableStatisticProto, error) {
	colStats, err := createStatsDefaultColumns(
		context.Background(), desc,
		false /* virtColEnabled */, false, /* multiColEnabled */
		false /* nonIndexJSONHistograms */, false, /* partialStats */
		nonIndexColHistogramBuckets, nil, /* evalCtx */
	)
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
// runJob function starts a Job during Start, and the remainder of the
// CREATE STATISTICS planning and execution is performed within the jobs
// framework.
type createStatsNode struct {
	zeroInputPlanNode
	tree.CreateStats

	// p is the "outer planner" from planning the CREATE STATISTICS
	// statement. When we startExec the createStatsNode, it creates a job which
	// has a second planner (the JobExecContext). When the job resumes, it does
	// its work using a retrying internal transaction for which we create a third
	// "inner planner".
	p *planner

	// runAsJob is true by default, and causes the code below to be executed,
	// which sets up a job and waits for it.
	//
	// If it is false, the flow for create statistics is planned directly; this
	// is used when the statement is under EXPLAIN or EXPLAIN ANALYZE.
	runAsJob bool
}

func (n *createStatsNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("stats"))
	return n.runJob(params.ctx)
}

func (n *createStatsNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (*createStatsNode) Close(context.Context) {}
func (*createStatsNode) Values() tree.Datums   { return nil }

// runJob starts a CreateStats job synchronously to plan and execute
// statistics creation and then waits for the job to complete.
func (n *createStatsNode) runJob(ctx context.Context) error {
	record, err := n.makeJobRecord(ctx)
	if err != nil {
		return err
	}
	details := record.Details.(jobspb.CreateStatsDetails)

	if n.Name != jobspb.AutoStatsName && n.Name != jobspb.AutoPartialStatsName {
		telemetry.Inc(sqltelemetry.CreateStatisticsUseCounter)
	}

	var job *jobs.StartableJob
	jobID := n.p.ExecCfg().JobRegistry.MakeJobID()
	if err := n.p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		if n.Name == jobspb.AutoStatsName || n.Name == jobspb.AutoPartialStatsName {
			// Don't start the job if there is already a CREATE STATISTICS job running.
			// (To handle race conditions we check this again after the job starts,
			// but this check is used to prevent creating a large number of jobs that
			// immediately fail).
			if err := checkRunningJobsInTxn(ctx, jobspb.InvalidJobID, txn); err != nil {
				return err
			}
			// Don't start auto partial stats jobs if there is another auto partial
			// stats job running on the same table.
			if n.Name == jobspb.AutoPartialStatsName {
				if err := checkRunningAutoPartialJobsInTxn(ctx, jobspb.InvalidJobID, txn, n.p.ExecCfg().JobRegistry, details.Table.ID); err != nil {
					return err
				}
			}
		}
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
	if err = job.AwaitCompletion(ctx); err != nil {
		if errors.Is(err, stats.ConcurrentCreateStatsError) {
			// Delete the job so users don't see it and get confused by the error.
			if delErr := n.p.ExecCfg().JobRegistry.DeleteTerminalJobByID(ctx, job.ID()); delErr != nil {
				log.Warningf(ctx, "failed to delete job: %v", delErr)
			}
		}
	}
	return err
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
		tableDesc, err = n.p.byIDGetterBuilder().WithoutNonPublic().Get().Table(ctx, descpb.ID(t.TableID))
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

	if stats.DisallowedOnSystemTable(tableDesc.GetID()) {
		return nil, pgerror.Newf(
			pgcode.WrongObjectType, "cannot create statistics on system.%s", tableDesc.GetName(),
		)
	}

	if n.Options.UsingExtremes && !n.p.SessionData().EnableCreateStatsUsingExtremes {
		return nil, errors.Errorf(`creating partial statistics at extremes is disabled`)
	}

	// TODO(93998): Add support for WHERE.
	if n.Options.Where != nil {
		return nil, pgerror.New(pgcode.FeatureNotSupported,
			"creating partial statistics with a WHERE clause is not yet supported",
		)
	}

	if err := n.p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	var colStats []jobspb.CreateStatsDetails_ColStat
	var deleteOtherStats bool
	if len(n.ColumnNames) == 0 {
		virtColEnabled := statsOnVirtualCols.Get(n.p.ExecCfg().SV())
		// Disable multi-column stats and deleting stats if partial statistics at
		// the extremes are requested.
		// TODO(faizaanmadhani): Add support for multi-column stats.
		var multiColEnabled bool
		if !n.Options.UsingExtremes {
			multiColEnabled = stats.MultiColumnStatisticsClusterMode.Get(n.p.ExecCfg().SV())
			deleteOtherStats = true
		}
		defaultHistogramBuckets := stats.GetDefaultHistogramBuckets(n.p.ExecCfg().SV(), tableDesc)
		if colStats, err = createStatsDefaultColumns(
			ctx,
			tableDesc,
			virtColEnabled,
			multiColEnabled,
			nonIndexJSONHistograms.Get(n.p.ExecCfg().SV()),
			n.Options.UsingExtremes,
			defaultHistogramBuckets,
			n.p.EvalContext(),
		); err != nil {
			return nil, err
		}
	} else {
		columns, err := catalog.MustFindPublicColumnsByNameList(tableDesc, n.ColumnNames)
		if err != nil {
			return nil, err
		}

		columnIDs := make([]descpb.ColumnID, len(columns))
		for i := range columns {
			if columns[i].IsVirtual() && !statsOnVirtualCols.Get(n.p.ExecCfg().SV()) {
				err := pgerror.Newf(
					pgcode.InvalidColumnReference,
					"cannot create statistics on virtual column %q",
					columns[i].ColName(),
				)
				return nil, errors.WithHint(err,
					"set cluster setting sql.stats.virtual_computed_columns.enabled to collect statistics "+
						"on virtual columns",
				)
			}
			if typFam := columns[i].GetType().Family(); n.Options.UsingExtremes &&
				(typFam == types.BoolFamily || typFam == types.EnumFamily) &&
				!n.p.SessionData().EnableCreateStatsUsingExtremesBoolEnum {
				return nil, pgerror.Newf(pgcode.FeatureNotSupported, "creating partial statistics at extremes on bool and enum columns is disabled")
			}
			columnIDs[i] = columns[i].GetID()
		}
		col, err := catalog.MustFindColumnByID(tableDesc, columnIDs[0])
		if err != nil {
			return nil, err
		}
		// Sort columnIDs to make equivalent column sets equal when using SHOW
		// STATISTICS or other SQL on table_statistics.
		_ = stats.MakeSortedColStatKey(columnIDs)
		isInvIndex := colinfo.ColumnTypeIsOnlyInvertedIndexable(col.GetType())
		defaultHistogramBuckets := stats.GetDefaultHistogramBuckets(n.p.ExecCfg().SV(), tableDesc)
		colStats = []jobspb.CreateStatsDetails_ColStat{{
			ColumnIDs: columnIDs,
			// By default, create histograms on all explicitly requested column stats
			// with a single column that doesn't use an inverted index.
			HasHistogram:        len(columnIDs) == 1 && !isInvIndex,
			HistogramMaxBuckets: defaultHistogramBuckets,
		}}
		// Make histograms for inverted index column types.
		if len(columnIDs) == 1 && isInvIndex {
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:           columnIDs,
				HasHistogram:        true,
				Inverted:            true,
				HistogramMaxBuckets: defaultHistogramBuckets,
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
			Name:             string(n.Name),
			FQTableName:      fqTableName,
			Table:            *tableDesc.TableDesc(),
			ColumnStats:      colStats,
			Statement:        eventLogStatement,
			AsOf:             asOfTimestamp,
			MaxFractionIdle:  n.Options.Throttling,
			DeleteOtherStats: deleteOtherStats,
			UsingExtremes:    n.Options.UsingExtremes,
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
// If nonIndexJsonHistograms is true, 2-bucket histograms are collected for
// non-indexed JSON columns.
//
// If partialStats is true, we only collect statistics on single columns that
// are prefixes of forward indexes, and skip over partial, sharded, and
// implicitly partitioned indexes. Partial statistic creation only supports
// these columns.
//
// In addition to the index columns, we collect stats on up to maxNonIndexCols
// other columns from the table. We only collect histograms for index columns,
// plus any other boolean or enum columns (where the "histogram" is tiny).
func createStatsDefaultColumns(
	ctx context.Context,
	desc catalog.TableDescriptor,
	virtColEnabled, multiColEnabled, nonIndexJSONHistograms, partialStats bool,
	defaultHistogramBuckets uint32,
	evalCtx *eval.Context,
) ([]jobspb.CreateStatsDetails_ColStat, error) {
	colStats := make([]jobspb.CreateStatsDetails_ColStat, 0, len(desc.ActiveIndexes()))

	requestedStats := make(map[string]struct{})

	// CREATE STATISTICS only runs as a fully-distributed plan. If statistics on
	// virtual computed columns are enabled, we must check whether each virtual
	// computed column expression is safe to distribute. Virtual computed columns
	// with expressions *not* safe to distribute will be skipped, even if
	// sql.stats.virtual_computed_columns.enabled is true.
	// TODO(michae2): Add the ability to run CREATE STATISTICS locally if a
	// local-only virtual computed column expression is needed.
	cannotDistribute := make([]bool, len(desc.PublicColumns()))
	if virtColEnabled {
		semaCtx := tree.MakeSemaContext(evalCtx.Planner)
		exprs, _, err := schemaexpr.MakeComputedExprs(
			ctx,
			desc.PublicColumns(),
			desc.PublicColumns(),
			desc,
			tree.NewUnqualifiedTableName(tree.Name(desc.GetName())),
			evalCtx,
			&semaCtx,
		)
		if err != nil {
			return nil, err
		}
		var distSQLVisitor distSQLExprCheckVisitor
		for i, col := range desc.PublicColumns() {
			cannotDistribute[i] = col.IsVirtual() && checkExprForDistSQL(exprs[i], &distSQLVisitor) != nil
		}
	}

	isUnsupportedVirtual := func(col catalog.Column) bool {
		return col.IsVirtual() && (!virtColEnabled || cannotDistribute[col.Ordinal()])
	}

	// sortAndTrackStatsExists adds the given column IDs as a set to the
	// requestedStats set. If the columnIDs were already in the set, it returns
	// true. As a side-effect sortAndTrackStatsExists also sorts colIDs. NOTE:
	// This assumes that ordering is not significant for multi-column stats.
	sortAndTrackStatsExists := func(colIDs []descpb.ColumnID) bool {
		key := stats.MakeSortedColStatKey(colIDs)
		if _, ok := requestedStats[key]; ok {
			return true
		}
		requestedStats[key] = struct{}{}
		return false
	}

	// addIndexColumnStatsIfNotExists appends column stats for the given column
	// ID if they have not already been added. Histogram stats are collected for
	// every indexed column.
	addIndexColumnStatsIfNotExists := func(colID descpb.ColumnID, isInverted bool) error {
		col, err := catalog.MustFindColumnByID(desc, colID)
		if err != nil {
			return err
		}

		// There shouldn't be any non-public columns, but defensively skip over them
		// if there are.
		if !col.Public() {
			return nil
		}

		// Skip unsupported virtual computed columns.
		if isUnsupportedVirtual(col) {
			return nil
		}

		colIDs := []descpb.ColumnID{colID}

		// Check for existing stats and remember the requested stats.
		if ok := sortAndTrackStatsExists(colIDs); ok {
			return nil
		}

		colStat := jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:           colIDs,
			HasHistogram:        !isInverted,
			HistogramMaxBuckets: defaultHistogramBuckets,
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

	// Only collect statistics on single columns that are prefixes of forward
	// indexes for partial statistics, and skip over partial, sharded, and
	// implicitly partitioned indexes.
	if partialStats {
		for _, idx := range desc.ActiveIndexes() {
			if idx.GetType() != idxtype.FORWARD ||
				idx.IsPartial() ||
				idx.IsSharded() ||
				idx.ImplicitPartitioningColumnCount() > 0 {
				continue
			}
			if idx.NumKeyColumns() != 0 {
				colID := idx.GetKeyColumnID(0)
				if err := addIndexColumnStatsIfNotExists(colID, false /* isInverted */); err != nil {
					return nil, err
				}
			}
		}
		return colStats, nil
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

		colIDs := make([]descpb.ColumnID, 0, i+1)
		for j := 0; j <= i; j++ {
			col, err := catalog.MustFindColumnByID(desc, desc.GetPrimaryIndex().GetKeyColumnID(j))
			if err != nil {
				return nil, err
			}

			// There shouldn't be any non-public columns, but defensively skip over
			// them if there are.
			if !col.Public() {
				continue
			}

			// Skip unsupported virtual computed columns.
			if isUnsupportedVirtual(col) {
				continue
			}
			colIDs = append(colIDs, col.GetID())
		}

		// Do not attempt to create multi-column stats with < 2 columns. This can
		// happen when an index contains only virtual computed columns.
		if len(colIDs) < 2 {
			continue
		}

		// Remember the requested stats so we don't request duplicates.
		_ = sortAndTrackStatsExists(colIDs)

		// Only generate non-histogram multi-column stats.
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:    colIDs,
			HasHistogram: false,
		})
	}

	// Add column stats for each secondary index.
	for _, idx := range desc.PublicNonPrimaryIndexes() {
		if idx.GetType() == idxtype.VECTOR {
			// Skip vector indexes for now.
			continue
		}
		for j, n := 0, idx.NumKeyColumns(); j < n; j++ {
			colID := idx.GetKeyColumnID(j)
			isInverted := idx.GetType() == idxtype.INVERTED && colID == idx.InvertedColumnID()

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
				col, err := catalog.MustFindColumnByID(desc, idx.GetKeyColumnID(k))
				if err != nil {
					return nil, err
				}

				// There shouldn't be any non-public columns, but defensively skip them
				// if there are.
				if !col.Public() {
					continue
				}

				// Skip unsupported virtual computed columns.
				if isUnsupportedVirtual(col) {
					continue
				}
				colIDs = append(colIDs, col.GetID())
			}

			// Do not attempt to create multi-column stats with < 2 columns. This can
			// happen when an index contains only virtual computed columns.
			if len(colIDs) < 2 {
				continue
			}

			// Check for existing stats and remember the requested stats.
			if ok := sortAndTrackStatsExists(colIDs); ok {
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
				col, err := catalog.MustFindColumnByID(desc, colID)
				if err != nil {
					return nil, err
				}
				isInverted := colinfo.ColumnTypeIsOnlyInvertedIndexable(col.GetType())
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

		// Skip unsupported virtual computed columns.
		if isUnsupportedVirtual(col) {
			continue
		}

		colIDs := []descpb.ColumnID{col.GetID()}

		// Check for existing stats.
		if ok := sortAndTrackStatsExists(colIDs); ok {
			continue
		}

		// Non-index columns have very small histograms since it's not worth the
		// overhead of storing large histograms for these columns. Since bool and
		// enum types only have a few values anyway, include all possible values
		// for those types, up to DefaultHistogramBuckets.
		maxHistBuckets := uint32(nonIndexColHistogramBuckets)
		if col.GetType().Family() == types.BoolFamily || col.GetType().Family() == types.EnumFamily {
			maxHistBuckets = defaultHistogramBuckets
		}
		hasHistogram := !colinfo.ColumnTypeIsOnlyInvertedIndexable(col.GetType())
		if col.GetType().Family() == types.JsonFamily {
			hasHistogram = nonIndexJSONHistograms
		}
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:           colIDs,
			HasHistogram:        hasHistogram,
			HistogramMaxBuckets: maxHistBuckets,
		})
		nonIdxCols++
	}

	return colStats, nil
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
	// jobsPlanner is a second planner distinct from the "outer planner" in the
	// createStatsNode. It comes from the jobs system and does not have an
	// associated txn.
	jobsPlanner := execCtx.(JobExecContext)
	details := r.job.Details().(jobspb.CreateStatsDetails)
	if details.Name == jobspb.AutoStatsName || details.Name == jobspb.AutoPartialStatsName {
		jobRegistry := jobsPlanner.ExecCfg().JobRegistry
		// We want to make sure that an automatic CREATE STATISTICS job only runs if
		// there are no other CREATE STATISTICS jobs running, automatic or manual.
		if err := checkRunningJobs(
			ctx,
			r.job,
			jobsPlanner,
			details.Name == jobspb.AutoPartialStatsName,
			jobRegistry,
			details.Table.ID,
		); err != nil {
			return err
		}
	}

	r.tableID = details.Table.ID

	if err := jobsPlanner.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// We create a third "inner planner" associated with this txn in order to
		// have (a) use of the txn during type checking of any virtual computed
		// column expressions, and (b) use of the txn during processor setup during
		// the execution of the flow.
		innerPlanner, cleanup := NewInternalPlanner(
			"create-stats-resume-job",
			txn.KV(),
			jobsPlanner.User(),
			&MemoryMetrics{},
			jobsPlanner.ExecCfg(),
			jobsPlanner.SessionData(),
		)
		defer cleanup()
		innerP := innerPlanner.(*planner)
		innerEvalCtx := innerP.ExtendedEvalContext()
		if details.AsOf != nil {
			innerP.ExtendedEvalContext().AsOfSystemTime = &eval.AsOfSystemTime{Timestamp: *details.AsOf}
			innerP.ExtendedEvalContext().SetTxnTimestamp(details.AsOf.GoTime())
			if err := txn.KV().SetFixedTimestamp(ctx, *details.AsOf); err != nil {
				return err
			}
		}

		dsp := innerP.DistSQLPlanner()
		// CREATE STATS flow doesn't produce any rows and only emits the
		// metadata, so we can use a nil rowContainerHelper.
		resultWriter := NewRowResultWriter(nil /* rowContainer */)

		var err error
		if details.UsingExtremes {
			for i, colStat := range details.ColumnStats {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Plan and run partial stats on multiple columns separately since each
				// partial stat collection will use a different index and have different
				// plans.
				singleColDetails := protoutil.Clone(&details).(*jobspb.CreateStatsDetails)
				singleColDetails.ColumnStats = []jobspb.CreateStatsDetails_ColStat{colStat}
				planCtx := dsp.NewPlanningCtx(ctx, innerEvalCtx, innerP, txn.KV(), FullDistribution)
				if err = dsp.planAndRunCreateStats(
					ctx, innerEvalCtx, planCtx, innerP.SemaCtx(), txn.KV(), resultWriter, r.job.ID(), *singleColDetails,
					len(details.ColumnStats), i,
				); err != nil {
					break
				}
			}
		} else {
			planCtx := dsp.NewPlanningCtx(ctx, innerEvalCtx, innerP, txn.KV(), FullDistribution)
			err = dsp.planAndRunCreateStats(
				ctx, innerEvalCtx, planCtx, innerP.SemaCtx(), txn.KV(), resultWriter, r.job.ID(), details,
				1 /* numIndexes */, 0, /* curIndex */
			)
		}

		if err != nil {
			// Check if this was a context canceled error and restart if it was.
			if grpcutil.IsContextCanceled(err) {
				return jobs.MarkAsRetryJobError(err)
			}

			// If the job was canceled, any of the distsql processors could have been
			// the first to encounter the .Progress error. This error's string is sent
			// through distsql back here, so we can't examine the err type in this case
			// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
			// job progress to coerce out the correct error type. If the update succeeds
			// then return the original error, otherwise return this error instead so
			// it can be cleaned up at a higher level.
			if jobErr := r.job.NoTxn().FractionProgressed(ctx, func(
				ctx context.Context, _ jobspb.ProgressDetails,
			) float32 {
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

	evalCtx := jobsPlanner.ExtendedEvalContext()

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
	return evalCtx.ExecCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
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
				DescriptorID:      uint32(details.Table.ID),
			},
			&eventpb.CreateStatistics{
				TableName: details.FQTableName,
			},
		)
	})
}

// checkRunningJobs checks whether there are any other CreateStats jobs in the
// pending, running, or paused status that started earlier than this one. If
// there are, checkRunningJobs returns an error. If job is nil, checkRunningJobs
// just checks if there are any pending, running, or paused CreateStats jobs.
// If autoPartial is true, checkRunningJobs also checks if there are any other
// AutoCreatePartialStats jobs in the pending, running, or paused status that
// started earlier than this one for the same table.
func checkRunningJobs(
	ctx context.Context,
	job *jobs.Job,
	p JobExecContext,
	autoPartial bool,
	jobRegistry *jobs.Registry,
	tableID descpb.ID,
) error {
	jobID := jobspb.InvalidJobID
	if job != nil {
		jobID = job.ID()
	}
	return p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		if err = checkRunningJobsInTxn(ctx, jobID, txn); err != nil {
			return err
		}
		if autoPartial {
			return checkRunningAutoPartialJobsInTxn(ctx, jobID, txn, jobRegistry, tableID)
		}
		return nil
	})
}

// checkRunningJobsInTxn checks whether there are any other CreateStats jobs
// (excluding auto partial stats jobs) in the pending, running, or paused status
// that started earlier than this one. If there are, checkRunningJobsInTxn
// returns an error. If jobID is jobspb.InvalidJobID, checkRunningJobsInTxn just
// checks if there are any pending, running, or paused CreateStats jobs.
func checkRunningJobsInTxn(ctx context.Context, jobID jobspb.JobID, txn isql.Txn) error {
	exists, err := jobs.RunningJobExists(ctx, jobID, txn,
		jobspb.TypeCreateStats, jobspb.TypeAutoCreateStats,
	)
	if err != nil {
		return err
	}

	if exists {
		return stats.ConcurrentCreateStatsError
	}

	return nil
}

// checkRunningAutoPartialJobsInTxn checks whether there are any other
// AutoCreatePartialStats jobs in the pending, running, or paused status that
// started earlier than this one for the same table. If there are, an error is
// returned. If jobID is jobspb.InvalidJobID, checkRunningAutoPartialJobsInTxn
// just checks if there are any pending, running, or paused
// AutoCreatePartialStats jobs for the same table.
func checkRunningAutoPartialJobsInTxn(
	ctx context.Context,
	jobID jobspb.JobID,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	tableID descpb.ID,
) error {
	autoPartialStatJobIDs, err := jobs.RunningJobs(ctx, jobID, txn,
		jobspb.TypeAutoCreatePartialStats,
	)
	if err != nil {
		return err
	}

	for _, id := range autoPartialStatJobIDs {
		job, err := jobRegistry.LoadJobWithTxn(ctx, id, txn)
		if err != nil {
			return err
		}
		jobDetails := job.Details().(jobspb.CreateStatsDetails)
		if jobDetails.Table.ID == tableID {
			return stats.ConcurrentCreateStatsError
		}
	}

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnFailOrCancel(context.Context, interface{}, error) error { return nil }

// CollectProfile is part of the jobs.Resumer interface.
func (r *createStatsResumer) CollectProfile(context.Context, interface{}) error { return nil }

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &createStatsResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeCreateStats, createResumerFn, jobs.UsesTenantCostControl)
	jobs.RegisterConstructor(jobspb.TypeAutoCreateStats, createResumerFn, jobs.UsesTenantCostControl)
	jobs.RegisterConstructor(jobspb.TypeAutoCreatePartialStats, createResumerFn, jobs.UsesTenantCostControl)
}
