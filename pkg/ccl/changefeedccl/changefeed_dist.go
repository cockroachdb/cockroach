// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvfollowerreadsccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
)

func init() {
	rowexec.NewChangeAggregatorProcessor = newChangeAggregatorProcessor
	rowexec.NewChangeFrontierProcessor = newChangeFrontierProcessor
}

const (
	changeAggregatorProcName = `changeagg`
	changeFrontierProcName   = `changefntr`
)

// distChangefeedFlow plans and runs a distributed changefeed.
//
// One or more ChangeAggregator processors watch table data for changes. These
// transform the changed kvs into changed rows and either emit them to a sink
// (such as kafka) or, if there is no sink, forward them in columns 1,2,3 (where
// they will be eventually returned directly via pgwire). In either case,
// periodically a span will become resolved as of some timestamp, meaning that
// no new rows will ever be emitted at or below that timestamp. These span-level
// resolved timestamps are emitted as a marshaled `jobspb.ResolvedSpan` proto in
// column 0.
//
// The flow will always have exactly one ChangeFrontier processor which all the
// ChangeAggregators feed into. It collects all span-level resolved timestamps
// and aggregates them into a changefeed-level resolved timestamp, which is the
// minimum of the span-level resolved timestamps. This changefeed-level resolved
// timestamp is emitted into the changefeed sink (or returned to the gateway if
// there is no sink) whenever it advances. ChangeFrontier also updates the
// progress of the changefeed's corresponding system job.
func distChangefeedFlow(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	description string,
	localState *cachedState,
	resultsCh chan<- tree.Datums,
) error {
	opts := changefeedbase.MakeStatementOptions(details.Opts)
	progress := localState.progress

	// NB: A non-empty high water indicates that we have checkpointed a resolved
	// timestamp. Skipping the initial scan is equivalent to starting the
	// changefeed from a checkpoint at its start time. Initialize the progress
	// based on whether we should perform an initial scan.
	{
		h := progress.GetHighWater()
		noHighWater := (h == nil || h.IsEmpty())
		// We want to set the highWater and thus avoid an initial scan if either
		// this is a cursor and there was no request for one, or we don't have a
		// cursor but we have a request to not have an initial scan.
		initialScanType, err := opts.GetInitialScanType()
		if err != nil {
			return err
		}
		if noHighWater && initialScanType == changefeedbase.NoInitialScan {
			// If there is a cursor, the statement time has already been set to it.
			progress.Progress = &jobspb.Progress_HighWater{HighWater: &details.StatementTime}
		}
	}

	var initialHighWater hlc.Timestamp
	schemaTS := details.StatementTime
	{
		if h := progress.GetHighWater(); h != nil && !h.IsEmpty() {
			initialHighWater = *h
			// If we have a high-water set, use it to compute the spans, since the
			// ones at the statement time may have been garbage collected by now.
			schemaTS = initialHighWater
		}

		// We want to fetch the target spans as of the timestamp following the
		// highwater unless the highwater corresponds to a timestamp of an initial
		// scan. This logic is irritatingly complex but extremely important. Namely,
		// we may be here because the schema changed at the current resolved
		// timestamp. However, an initial scan should be performed at exactly the
		// timestamp specified; initial scans can be created at the timestamp of a
		// schema change and thus should see the side-effect of the schema change.
		isRestartAfterCheckpointOrNoInitialScan := progress.GetHighWater() != nil
		if isRestartAfterCheckpointOrNoInitialScan {
			schemaTS = schemaTS.Next()
		}
	}

	if knobs, ok := execCtx.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
		if knobs != nil && knobs.StartDistChangefeedInitialHighwater != nil {
			knobs.StartDistChangefeedInitialHighwater(ctx, initialHighWater)
		}
	}
	return startDistChangefeed(
		ctx, execCtx, jobID, schemaTS, details, description, initialHighWater, localState, resultsCh)
}

func fetchTableDescriptors(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	targets changefeedbase.Targets,
	ts hlc.Timestamp,
) ([]catalog.TableDescriptor, error) {
	var targetDescs []catalog.TableDescriptor

	fetchSpans := func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		targetDescs = make([]catalog.TableDescriptor, 0, targets.NumUniqueTables())
		if err := txn.KV().SetFixedTimestamp(ctx, ts); err != nil {
			return errors.Wrapf(err, "setting timestamp for table descriptor fetch")
		}
		// Note that all targets are currently guaranteed to have a Table ID
		// and lie within the primary index span. Deduplication is important
		// here as requesting the same span twice will deadlock.
		return targets.EachTableID(func(id catid.DescID) error {
			tableDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
			if err != nil {
				return errors.Wrapf(err, "fetching table descriptor %d", id)
			}
			targetDescs = append(targetDescs, tableDesc)
			return nil
		})
	}
	if err := sql.DescsTxn(ctx, execCfg, fetchSpans); err != nil {
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			return nil, changefeedbase.WithTerminalError(err)
		}
		return nil, err
	}
	return targetDescs, nil
}

// changefeedResultTypes is the types returned by changefeed stream.
var changefeedResultTypes = []*types.T{
	types.Bytes,  // aggregator progress update
	types.String, // topic
	types.Bytes,  // key
	types.Bytes,  // value
}

// fetchSpansForTable returns the set of spans for the specified table.
// Usually, this is just the primary index span.
// However, if details.Select is not empty, the set of spans returned may be
// restricted to satisfy predicate in the select clause.
func fetchSpansForTables(
	ctx context.Context,
	execCtx sql.JobExecContext,
	tableDescs []catalog.TableDescriptor,
	details jobspb.ChangefeedDetails,
	initialHighwater hlc.Timestamp,
) (roachpb.Spans, error) {
	var trackedSpans []roachpb.Span
	if details.Select == "" {
		for _, d := range tableDescs {
			trackedSpans = append(trackedSpans, d.PrimaryIndexSpan(execCtx.ExecCfg().Codec))
		}
		return trackedSpans, nil
	}

	if len(tableDescs) != 1 {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"filter can only be used with single target (found %d)",
			len(tableDescs))
	}
	target := details.TargetSpecifications[0]
	sc, err := cdceval.ParseChangefeedExpression(details.Select)
	if err != nil {
		return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue,
			"could not parse changefeed expression")
	}

	// SessionData is nil if the changefeed was created prior to
	// clusterversion.V23_1_ChangefeedExpressionProductionReady
	sd := sql.NewInternalSessionData(ctx, execCtx.ExecCfg().Settings, "changefeed-fetchSpansForTables")
	if details.SessionData != nil {
		sd.SessionData = *details.SessionData
	}
	return cdceval.SpansForExpression(ctx, execCtx.ExecCfg(), execCtx.User(),
		sd, tableDescs[0], initialHighwater, target, sc)
}

// startDistChangefeed starts distributed changefeed execution.
func startDistChangefeed(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	schemaTS hlc.Timestamp,
	details jobspb.ChangefeedDetails,
	description string,
	initialHighWater hlc.Timestamp,
	localState *cachedState,
	resultsCh chan<- tree.Datums,
) error {
	execCfg := execCtx.ExecCfg()
	tableDescs, err := fetchTableDescriptors(ctx, execCfg, AllTargets(details), schemaTS)
	if err != nil {
		return err
	}

	if schemaTS.IsEmpty() {
		schemaTS = details.StatementTime
	}
	trackedSpans, err := fetchSpansForTables(ctx, execCtx, tableDescs, details, schemaTS)
	if err != nil {
		return err
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, "tracked spans: %s", trackedSpans)
	}
	localState.trackedSpans = trackedSpans

	// Changefeed flows handle transactional consistency themselves.
	var noTxn *kv.Txn

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	//lint:ignore SA1019 deprecated usage
	var checkpoint *jobspb.ChangefeedProgress_Checkpoint
	if progress := localState.progress.GetChangefeed(); progress != nil && progress.Checkpoint != nil {
		checkpoint = progress.Checkpoint
	}
	var spanLevelCheckpoint *jobspb.TimestampSpansMap
	if progress := localState.progress.GetChangefeed(); progress != nil && progress.SpanLevelCheckpoint != nil {
		spanLevelCheckpoint = progress.SpanLevelCheckpoint
	}
	p, planCtx, err := makePlan(execCtx, jobID, details, description, initialHighWater,
		trackedSpans, checkpoint, spanLevelCheckpoint, localState.drainingNodes)(ctx, dsp)
	if err != nil {
		return err
	}

	execPlan := func(ctx context.Context) error {
		// Derive a separate context so that we can shut down the changefeed
		// as soon as we see an error.
		ctx, cancel := execCtx.ExecCfg().DistSQLSrv.Stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		// clear out previous drain/shutdown information.
		localState.drainingNodes = localState.drainingNodes[:0]
		localState.aggregatorFrontier = localState.aggregatorFrontier[:0]

		resultRows := sql.NewMetadataCallbackWriter(
			makeChangefeedResultWriter(resultsCh, cancel),
			func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
				if meta.Changefeed != nil {
					if meta.Changefeed.DrainInfo != nil {
						localState.drainingNodes = append(localState.drainingNodes, meta.Changefeed.DrainInfo.NodeID)
					}
					localState.aggregatorFrontier = append(localState.aggregatorFrontier, meta.Changefeed.Checkpoint...)
				}
				return nil
			},
		)

		recv := sql.MakeDistSQLReceiver(
			ctx,
			resultRows,
			tree.Rows,
			execCtx.ExecCfg().RangeDescriptorCache,
			noTxn,
			nil, /* clockUpdater */
			evalCtx.Tracing,
		)
		defer recv.Release()

		var finishedSetupFn func(flowinfra.Flow)
		if details.SinkURI != `` {
			// We abuse the job's results channel to make CREATE CHANGEFEED wait for
			// this before returning to the user to ensure the setup went okay. Job
			// resumption doesn't have the same hack, but at the moment ignores
			// results and so is currently okay. Return nil instead of anything
			// meaningful so that if we start doing anything with the results
			// returned by resumed jobs, then it breaks instead of returning
			// nonsense.
			finishedSetupFn = func(flowinfra.Flow) { resultsCh <- tree.Datums(nil) }
		}

		jobsprofiler.StorePlanDiagram(ctx, execCfg.DistSQLSrv.Stopper, p, execCfg.InternalDB, jobID)

		// Make sure to use special changefeed monitor going forward as the
		// parent monitor for the DistSQL infrastructure. This is needed to
		// prevent a race between the connection that started the changefeed
		// closing (which closes the current planner's monitor) and changefeed
		// DistSQL flow being cleaned up.
		planCtx.OverridePlannerMon = execCfg.DistSQLSrv.ChangefeedMonitor
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		// p is the physical plan, recv is the DistSQLReceiver.
		dsp.Run(ctx, planCtx, noTxn, p, recv, &evalCtxCopy, finishedSetupFn)
		return resultRows.Err()
	}

	return ctxgroup.GoAndWait(ctx, execPlan)
}

// The bin packing choice gives preference to leaseholder replicas if possible.
var replicaOracleChoice = replicaoracle.BinPackingChoice

type rangeDistributionType int

const (
	// defaultDistribution employs no load balancing on the changefeed
	// side. We defer to distsql to select nodes and distribute work.
	defaultDistribution rangeDistributionType = 0
	// balancedSimpleDistribution defers to distsql for selecting the
	// set of nodes to distribute work to. However, changefeeds will try to
	// distribute work evenly across this set of nodes.
	balancedSimpleDistribution rangeDistributionType = 1
	// TODO(jayant): add balancedFullDistribution which takes
	// full control of node selection and distribution.
)

// RangeDistributionStrategy is used to determine how the changefeed balances
// ranges between nodes.
// TODO: deprecate this setting in favor of a changefeed option.
var RangeDistributionStrategy = settings.RegisterEnumSetting(
	settings.ApplicationLevel,
	"changefeed.default_range_distribution_strategy",
	"configures how work is distributed among nodes for a given changefeed. "+
		"for the most balanced distribution, use `balanced_simple`. changing this setting "+
		"will not override locality restrictions",
	metamorphic.ConstantWithTestChoice("default_range_distribution_strategy",
		"default", "balanced_simple"),
	map[rangeDistributionType]string{
		defaultDistribution:        "default",
		balancedSimpleDistribution: "balanced_simple",
	},
	settings.WithPublic)

var useBulkOracle = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.random_replica_selection.enabled",
	"randomize the selection of which replica backs up each range",
	true)

func makePlan(
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	description string,
	initialHighWater hlc.Timestamp,
	trackedSpans []roachpb.Span,
	//lint:ignore SA1019 deprecated usage
	legacyCheckpoint *jobspb.ChangefeedProgress_Checkpoint,
	spanLevelCheckpoint *jobspb.TimestampSpansMap,
	drainingNodes []roachpb.NodeID,
) func(context.Context, *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	return func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
		sv := &execCtx.ExecCfg().Settings.SV
		maybeCfKnobs, haveKnobs := execCtx.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)
		var blankTxn *kv.Txn

		distMode := sql.FullDistribution
		if details.SinkURI == `` {
			// Sinkless feeds get one ChangeAggregator on this node.
			distMode = sql.LocalDistribution
		}

		var locFilter roachpb.Locality
		if loc := details.Opts[changefeedbase.OptExecutionLocality]; loc != "" {
			if err := locFilter.Set(loc); err != nil {
				return nil, nil, err
			}
		}

		rangeDistribution := RangeDistributionStrategy.Get(sv)
		evalCtx := execCtx.ExtendedEvalContext()
		oracle := replicaoracle.NewOracle(replicaOracleChoice, dsp.ReplicaOracleConfig(locFilter))
		if useBulkOracle.Get(&evalCtx.Settings.SV) {
			log.Infof(ctx, "using bulk oracle for DistSQL planning")
			oracle = kvfollowerreadsccl.NewBulkOracle(dsp.ReplicaOracleConfig(evalCtx.Locality), locFilter, kvfollowerreadsccl.StreakConfig{})
		}
		planCtx := dsp.NewPlanningCtxWithOracle(ctx, execCtx.ExtendedEvalContext(), nil, /* planner */
			blankTxn, sql.DistributionType(distMode), oracle, locFilter)
		spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, trackedSpans, sql.PartitionSpansBoundDefault)
		if err != nil {
			return nil, nil, err
		}
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "spans returned by DistSQL: %v", spanPartitions)
		}
		switch {
		case distMode == sql.LocalDistribution || rangeDistribution == defaultDistribution:
		case rangeDistribution == balancedSimpleDistribution:
			log.Infof(ctx, "rebalancing ranges using balanced simple distribution")
			sender := execCtx.ExecCfg().DB.NonTransactionalSender()
			distSender := sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
			ri := kvcoord.MakeRangeIterator(distSender)
			spanPartitions, err = rebalanceSpanPartitions(
				ctx, &ri, rebalanceThreshold.Get(sv), spanPartitions)
			if err != nil {
				return nil, nil, err
			}
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.Infof(ctx, "spans after balanced simple distribution rebalancing: %v", spanPartitions)
			}
		default:
			return nil, nil, errors.AssertionFailedf("unsupported dist strategy %d and dist mode %d",
				rangeDistribution, distMode)
		}

		if haveKnobs && maybeCfKnobs.FilterDrainingNodes != nil && len(drainingNodes) > 0 {
			spanPartitions, err = maybeCfKnobs.FilterDrainingNodes(spanPartitions, drainingNodes)
			if err != nil {
				return nil, nil, err
			}
		}

		if haveKnobs && maybeCfKnobs.SpanPartitionsCallback != nil {
			maybeCfKnobs.SpanPartitionsCallback(spanPartitions)
		}

		// Use the same checkpoint for all aggregators; each aggregator will only look at
		// spans that are assigned to it.
		// We could compute per-aggregator checkpoint, but that's probably an overkill.
		//lint:ignore SA1019 deprecated usage
		var aggregatorCheckpoint execinfrapb.ChangeAggregatorSpec_Checkpoint
		var checkpointSpanGroup roachpb.SpanGroup

		if legacyCheckpoint != nil {
			checkpointSpanGroup.Add(legacyCheckpoint.Spans...)
			aggregatorCheckpoint.Spans = legacyCheckpoint.Spans
			aggregatorCheckpoint.Timestamp = legacyCheckpoint.Timestamp
		}
		if log.V(2) {
			log.Infof(ctx, "aggregator checkpoint: %s", aggregatorCheckpoint)
		}

		aggregatorSpecs := make([]*execinfrapb.ChangeAggregatorSpec, len(spanPartitions))
		for i, sp := range spanPartitions {
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.Infof(ctx, "watched spans for node %d: %v", sp.SQLInstanceID, sp)
			}
			watches := make([]execinfrapb.ChangeAggregatorSpec_Watch, len(sp.Spans))

			var initialHighWaterPtr *hlc.Timestamp
			for watchIdx, nodeSpan := range sp.Spans {
				if evalCtx.Settings.Version.IsActive(ctx, clusterversion.V25_2) {
					// If the cluster has been fully upgraded to v25.2, we should populate
					// the initial highwater of ChangeAggregatorSpec_Watch and leave the
					// initial resolved of each span empty. We rely on the aggregators to
					// forward the checkpointed timestamp for every span based on
					// aggregatorCheckpoint.
					watches[watchIdx] = execinfrapb.ChangeAggregatorSpec_Watch{
						Span: nodeSpan,
					}
					initialHighWaterPtr = &initialHighWater
				} else {
					// If the cluster has not been fully upgraded to v25.2, we should
					// leave the initial highwater of ChangeAggregatorSpec_Watch as nil.
					// We rely on this to tell the aggregators to the initial resolved
					// timestamp for each span to infer the initial highwater. Read more
					// from changeAggregator.getInitialHighWaterAndSpans.
					initialResolved := initialHighWater
					if checkpointSpanGroup.Encloses(nodeSpan) {
						initialResolved = legacyCheckpoint.Timestamp
					}
					watches[watchIdx] = execinfrapb.ChangeAggregatorSpec_Watch{
						Span:            nodeSpan,
						InitialResolved: initialResolved,
					}
				}
			}

			aggregatorSpecs[i] = &execinfrapb.ChangeAggregatorSpec{
				Watches:             watches,
				Checkpoint:          aggregatorCheckpoint,
				InitialHighWater:    initialHighWaterPtr,
				SpanLevelCheckpoint: spanLevelCheckpoint,
				Feed:                details,
				UserProto:           execCtx.User().EncodeProto(),
				JobID:               jobID,
				Select:              execinfrapb.Expression{Expr: details.Select},
				Description:         description,
			}
		}

		// NB: This SpanFrontier processor depends on the set of tracked spans being
		// static. Currently there is no way for them to change after the changefeed
		// is created, even if it is paused and unpaused, but #28982 describes some
		// ways that this might happen in the future.
		changeFrontierSpec := execinfrapb.ChangeFrontierSpec{
			TrackedSpans: trackedSpans,
			Feed:         details,
			JobID:        jobID,
			UserProto:    execCtx.User().EncodeProto(),
			Description:  description,
		}

		if spanLevelCheckpoint != nil {
			changeFrontierSpec.SpanLevelCheckpoint = spanLevelCheckpoint
		} else {
			changeFrontierSpec.SpanLevelCheckpoint = checkpoint.ConvertFromLegacyCheckpoint(legacyCheckpoint, details.StatementTime, initialHighWater)
		}

		if haveKnobs && maybeCfKnobs.OnDistflowSpec != nil {
			maybeCfKnobs.OnDistflowSpec(aggregatorSpecs, &changeFrontierSpec)
		}

		aggregatorCorePlacement := make([]physicalplan.ProcessorCorePlacement, len(spanPartitions))
		for i, sp := range spanPartitions {
			aggregatorCorePlacement[i].SQLInstanceID = sp.SQLInstanceID
			aggregatorCorePlacement[i].Core.ChangeAggregator = aggregatorSpecs[i]
		}

		p := planCtx.NewPhysicalPlan()
		p.AddNoInputStage(aggregatorCorePlacement, execinfrapb.PostProcessSpec{}, changefeedResultTypes, execinfrapb.Ordering{})
		p.AddSingleGroupStage(
			ctx,
			dsp.GatewayID(),
			execinfrapb.ProcessorCoreUnion{ChangeFrontier: &changeFrontierSpec},
			execinfrapb.PostProcessSpec{},
			changefeedResultTypes,
		)

		p.PlanToStreamColMap = []int{1, 2, 3}
		sql.FinalizePlan(ctx, planCtx, p)

		// Log the plan diagram URL so that we don't have to rely on it being in system.job_info.
		const maxLenDiagURL = 1 << 20 // 1 MiB
		flowSpecs, cleanup := p.GenerateFlowSpecs()
		defer cleanup(flowSpecs)
		if _, diagURL, err := execinfrapb.GeneratePlanDiagramURL(
			fmt.Sprintf("changefeed: %d", jobID),
			flowSpecs,
			execinfrapb.DiagramFlags{},
		); err != nil {
			log.Warningf(ctx, "failed to generate changefeed plan diagram: %s", err)
		} else if diagURL := diagURL.String(); len(diagURL) > maxLenDiagURL {
			log.Warningf(ctx, "changefeed plan diagram length is too large to be logged: %d", len(diagURL))
		} else {
			log.Infof(ctx, "changefeed plan diagram: %s", diagURL)
		}

		return p, planCtx, nil
	}
}

// changefeedResultWriter implements the `sql.rowResultWriter` that sends
// the received rows back over the given channel.
type changefeedResultWriter struct {
	rowsCh       chan<- tree.Datums
	rowsAffected int
	err          error
	cancel       context.CancelFunc
}

func makeChangefeedResultWriter(
	rowsCh chan<- tree.Datums, cancel context.CancelFunc,
) *changefeedResultWriter {
	return &changefeedResultWriter{rowsCh: rowsCh, cancel: cancel}
}

func (w *changefeedResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	// Copy the row because it's not guaranteed to exist after this function
	// returns.
	row = append(tree.Datums(nil), row...)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.rowsCh <- row:
		return nil
	}
}
func (w *changefeedResultWriter) SetRowsAffected(ctx context.Context, n int) {
	w.rowsAffected = n
}
func (w *changefeedResultWriter) SetError(err error) {
	w.err = err
	switch {
	case errors.Is(err, changefeedbase.ErrNodeDraining):
		// Let drain signal proceed w/out cancellation.
		// We want to make sure change frontier processor gets a chance
		// to send out cancellation to the aggregator so that everything
		// transitions to "drain metadata" stage.
	default:
		w.cancel()
	}
}

func (w *changefeedResultWriter) Err() error {
	return w.err
}

// TODO(#120427): improve this to be more useful.
var rebalanceThreshold = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"changefeed.balance_range_distribution.sensitivity",
	"rebalance if the number of ranges on a node exceeds the average by this fraction",
	0.05,
	settings.PositiveFloat,
)

type rangeIterator interface {
	Desc() *roachpb.RangeDescriptor
	NeedAnother(rs roachpb.RSpan) bool
	Valid() bool
	Error() error
	Next(ctx context.Context)
	Seek(ctx context.Context, key roachpb.RKey, scanDir kvcoord.ScanDirection)
}

// rebalancingPartition is a container used to store a partition undergoing
// rebalancing.
type rebalancingPartition struct {
	// These fields store the current number of ranges and spans in this partition.
	// They are initialized corresponding to the sql.SpanPartition partition below
	// and mutated during rebalancing.
	numRanges int
	group     roachpb.SpanGroup

	// The original span partition corresponding to this bucket and its
	// index in the original []sql.SpanPartition.
	part sql.SpanPartition
	pIdx int
}

// Setting expensiveReblanceChecksEnabled = true will cause re-balancing to
// panic if the output list of partitions does not cover the same keys as the
// input list of partitions.
var expensiveReblanceChecksEnabled = buildutil.CrdbTestBuild || envutil.EnvOrDefaultBool(
	"COCKROACH_CHANGEFEED_TESTING_REBALANCING_CHECKS", false)

func rebalanceSpanPartitions(
	ctx context.Context, ri rangeIterator, sensitivity float64, partitions []sql.SpanPartition,
) ([]sql.SpanPartition, error) {
	if len(partitions) <= 1 {
		return partitions, nil
	}

	// Create partition builder structs for the partitions array above.
	var builders = make([]rebalancingPartition, len(partitions))
	var totalRanges int
	for i, p := range partitions {
		builders[i].part = p
		builders[i].pIdx = i
		nRanges, ok := p.NumRanges()
		// We cannot rebalance if we're missing range information.
		if !ok {
			log.Warning(ctx, "skipping rebalance due to missing range info")
			return partitions, nil
		}
		builders[i].numRanges = nRanges
		totalRanges += nRanges
		builders[i].group.Add(p.Spans...)
	}

	// Sort descending based on the number of ranges.
	sort.Slice(builders, func(i, j int) bool {
		return builders[i].numRanges > builders[j].numRanges
	})

	targetRanges := int(math.Ceil((1 + sensitivity) * float64(totalRanges) / float64(len(partitions))))
	to := len(builders) - 1
	from := 0

	// In each iteration of the outer loop, check if `from` has too many ranges.
	// If so, move them to other partitions which need more ranges
	// starting from `to` and moving down. Otherwise, increment `from` and check
	// again.
	for ; from < to && builders[from].numRanges > targetRanges; from++ {
		// numToMove is the number of ranges which need to be moved out of `from`
		// to other partitions.
		numToMove := builders[from].numRanges - targetRanges
		count := 0
		needMore := func() bool {
			return count < numToMove
		}
		// Iterate over all the spans in `from`.
		for spanIdx := 0; from < to && needMore() && spanIdx < len(builders[from].part.Spans); spanIdx++ {
			sp := builders[from].part.Spans[spanIdx]
			rSpan, err := keys.SpanAddr(sp)
			if err != nil {
				return nil, err
			}
			// Iterate over the ranges in the current span.
			for ri.Seek(ctx, rSpan.Key, kvcoord.Ascending); from < to && needMore(); ri.Next(ctx) {
				// Error check.
				if !ri.Valid() {
					return nil, ri.Error()
				}

				// Move one range from `from` to `to`.
				count += 1
				builders[from].numRanges -= 1
				builders[to].numRanges += 1
				// If the range boundaries are outside the original span, trim
				// the range.
				startKey := ri.Desc().StartKey
				if startKey.Compare(rSpan.Key) == -1 {
					startKey = rSpan.Key
				}
				endKey := ri.Desc().EndKey
				if endKey.Compare(rSpan.EndKey) == 1 {
					endKey = rSpan.EndKey
				}
				diff := roachpb.Span{
					Key: startKey.AsRawKey(), EndKey: endKey.AsRawKey(),
				}
				builders[from].group.Sub(diff)
				builders[to].group.Add(diff)

				// Since we moved a range, `to` may have enough ranges.
				// Decrement `to` until we find a new partition which needs more
				// ranges.
				for from < to && builders[to].numRanges >= targetRanges {
					to--
				}
				// No more ranges in this span.
				if !ri.NeedAnother(rSpan) {
					break
				}
			}
		}
	}

	// Overwrite the original partitions slice with the balanced partitions.
	for _, b := range builders {
		partitions[b.pIdx] = sql.MakeSpanPartitionWithRangeCount(
			b.part.SQLInstanceID, b.group.Slice(), b.numRanges)
	}

	if err := verifyPartitionsIfExpensiveChecksEnabled(builders, partitions, targetRanges); err != nil {
		return nil, err
	}

	return partitions, nil
}

// verifyPartitionsIfExpensiveChecksEnabled panics if the output partitions
// cover a different set of keys than the input partitions.
func verifyPartitionsIfExpensiveChecksEnabled(
	builderWithInputSpans []rebalancingPartition,
	outputPartitions []sql.SpanPartition,
	targetRanges int,
) error {
	if !expensiveReblanceChecksEnabled {
		return nil
	}
	var originalSpansG roachpb.SpanGroup
	var originalSpansArr []roachpb.Span
	var newSpansG roachpb.SpanGroup
	var newSpansArr []roachpb.Span
	for _, b := range builderWithInputSpans {
		originalSpansG.Add(b.part.Spans...)
		originalSpansArr = append(originalSpansArr, b.part.Spans...)
	}
	for _, p := range outputPartitions {
		if numRanges, ok := p.NumRanges(); !ok {
			return changefeedbase.WithTerminalError(
				errors.Newf("partition missing number of ranges info, partition: %v, partitions: %v", p, outputPartitions))
		} else if numRanges > targetRanges {
			return changefeedbase.WithTerminalError(
				errors.Newf("found partition with too many ranges, target: %d, partition: %v, partitions: %v",
					targetRanges, p, outputPartitions))
		}

		newSpansG.Add(p.Spans...)
		newSpansArr = append(newSpansArr, p.Spans...)
	}
	// If the original spans enclose the new spans and the new spans enclose the original spans,
	// then the two groups must cover exactly the same keys.
	if !originalSpansG.Encloses(newSpansArr...) || !newSpansG.Encloses(originalSpansArr...) {
		return changefeedbase.WithTerminalError(errors.Newf("incorrect rebalance. input spans: %v, output spans: %v",
			originalSpansArr, newSpansArr))
	}
	return nil
}
