// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/physical"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

var (
	// jobCheckpointFrequency controls the frequency of frontier
	// checkpoints into the jobs table.
	jobCheckpointFrequency = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"logical_replication.consumer.job_checkpoint_frequency",
		"controls the frequency with which the job updates their progress; if 0, disabled",
		10*time.Second,
		settings.NonNegativeDuration)

	// heartbeatFrequency controls frequency the stream replication
	// destination cluster sends heartbeat to the source cluster to keep
	// the stream alive.
	heartbeatFrequency = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"logical_replication.consumer.heartbeat_frequency",
		"controls frequency the stream replication destination cluster sends heartbeat "+
			"to the source cluster to keep the stream alive",
		30*time.Second,
		settings.NonNegativeDuration,
	)
)

type logicalReplicationResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*logicalReplicationResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (r *logicalReplicationResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	return r.handleResumeError(ctx, jobExecCtx, r.ingestWithRetries(ctx, jobExecCtx))
}

// The ingestion job should never fail, only pause, as progress should never be lost.
func (r *logicalReplicationResumer) handleResumeError(
	ctx context.Context, execCtx sql.JobExecContext, err error,
) error {
	if err == nil {
		return nil
	}
	r.updateRunningStatus(ctx, redact.Sprintf("pausing after error: %s", err.Error()))
	return jobs.MarkPauseRequestError(err)
}

func (r *logicalReplicationResumer) updateRunningStatus(
	ctx context.Context, runningStatus redact.RedactableString,
) {
	log.Infof(ctx, "%s", runningStatus)
	err := r.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		md.Progress.RunningStatus = string(runningStatus.Redact())
		ju.UpdateProgress(md.Progress)
		return nil
	})
	if err != nil {
		log.Warningf(ctx, "error when updating job running status: %s", err)
	}
}

func (r *logicalReplicationResumer) ingest(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {

	var (
		execCfg        = jobExecCtx.ExecCfg()
		distSQLPlanner = jobExecCtx.DistSQLPlanner()
		evalCtx        = jobExecCtx.ExtendedEvalContext()

		progress = r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = r.job.Details().(jobspb.LogicalReplicationDetails)

		streamID              = payload.StreamID
		jobID                 = r.job.ID()
		replicatedTimeAtStart = progress.ReplicatedTime
	)

	client, err := streamclient.GetFirstActiveClient(ctx,
		append([]string{payload.SourceClusterConnStr}, progress.StreamAddresses...),
		execCfg.InternalDB,
		streamclient.WithStreamID(streampb.StreamID(streamID)),
		streamclient.WithLogical(),
	)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close(ctx) }()

	asOf := replicatedTimeAtStart
	if asOf.IsEmpty() {
		asOf = payload.ReplicationStartTime
	}
	req := streampb.LogicalReplicationPlanRequest{
		PlanAsOf: asOf,
	}
	for _, pair := range payload.ReplicationPairs {
		req.TableIDs = append(req.TableIDs, pair.SrcDescriptorID)
	}

	planner := makeLogicalReplicationPlanner(jobExecCtx, r.job, client)
	initialPlan, initialPlanCtx, planInfo, err := planner.generateInitialPlan(ctx, distSQLPlanner)
	if err != nil {
		return err
	}
	if err := r.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		ldrProg := md.Progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		ldrProg.StreamAddresses = planInfo.streamAddress
		ju.UpdateProgress(md.Progress)
		return nil
	}); err != nil {
		return err
	}

	// TODO(azhu): add a flag to avoid recreating dlq tables during replanning
	dlqClient := InitDeadLetterQueueClient(execCfg.InternalDB.Executor(), planInfo.destTableBySrcID)
	if err := dlqClient.Create(ctx); err != nil {
		return errors.Wrap(err, "failed to create dead letter queue")
	}

	frontier, err := span.MakeFrontierAt(replicatedTimeAtStart, planInfo.sourceSpans...)
	if err != nil {
		return err
	}

	for _, resolvedSpan := range progress.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return err
		}
	}

	replanOracle := sql.ReplanOnCustomFunc(
		getNodes,
		func() float64 {
			return crosscluster.LogicalReplanThreshold.Get(execCfg.SV())
		},
	)

	replanner, stopReplanner := sql.PhysicalPlanChangeChecker(ctx,
		initialPlan,
		planner.generatePlan,
		jobExecCtx,
		replanOracle,
		func() time.Duration { return crosscluster.LogicalReplanFrequency.Get(execCfg.SV()) },
	)
	metrics := execCfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

	// Store only the original plan diagram
	jobsprofiler.StorePlanDiagram(ctx,
		execCfg.DistSQLSrv.Stopper,
		initialPlan,
		execCfg.InternalDB,
		jobID)

	heartbeatSender := streamclient.NewHeartbeatSender(ctx, client, streampb.StreamID(streamID),
		func() time.Duration {
			return heartbeatFrequency.Get(&execCfg.Settings.SV)
		})
	defer func() {
		_ = heartbeatSender.Stop()
		stopReplanner()
	}()

	execPlan := func(ctx context.Context) error {

		metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
			log.VInfof(ctx, 2, "received producer meta: %v", meta)
			return nil
		}
		rh := rowHandler{
			replicatedTimeAtStart: replicatedTimeAtStart,
			frontier:              frontier,
			metrics:               metrics,
			settings:              &execCfg.Settings.SV,
			job:                   r.job,
			frontierUpdates:       heartbeatSender.FrontierUpdates,
		}
		rowResultWriter := sql.NewCallbackResultWriter(rh.handleRow)
		distSQLReceiver := sql.MakeDistSQLReceiver(
			ctx,
			sql.NewMetadataCallbackWriter(rowResultWriter, metaFn),
			tree.Rows,
			execCfg.RangeDescriptorCache,
			nil, /* txn */
			nil, /* clockUpdater */
			evalCtx.Tracing,
		)
		defer distSQLReceiver.Release()
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		distSQLPlanner.Run(
			ctx,
			initialPlanCtx,
			nil, /* txn */
			initialPlan,
			distSQLReceiver,
			&evalCtxCopy,
			nil, /* finishedSetupFn */
		)

		return rowResultWriter.Err()
	}

	startHeartbeat := func(ctx context.Context) error {
		heartbeatSender.Start(ctx, timeutil.DefaultTimeSource{})
		err := heartbeatSender.Wait()
		return err
	}

	err = ctxgroup.GoAndWait(ctx, execPlan, replanner, startHeartbeat)
	if errors.Is(err, sql.ErrPlanChanged) {
		metrics.ReplanCount.Inc(1)
	}
	return err
}

func getNodes(plan *sql.PhysicalPlan) (src, dst map[string]struct{}, nodeCount int) {
	dst = make(map[string]struct{})
	src = make(map[string]struct{})
	count := 0
	for _, proc := range plan.Processors {
		if proc.Spec.Core.LogicalReplicationWriter == nil {
			// Skip other processors in the plan (like the Frontier processor).
			continue
		}
		if _, ok := dst[proc.SQLInstanceID.String()]; !ok {
			dst[proc.SQLInstanceID.String()] = struct{}{}
			count += 1
		}
		if _, ok := src[proc.Spec.Core.LogicalReplicationWriter.PartitionSpec.SrcInstanceID.String()]; !ok {
			src[proc.Spec.Core.LogicalReplicationWriter.PartitionSpec.SrcInstanceID.String()] = struct{}{}
			count += 1
		}
	}
	return src, dst, count
}

// logicalReplicationPlanner generates a physical plan for logical replication.
// An initial plan is generated during job startup and the replanner will
// periodically call generatePlan to recalculate the best plan. If the newly
// generated plan differs significantly from the initial plan, the entire
// distSQL flow is shut down and a new initial plan will be created.
type logicalReplicationPlanner struct {
	job        *jobs.Job
	jobExecCtx sql.JobExecContext
	client     streamclient.Client
}

type logicalReplicationPlanInfo struct {
	sourceSpans      []roachpb.Span
	streamAddress    []string
	destTableBySrcID map[descpb.ID]dstTableMetadata
}

func makeLogicalReplicationPlanner(
	jobExecCtx sql.JobExecContext, job *jobs.Job, client streamclient.Client,
) logicalReplicationPlanner {
	return logicalReplicationPlanner{
		job:        job,
		jobExecCtx: jobExecCtx,
		client:     client,
	}
}

// generateInitialPlan generates a plan along with the information required to
// initialize the job.
func (p *logicalReplicationPlanner) generateInitialPlan(
	ctx context.Context, dsp *sql.DistSQLPlanner,
) (*sql.PhysicalPlan, *sql.PlanningCtx, logicalReplicationPlanInfo, error) {
	return p.generatePlanImpl(ctx, dsp)
}

func (p *logicalReplicationPlanner) generatePlan(
	ctx context.Context, dsp *sql.DistSQLPlanner,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	plan, planCtx, _, err := p.generatePlanImpl(ctx, dsp)
	return plan, planCtx, err
}

func (p *logicalReplicationPlanner) generatePlanImpl(
	ctx context.Context, dsp *sql.DistSQLPlanner,
) (*sql.PhysicalPlan, *sql.PlanningCtx, logicalReplicationPlanInfo, error) {
	var (
		execCfg  = p.jobExecCtx.ExecCfg()
		evalCtx  = p.jobExecCtx.ExtendedEvalContext()
		progress = p.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = p.job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails
		info     = logicalReplicationPlanInfo{
			destTableBySrcID: make(map[descpb.ID]dstTableMetadata),
		}
	)
	asOf := progress.ReplicatedTime
	if asOf.IsEmpty() {
		asOf = payload.ReplicationStartTime
	}
	req := streampb.LogicalReplicationPlanRequest{
		PlanAsOf: asOf,
	}
	for _, pair := range payload.ReplicationPairs {
		req.TableIDs = append(req.TableIDs, pair.SrcDescriptorID)
	}

	plan, err := p.client.PlanLogicalReplication(ctx, req)
	if err != nil {
		return nil, nil, info, err
	}
	info.sourceSpans = plan.SourceSpans
	info.streamAddress = plan.Topology.StreamAddresses()

	var defaultFnOID oid.Oid
	if defaultFnID := payload.DefaultConflictResolution.FunctionId; defaultFnID != 0 {
		defaultFnOID = catid.FuncIDToOID(catid.DescID(defaultFnID))
	}

	crossClusterResolver := crosscluster.MakeCrossClusterTypeResolver(plan.SourceTypes)
	tableMetadataByDestID := make(map[int32]execinfrapb.TableReplicationMetadata)
	if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, descriptors *descs.Collection) error {
		for _, pair := range payload.ReplicationPairs {
			srcTableDesc := plan.DescriptorMap[pair.SrcDescriptorID]
			cpy := tabledesc.NewBuilder(&srcTableDesc).BuildCreatedMutableTable()
			if err := typedesc.HydrateTypesInDescriptor(ctx, cpy, crossClusterResolver); err != nil {
				return err
			}
			srcTableDesc = *cpy.TableDesc()

			// Look up fully qualified destination table name
			dstTableDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, descpb.ID(pair.DstDescriptorID))
			if err != nil {
				return errors.Wrapf(err, "failed to look up table descriptor %d", pair.DstDescriptorID)
			}
			dbDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dstTableDesc.GetParentID())
			if err != nil {
				return errors.Wrapf(err, "failed to look up database descriptor for table %d", pair.DstDescriptorID)
			}
			scDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, dstTableDesc.GetParentSchemaID())
			if err != nil {
				return errors.Wrapf(err, "failed to look up schema descriptor for table %d", pair.DstDescriptorID)
			}

			var fnOID oid.Oid
			if pair.DstFunctionID != 0 {
				fnOID = catid.FuncIDToOID(catid.DescID(pair.DstFunctionID))
			} else if defaultFnOID != 0 {
				fnOID = defaultFnOID
			}

			tableMetadataByDestID[pair.DstDescriptorID] = execinfrapb.TableReplicationMetadata{
				SourceDescriptor:              srcTableDesc,
				DestinationParentDatabaseName: dbDesc.GetName(),
				DestinationParentSchemaName:   scDesc.GetName(),
				DestinationTableName:          dstTableDesc.GetName(),
				DestinationFunctionOID:        uint32(fnOID),
			}
			info.destTableBySrcID[descpb.ID(pair.SrcDescriptorID)] = dstTableMetadata{
				database: dbDesc.GetName(),
				schema:   scDesc.GetName(),
				table:    dstTableDesc.GetName(),
				tableID:  descpb.ID(pair.DstDescriptorID),
			}
		}
		return nil
	}); err != nil {
		return nil, nil, info, err
	}

	planCtx, nodes, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCfg)
	if err != nil {
		return nil, nil, info, err
	}
	destNodeLocalities, err := physical.GetDestNodeLocalities(ctx, dsp, nodes)
	if err != nil {
		return nil, nil, info, err
	}

	specs, err := constructLogicalReplicationWriterSpecs(ctx,
		crosscluster.StreamAddress(payload.SourceClusterConnStr),
		plan.Topology,
		destNodeLocalities,
		payload.ReplicationStartTime,
		progress.ReplicatedTime,
		progress.Checkpoint,
		tableMetadataByDestID,
		p.job.ID(),
		streampb.StreamID(payload.StreamID),
		payload.Discard,
		payload.Mode,
		payload.MetricsLabel,
	)
	if err != nil {
		return nil, nil, info, err
	}

	// Setup a one-stage plan with one proc per input spec.
	//
	// TODO(ssd): We should add a frontier processor like we have
	// for PCR. I still think that the job update should happen in
	// the resumer itself so that we can assign the frontier
	// processor to a random node.
	//
	// TODO(ssd): Now that we have more than one processor per
	// node, we might actually want a tree of frontier processors
	// spread out CPU load.
	processorCorePlacements := make([]physicalplan.ProcessorCorePlacement, 0, len(plan.Topology.Partitions))
	for nodeID, parts := range specs {
		for _, part := range parts {
			sp := part
			processorCorePlacements = append(processorCorePlacements, physicalplan.ProcessorCorePlacement{
				SQLInstanceID: nodeID,
				Core: execinfrapb.ProcessorCoreUnion{
					LogicalReplicationWriter: &sp,
				},
			})
		}
	}

	physicalPlan := planCtx.NewPhysicalPlan()
	physicalPlan.AddNoInputStage(
		processorCorePlacements,
		execinfrapb.PostProcessSpec{},
		logicalReplicationWriterResultType,
		execinfrapb.Ordering{},
	)
	physicalPlan.PlanToStreamColMap = []int{0}
	sql.FinalizePlan(ctx, planCtx, physicalPlan)

	return physicalPlan, planCtx, info, nil
}

// rowHandler is responsible for handling checkpoints sent by logical
// replication writer processors.
type rowHandler struct {
	replicatedTimeAtStart hlc.Timestamp
	frontier              span.Frontier
	metrics               *Metrics
	settings              *settings.Values
	job                   *jobs.Job
	frontierUpdates       chan hlc.Timestamp

	lastPartitionUpdate time.Time
}

func (rh *rowHandler) handleRow(ctx context.Context, row tree.Datums) error {
	raw, ok := row[0].(*tree.DBytes)
	if !ok {
		return errors.AssertionFailedf(`unexpected datum type %T`, row[0])
	}
	var resolvedSpans jobspb.ResolvedSpans
	if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved timestamp: %x`, raw)
	}

	for _, sp := range resolvedSpans.ResolvedSpans {
		if _, err := rh.frontier.Forward(sp.Span, sp.Timestamp); err != nil {
			return err
		}
	}

	updateFreq := jobCheckpointFrequency.Get(rh.settings)
	if updateFreq == 0 || timeutil.Since(rh.lastPartitionUpdate) < updateFreq {
		return nil
	}

	frontierResolvedSpans := make([]jobspb.ResolvedSpan, 0)
	rh.frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		frontierResolvedSpans = append(frontierResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})
	replicatedTime := rh.frontier.Frontier()

	rh.lastPartitionUpdate = timeutil.Now()
	log.VInfof(ctx, 2, "persisting replicated time of %s", replicatedTime.GoTime())
	if err := rh.job.NoTxn().Update(ctx,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}
			progress := md.Progress
			prog := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
			prog.Checkpoint.ResolvedSpans = frontierResolvedSpans
			if rh.replicatedTimeAtStart.Less(replicatedTime) {
				prog.ReplicatedTime = replicatedTime
				// The HighWater is for informational purposes
				// only.
				progress.Progress = &jobspb.Progress_HighWater{
					HighWater: &replicatedTime,
				}
			}
			progress.RunningStatus = fmt.Sprintf("logical replication running: %s", replicatedTime.GoTime())
			ju.UpdateProgress(progress)
			if md.RunStats != nil && md.RunStats.NumRuns > 1 {
				ju.UpdateRunStats(1, md.RunStats.LastRun)
			}
			if l := rh.job.Details().(jobspb.LogicalReplicationDetails).MetricsLabel; l != "" {
				rh.metrics.LabeledReplicatedTime.Update(map[string]string{"label": l}, replicatedTime.GoTime().Unix())
			}
			return nil
		}); err != nil {
		return err
	}
	select {
	case rh.frontierUpdates <- replicatedTime:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (r *logicalReplicationResumer) ingestWithRetries(
	ctx context.Context, execCtx sql.JobExecContext,
) error {
	ingestionJob := r.job
	ro := getRetryPolicy(execCtx.ExecCfg().StreamingTestingKnobs)
	var err error
	var lastReplicatedTime hlc.Timestamp
	for retrier := retry.Start(ro); retrier.Next(); {
		err = r.ingest(ctx, execCtx)
		if err == nil {
			break
		}
		// By default, all errors are retryable unless it's marked as
		// permanent job error in which case we pause the job.
		// We also stop the job when this is a context cancellation error
		// as requested pause or cancel will trigger a context cancellation.
		if jobs.IsPermanentJobError(err) || ctx.Err() != nil {
			break
		}

		log.Infof(ctx, "hit retryable error %s", err)
		newReplicatedTime := loadOnlineReplicatedTime(ctx, execCtx.ExecCfg().InternalDB, ingestionJob)
		if lastReplicatedTime.Less(newReplicatedTime) {
			retrier.Reset()
			lastReplicatedTime = newReplicatedTime
		}
		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.AfterRetryIteration != nil {
			knobs.AfterRetryIteration(err)
		}
	}
	return err
}

func loadOnlineReplicatedTime(
	ctx context.Context, db isql.DB, ingestionJob *jobs.Job,
) hlc.Timestamp {
	// TODO(ssd): Isn't this load redundant? The Update API for
	// the job also updates the local copy of the job with the
	// latest progress.
	progress, err := jobs.LoadJobProgress(ctx, db, ingestionJob.ID())
	if err != nil {
		log.Warningf(ctx, "error loading job progress: %s", err)
		return hlc.Timestamp{}
	}
	if progress == nil {
		log.Warningf(ctx, "no job progress yet: %s", err)
		return hlc.Timestamp{}
	}
	return progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
}

// OnFailOrCancel implements jobs.Resumer interface
func (r *logicalReplicationResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	execCfg := execCtx.(sql.JobExecContext).ExecCfg()

	// Remove the LDR job ID from the destination table descriptors.
	details := r.job.Details().(jobspb.LogicalReplicationDetails)
	if err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		b := txn.KV().NewBatch()
		for _, repPair := range details.ReplicationPairs {
			td, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, descpb.ID(repPair.DstDescriptorID))
			if err != nil {
				return err
			}
			td.LDRJobIDs = slices.DeleteFunc(td.LDRJobIDs, func(thisID catpb.JobID) bool {
				return thisID == r.job.ID()
			})
			if err := txn.Descriptors().WriteDescToBatch(ctx, true /* kvTrace */, td, b); err != nil {
				return err
			}
		}
		if err := txn.KV().Run(ctx, b); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	r.completeProducerJob(ctx, execCfg.InternalDB)
	return nil
}

// CollectProfile implements jobs.Resumer interface
func (r *logicalReplicationResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func (r *logicalReplicationResumer) completeProducerJob(
	ctx context.Context, internalDB *sql.InternalDB,
) {
	var (
		progress = r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = r.job.Details().(jobspb.LogicalReplicationDetails)
	)

	streamID := streampb.StreamID(payload.StreamID)
	log.Infof(ctx, "attempting to update producer job %d", streamID)
	if err := timeutil.RunWithTimeout(ctx, "complete producer job", 30*time.Second,
		func(ctx context.Context) error {
			client, err := streamclient.GetFirstActiveClient(ctx,
				append([]string{payload.SourceClusterConnStr}, progress.StreamAddresses...),
				internalDB,
				streamclient.WithStreamID(streamID),
				streamclient.WithLogical(),
			)
			if err != nil {
				return err
			}
			defer closeAndLog(ctx, client)
			return client.Complete(ctx, streamID, false /* successfulIngestion */)
		},
	); err != nil {
		log.Warningf(ctx, "error completing the source cluster producer job %d: %s", streamID, err.Error())
	}
}

func closeAndLog(ctx context.Context, d streamclient.Dialer) {
	if err := d.Close(ctx); err != nil {
		log.Warningf(ctx, "error closing stream client: %s", err.Error())
	}
}

func getRetryPolicy(knobs *sql.StreamingTestingKnobs) retry.Options {
	if knobs != nil && knobs.DistSQLRetryPolicy != nil {
		return *knobs.DistSQLRetryPolicy
	}

	// This feature is potentially running over WAN network links / the public
	// internet, so we want to recover on our own from hiccups that could last a
	// few seconds or even minutes. Thus we allow a relatively long MaxBackoff and
	// number of retries that should cause us to retry for several minutes.
	return retry.Options{
		InitialBackoff: 250 * time.Millisecond,
		MaxBackoff:     1 * time.Minute,
		MaxRetries:     30,
	}
}

func init() {
	m := MakeMetrics(base.DefaultHistogramWindowInterval())
	jobs.RegisterConstructor(
		jobspb.TypeLogicalReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &logicalReplicationResumer{
				job: job,
			}
		},
		jobs.WithJobMetrics(m),
		jobs.WithResolvedMetric(m.(*Metrics).ReplicatedTimeSeconds),
		jobs.UsesTenantCostControl,
	)
}
