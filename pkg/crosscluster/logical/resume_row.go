// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/physical"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// resumeRowLdr runs the row-level LDR ingestion loop with retries.
func (r *logicalReplicationResumer) resumeRowLdr(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	return r.handleResumeError(ctx, jobExecCtx,
		r.resumeWithRetries(ctx, jobExecCtx, func() error {
			return r.ingest(ctx, jobExecCtx)
		}))
}

func (r *logicalReplicationResumer) ingest(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	var (
		execCfg        = jobExecCtx.ExecCfg()
		distSQLPlanner = jobExecCtx.DistSQLPlanner()

		progress = r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = r.job.Details().(jobspb.LogicalReplicationDetails)

		streamID              = payload.StreamID
		jobID                 = r.job.ID()
		replicatedTimeAtStart = progress.ReplicatedTime
	)

	uris, err := r.getClusterUris(ctx, r.job, execCfg.InternalDB)
	if err != nil {
		return err
	}

	client, err := r.getActiveClient(ctx, execCfg.InternalDB)
	if err != nil {
		return err
	}
	defer closeAndLog(ctx, client)

	if err := r.heartbeatAndCheckActive(ctx, client); err != nil {
		return err
	}

	planner := makeLogicalReplicationPlanner(jobExecCtx, r.job, client)
	sourcePlan, err := planner.getSourcePlan(ctx)
	if err != nil {
		return err
	}
	initialPlan, initialPlanCtx, planInfo, err := planner.planRowReplication(ctx, distSQLPlanner, sourcePlan)
	if err != nil {
		return err
	}

	if err := r.checkpointPartitionURIs(ctx, uris, planInfo.partitionPgUrls); err != nil {
		return err
	}
	// Update the local progress copy as it was just updated.
	progress = r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication

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

	confPoller := make(chan struct{})
	execPlan := func(ctx context.Context) error {
		defer close(confPoller)
		rh := rowHandler{
			replicatedTimeAtStart: replicatedTimeAtStart,
			frontier:              frontier,
			metrics:               metrics,
			settings:              &execCfg.Settings.SV,
			job:                   r.job,
			frontierUpdates:       heartbeatSender.FrontierUpdates,
			rangeStats: replicationutils.NewAggregateRangeStatsCollector(
				planInfo.writeProcessorCount,
			),
			r: r,
		}
		return rh.runPlan(ctx, jobExecCtx, initialPlan, initialPlanCtx)
	}

	startHeartbeat := func(ctx context.Context) error {
		heartbeatSender.Start(ctx, timeutil.DefaultTimeSource{})
		err := heartbeatSender.Wait()
		return err
	}

	refreshConn := replicationutils.GetAlterConnectionChecker(
		r.job.ID(),
		uris[0].Serialize(),
		geURIFromLoadedJobDetails,
		execCfg,
		confPoller,
	)

	defer func() {
		if l := payload.MetricsLabel; l != "" {
			metrics.LabeledScanningRanges.Update(map[string]string{"label": l}, 0)
			metrics.LabeledCatchupRanges.Update(map[string]string{"label": l}, 0)
		}
		metrics.ScanningRanges.Update(0)
		metrics.CatchupRanges.Update(0)
	}()

	err = ctxgroup.GoAndWait(ctx, execPlan, replanner, startHeartbeat, refreshConn)
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

func (p *logicalReplicationPlanner) planRowReplication(
	ctx context.Context, dsp *sql.DistSQLPlanner, sourcePlan streamclient.LogicalReplicationPlan,
) (*sql.PhysicalPlan, *sql.PlanningCtx, logicalReplicationPlanInfo, error) {
	var (
		execCfg  = p.jobExecCtx.ExecCfg()
		evalCtx  = p.jobExecCtx.ExtendedEvalContext()
		progress = p.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = p.job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails
		info     = logicalReplicationPlanInfo{
			sourceSpans:      sourcePlan.SourceSpans,
			partitionPgUrls:  sourcePlan.Topology.SerializedClusterUris(),
			destTableBySrcID: make(map[descpb.ID]dstTableMetadata),
		}
	)

	var defaultFnOID oid.Oid
	if defaultFnID := payload.DefaultConflictResolution.FunctionId; defaultFnID != 0 {
		defaultFnOID = catid.FuncIDToOID(catid.DescID(defaultFnID))
	}
	writer, err := getWriterType(ctx, payload.Mode, execCfg.Settings)
	if err != nil {
		return nil, nil, info, err
	}
	crossClusterResolver := crosscluster.MakeCrossClusterTypeResolver(sourcePlan.SourceTypes)
	tableMetadataByDestID := make(map[int32]execinfrapb.TableReplicationMetadata)
	if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, descriptors *descs.Collection) error {
		for _, pair := range payload.ReplicationPairs {
			srcTableDesc := sourcePlan.DescriptorMap[pair.SrcDescriptorID]
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

			if err := tabledesc.CheckLogicalReplicationCompatibility(&srcTableDesc, dstTableDesc.TableDesc(), payload.SkipSchemaCheck || payload.CreateTable, writer == sqlclustersettings.LDRWriterTypeLegacyKV); err != nil {
				return err
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

	sourceUri, err := streamclient.LookupClusterUri(ctx, payload.SourceClusterConnUri, execCfg.InternalDB)
	if err != nil {
		return nil, nil, info, err
	}

	specs, err := constructLogicalReplicationWriterSpecs(ctx,
		sourceUri,
		sourcePlan.Topology,
		destNodeLocalities,
		payload.ReplicationStartTime,
		progress.ReplicatedTime,
		progress.Checkpoint,
		tableMetadataByDestID,
		sourcePlan.SourceTypes,
		p.job.ID(),
		streampb.StreamID(payload.StreamID),
		payload.Discard,
		payload.Mode,
		payload.MetricsLabel,
		writer,
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
	processorCorePlacements := make([]physicalplan.ProcessorCorePlacement, 0, len(sourcePlan.Topology.Partitions))
	for nodeID, parts := range specs {
		for _, part := range parts {
			sp := part
			processorCorePlacements = append(processorCorePlacements, physicalplan.ProcessorCorePlacement{
				SQLInstanceID: nodeID,
				Core: execinfrapb.ProcessorCoreUnion{
					LogicalReplicationWriter: &sp,
				},
			})
			info.writeProcessorCount++
		}
	}

	physicalPlan := planCtx.NewPhysicalPlan()
	physicalPlan.AddNoInputStage(
		processorCorePlacements,
		execinfrapb.PostProcessSpec{},
		logicalReplicationWriterResultType,
		execinfrapb.Ordering{},
		nil, /* finalizeLastStageCb */
	)
	physicalPlan.PlanToStreamColMap = []int{0}
	sql.FinalizePlan(ctx, planCtx, physicalPlan)

	return physicalPlan, planCtx, info, nil
}
