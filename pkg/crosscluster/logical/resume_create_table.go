// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/physical"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/ingeststopped"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// resumeCreateTable runs the create-table workflow: offline initial scan
// followed by table publication. Once complete, the job transitions to
// steady-state ingestion. The offline scan and publish steps share a single
// retry loop. Each step is guarded by progress flags, so on retry the
// completed steps are skipped.
func (r *logicalReplicationResumer) resumeCreateTable(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	progress := r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	if progress.PublishedNewTables {
		return nil
	}

	err := r.resumeWithRetries(ctx, jobExecCtx, func() error {
		return r.runCreateTableWorkflow(ctx, jobExecCtx)
	})
	return r.handleResumeError(ctx, jobExecCtx, err)
}

// runCreateTableWorkflow runs the offline initial scan (if not already
// complete) followed by reverse stream start and table publication.
func (r *logicalReplicationResumer) runCreateTableWorkflow(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	progress := r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	if progress.ReplicatedTime.IsEmpty() {
		if err := r.runOfflineInitialScan(ctx, jobExecCtx); err != nil {
			return err
		}
	}

	if err := jobExecCtx.ExecCfg().JobRegistry.CheckPausepoint("logical_replication.create_table.after_initial_scan"); err != nil {
		return err
	}

	// After the offline scan completes, heartbeat, start the reverse stream,
	// and publish the new tables.
	client, err := r.getActiveClient(ctx, jobExecCtx.ExecCfg().InternalDB)
	if err != nil {
		return err
	}
	defer closeAndLog(ctx, client)

	if err := r.heartbeatAndCheckActive(ctx, client); err != nil {
		return err
	}

	if err := r.maybeStartReverseStream(ctx, jobExecCtx, client); err != nil {
		return err
	}

	return r.maybePublishCreatedTables(ctx, jobExecCtx)
}

// runOfflineInitialScan executes the distSQL plan for the offline initial scan
// phase of a create-table LDR job. It runs until all source data has been
// ingested (signaled by errOfflineInitialScanComplete).
func (r *logicalReplicationResumer) runOfflineInitialScan(
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

	client, err := r.getActiveClient(ctx, execCfg.InternalDB)
	if err != nil {
		return err
	}
	defer closeAndLog(ctx, client)

	if err := r.heartbeatAndCheckActive(ctx, client); err != nil {
		return err
	}

	planner := MakeLogicalReplicationPlanner(jobExecCtx, r.job, client)
	sourcePlan, err := planner.GetSourcePlan(ctx)
	if err != nil {
		return err
	}
	initialPlan, initialPlanCtx, planInfo, err := planner.planOfflineInitialScan(ctx, distSQLPlanner, sourcePlan)
	if err != nil {
		return err
	}

	uris, err := r.getClusterUris(ctx, r.job, execCfg.InternalDB)
	if err != nil {
		return err
	}
	if err := r.checkpointPartitionURIs(ctx, uris, planInfo.PartitionPgUrls); err != nil {
		return err
	}

	frontier, err := span.MakeFrontierAt(replicatedTimeAtStart, planInfo.SourceSpans...)
	if err != nil {
		return err
	}

	for _, resolvedSpan := range progress.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return err
		}
	}

	metrics := execCfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

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
	}()

	execPlan := func(ctx context.Context) error {
		rh := rowHandler{
			replicatedTimeAtStart: replicatedTimeAtStart,
			frontier:              frontier,
			metrics:               metrics,
			settings:              &execCfg.Settings.SV,
			job:                   r.job,
			frontierUpdates:       heartbeatSender.FrontierUpdates,
			rangeStats: replicationutils.NewAggregateRangeStatsCollector(
				planInfo.WriteProcessorCount,
			),
			r: r,
		}
		return rh.runPlan(ctx, jobExecCtx, initialPlan, initialPlanCtx)
	}

	startHeartbeat := func(ctx context.Context) error {
		heartbeatSender.Start(ctx, timeutil.DefaultTimeSource{})
		return heartbeatSender.Wait()
	}

	defer func() {
		if l := payload.MetricsLabel; l != "" {
			metrics.LabeledScanningRanges.Update(map[string]string{"label": l}, 0)
			metrics.LabeledCatchupRanges.Update(map[string]string{"label": l}, 0)
		}
		metrics.ScanningRanges.Update(0)
		metrics.CatchupRanges.Update(0)
	}()

	err = ctxgroup.GoAndWait(ctx, execPlan, startHeartbeat)
	if errors.Is(err, errOfflineInitialScanComplete) {
		log.Dev.Infof(ctx, "offline initial scan complete")
		return nil
	}
	return err
}

func (r *logicalReplicationResumer) maybeStartReverseStream(
	ctx context.Context, jobExecCtx sql.JobExecContext, client streamclient.Client,
) error {
	// Instantiate a local copy of progress and details as they are gated behind a mutex.
	progress := r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	details := r.job.Details().(jobspb.LogicalReplicationDetails)

	if !(details.ReverseStreamCommand != "" && progress.ReplicatedTime.IsSet() && !progress.StartedReverseStream) {
		return nil
	}

	// Begin the reverse stream at a system time before source tables have been
	// published but after they have been created during this job's planning.
	now := jobExecCtx.ExecCfg().Clock.Now()
	if err := client.ExecStatement(ctx, details.ReverseStreamCommand, "start-reverse-stream", now.AsOfSystemTime()); err != nil {
		return errors.Wrapf(err, "failed to start reverse stream")
	}

	if err := r.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		md.Progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.StartedReverseStream = true
		ju.UpdateProgress(md.Progress)
		return nil
	}); err != nil {
		return err
	}
	log.Dev.Infof(ctx, "started reverse stream")
	return nil
}

func (r *logicalReplicationResumer) maybePublishCreatedTables(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	// Instantiate a local copy of progress and details as they are gated behind a mutex.
	progress := r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	details := r.job.Details().(jobspb.LogicalReplicationDetails)

	if !(details.CreateTable && progress.ReplicatedTime.IsSet() && !progress.PublishedNewTables) {
		return nil
	}
	if details.ReverseStreamCommand != "" && !progress.StartedReverseStream {
		return errors.AssertionFailedf("attempting to publish descriptors before starting reverse stream")
	}

	if err := ingeststopped.WaitForNoIngestingNodes(ctx, jobExecCtx, r.job, maxWait); err != nil {
		return errors.Wrapf(err, "unable to verify that attempted LDR job %d had stopped offline ingesting %s", r.job.ID(), maxWait)
	}
	log.Dev.Infof(ctx, "verified no nodes still offline ingesting on behalf of job %d", r.job.ID())

	return sql.DescsTxn(ctx, jobExecCtx.ExecCfg(), func(ctx context.Context, txn isql.Txn, descCol *descs.Collection) error {
		b := txn.KV().NewBatch()
		for i := range details.IngestedExternalCatalog.Tables {
			td, err := descCol.MutableByID(txn.KV()).Table(ctx, details.IngestedExternalCatalog.Tables[i].GetID())
			if err != nil {
				return err
			}
			td.SetPublic()
			if err := descCol.WriteDescToBatch(ctx, true /* kvTrace */, td, b); err != nil {
				return err
			}
		}
		if err := txn.KV().Run(ctx, b); err != nil {
			return err
		}
		return r.job.WithTxn(txn).Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			md.Progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.PublishedNewTables = true
			ju.UpdateProgress(md.Progress)
			return nil
		})
	})
}

func (p *LogicalReplicationPlanner) planOfflineInitialScan(
	ctx context.Context, dsp *sql.DistSQLPlanner, sourcePlan streamclient.LogicalReplicationPlan,
) (*sql.PhysicalPlan, *sql.PlanningCtx, LogicalReplicationPlanInfo, error) {
	var (
		execCfg  = p.JobExecCtx.ExecCfg()
		evalCtx  = p.JobExecCtx.ExtendedEvalContext()
		progress = p.Job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = p.Job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails
		info     = LogicalReplicationPlanInfo{
			SourceSpans:     sourcePlan.SourceSpans,
			PartitionPgUrls: sourcePlan.Topology.SerializedClusterUris(),
		}
	)

	rekeys := make([]execinfrapb.TableRekey, 0, len(payload.ReplicationPairs))
	if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, descriptors *descs.Collection) error {
		for _, pair := range payload.ReplicationPairs {
			// TODO(msbutler): avoid a marshalling round trip.
			dstTableDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, descpb.ID(pair.DstDescriptorID))
			if err != nil {
				return errors.Wrapf(err, "failed to look up table descriptor %d", pair.DstDescriptorID)
			}
			newDescBytes, err := protoutil.Marshal(dstTableDesc.DescriptorProto())
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"marshalling descriptor")
			}
			rekeys = append(rekeys, execinfrapb.TableRekey{
				OldID:   uint32(pair.SrcDescriptorID),
				NewDesc: newDescBytes,
			})
		}
		return nil
	}); err != nil {
		return nil, nil, info, err
	}

	// TODO(msbutler): consider repartitioning topology

	planCtx, nodes, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCfg)
	if err != nil {
		return nil, nil, info, err
	}
	destNodeLocalities, err := physical.GetDestNodeLocalities(ctx, dsp, nodes)
	if err != nil {
		return nil, nil, info, err
	}

	uri, err := streamclient.LookupClusterUri(ctx, payload.SourceClusterConnUri, execCfg.InternalDB)
	if err != nil {
		return nil, nil, info, err
	}

	specs, err := constructOfflineInitialScanSpecs(ctx,
		uri,
		sourcePlan.Topology,
		destNodeLocalities,
		payload.ReplicationStartTime,
		progress.Checkpoint,
		p.Job.ID(),
		streampb.StreamID(payload.StreamID),
		rekeys,
		payload.MetricsLabel,
	)
	if err != nil {
		return nil, nil, info, err
	}

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, 0, len(specs))
	for instanceID := range specs {
		for _, spec := range specs[instanceID] {
			corePlacement = append(corePlacement, physicalplan.ProcessorCorePlacement{
				SQLInstanceID: instanceID,
				Core:          execinfrapb.ProcessorCoreUnion{LogicalReplicationOfflineScan: &spec},
			})
			info.WriteProcessorCount++
		}
	}

	physPlan := planCtx.NewPhysicalPlan()
	physPlan.AddNoInputStage(
		corePlacement,
		execinfrapb.PostProcessSpec{},
		logicalReplicationWriterResultType,
		execinfrapb.Ordering{},
		nil, /* finalizeLastStageCb */
	)

	physPlan.PlanToStreamColMap = []int{0}
	sql.FinalizePlan(ctx, planCtx, physPlan)
	return physPlan, planCtx, info, nil
}
