// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

func init() {
	repstream.CreateRemoteProduceJobForLogicalReplicationHook = createRemoteProduceJobForLogicalReplication
}

func createRemoteProduceJobForLogicalReplication(
	ctx context.Context, srcAddr string, tableNames []string,
) (*streampb.ReplicationProducerSpec, error) {
	// TODO(ssd): Copy over our GetFirstActiveClient logic
	// so that we can connect to any node that we've seen
	// in a previous Topology.
	streamAddr, err := url.Parse(srcAddr)
	if err != nil {
		return nil, err
	}

	client, err := streamclient.NewPartitionedStreamClient(ctx, streamAddr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close(ctx) }()

	spec, err := client.CreateForTables(ctx, &streampb.ReplicationProducerRequest{
		TableNames: tableNames,
	})
	if err != nil {
		return nil, err
	}

	return spec, err
}

// Resume is part of the jobs.Resumer interface.
func (r *logicalReplicationResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	return r.handleResumeError(ctx, jobExecCtx, r.ingestWithRetries(ctx, jobExecCtx))
}

// The ingestion job should never fail, only pause, as progress should never be lost.
func (r *logicalReplicationResumer) handleResumeError(
	ctx context.Context, execCtx sql.JobExecContext, err error,
) error {
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

	streamAddr, err := url.Parse(payload.TargetClusterConnStr)
	if err != nil {
		return err
	}

	client, err := streamclient.NewPartitionedStreamClient(ctx, streamAddr, streamclient.WithStreamID(streampb.StreamID(streamID)))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close(ctx) }()

	req := streampb.LogicalReplicationPlanRequest{}
	for _, pair := range payload.ReplicationPairs {
		req.TableIDs = append(req.TableIDs, pair.SrcDescriptorID)
	}

	plan, err := client.PlanLogicalReplication(ctx, req)
	if err != nil {
		return err
	}

	dstToSrcDescMap := make(map[int32]descpb.TableDescriptor)
	for _, pair := range payload.ReplicationPairs {
		dstToSrcDescMap[pair.DstDescriptorID] = plan.DescriptorMap[pair.SrcDescriptorID]
	}

	frontier, err := span.MakeFrontierAt(replicatedTimeAtStart, plan.SourceSpans...)
	if err != nil {
		return err
	}
	for _, resolvedSpan := range progress.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return err
		}
	}
	planCtx, nodes, err := distSQLPlanner.SetupAllNodesPlanning(ctx, evalCtx, execCfg)
	if err != nil {
		return err
	}
	destNodeLocalities, err := streamingest.GetDestNodeLocalities(ctx, distSQLPlanner, nodes)
	if err != nil {
		return err
	}

	specs, err := constructLogicalReplicationWriterSpecs(ctx,
		streamingccl.StreamAddress(payload.TargetClusterConnStr),
		plan.Topology,
		destNodeLocalities,
		payload.ReplicationStartTime,
		progress.ReplicatedTime,
		progress.Checkpoint,
		dstToSrcDescMap,
		jobID,
		streampb.StreamID(streamID))
	if err != nil {
		return err
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

	jobsprofiler.StorePlanDiagram(ctx,
		execCfg.DistSQLSrv.Stopper,
		physicalPlan,
		execCfg.InternalDB,
		jobID)

	metrics := execCfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)
	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		log.Warningf(ctx, "received unexpected producer meta: %v", meta)
		return nil
	}
	// TODO(ssd): Replan
	heartbeatSender := streamclient.NewHeartbeatSender(ctx, client, streampb.StreamID(streamID),
		func() time.Duration {
			return heartbeatFrequency.Get(&execCfg.Settings.SV)
		})
	defer func() { _ = heartbeatSender.Stop() }()
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
		planCtx,
		nil, /* txn */
		physicalPlan,
		distSQLReceiver,
		&evalCtxCopy,
		nil, /* finishedSetupFn */
	)

	return rowResultWriter.Err()
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

	advanced := false
	for _, sp := range resolvedSpans.ResolvedSpans {
		adv, err := rh.frontier.Forward(sp.Span, sp.Timestamp)
		if err != nil {
			return err
		}
		advanced = advanced || adv
	}

	if !advanced {
		return nil
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
			return nil
		}); err != nil {
		return err
	}

	rh.metrics.ReplicatedTimeSeconds.Update(replicatedTime.GoTime().Unix())
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
func (h *logicalReplicationResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	metrics := execCfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)
	metrics.ReplicatedTimeSeconds.Update(0)
	return nil
}

// CollectProfile implements jobs.Resumer interface
func (h *logicalReplicationResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func getRetryPolicy(knobs *sql.StreamingTestingKnobs) retry.Options {
	if knobs != nil && knobs.DistSQLRetryPolicy != nil {
		return *knobs.DistSQLRetryPolicy
	}

	// This feature is potentially running over WAN network links / the public
	// internet, so we want to recover on our own from hiccups that could last a
	// few seconds or even minutes. Thus we allow a relatively long MaxBackoff and
	// number of retries that should cause us to retry for a few minutes.
	return retry.Options{MaxBackoff: 15 * time.Second, MaxRetries: 20} // 205.5s.
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeLogicalReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &logicalReplicationResumer{
				job: job,
			}
		},
		jobs.WithJobMetrics(MakeMetrics(base.DefaultHistogramWindowInterval())),
		jobs.UsesTenantCostControl,
	)
}
