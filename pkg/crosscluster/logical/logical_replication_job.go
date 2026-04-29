// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/ingeststopped"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		10*time.Second)

	// heartbeatFrequency controls frequency the stream replication
	// destination cluster sends heartbeat to the source cluster to keep
	// the stream alive.
	heartbeatFrequency = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"logical_replication.consumer.heartbeat_frequency",
		"controls frequency the stream replication destination cluster sends heartbeat "+
			"to the source cluster to keep the stream alive",
		30*time.Second,
	)
)

var errOfflineInitialScanComplete = errors.New("spinning down offline initial scan")
var maxWait = time.Minute * 5

type logicalReplicationResumer struct {
	job *jobs.Job

	mu struct {
		syncutil.Mutex
		// perNodeAggregatorStats is a per component running aggregate of trace
		// driven AggregatorStats emitted by the processors.
		perNodeAggregatorStats bulk.ComponentAggregatorStats
	}
}

var _ jobs.Resumer = (*logicalReplicationResumer)(nil)

func (r *logicalReplicationResumer) jobUsesUDF() bool {
	payload := r.job.Details().(jobspb.LogicalReplicationDetails)

	if payload.DefaultConflictResolution.FunctionId != 0 {
		return true
	}

	for _, pair := range payload.ReplicationPairs {
		if pair.DstFunctionID != 0 {
			return true
		}
	}

	return false
}

// Resume is part of the jobs.Resumer interface.
func (r *logicalReplicationResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)

	if r.jobUsesUDF() && !crosscluster.LogicalReplicationUDFWriterEnabled.Get(&jobExecCtx.ExecCfg().Settings.SV) {
		r.updateStatusMessage(ctx, "job paused because UDF-based logical replication writer is disabled")
		return jobs.MarkPauseRequestError(errors.Newf("UDF-based logical replication writer is disabled and will be deleted in a future CockroachDB release"))
	}

	payload := r.job.Details().(jobspb.LogicalReplicationDetails)
	if payload.CreateTable {
		if err := r.resumeCreateTable(ctx, jobExecCtx); err != nil {
			return err
		}
	}

	switch {
	case payload.Mode == jobspb.LogicalReplicationDetails_Transactional:
		return r.resumeTransactionalLdr(ctx, jobExecCtx)
	default:
		return r.resumeRowLdr(ctx, jobExecCtx)
	}
}

// The ingestion job should always pause, unless the error is marked as a permanent job error.
func (r *logicalReplicationResumer) handleResumeError(
	ctx context.Context, execCtx sql.JobExecContext, err error,
) error {
	if err == nil {
		r.updateStatusMessage(ctx, "")
		return nil
	}
	if jobs.IsPermanentJobError(err) {
		r.updateStatusMessage(ctx, redact.Sprintf("permanent error: %v", err))
		return err
	}
	r.updateStatusMessage(ctx, redact.Sprintf("pausing after error: %v", err))
	return jobs.MarkPauseRequestError(err)
}

func (r *logicalReplicationResumer) updateStatusMessage(
	ctx context.Context, status redact.RedactableString,
) {
	log.Dev.Infof(ctx, "%s", status)
	//lint:ignore SA1019 TODO: migrate to job_info_storage.go API
	err := r.job.DeprecatedNoTxn().Update(ctx, func(txn isql.Txn, md jobs.DeprecatedJobMetadata, ju *jobs.DeprecatedJobUpdater) error {
		md.Progress.StatusMessage = string(status.Redact())
		ju.UpdateProgress(md.Progress)
		return nil
	})
	if err != nil {
		log.Dev.Warningf(ctx, "error when updating job running status: %s", err)
	}
}

func (r *logicalReplicationResumer) getClusterUris(
	ctx context.Context, job *jobs.Job, db *sql.InternalDB,
) ([]streamclient.ClusterUri, error) {
	var (
		progress = job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		payload  = job.Details().(jobspb.LogicalReplicationDetails)
	)

	clusterUri, err := streamclient.LookupClusterUri(ctx, payload.SourceClusterConnUri, db)
	if err != nil {
		return nil, err
	}

	uris := []streamclient.ClusterUri{clusterUri}
	for _, uri := range progress.PartitionConnUris {
		parsed, err := streamclient.ParseClusterUri(uri)
		if err != nil {
			return nil, err
		}
		uris = append(uris, parsed)
	}

	return uris, nil
}

// getActiveClient creates a stream client from the job's cluster URIs.
func (r *logicalReplicationResumer) getActiveClient(
	ctx context.Context, db *sql.InternalDB,
) (streamclient.Client, error) {
	uris, err := r.getClusterUris(ctx, r.job, db)
	if err != nil {
		return nil, err
	}
	payload := r.job.Details().(jobspb.LogicalReplicationDetails)
	return streamclient.GetFirstActiveClient(ctx, uris, db,
		streamclient.WithStreamID(streampb.StreamID(payload.StreamID)),
		streamclient.WithLogical(),
	)
}

// checkpointPartitionURIs saves the partition connection URIs from a plan
// into job progress, unless the routing mode is gateway (where addresses
// may not be in the same network).
func (r *logicalReplicationResumer) checkpointPartitionURIs(
	ctx context.Context, uris []streamclient.ClusterUri, partitionPgUrls []string,
) error {
	if uris[0].RoutingMode() == streamclient.RoutingModeGateway {
		return nil
	}
	//lint:ignore SA1019 TODO: migrate to job_info_storage.go API
	return r.job.DeprecatedNoTxn().Update(ctx, func(txn isql.Txn, md jobs.DeprecatedJobMetadata, ju *jobs.DeprecatedJobUpdater) error {
		ldrProg := md.Progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
		ldrProg.PartitionConnUris = partitionPgUrls
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// heartbeatAndCheckActive sends a heartbeat to the source cluster and returns
// an error if the stream is no longer active.
func (r *logicalReplicationResumer) heartbeatAndCheckActive(
	ctx context.Context, client streamclient.Client,
) error {
	payload := r.job.Details().(jobspb.LogicalReplicationDetails)
	progress := r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	status, err := client.Heartbeat(ctx, streampb.StreamID(payload.StreamID), progress.ReplicatedTime)
	if err != nil {
		log.Dev.Warningf(ctx, "could not heartbeat source cluster with stream id %d", payload.StreamID)
	}
	if status.StreamStatus == streampb.StreamReplicationStatus_STREAM_INACTIVE {
		return jobs.MarkAsPermanentJobError(errors.Newf("history retention job is no longer active"))
	}
	return nil
}

// resumeWithRetries wraps a resume function with retry logic that resets on
// progress advancement.
func (r *logicalReplicationResumer) resumeWithRetries(
	ctx context.Context, execCtx sql.JobExecContext, fn func() error,
) error {
	ingestionJob := r.job
	ro := getRetryPolicy(execCtx.ExecCfg().StreamingTestingKnobs)
	var err error
	var lastReplicatedTime hlc.Timestamp

	for retrier := retry.Start(ro); retrier.Next(); {
		err = fn()
		if err == nil {
			break
		}
		// By default, all errors are retryable unless it's marked as
		// permanent job error in which case we pause the job.
		// We also stop the job when this is a context cancellation error
		// as requested pause or cancel will trigger a context cancellation.
		if jobs.IsPermanentJobError(err) || jobs.IsPauseSelfError(err) || ctx.Err() != nil {
			break
		}

		log.Dev.Infof(ctx, "hit retryable error %s", err)
		newReplicatedTime := loadOnlineReplicatedTime(ctx, execCtx.ExecCfg().InternalDB, ingestionJob)
		if lastReplicatedTime.Less(newReplicatedTime) {
			retrier.Reset()
			lastReplicatedTime = newReplicatedTime
		}
		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.AfterRetryIteration != nil {
			knobs.AfterRetryIteration(err)
		}
		if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("logical_replication.after.retryable_error"); err != nil {
			return err
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
		log.Dev.Warningf(ctx, "error loading job progress: %s", err)
		return hlc.Timestamp{}
	}
	if progress == nil {
		log.Dev.Warningf(ctx, "no job progress yet: %s", err)
		return hlc.Timestamp{}
	}
	return progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
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

	rangeStats replicationutils.AggregateRangeStatsCollector

	lastPartitionUpdate time.Time

	r *logicalReplicationResumer
}

// runPlan executes the given physical plan, routing row and metadata
// results through the rowHandler's callbacks.
func (rh *rowHandler) runPlan(
	ctx context.Context,
	jobExecCtx sql.JobExecContext,
	plan *sql.PhysicalPlan,
	planCtx *sql.PlanningCtx,
) error {
	execCfg := jobExecCtx.ExecCfg()
	rowResultWriter := sql.NewCallbackResultWriter(rh.handleRow)
	distSQLReceiver := sql.MakeDistSQLReceiver(
		ctx,
		sql.NewMetadataCallbackWriter(rowResultWriter, rh.handleMeta),
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil, /* txn */
		nil, /* clockUpdater */
		jobExecCtx.ExtendedEvalContext().Tracing,
	)
	defer distSQLReceiver.Release()
	evalCtxCopy := jobExecCtx.ExtendedEvalContext().Context.Copy()
	jobExecCtx.DistSQLPlanner().Run(
		ctx, planCtx, nil /* txn */, plan,
		distSQLReceiver, evalCtxCopy, nil /* finishedSetupFn */)
	return rowResultWriter.Err()
}

func (rh *rowHandler) handleTraceAgg(agg *execinfrapb.TracingAggregatorEvents) {
	componentID := execinfrapb.ComponentID{
		FlowID:        agg.FlowID,
		SQLInstanceID: agg.SQLInstanceID,
	}
	// Update the running aggregate of the component with the latest received
	// aggregate.
	rh.r.mu.Lock()
	defer rh.r.mu.Unlock()
	rh.r.mu.perNodeAggregatorStats[componentID] = *agg
}

func (rh *rowHandler) handleMeta(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
	if meta.AggregatorEvents != nil {
		rh.handleTraceAgg(meta.AggregatorEvents)
	}

	if meta.BulkProcessorProgress == nil {
		log.Dev.VInfof(ctx, 2, "received non progress producer meta: %v", meta)
		return nil
	}

	stats, err := replicationutils.UnmarshalRangeStats(&meta.BulkProcessorProgress.ProgressDetails)
	if err != nil {
		return err
	}

	rh.rangeStats.Add(meta.BulkProcessorProgress.ProcessorID, stats)

	return nil
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
	replicatedTime := rh.frontier.Frontier()
	alwaysPersist := rh.replicatedTimeAtStart.Less(replicatedTime) && rh.replicatedTimeAtStart.IsEmpty()

	updateFreq := jobCheckpointFrequency.Get(rh.settings)
	if !alwaysPersist && (updateFreq == 0 || timeutil.Since(rh.lastPartitionUpdate) < updateFreq) {
		return nil
	}

	frontierResolvedSpans := make([]jobspb.ResolvedSpan, 0)
	for sp, ts := range rh.frontier.Entries() {
		frontierResolvedSpans = append(frontierResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
	}

	rh.lastPartitionUpdate = timeutil.Now()
	log.Dev.VInfof(ctx, 2, "persisting replicated time of %s", replicatedTime.GoTime())
	//lint:ignore SA1019 TODO: migrate to job_info_storage.go API
	if err := rh.job.DeprecatedNoTxn().Update(ctx,
		func(txn isql.Txn, md jobs.DeprecatedJobMetadata, ju *jobs.DeprecatedJobUpdater) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}
			progress := md.Progress
			prog := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
			prog.Checkpoint.ResolvedSpans = frontierResolvedSpans

			// TODO (msbutler): add ldr initial and lagging range timeseries metrics.
			aggRangeStats, fractionCompleted, status := rh.rangeStats.RollupStats()
			progress.StatusMessage = status

			if replicatedTime.IsSet() {
				prog.ReplicatedTime = replicatedTime
				// The HighWater is for informational purposes
				// only.
				progress.Progress = &jobspb.Progress_HighWater{
					HighWater: &replicatedTime,
				}
			} else if fractionCompleted > 0 && fractionCompleted < 1 {
				// If 0, the coordinator has not gotten a complete range stats update
				// from all nodes yet.
				//
				// If 1, the job is all caught up.
				progress.Progress = &jobspb.Progress_FractionCompleted{
					FractionCompleted: fractionCompleted,
				}
			}
			ju.UpdateProgress(progress)
			if l := rh.job.Details().(jobspb.LogicalReplicationDetails).MetricsLabel; l != "" {
				rh.metrics.LabeledReplicatedTime.Update(map[string]string{"label": l}, replicatedTime.GoTime().Unix())

				if aggRangeStats.RangeCount != 0 {
					rh.metrics.LabeledScanningRanges.Update(map[string]string{"label": l}, aggRangeStats.ScanningRangeCount)
					rh.metrics.LabeledCatchupRanges.Update(map[string]string{"label": l}, aggRangeStats.LaggingRangeCount)
				}
			}
			if aggRangeStats.RangeCount != 0 {
				rh.metrics.ScanningRanges.Update(aggRangeStats.ScanningRangeCount)
				rh.metrics.CatchupRanges.Update(aggRangeStats.LaggingRangeCount)
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
	if rh.job.Details().(jobspb.LogicalReplicationDetails).CreateTable && replicatedTime.IsSet() && rh.replicatedTimeAtStart.IsEmpty() {
		// NB: the frontier update for the replicated time may not get sent over to
		// the source, but this isn't a big deal-- when the distsql flow retries a
		// new heartbeat will be sent.
		return errOfflineInitialScanComplete
	}
	return nil
}

// OnFailOrCancel implements jobs.Resumer interface
func (r *logicalReplicationResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()

	// Remove the LDR job ID from the destination table descriptors.
	details := r.job.Details().(jobspb.LogicalReplicationDetails)
	progress := r.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	destTableIDs := make([]uint32, 0, len(details.ReplicationPairs))
	for _, pair := range details.ReplicationPairs {
		destTableIDs = append(destTableIDs, uint32(pair.DstDescriptorID))
	}
	if err := replicationutils.UnlockLDRTables(ctx, execCfg, destTableIDs, r.job.ID()); err != nil {
		return err
	}
	if details.CreateTable && !progress.PublishedNewTables {
		if err := ingeststopped.WaitForNoIngestingNodes(ctx, jobExecCtx, r.job, maxWait); err != nil {
			log.Dev.Errorf(ctx, "unable to verify that attempted LDR job %d had stopped offline ingesting %s: %v", r.job.ID(), maxWait, err)
		} else {
			log.Dev.Infof(ctx, "verified no nodes still offline ingesting on behalf of job %d", r.job.ID())
		}
		if err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			return externalcatalog.DropIngestedExternalCatalog(ctx, execCfg, jobExecCtx.User(), details.IngestedExternalCatalog, txn, execCfg.JobRegistry, txn.Descriptors(), fmt.Sprintf("gc for ldr job %d", r.job.ID()))
		}); err != nil {
			return err
		}
	}

	r.completeProducerJob(ctx, execCfg.InternalDB)
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *logicalReplicationResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	var aggStatsCopy bulk.ComponentAggregatorStats
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		aggStatsCopy = r.mu.perNodeAggregatorStats.DeepCopy()
	}()

	if err := bulk.FlushTracingAggregatorStats(ctx, r.job.ID(),
		p.ExecCfg().InternalDB, aggStatsCopy); err != nil {
		return errors.Wrap(err, "failed to flush aggregator stats")
	}

	return nil
}

// ForceRealSpan implements the TraceableJob interface.
func (r *logicalReplicationResumer) ForceRealSpan() bool {
	return true
}

// DumpTraceAfterRun implements the TraceableJob interface.
func (r *logicalReplicationResumer) DumpTraceAfterRun() bool {
	return false
}

func (r *logicalReplicationResumer) completeProducerJob(
	ctx context.Context, internalDB *sql.InternalDB,
) {
	var (
		payload = r.job.Details().(jobspb.LogicalReplicationDetails)
	)

	streamID := streampb.StreamID(payload.StreamID)
	log.Dev.Infof(ctx, "attempting to update producer job %d", streamID)
	if err := timeutil.RunWithTimeout(ctx, "complete producer job", 30*time.Second,
		func(ctx context.Context) error {
			uris, err := r.getClusterUris(ctx, r.job, internalDB)
			if err != nil {
				return err
			}

			client, err := streamclient.GetFirstActiveClient(ctx,
				uris,
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
		log.Dev.Warningf(ctx, "error completing the source cluster producer job %d: %s", streamID, err.Error())
	}
}

func closeAndLog(ctx context.Context, d streamclient.Client) {
	if err := d.Close(ctx); err != nil {
		log.Dev.Warningf(ctx, "error closing stream client: %s", err.Error())
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

func geURIFromLoadedJobDetails(details jobspb.Details) string {
	return details.(jobspb.LogicalReplicationDetails).SourceClusterConnUri
}

func init() {
	m := MakeMetrics(base.DefaultHistogramWindowInterval())
	jobs.RegisterConstructor(
		jobspb.TypeLogicalReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &logicalReplicationResumer{
				job: job,
				mu: struct {
					syncutil.Mutex
					perNodeAggregatorStats bulk.ComponentAggregatorStats
				}{
					perNodeAggregatorStats: make(bulk.ComponentAggregatorStats),
				},
			}
		},
		jobs.WithJobMetrics(m),
		jobs.WithResolvedMetric(m.(*Metrics).ReplicatedTimeSeconds),
		jobs.UsesTenantCostControl,
	)
}
