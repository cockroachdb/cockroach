// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/producer"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/ingeststopped"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/revert"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	bulkutil "github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var maxIngestionProcessorShutdownWait = 5 * time.Minute

type streamIngestionResumer struct {
	job *jobs.Job

	mu struct {
		syncutil.Mutex
		// perNodeAggregatorStats is a per component running aggregate of trace
		// driven AggregatorStats pushed backed to the resumer from all the
		// processors running the backup.
		perNodeAggregatorStats bulkutil.ComponentAggregatorStats
	}
}

func getClusterUris(
	ctx context.Context, ingestionJob *jobs.Job, db descs.DB,
) ([]streamclient.ClusterUri, error) {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	sourceUri, err := streamclient.LookupClusterUri(ctx, details.SourceClusterConnUri, db)
	if err != nil {
		return nil, err
	}

	// Always use the configured URI as the the first conneciton target. It may
	// be a load balancer or an external connection.
	uris := []streamclient.ClusterUri{sourceUri}

	progress := ingestionJob.Progress()
	for _, uri := range progress.GetStreamIngest().PartitionConnUris {
		parsed, err := streamclient.ParseClusterUri(uri)
		if err != nil {
			return nil, err
		}
		uris = append(uris, parsed)
	}

	return uris, nil
}

func connectToActiveClient(
	ctx context.Context, ingestionJob *jobs.Job, db descs.DB, opts ...streamclient.Option,
) (streamclient.Client, error) {
	clusterUris, err := getClusterUris(ctx, ingestionJob, db)
	if err != nil {
		return nil, err
	}
	client, err := streamclient.GetFirstActiveClient(ctx, clusterUris, db, opts...)
	return client, errors.Wrapf(err, "ingestion job %d failed to connect to stream address or existing topology for planning", ingestionJob.ID())
}

func updateStatus(
	ctx context.Context,
	ingestionJob *jobs.Job,
	replicationStatus jobspb.ReplicationStatus,
	status redact.RedactableString,
) {
	err := ingestionJob.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		updateStatusInternal(md, ju, replicationStatus, string(status.Redact()))
		return nil
	})
	if err != nil {
		log.Warningf(ctx, "error when updating job running status: %s", err)
	} else if replicationStatus == jobspb.ReplicationError {
		log.Warningf(ctx, "%s", status)
	} else {
		log.Infof(ctx, "%s", status)
	}
}

func updateStatusInternal(
	md jobs.JobMetadata,
	ju *jobs.JobUpdater,
	replicationStatus jobspb.ReplicationStatus,
	status string,
) {
	md.Progress.GetStreamIngest().ReplicationStatus = replicationStatus
	md.Progress.StatusMessage = status
	ju.UpdateProgress(md.Progress)
}

func completeIngestion(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ingestionJob *jobs.Job,
	cutoverTimestamp hlc.Timestamp,
) error {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	log.Infof(ctx, "activating destination tenant %d", details.DestinationTenantID)
	if err := activateTenant(ctx, execCtx, details, cutoverTimestamp); err != nil {
		return err
	}

	msg := redact.Sprintf("completing the producer job %d in the source cluster",
		details.StreamID)
	updateStatus(ctx, ingestionJob, jobspb.ReplicationFailingOver, msg)
	completeProducerJob(ctx, ingestionJob, execCtx.ExecCfg().InternalDB, true)
	evalContext := &execCtx.ExtendedEvalContext().Context
	if err := startPostCutoverRetentionJob(ctx, execCtx.ExecCfg(), details, evalContext, cutoverTimestamp); err != nil {
		log.Warningf(ctx, "failed to begin post cutover retention job: %s", err.Error())
	}

	// Now that we have completed the cutover we can release the protected
	// timestamp record on the destination tenant's keyspace.
	if details.ProtectedTimestampRecordID != nil {
		if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(
			ctx context.Context, txn isql.Txn,
		) error {
			ptp := execCtx.ExecCfg().ProtectedTimestampProvider.WithTxn(txn)
			return releaseDestinationTenantProtectedTimestamp(
				ctx, ptp, *details.ProtectedTimestampRecordID,
			)
		}); err != nil {
			return err
		}
	}
	return nil
}

// completeProducerJob on the source cluster is best effort. In a real
// disaster recovery scenario, who knows what state the source cluster will be
// in; thus, we should not fail the cutover step on the consumer side if we
// cannot complete the producer job.
func completeProducerJob(
	ctx context.Context, ingestionJob *jobs.Job, internalDB *sql.InternalDB, successfulIngestion bool,
) {
	streamID := streampb.StreamID(ingestionJob.Details().(jobspb.StreamIngestionDetails).StreamID)
	if err := timeutil.RunWithTimeout(ctx, "complete producer job", 30*time.Second,
		func(ctx context.Context) error {
			client, err := connectToActiveClient(ctx, ingestionJob, internalDB,
				streamclient.WithStreamID(streamID))
			if err != nil {
				return err
			}
			defer closeAndLog(ctx, client)
			return client.Complete(ctx, streamID, successfulIngestion)
		},
	); err != nil {
		log.Warningf(ctx, `encountered error when completing the source cluster producer job %d: %s`, streamID, err.Error())
	}
}

// startPostCutoverRetentionJob begins a dummy producer job on the newly cutover
// to tenant. This producer job will lay PTS over the whole tenant, enabling a
// fast failback to the original source cluster.
func startPostCutoverRetentionJob(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details jobspb.StreamIngestionDetails,
	evalCtx *eval.Context,
	cutoverTime hlc.Timestamp,
) error {

	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		info, err := sql.GetTenantRecordByID(ctx, txn, details.DestinationTenantID, execCfg.Settings)
		if err != nil {
			return err
		}
		req := streampb.ReplicationProducerRequest{
			ReplicationStartTime: cutoverTime,
		}
		_, err = producer.StartReplicationProducerJob(ctx, evalCtx, txn, info.Name, req, true)
		return err
	})
}

func ingest(
	ctx context.Context, execCtx sql.JobExecContext, resumer *streamIngestionResumer,
) error {
	ingestionJob := resumer.job
	// Cutover should be the *first* thing checked upon resumption as it is the
	// most critical task in disaster recovery.
	cutoverTimestamp, reverted, err := maybeRevertToCutoverTimestamp(ctx, execCtx, ingestionJob)
	if err != nil {
		return err
	}
	if reverted {
		log.Infof(ctx, "job completed cutover on resume")
		return completeIngestion(ctx, execCtx, ingestionJob, cutoverTimestamp)
	}
	if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.BeforeIngestionStart != nil {
		if err := knobs.BeforeIngestionStart(ctx); err != nil {
			return err
		}
	}
	// A nil error is only possible if the job was signaled to cutover and the
	// processors shut down gracefully, i.e stopped ingesting any additional
	// events from the replication stream. At this point it is safe to revert to
	// the cutoff time to leave the cluster in a consistent state.
	if err := startDistIngestion(ctx, execCtx, resumer); err != nil {
		return err
	}

	cutoverTimestamp, err = revertToCutoverTimestamp(ctx, execCtx, ingestionJob)
	if err != nil {
		return err
	}
	return completeIngestion(ctx, execCtx, ingestionJob, cutoverTimestamp)
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

func ingestWithRetries(
	ctx context.Context, execCtx sql.JobExecContext, resumer *streamIngestionResumer,
) error {
	ingestionJob := resumer.job
	ro := getRetryPolicy(execCtx.ExecCfg().StreamingTestingKnobs)
	var (
		err                    error
		previousPersistedSpans jobspb.ResolvedSpanEntries
		currentPersistedSpans  jobspb.ResolvedSpanEntries
	)

	for r := retry.Start(ro); r.Next(); {
		err = ingest(ctx, execCtx, resumer)
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

		currentPersistedSpans = resumer.job.Progress().Details.(*jobspb.Progress_StreamIngest).StreamIngest.Checkpoint.ResolvedSpans
		if !currentPersistedSpans.Equal(previousPersistedSpans) {
			// If the previous persisted spans are different than the current, it
			// implies that further progress has been persisted.
			r.Reset()
			log.Infof(ctx, "resolved spans have advanced since last retry, resetting retry counter")
		}
		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.AfterRetryIteration != nil {
			knobs.AfterRetryIteration(err)
		}
	}
	if err != nil {
		return err
	}
	updateStatus(ctx, ingestionJob, jobspb.ReplicationFailingOver,
		"stream ingestion finished successfully")
	return nil
}

// The ingestion job should never fail, only pause, as progress should never be lost.
func (s *streamIngestionResumer) handleResumeError(
	ctx context.Context, execCtx sql.JobExecContext, err error,
) error {
	msg := redact.Sprintf("ingestion job failed (%s) but is being paused", err)
	updateStatus(ctx, s.job, jobspb.ReplicationError, msg)
	// The ingestion job is paused but the producer job will keep
	// running until it times out. Users can still resume ingestion before
	// the producer job times out.
	return jobs.MarkPauseRequestError(err)
}

// Resume is part of the jobs.Resumer interface.  Ensure that any errors
// produced here are returned as s.handleResumeError.
func (s *streamIngestionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	// Protect the destination tenant's keyspan from garbage collection.
	jobExecCtx := execCtx.(sql.JobExecContext)

	if err := jobExecCtx.ExecCfg().JobRegistry.CheckPausepoint("stream_ingestion.before_protection"); err != nil {
		return err
	}

	// If we got replicated into another tenant, bail out.
	if !jobExecCtx.ExecCfg().Codec.ForSystemTenant() {
		return errors.New("replicated job only runs in system tenant")
	}

	err := s.protectDestinationTenant(ctx, jobExecCtx)
	if err != nil {
		return s.handleResumeError(ctx, jobExecCtx, err)
	}

	if err := jobExecCtx.ExecCfg().JobRegistry.CheckPausepoint("stream_ingestion.before_ingestion"); err != nil {
		return err
	}

	// Start ingesting KVs from the replication stream.
	err = ingestWithRetries(ctx, jobExecCtx, s)
	if err != nil {
		return s.handleResumeError(ctx, jobExecCtx, err)
	}
	return nil
}

func releaseDestinationTenantProtectedTimestamp(
	ctx context.Context, ptp protectedts.Storage, ptsID uuid.UUID,
) error {
	if err := ptp.Release(ctx, ptsID); err != nil {
		if errors.Is(err, protectedts.ErrNotExists) {
			log.Warningf(ctx, "failed to release protected ts as it does not to exist: %s", err)
			err = nil
		}
		return err
	}
	return nil
}

// protectDestinationTenant writes a protected timestamp record protecting the
// destination tenant's keyspace from garbage collection. This protected
// timestamp record is updated everytime the replication job records a new
// frontier timestamp, and is released OnFailOrCancel.
//
// The method persists the ID of the protected timestamp record in the
// replication job's Payload.
func (s *streamIngestionResumer) protectDestinationTenant(
	ctx context.Context, execCtx sql.JobExecContext,
) error {
	oldDetails := s.job.Details().(jobspb.StreamIngestionDetails)

	// If we have already protected the destination tenant keyspan in a previous
	// resumption of the stream ingestion job, then there is nothing to do.
	if oldDetails.ProtectedTimestampRecordID != nil {
		return nil
	}

	execCfg := execCtx.ExecCfg()
	target := ptpb.MakeTenantsTarget([]roachpb.TenantID{oldDetails.DestinationTenantID})
	ptsID := uuid.MakeV4()

	// Note that the protected timestamps are in the context of the source cluster
	// clock, not the destination. This is because the data timestamps are also
	// decided on the source cluster. Replication start time is picked on the
	// producer job on the source cluster.
	replicationStartTime := oldDetails.ReplicationStartTime
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptp := execCfg.ProtectedTimestampProvider.WithTxn(txn)
		pts := jobsprotectedts.MakeRecord(ptsID, int64(s.job.ID()), replicationStartTime,
			nil /* deprecatedSpans */, jobsprotectedts.Jobs, target)
		if err := ptp.Protect(ctx, pts); err != nil {
			return err
		}
		return s.job.WithTxn(txn).Update(ctx, func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}

			details := md.Payload.GetStreamIngestion()
			details.ProtectedTimestampRecordID = &ptsID
			oldDetails.ProtectedTimestampRecordID = &ptsID

			ju.UpdatePayload(md.Payload)
			return nil
		})
	})
}

// revertToCutoverTimestamp attempts a cutover and errors out if one was not
// executed.
func revertToCutoverTimestamp(
	ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job,
) (hlc.Timestamp, error) {
	cutoverTimestamp, reverted, err := maybeRevertToCutoverTimestamp(ctx, execCtx, ingestionJob)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if !reverted {
		return hlc.Timestamp{}, errors.Errorf("required cutover was not completed")
	}

	return cutoverTimestamp, nil
}

func cutoverTimeIsEligibleForCutover(
	ctx context.Context, cutoverTime hlc.Timestamp, progress *jobspb.Progress,
) bool {
	if cutoverTime.IsEmpty() {
		log.Infof(ctx, "empty cutover time, no revert required")
		return false
	}

	replicatedTime := replicationutils.ReplicatedTimeFromProgress(progress)
	if replicatedTime.Less(cutoverTime) {
		log.Infof(ctx, "job with replicated time %s not yet ready to revert to cutover at %s",
			replicatedTime,
			cutoverTime.String())
		return false
	}
	return true
}

// maybeRevertToCutoverTimestamp reads the job progress for the cutover time and
// if the job has progressed passed the cutover time issues a RevertRangeRequest
// with the target time set to that cutover time, to bring the ingesting cluster
// to a consistent state.
func maybeRevertToCutoverTimestamp(
	ctx context.Context, p sql.JobExecContext, ingestionJob *jobs.Job,
) (hlc.Timestamp, bool, error) {

	ctx, span := tracing.ChildSpan(ctx, "physical.revertToCutoverTimestamp")
	defer span.Finish()

	// The update below sets the ReplicationStatus to
	// CuttingOver. Once set, the cutoverTimestamp cannot be
	// changed. We want to be sure to read the timestamp that
	// existed in the record at the point of the update rather the
	// value that may be in the job record before the update.
	var (
		shouldRevertToCutover   bool
		cutoverTimestamp        hlc.Timestamp
		originalSpanToRevert    roachpb.Span
		remainingSpansToRevert  roachpb.Spans
		replicatedTimeAtCutover hlc.Timestamp
		readerTenantID          roachpb.TenantID
	)
	if err := ingestionJob.NoTxn().Update(ctx,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			streamIngestionDetails := md.Payload.GetStreamIngestion()
			if streamIngestionDetails == nil {
				return errors.AssertionFailedf("unknown payload %v in stream ingestion job %d",
					md.Payload, ingestionJob.ID())
			}

			streamIngestionProgress := md.Progress.GetStreamIngest()
			if streamIngestionProgress == nil {
				return errors.AssertionFailedf("unknown progress %v in stream ingestion job %d",
					md.Progress, ingestionJob.ID())
			}

			cutoverTimestamp = streamIngestionProgress.CutoverTime
			replicatedTimeAtCutover = streamIngestionProgress.ReplicatedTimeAtCutover
			readerTenantID = streamIngestionDetails.ReadTenantID
			originalSpanToRevert = streamIngestionDetails.Span
			remainingSpansToRevert = streamIngestionProgress.RemainingCutoverSpans
			shouldRevertToCutover = cutoverTimeIsEligibleForCutover(ctx, cutoverTimestamp, md.Progress)

			if shouldRevertToCutover {
				updateStatusInternal(md, ju, jobspb.ReplicationFailingOver,
					fmt.Sprintf("starting to cut over to the given timestamp %s", cutoverTimestamp))
			} else {
				if streamIngestionProgress.ReplicationStatus == jobspb.ReplicationFailingOver {
					return errors.AssertionFailedf("cutover already started but cutover time %s is not eligible for cutover",
						cutoverTimestamp)
				}
			}
			return nil
		}); err != nil {
		return cutoverTimestamp, false, err
	}
	if !shouldRevertToCutover {
		return cutoverTimestamp, false, nil
	}
	// Identical cutoverTimestamp and replicatedTimeAtCutover implies that
	// CUTOVER TO LATEST command was run. Destroy reader tenant if not CUTOVER TO LATEST.
	if !cutoverTimestamp.Equal(replicatedTimeAtCutover) && readerTenantID.IsSet() {
		if err := stopTenant(ctx, p.ExecCfg(), readerTenantID); err != nil {
			return cutoverTimestamp, false, errors.Wrapf(err, "failed to stop reader tenant")
		}
	}
	if err := ingeststopped.WaitForNoIngestingNodes(ctx, p, ingestionJob, maxIngestionProcessorShutdownWait); err != nil {
		return cutoverTimestamp, false, errors.Wrapf(err, "unable to verify that attempted LDR job %d had stopped offline ingesting %s", ingestionJob.ID(), maxIngestionProcessorShutdownWait)
	}
	log.Infof(ctx, "verified no nodes still offline ingesting on behalf of job %d", ingestionJob.ID())

	log.Infof(ctx, "reverting to cutover timestamp %s", cutoverTimestamp)
	if p.ExecCfg().StreamingTestingKnobs != nil && p.ExecCfg().StreamingTestingKnobs.AfterCutoverStarted != nil {
		p.ExecCfg().StreamingTestingKnobs.AfterCutoverStarted()
	}

	minProgressUpdateInterval := 15 * time.Second
	progMetric := p.ExecCfg().JobRegistry.MetricsStruct().StreamIngest.(*Metrics).ReplicationCutoverProgress
	progUpdater, err := newCutoverProgressTracker(ctx, p, originalSpanToRevert, remainingSpansToRevert, ingestionJob,
		progMetric, minProgressUpdateInterval)
	if err != nil {
		return cutoverTimestamp, false, err
	}

	batchSize := int64(revert.RevertDefaultBatchSize)
	if p.ExecCfg().StreamingTestingKnobs != nil && p.ExecCfg().StreamingTestingKnobs.OverrideRevertRangeBatchSize != 0 {
		batchSize = p.ExecCfg().StreamingTestingKnobs.OverrideRevertRangeBatchSize
	}
	// On cutover, replication has stopped so therefore should set replicated time to 0
	p.ExecCfg().JobRegistry.MetricsStruct().StreamIngest.(*Metrics).ReplicatedTimeSeconds.Update(0)
	if err := revert.RevertSpansFanout(ctx,
		p.ExecCfg().DB,
		p,
		remainingSpansToRevert,
		cutoverTimestamp,
		// TODO(ssd): It should be safe for us to ignore the
		// GC threshold. Why aren't we?
		false, /* ignoreGCThreshold */
		batchSize,
		progUpdater.onCompletedCallback); err != nil {
		return cutoverTimestamp, false, err
	}

	return cutoverTimestamp, true, nil
}

func activateTenant(
	ctx context.Context,
	execCtx sql.JobExecContext,
	details jobspb.StreamIngestionDetails,
	cutoverTimestamp hlc.Timestamp,
) error {
	execCfg := execCtx.ExecCfg()

	return execCfg.InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		info, err := sql.GetTenantRecordByID(ctx, txn, details.DestinationTenantID, execCfg.Settings)
		if err != nil {
			return err
		}

		info.DataState = mtinfopb.DataStateReady
		info.PhysicalReplicationConsumerJobID = 0
		info.PreviousSourceTenant = &mtinfopb.PreviousSourceTenant{
			TenantID:         details.SourceTenantID,
			ClusterID:        details.SourceClusterID,
			CutoverTimestamp: cutoverTimestamp,
		}

		return sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, info)
	})
}

func stopTenant(ctx context.Context, execCfg *sql.ExecutorConfig, tenantID roachpb.TenantID) error {
	var tenantInfo *mtinfopb.TenantInfo

	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		tenantInfo, err = sql.GetTenantRecordByID(ctx, txn, tenantID, execCfg.Settings)
		return err
	}); err != nil {
		return err
	}

	ie := execCfg.InternalDB.Executor()
	if _, err := ie.Exec(ctx, "stop tenant", nil, `ALTER VIRTUAL CLUSTER $1 STOP SERVICE`, tenantInfo.Name); err != nil {
		return err
	}

	tenantInfo.ServiceMode = mtinfopb.ServiceModeNone
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, tenantInfo)
	}); err != nil {
		return err
	}

	if _, err := ie.Exec(ctx, "drop tenant", nil, `DROP VIRTUAL CLUSTER IF EXISTS $1 IMMEDIATE`, tenantInfo.Name); err != nil {
		return err
	}
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. After ingestion job
// fails or gets cancelled, the tenant should be dropped.
func (s *streamIngestionResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	// Cancel the producer job on best effort. The source job's protected timestamp is no
	// longer needed as this ingestion job is in 'reverting' status and we won't resume
	// ingestion anymore.
	jobExecCtx := execCtx.(sql.JobExecContext)
	completeProducerJob(ctx, s.job, jobExecCtx.ExecCfg().InternalDB, false)
	// On a job fail or cancel, replication has permanently stopped so set replicated time to 0.
	// This value can be inadvertently overriden due to the race condition between job cancellation/failure
	// and the shutdown of ingestion processors.
	jobExecCtx.ExecCfg().JobRegistry.MetricsStruct().StreamIngest.(*Metrics).ReplicatedTimeSeconds.Update(0)

	details := s.job.Details().(jobspb.StreamIngestionDetails)
	execCfg := jobExecCtx.ExecCfg()
	// If we got replicated into another tenant, bail out.
	if !execCfg.Codec.ForSystemTenant() {
		return nil
	}

	if jobs.HasErrJobCanceled(
		errors.DecodeError(ctx, *s.job.Payload().FinalResumeError),
	) {
		telemetry.Count("physical_replication.canceled")
	} else {
		telemetry.Count("physical_replication.failed")
	}

	// Ensure no sip processors are still ingesting data, so a subsequent DROP
	// TENANT cmd will cleanly wipe out all data.
	if err := ingeststopped.WaitForNoIngestingNodes(ctx, jobExecCtx, s.job, maxIngestionProcessorShutdownWait); err != nil {
		log.Warningf(ctx, "unable to verify that attempted LDR job %d had stopped offline ingesting %s: %v", s.job.ID(), maxIngestionProcessorShutdownWait, err)
	} else {
		log.Infof(ctx, "verified no nodes still offline ingesting on behalf of job %d", s.job.ID())
	}

	return execCfg.InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		tenInfo, err := sql.GetTenantRecordByID(ctx, txn, details.DestinationTenantID, execCfg.Settings)
		if err != nil {
			return errors.Wrap(err, "fetch tenant info")
		}

		tenInfo.PhysicalReplicationConsumerJobID = 0
		if err := sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, tenInfo); err != nil {
			return errors.Wrap(err, "update tenant record")
		}

		if details.ProtectedTimestampRecordID != nil {
			ptp := execCfg.ProtectedTimestampProvider.WithTxn(txn)
			if err := releaseDestinationTenantProtectedTimestamp(
				ctx, ptp, *details.ProtectedTimestampRecordID,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

// CollectProfile implements the jobs.Resumer interface.
func (s *streamIngestionResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	var aggStatsCopy bulkutil.ComponentAggregatorStats
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		aggStatsCopy = s.mu.perNodeAggregatorStats.DeepCopy()
	}()

	var combinedErr error
	if err := bulkutil.FlushTracingAggregatorStats(ctx, s.job.ID(),
		p.ExecCfg().InternalDB, aggStatsCopy); err != nil {
		combinedErr = errors.CombineErrors(combinedErr, errors.Wrap(err, "failed to flush aggregator stats"))
	}
	if err := generateSpanFrontierExecutionDetailFile(ctx, p.ExecCfg(),
		s.job.ID(), false /* skipBehindBy */); err != nil {
		combinedErr = errors.CombineErrors(combinedErr, errors.Wrap(err, "failed to generate span frontier execution details"))
	}

	return combinedErr
}

func closeAndLog(ctx context.Context, d streamclient.Client) {
	if err := d.Close(ctx); err != nil {
		log.Warningf(ctx, "error closing stream client: %s", err.Error())
	}
}

// cutoverProgressTracker updates the job progress and the given
// metric with the number of ranges still remainng to revert during
// the cutover process.
type cutoverProgressTracker struct {
	minProgressUpdateInterval time.Duration
	progMetric                *metric.Gauge
	job                       *jobs.Job

	remainingSpans     roachpb.SpanGroup
	lastUpdatedAt      time.Time
	originalRangeCount int

	getRangeCount                   func(context.Context, roachpb.Spans) (int, error)
	onJobProgressUpdate             func(remainingSpans roachpb.Spans)
	overrideShouldUpdateJobProgress func() bool
}

func newCutoverProgressTracker(
	ctx context.Context,
	p sql.JobExecContext,
	originalSpanToRevert roachpb.Span,
	remainingSpansToRevert roachpb.Spans,
	job *jobs.Job,
	progMetric *metric.Gauge,
	minProgressUpdateInterval time.Duration,
) (*cutoverProgressTracker, error) {
	var sg roachpb.SpanGroup
	for i := range remainingSpansToRevert {
		sg.Add(remainingSpansToRevert[i])
	}

	originalRangeCount, err := sql.NumRangesInSpans(ctx, p.ExecCfg().DB, p.DistSQLPlanner(),
		roachpb.Spans{originalSpanToRevert})
	if err != nil {
		return nil, err
	}
	c := &cutoverProgressTracker{
		job:                       job,
		progMetric:                progMetric,
		minProgressUpdateInterval: minProgressUpdateInterval,

		remainingSpans:     sg,
		originalRangeCount: originalRangeCount,

		getRangeCount: func(ctx context.Context, sps roachpb.Spans) (int, error) {
			return sql.NumRangesInSpans(ctx, p.ExecCfg().DB, p.DistSQLPlanner(), sps)
		},
	}
	if testingKnobs := p.ExecCfg().StreamingTestingKnobs; testingKnobs != nil {
		c.overrideShouldUpdateJobProgress = testingKnobs.CutoverProgressShouldUpdate
		c.onJobProgressUpdate = testingKnobs.OnCutoverProgressUpdate
	}
	return c, nil

}

func (c *cutoverProgressTracker) shouldUpdateJobProgress() bool {
	if c.overrideShouldUpdateJobProgress != nil {
		return c.overrideShouldUpdateJobProgress()
	}
	return timeutil.Since(c.lastUpdatedAt) >= c.minProgressUpdateInterval
}

func (c *cutoverProgressTracker) updateJobProgress(
	ctx context.Context, remainingSpans []roachpb.Span,
) error {
	nRanges, err := c.getRangeCount(ctx, remainingSpans)
	if err != nil {
		return err
	}

	c.progMetric.Update(int64(nRanges))

	// We set lastUpdatedAt even though we might not actually
	// update the job record below. We do this to avoid asking for
	// the range count too often.
	c.lastUpdatedAt = timeutil.Now()

	continueUpdate := c.overrideShouldUpdateJobProgress != nil && c.overrideShouldUpdateJobProgress()

	// If our fraction is not going to actually move, avoid touching
	// the job record.
	if nRanges >= c.originalRangeCount && !continueUpdate {
		return nil
	}

	fractionRangesFinished := float32(c.originalRangeCount-nRanges) / float32(c.originalRangeCount)

	persistProgress := func(ctx context.Context, details jobspb.ProgressDetails) float32 {
		prog := details.(*jobspb.Progress_StreamIngest).StreamIngest
		prog.RemainingCutoverSpans = remainingSpans
		return fractionRangesFinished
	}

	if err := c.job.NoTxn().FractionProgressed(ctx, persistProgress); err != nil {
		return jobs.SimplifyInvalidStateError(err)
	}
	if c.onJobProgressUpdate != nil {
		c.onJobProgressUpdate(remainingSpans)
	}
	return nil
}

func (c *cutoverProgressTracker) onCompletedCallback(
	ctx context.Context, completed roachpb.Span,
) error {
	c.remainingSpans.Sub(completed)
	if !c.shouldUpdateJobProgress() {
		return nil
	}

	if err := c.updateJobProgress(ctx, c.remainingSpans.Slice()); err != nil {
		log.Warningf(ctx, "failed to update job progress: %s", err)
	}
	return nil
}

func (s *streamIngestionResumer) ForceRealSpan() bool     { return true }
func (s *streamIngestionResumer) DumpTraceAfterRun() bool { return true }

var _ jobs.TraceableJob = &streamIngestionResumer{}
var _ jobs.Resumer = &streamIngestionResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeReplicationStreamIngestion,
		func(job *jobs.Job,
			settings *cluster.Settings) jobs.Resumer {
			s := &streamIngestionResumer{job: job}
			s.mu.perNodeAggregatorStats = make(bulkutil.ComponentAggregatorStats)
			return s
		},
		jobs.UsesTenantCostControl,
	)
}
