// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package physical implements the consumer side of physical cluster.

Physical cluster replication (PCR) uses a pull-based model where the destination
cluster (consumer) initiates connections to the source cluster (producer) to
request change streams. The consumer provides a frontier timestamp indicating
where to resume from, enabling crash recovery and incremental replication.

# High-Level Architecture (as summarized by Claude Code)

┌─────────────────────────────────────────────────────────────────────────────┐
│ SOURCE CLUSTER (Producer, pkg/crosscluster/producer)                        │
│                                                                             │
│  ┌──────────────┐    ┌──────────────────┐    ┌─────────────────────────┐    │
│  │  Rangefeeds  │───▶│  eventStream     │───▶│  streamCh (pgwire)      │    │
│  │  (per range) │    │  (batches events)│    │  (sends to consumer)    │    │
│  └──────────────┘    └──────────────────┘    └─────────────────────────┘    │
│                                                        │                    │
│           GC protected by                              │                    │
│           heartbeat timestamp                          │                    │
│                                                        ▼                    │
│  ┌────── ───────┐◀─────────────── heartbeats ──────────────────────────┐    │
│  │  Rangefeed   │    (consumer reports durably replicated time         │    │
│  │  GC threshold│     so producer can advance GC)                      │    │
│  └──────────────┘                                                      │    │
│                                                                        │    │
│  Timing metrics:                                                       │    │
│  - ProduceWait: time waiting to produce (rangefeed → batch)            │    │
│  - EmitWait: time waiting to emit (batch → consumer reads)             │    │
└────────────────────────────────────────────────────────────────────────│────┘

	                                                                         │
		                                                                       │
		┌──────────────────────────────────────────────────────────────────────┘
		│ pgwire connection (pkg/crosscluster/streamclient)
		│ Consumer calls: crdb_internal.stream_partition(streamID, spec)
		│ Spec includes InitialScanTimestamp and per-span resume timestamps
		▼

┌─────────────────────────────────────────────────────────────────────────────┐
│ DESTINATION CLUSTER (Consumer, pkg/crosscluster/physical)                   │
│                                                                             │
│  Job Resumer (stream_ingestion_job.go)                                      │
│    - Reads ReplicatedTime from job progress (or InitialScanTimestamp)       │
│    - Constructs DistSQL flow with frontier as resume point                  │
│    - On restart, resumes from persisted checkpoint                          │
│                       │                                                     │
│                       ▼                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ DistSQL Flow (one ingestion processor per source partition)         │    │
│  │                                                                     │    │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │    │
│  │  │ streamIngestion │  │ streamIngestion │  │ streamIngestion │      │    │
│  │  │ Processor (N1)  │  │ Processor (N2)  │  │ Processor (N3)  │      │    │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘      │    │
│  │           │ ResolvedSpans      │                    │               │    │
│  │           └────────────────────┼────────────────────┘               │    │
│  │                                ▼                                    │    │
│  │                    ┌────────────────────────┐                       │    │
│  │                    │ streamIngestionFrontier│                       │    │
│  │                    │ Processor (coordinator)│                       │    │
│  │                    └───────────┬────────────┘                       │    │
│  │                                │                                    │    │
│  └────────────────────────────────│────────────────────────────────────┘    │
│                                   ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Frontier Processor responsibilities:                                │    │
│  │  - Merges ResolvedSpans from all ingestion processors               │    │
│  │  - Maintains span.Frontier (min resolved timestamp across spans)    │    │
│  │  - Persists ReplicatedTime to job progress periodically             │    │
│  │  - Sends heartbeats to producer with durably replicated time        │    │
│  │  - ReplicatedTime = safe point for cutover or job restart           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Timing metrics:                                                            │
│  - AdmitLatency: MVCC timestamp → event received                            │
│  - FlushHistNanos: time spent in flush operation                            │
│  - CommitLatency: oldest MVCC timestamp → flush complete                    │
│  - ReceiveWaitNanos: time blocked waiting for producer data                 │
│  - FlushWaitNanos: time blocked waiting for flush pipeline                  │
└─────────────────────────────────────────────────────────────────────────────┘

# Checkpoint and Frontier Flow

 1. Producer sends "resolved span" events indicating all data up to timestamp T
    for a given span has been sent.

 2. Each ingestion processor receives resolved spans for its partitions, flushes
    buffered data, then emits ResolvedSpans to the frontier processor.

 3. Frontier processor maintains a span.Frontier tracking the minimum resolved
    timestamp across ALL spans. This is the ReplicatedTime.

4. ReplicatedTime is persisted to job progress and used for:
  - Cutover: destination can serve reads as of ReplicatedTime
  - Resumption: on restart, consumer requests changes from ReplicatedTime
  - GC coordination: heartbeat tells producer it can GC data before this time

# Detailed Data Flow

## Producer Side (pkg/crosscluster/producer/event_stream.go)

1. Rangefeed callbacks (onValue, onValues, onSSTable, onDeleteRange):
  - Receive events from rangefeed
  - Add to streamEventBatcher (accumulates events)
  - Call maybeFlushBatch() which flushes when:
  - Batch size exceeds BatchByteSize (1 MiB default)
  - Consumer is ready AND batch > minBatchByteSize (1 MiB)

2. Batching and sending (flushBatch, sendFlush):
  - Serialize batch to protobuf
  - Optionally compress with Snappy
  - BLOCKING: Send on streamCh - blocks until consumer's Next() reads it
  - This is where producer waits for slow consumer

3. Checkpoint emission (maybeCheckpoint, sendCheckpoint):
  - Triggered on rangefeed frontier advance
  - Respects MinCheckpointFrequency setting
  - Sends resolved spans to consumer

## Transport Layer (pkg/crosscluster/streamclient/)

- partitionedStreamClient.Subscribe() initiates connection to producer
- Passes InitialScanTimestamp and per-span frontier for resumption
- Producer starts rangefeeds from the requested timestamps
- Events flow via pgwire as rows, decoded into StreamEvent channel

## Consumer Side (pkg/crosscluster/physical/stream_ingestion_processor.go)

1. Subscription setup (Start()):
  - Creates streamclient.Client per partition
  - Each client calls Subscribe() with frontier timestamps
  - All subscriptions merged via MergedSubscription

2. Event consumption (consumeEvents()):
  - BLOCKING select on mergedSubscription.Events()
  - This is where consumer waits for data from producer
  - Buffers KVs into streamIngestionBuffer
  - Flushes on:
  - Checkpoint event (if minimumFlushInterval elapsed)
  - Buffer size threshold (maxKVBufferSize 128 MiB)
  - Periodic timer

3. Flush pipeline (flush() → flushLoop() → flushBuffer()):
  - Swaps buffer, sends to flushCh (1 in-flight flush allowed)
  - flushLoop receives buffer, calls flushBuffer()
  - Sorts KVs, writes via SSTBatcher
  - BLOCKING: sip.batcher.Flush() does actual I/O
  - Emits ResolvedSpans to downstream frontier processor

## Frontier Processor (stream_ingestion_frontier_processor.go)

1. Receives ResolvedSpans from all ingestion processors
2. Updates span.Frontier with each resolved span
3. Periodically persists frontier to job progress (JobCheckpointFrequency)
4. Sends heartbeats to producer via HeartbeatSender with replicated time
5. Producer uses heartbeat time to advance GC threshold
*/
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

// loadStreamIngestionProgress loads the StreamIngestionProgress from the database.
func loadStreamIngestionProgress(
	ctx context.Context, db isql.DB, jobID jobspb.JobID,
) (jobspb.StreamIngestionProgress, error) {
	var progress jobspb.StreamIngestionProgress
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		details, err := jobs.LoadLegacyProgress(ctx, txn, jobID)
		if err != nil {
			return err
		}
		if details == nil {
			return errors.Errorf("job %d progress not found", jobID)
		}
		progress = details.(jobspb.StreamIngestionProgress)
		return nil
	}); err != nil {
		return jobspb.StreamIngestionProgress{}, err
	}
	return progress, nil
}

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
	ctx context.Context,
	db descs.DB,
	details jobspb.StreamIngestionDetails,
	progress jobspb.StreamIngestionProgress,
) ([]streamclient.ClusterUri, error) {
	sourceUri, err := streamclient.LookupClusterUri(ctx, details.SourceClusterConnUri, db)
	if err != nil {
		return nil, err
	}

	// Always use the configured URI as the the first conneciton target. It may
	// be a load balancer or an external connection.
	uris := []streamclient.ClusterUri{sourceUri}

	for _, uri := range progress.PartitionConnUris {
		parsed, err := streamclient.ParseClusterUri(uri)
		if err != nil {
			return nil, err
		}
		uris = append(uris, parsed)
	}

	return uris, nil
}

func connectToActiveClient(
	ctx context.Context,
	ingestionJob *jobs.Job,
	db descs.DB,
	progress jobspb.StreamIngestionProgress,
	opts ...streamclient.Option,
) (streamclient.Client, error) {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	clusterUris, err := getClusterUris(ctx, db, details, progress)
	if err != nil {
		return nil, err
	}
	client, err := streamclient.GetFirstActiveClient(ctx, clusterUris, db, opts...)
	return client, errors.Wrapf(err, "ingestion job %d failed to connect to stream address or existing topology for planning", ingestionJob.ID())
}

func updateStatus(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ingestionJob *jobs.Job,
	replicationStatus jobspb.ReplicationStatus,
	status redact.RedactableString,
) {
	statusMessage := string(status.Redact())
	err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		details, err := jobs.LoadLegacyProgress(ctx, txn, ingestionJob.ID())
		if err != nil {
			return err
		}
		progress := details.(jobspb.StreamIngestionProgress)
		progress.ReplicationStatus = replicationStatus
		if err := jobs.StoreLegacyProgress(ctx, txn, ingestionJob.ID(), progress); err != nil {
			return err
		}
		return jobs.StatusStorage(ingestionJob.ID()).Set(ctx, txn, statusMessage)
	})
	if err != nil {
		log.Dev.Warningf(ctx, "error when updating job running status: %s", err)
	} else if replicationStatus == jobspb.ReplicationError {
		log.Dev.Warningf(ctx, "%s", status)
	} else {
		log.Dev.Infof(ctx, "%s", status)
	}
}

func completeIngestion(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ingestionJob *jobs.Job,
	cutoverTimestamp hlc.Timestamp,
) error {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	log.Dev.Infof(ctx, "activating destination tenant %d", details.DestinationTenantID)
	if err := activateTenant(ctx, execCtx, details, cutoverTimestamp); err != nil {
		return err
	}

	// Load fresh progress for completing the producer job.
	progress, err := loadStreamIngestionProgress(ctx, execCtx.ExecCfg().InternalDB, ingestionJob.ID())
	if err != nil {
		return err
	}

	msg := redact.Sprintf("completing the producer job %d in the source cluster",
		details.StreamID)
	updateStatus(ctx, execCtx.ExecCfg(), ingestionJob, jobspb.ReplicationFailingOver, msg)
	completeProducerJob(ctx, ingestionJob, execCtx.ExecCfg().InternalDB, progress, true)
	evalContext := &execCtx.ExtendedEvalContext().Context
	if err := startPostCutoverRetentionJob(ctx, execCtx.ExecCfg(), details, evalContext, cutoverTimestamp); err != nil {
		log.Dev.Warningf(ctx, "failed to begin post cutover retention job: %s", err.Error())
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
	ctx context.Context,
	ingestionJob *jobs.Job,
	internalDB *sql.InternalDB,
	progress jobspb.StreamIngestionProgress,
	successfulIngestion bool,
) {
	streamID := streampb.StreamID(ingestionJob.Details().(jobspb.StreamIngestionDetails).StreamID)
	if err := timeutil.RunWithTimeout(ctx, "complete producer job", 30*time.Second,
		func(ctx context.Context) error {
			client, err := connectToActiveClient(ctx, ingestionJob, internalDB, progress,
				streamclient.WithStreamID(streamID))
			if err != nil {
				return err
			}
			defer closeAndLog(ctx, client)
			return client.Complete(ctx, streamID, successfulIngestion)
		},
	); err != nil {
		log.Dev.Warningf(ctx, `encountered error when completing the source cluster producer job %d: %s`, streamID, err.Error())
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
		log.Dev.Infof(ctx, "job completed cutover on resume")
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
		log.Dev.Infof(ctx, "hit retryable error %s", err)

		// Reload progress from DB to check if spans have advanced.
		progress, loadErr := loadStreamIngestionProgress(ctx, execCtx.ExecCfg().InternalDB, ingestionJob.ID())
		if loadErr != nil {
			log.Dev.Warningf(ctx, "failed to load progress for retry check: %s", loadErr)
			continue
		}
		currentPersistedSpans := jobspb.ResolvedSpanEntries(progress.Checkpoint.ResolvedSpans)
		if !currentPersistedSpans.Equal(previousPersistedSpans) {
			// If the previous persisted spans are different than the current, it
			// implies that further progress has been persisted.
			r.Reset()
			log.Dev.Infof(ctx, "resolved spans have advanced since last retry, resetting retry counter")
		}
		previousPersistedSpans = currentPersistedSpans
		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.AfterRetryIteration != nil {
			knobs.AfterRetryIteration(err)
		}
	}
	if err != nil {
		return err
	}
	updateStatus(ctx, execCtx.ExecCfg(), ingestionJob, jobspb.ReplicationFailingOver,
		"stream ingestion finished successfully")
	return nil
}

// The ingestion job should never fail, only pause, as progress should never be lost.
func (s *streamIngestionResumer) handleResumeError(
	ctx context.Context, execCtx sql.JobExecContext, err error,
) error {
	msg := redact.Sprintf("ingestion job failed (%s) but is being paused", err)
	updateStatus(ctx, execCtx.ExecCfg(), s.job, jobspb.ReplicationError, msg)
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
			log.Dev.Warningf(ctx, "failed to release protected ts as it does not to exist: %s", err)
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
			jobsprotectedts.Jobs, target)
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
		log.Dev.Infof(ctx, "empty cutover time, no revert required")
		return false
	}

	replicatedTime := replicationutils.ReplicatedTimeFromProgress(progress)
	if replicatedTime.Less(cutoverTime) {
		log.Dev.Infof(ctx, "job with replicated time %s not yet ready to revert to cutover at %s",
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

	streamIngestionDetails := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	readerTenantID := streamIngestionDetails.ReadTenantID
	originalSpanToRevert := streamIngestionDetails.Span

	// Read progress, check eligibility, and update status all in one transaction.
	var cutoverTimestamp hlc.Timestamp
	var remainingSpansToRevert roachpb.Spans
	var shouldRevertToCutover bool
	if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		prog, err := jobs.LoadLegacyProgress(ctx, txn, ingestionJob.ID())
		if err != nil {
			return err
		}
		streamProgress := prog.(jobspb.StreamIngestionProgress)

		cutoverTimestamp = streamProgress.CutoverTime
		remainingSpansToRevert = streamProgress.RemainingCutoverSpans

		// Build a minimal Progress proto for the eligibility check.
		progressForCheck := &jobspb.Progress{
			Details: &jobspb.Progress_StreamIngest{StreamIngest: &streamProgress},
		}
		shouldRevertToCutover = cutoverTimeIsEligibleForCutover(ctx, cutoverTimestamp, progressForCheck)

		if shouldRevertToCutover {
			streamProgress.ReplicationStatus = jobspb.ReplicationFailingOver
			if err := jobs.StoreLegacyProgress(ctx, txn, ingestionJob.ID(), streamProgress); err != nil {
				return err
			}
			statusMsg := fmt.Sprintf("starting to cut over to the given timestamp %s", cutoverTimestamp)
			return jobs.StatusStorage(ingestionJob.ID()).Set(ctx, txn, statusMsg)
		}

		if streamProgress.ReplicationStatus == jobspb.ReplicationFailingOver {
			return errors.AssertionFailedf("cutover already started but cutover time %s is not eligible for cutover",
				cutoverTimestamp)
		}
		return nil
	}); err != nil {
		return hlc.Timestamp{}, false, err
	}

	if !shouldRevertToCutover {
		return cutoverTimestamp, false, nil
	}
	if readerTenantID.IsSet() {
		if err := stopTenant(ctx, p.ExecCfg(), readerTenantID); err != nil {
			return cutoverTimestamp, false, errors.Wrapf(err, "failed to stop reader tenant")
		}
	}
	if err := ingeststopped.WaitForNoIngestingNodes(ctx, p, ingestionJob, maxIngestionProcessorShutdownWait); err != nil {
		return cutoverTimestamp, false, errors.Wrapf(err, "unable to verify that attempted LDR job %d had stopped offline ingesting %s", ingestionJob.ID(), maxIngestionProcessorShutdownWait)
	}
	log.Dev.Infof(ctx, "verified no nodes still offline ingesting on behalf of job %d", ingestionJob.ID())

	log.Dev.Infof(ctx, "reverting to cutover timestamp %s", cutoverTimestamp)
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

	// Load progress for this entry point.
	progress, err := loadStreamIngestionProgress(ctx, jobExecCtx.ExecCfg().InternalDB, s.job.ID())
	if err != nil {
		log.Dev.Warningf(ctx, "failed to load progress in OnFailOrCancel: %s", err)
	}
	completeProducerJob(ctx, s.job, jobExecCtx.ExecCfg().InternalDB, progress, false)
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
		log.Dev.Warningf(ctx, "unable to verify that attempted LDR job %d had stopped offline ingesting %s: %v", s.job.ID(), maxIngestionProcessorShutdownWait, err)
	} else {
		log.Dev.Infof(ctx, "verified no nodes still offline ingesting on behalf of job %d", s.job.ID())
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
		log.Dev.Warningf(ctx, "error closing stream client: %s", err.Error())
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
		log.Dev.Warningf(ctx, "failed to update job progress: %s", err)
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
