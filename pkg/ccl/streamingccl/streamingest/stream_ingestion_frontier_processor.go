// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// JobCheckpointFrequency controls the frequency of frontier checkpoints into
// the jobs table.
var JobCheckpointFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"stream_replication.job_checkpoint_frequency",
	"controls the frequency with which partitions update their progress; if 0, disabled.",
	10*time.Second,
	settings.NonNegativeDuration,
)

const streamIngestionFrontierProcName = `ingestfntr`

type streamIngestionFrontier struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionFrontierSpec
	alloc   tree.DatumAlloc

	// input returns rows from one or more streamIngestion processors.
	input execinfra.RowSource
	// highWaterAtStart is the job high-water. It's used in an assertion that we
	// never regress the job high-water.
	highWaterAtStart hlc.Timestamp

	// frontier contains the current resolved timestamp high-water for the tracked
	// span set.
	frontier *span.Frontier

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	// heartbeatSender sends heartbeats to the source cluster to keep the replication
	// stream alive.
	heartbeatSender *heartbeatSender

	// persistedHighWater stores the highwater mark of progress that is persisted
	// in the job record.
	persistedHighWater hlc.Timestamp

	lastPartitionUpdate time.Time
	partitionProgress   map[string]jobspb.StreamIngestionProgress_PartitionProgress
}

var _ execinfra.Processor = &streamIngestionFrontier{}
var _ execinfra.RowSource = &streamIngestionFrontier{}

func init() {
	rowexec.NewStreamIngestionFrontierProcessor = newStreamIngestionFrontierProcessor
}

func newStreamIngestionFrontierProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	frontier, err := span.MakeFrontierAt(spec.HighWaterAtStart, spec.TrackedSpans...)
	if err != nil {
		return nil, err
	}
	for _, resolvedSpan := range spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return nil, err
		}
	}

	heartbeatSender, err := newHeartbeatSender(ctx, flowCtx, spec)
	if err != nil {
		return nil, err
	}
	partitionProgress := make(map[string]jobspb.StreamIngestionProgress_PartitionProgress)
	for srcPartitionID, destSQLInstanceID := range spec.SubscribingSQLInstances {
		partitionProgress[srcPartitionID] = jobspb.StreamIngestionProgress_PartitionProgress{
			DestSQLInstanceID: base.SQLInstanceID(destSQLInstanceID),
		}
	}
	sf := &streamIngestionFrontier{
		flowCtx:            flowCtx,
		spec:               spec,
		input:              input,
		highWaterAtStart:   spec.HighWaterAtStart,
		frontier:           frontier,
		partitionProgress:  partitionProgress,
		metrics:            flowCtx.Cfg.JobRegistry.MetricsStruct().StreamIngest.(*Metrics),
		heartbeatSender:    heartbeatSender,
		persistedHighWater: spec.HighWaterAtStart,
	}
	if err := sf.Init(
		ctx,
		sf,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{sf.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				sf.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return sf, nil
}

// MustBeStreaming implements the execinfra.Processor interface.
func (sf *streamIngestionFrontier) MustBeStreaming() bool {
	return true
}

type heartbeatSender struct {
	lastSent        time.Time
	client          streamclient.Client
	streamID        streampb.StreamID
	frontierUpdates chan hlc.Timestamp
	frontier        hlc.Timestamp
	flowCtx         *execinfra.FlowCtx
	// cg runs the heartbeatSender thread.
	cg ctxgroup.Group
	// cancel stops heartbeat sender.
	cancel func()
	// heartbeatSender closes this channel when it stops.
	stoppedChan chan struct{}
}

func newHeartbeatSender(
	ctx context.Context, flowCtx *execinfra.FlowCtx, spec execinfrapb.StreamIngestionFrontierSpec,
) (*heartbeatSender, error) {
	streamClient, err := streamclient.GetFirstActiveClient(ctx, spec.StreamAddresses)
	if err != nil {
		return nil, err
	}
	return &heartbeatSender{
		client:          streamClient,
		streamID:        streampb.StreamID(spec.StreamID),
		flowCtx:         flowCtx,
		frontierUpdates: make(chan hlc.Timestamp),
		cancel:          func() {},
		stoppedChan:     make(chan struct{}),
	}, nil
}

func (h *heartbeatSender) maybeHeartbeat(
	ctx context.Context, frontier hlc.Timestamp,
) (bool, streampb.StreamReplicationStatus, error) {
	heartbeatFrequency := streamingccl.StreamReplicationConsumerHeartbeatFrequency.Get(&h.flowCtx.EvalCtx.Settings.SV)
	if h.lastSent.Add(heartbeatFrequency).After(timeutil.Now()) {
		return false, streampb.StreamReplicationStatus{}, nil
	}
	h.lastSent = timeutil.Now()
	s, err := h.client.Heartbeat(ctx, h.streamID, frontier)
	return true, s, err
}

func (h *heartbeatSender) startHeartbeatLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.cg = ctxgroup.WithContext(ctx)
	h.cg.GoCtx(func(ctx context.Context) error {
		sendHeartbeats := func() error {
			// The heartbeat thread send heartbeats when there is a frontier update,
			// and it has been a while since last time we sent it, or when we need
			// to heartbeat to keep the stream alive even if the frontier has no update.
			timer := time.NewTimer(streamingccl.StreamReplicationConsumerHeartbeatFrequency.
				Get(&h.flowCtx.EvalCtx.Settings.SV))
			defer timer.Stop()
			unknownStreamStatusRetryErr := log.Every(1 * time.Minute)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
					timer.Reset(streamingccl.StreamReplicationConsumerHeartbeatFrequency.
						Get(&h.flowCtx.EvalCtx.Settings.SV))
				case frontier := <-h.frontierUpdates:
					h.frontier.Forward(frontier)
				}
				sent, streamStatus, err := h.maybeHeartbeat(ctx, h.frontier)
				// TODO(casper): add unit tests to test different kinds of client errors.
				if err != nil {
					return err
				}

				if !sent || streamStatus.StreamStatus == streampb.StreamReplicationStatus_STREAM_ACTIVE {
					continue
				}

				if streamStatus.StreamStatus == streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY {
					if unknownStreamStatusRetryErr.ShouldLog() {
						log.Warningf(ctx, "replication stream %d has unknown stream status error and will retry later", h.streamID)
					}
					continue
				}
				// The replication stream is either paused or inactive.
				return streamingccl.NewStreamStatusErr(h.streamID, streamStatus.StreamStatus)
			}
		}
		err := errors.CombineErrors(sendHeartbeats(), h.client.Close(ctx))
		close(h.stoppedChan)
		return err
	})
}

// Stop the heartbeat loop and returns any error at time of heartbeatSender's exit.
// Can be called multiple times.
func (h *heartbeatSender) stop() error {
	h.cancel()
	return h.wait()
}

// Wait for heartbeatSender to be stopped and returns any error.
func (h *heartbeatSender) wait() error {
	err := h.cg.Wait()
	// We expect to see context cancelled when shutting down.
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Start is part of the RowSource interface.
func (sf *streamIngestionFrontier) Start(ctx context.Context) {
	ctx = sf.StartInternal(ctx, streamIngestionFrontierProcName)
	sf.metrics.RunningCount.Inc(1)
	sf.input.Start(ctx)
	sf.heartbeatSender.startHeartbeatLoop(ctx)
}

// Next is part of the RowSource interface.
func (sf *streamIngestionFrontier) Next() (
	row rowenc.EncDatumRow,
	meta *execinfrapb.ProducerMetadata,
) {
	for sf.State == execinfra.StateRunning {
		row, meta := sf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				sf.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			sf.MoveToDraining(nil /* err */)
			break
		}

		var err error
		if _, err = sf.noteResolvedTimestamps(row[0]); err != nil {
			sf.MoveToDraining(err)
			break
		}

		if err := sf.maybeUpdatePartitionProgress(); err != nil {
			// Updating the partition progress isn't a fatal error.
			log.Errorf(sf.Ctx(), "failed to update partition progress: %+v", err)
		}

		// Send back a row to the job so that it can update the progress.
		select {
		case <-sf.Ctx().Done():
			sf.MoveToDraining(sf.Ctx().Err())
			return nil, sf.DrainHelper()
			// Send the latest persisted highwater in the heartbeat to the source cluster
			// as even with retries we will never request an earlier row than it, and
			// the source cluster is free to clean up earlier data.
		case sf.heartbeatSender.frontierUpdates <- sf.persistedHighWater:
			// If heartbeatSender has error, it means remote has error, we want to
			// stop the processor.
		case <-sf.heartbeatSender.stoppedChan:
			err := sf.heartbeatSender.wait()
			if err != nil {
				log.Errorf(sf.Ctx(), "heartbeat sender exited with error: %s", err)
			}
			sf.MoveToDraining(err)
			return nil, sf.DrainHelper()
		}
	}
	return nil, sf.DrainHelper()
}

func (sf *streamIngestionFrontier) close() {
	if err := sf.heartbeatSender.stop(); err != nil {
		log.Errorf(sf.Ctx(), "heartbeat sender exited with error: %s", err)
	}
	if sf.InternalClose() {
		sf.metrics.RunningCount.Dec(1)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (sf *streamIngestionFrontier) ConsumerClosed() {
	sf.close()
}

// decodeResolvedSpans decodes an encoded datum of jobspb.ResolvedSpans into a
// jobspb.ResolvedSpans object.
func decodeResolvedSpans(
	alloc *tree.DatumAlloc, resolvedSpanDatums rowenc.EncDatum,
) (*jobspb.ResolvedSpans, error) {
	if err := resolvedSpanDatums.EnsureDecoded(streamIngestionResultTypes[0], alloc); err != nil {
		return nil, err
	}
	raw, ok := resolvedSpanDatums.Datum.(*tree.DBytes)
	if !ok {
		return nil, errors.AssertionFailedf(`unexpected datum type %T: %s`,
			resolvedSpanDatums.Datum, resolvedSpanDatums.Datum)
	}
	var resolvedSpans jobspb.ResolvedSpans
	if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved timestamp: %x`, raw)
	}
	return &resolvedSpans, nil
}

// noteResolvedTimestamps processes a batch of resolved timestamp events, and
// returns whether the frontier has moved forward after processing the batch.
func (sf *streamIngestionFrontier) noteResolvedTimestamps(
	resolvedSpanDatums rowenc.EncDatum,
) (bool, error) {
	var frontierChanged bool
	resolvedSpans, err := decodeResolvedSpans(&sf.alloc, resolvedSpanDatums)
	if err != nil {
		return false, err
	}
	for _, resolved := range resolvedSpans.ResolvedSpans {
		// Inserting a timestamp less than the one the ingestion flow started at could
		// potentially regress the job progress. This is not expected and thus we
		// assert to catch such unexpected behavior.
		if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(sf.highWaterAtStart) {
			return frontierChanged, errors.AssertionFailedf(
				`got a resolved timestamp %s that is less than the frontier processor start time %s`,
				redact.Safe(resolved.Timestamp), redact.Safe(sf.highWaterAtStart))
		}

		changed, err := sf.frontier.Forward(resolved.Span, resolved.Timestamp)
		if err != nil {
			return false, err
		}
		frontierChanged = frontierChanged || changed
	}

	return frontierChanged, nil
}

// maybeUpdatePartitionProgress polls the frontier and updates the job progress with
// partition-specific information to track the status of each partition.
func (sf *streamIngestionFrontier) maybeUpdatePartitionProgress() error {
	ctx := sf.Ctx()
	updateFreq := JobCheckpointFrequency.Get(&sf.flowCtx.Cfg.Settings.SV)
	if updateFreq == 0 || timeutil.Since(sf.lastPartitionUpdate) < updateFreq {
		return nil
	}
	f := sf.frontier
	registry := sf.flowCtx.Cfg.JobRegistry
	jobID := jobspb.JobID(sf.spec.JobID)

	frontierResolvedSpans := make([]jobspb.ResolvedSpan, 0)
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		frontierResolvedSpans = append(frontierResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})

	highWatermark := f.Frontier()
	partitionProgress := sf.partitionProgress

	sf.lastPartitionUpdate = timeutil.Now()

	if err := registry.UpdateJobWithTxn(ctx, jobID, nil, false, func(
		txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}

		progress := md.Progress
		// Keep the recorded highwater empty until some advancement has been made
		if sf.highWaterAtStart.Less(highWatermark) {
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &highWatermark,
			}
		}

		streamProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		streamProgress.PartitionProgress = partitionProgress
		streamProgress.Checkpoint.ResolvedSpans = frontierResolvedSpans

		ju.UpdateProgress(progress)

		// Reset RunStats.NumRuns to 1 since the stream ingestion has returned to
		// a steady state. By resetting NumRuns,we avoid future job system level
		// retries from having a large backoff because of past failures.
		if md.RunStats != nil && md.RunStats.NumRuns > 1 {
			ju.UpdateRunStats(1, md.RunStats.LastRun)
		}

		// Update the protected timestamp record protecting the destination tenant's
		// keyspan if the highWatermark has moved forward since the last time we
		// recorded progress. This makes older revisions of replicated values with a
		// timestamp less than highWatermark - ReplicationTTLSeconds, eligible for
		// garbage collection.
		replicationDetails := md.Payload.GetStreamIngestion()
		if replicationDetails.ProtectedTimestampRecordID == nil {
			return errors.AssertionFailedf("expected replication job to have a protected timestamp " +
				"record over the destination tenant's keyspan")
		}
		ptp := sf.flowCtx.Cfg.ProtectedTimestampProvider.WithTxn(txn)
		record, err := ptp.GetRecord(ctx, *replicationDetails.ProtectedTimestampRecordID)
		if err != nil {
			return err
		}
		newProtectAbove := highWatermark.Add(
			-int64(replicationDetails.ReplicationTTLSeconds)*time.Second.Nanoseconds(), 0)
		if record.Timestamp.Less(newProtectAbove) {
			return ptp.UpdateTimestamp(ctx, *replicationDetails.ProtectedTimestampRecordID, newProtectAbove)
		}
		return nil
	}); err != nil {
		return err
	}
	sf.metrics.JobProgressUpdates.Inc(1)
	sf.persistedHighWater = f.Frontier()
	sf.metrics.FrontierCheckpointSpanCount.Update(int64(len(frontierResolvedSpans)))
	if !sf.persistedHighWater.IsEmpty() {
		// Only update the frontier lag if the high water mark has been updated,
		// implying the initial scan has completed.
		sf.metrics.FrontierLagNanos.Update(timeutil.Since(sf.persistedHighWater.GoTime()).Nanoseconds())
	}
	return nil
}
