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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/streaming"
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

	lastPartitionUpdate time.Time
	partitionProgress   map[string]jobspb.StreamIngestionProgress_PartitionProgress
}

var _ execinfra.Processor = &streamIngestionFrontier{}
var _ execinfra.RowSource = &streamIngestionFrontier{}

func init() {
	rowexec.NewStreamIngestionFrontierProcessor = newStreamIngestionFrontierProcessor
}

func newStreamIngestionFrontierProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
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

	heartbeatSender, err := newHeartbeatSender(flowCtx, spec)
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
		flowCtx:           flowCtx,
		spec:              spec,
		input:             input,
		highWaterAtStart:  spec.HighWaterAtStart,
		frontier:          frontier,
		partitionProgress: partitionProgress,
		metrics:           flowCtx.Cfg.JobRegistry.MetricsStruct().StreamIngest.(*Metrics),
		heartbeatSender:   heartbeatSender,
	}
	if err := sf.Init(
		sf,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
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
	streamID        streaming.StreamID
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
	flowCtx *execinfra.FlowCtx, spec execinfrapb.StreamIngestionFrontierSpec,
) (*heartbeatSender, error) {
	streamClient, err := streamclient.NewStreamClient(
		flowCtx.EvalCtx.Ctx(),
		streamingccl.StreamAddress(spec.StreamAddress))
	if err != nil {
		return nil, err
	}
	return &heartbeatSender{
		client:          streamClient,
		streamID:        streaming.StreamID(spec.StreamID),
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

		var frontierChanged bool
		var err error
		if frontierChanged, err = sf.noteResolvedTimestamps(row[0]); err != nil {
			sf.MoveToDraining(err)
			break
		}

		if err := sf.maybeUpdatePartitionProgress(); err != nil {
			// Updating the partition progress isn't a fatal error.
			log.Errorf(sf.Ctx, "failed to update partition progress: %+v", err)
		}

		// Send back a row to the job so that it can update the progress.
		newResolvedTS := sf.frontier.Frontier()
		select {
		case <-sf.Ctx.Done():
			sf.MoveToDraining(sf.Ctx.Err())
			return nil, sf.DrainHelper()
		// Send the frontier update in the heartbeat to the source cluster.
		case sf.heartbeatSender.frontierUpdates <- newResolvedTS:
			if !frontierChanged {
				break
			}
			progressBytes, err := protoutil.Marshal(&newResolvedTS)
			if err != nil {
				sf.MoveToDraining(err)
				break
			}
			pushRow := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
			}
			if outRow := sf.ProcessRowHelper(pushRow); outRow != nil {
				return outRow, nil
			}
			// If heartbeatSender has error, it means remote has error, we want to
			// stop the processor.
		case <-sf.heartbeatSender.stoppedChan:
			err := sf.heartbeatSender.wait()
			if err != nil {
				log.Errorf(sf.Ctx, "heartbeat sender exited with error: %s", err)
			}
			sf.MoveToDraining(err)
			return nil, sf.DrainHelper()
		}
	}
	return nil, sf.DrainHelper()
}

func (sf *streamIngestionFrontier) close() {
	if err := sf.heartbeatSender.stop(); err != nil {
		log.Errorf(sf.Ctx, "heartbeat sender exited with error: %s", err)
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

		if changed, err := sf.frontier.Forward(resolved.Span, resolved.Timestamp); err == nil {
			frontierChanged = frontierChanged || changed
		} else {
			return false, err
		}
	}

	return frontierChanged, nil
}

// maybeUpdatePartitionProgress polls the frontier and updates the job progress with
// partition-specific information to track the status of each partition.
func (sf *streamIngestionFrontier) maybeUpdatePartitionProgress() error {
	ctx := sf.Ctx
	updateFreq := JobCheckpointFrequency.Get(&sf.flowCtx.Cfg.Settings.SV)
	if updateFreq == 0 || timeutil.Since(sf.lastPartitionUpdate) < updateFreq {
		return nil
	}
	registry := sf.flowCtx.Cfg.JobRegistry
	jobID := jobspb.JobID(sf.spec.JobID)
	f := sf.frontier
	job, err := registry.LoadJob(ctx, jobID)
	if err != nil {
		return err
	}

	frontierResolvedSpans := make([]jobspb.ResolvedSpan, 0)
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		frontierResolvedSpans = append(frontierResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})

	partitionProgress := sf.partitionProgress

	sf.lastPartitionUpdate = timeutil.Now()

	sf.metrics.JobProgressUpdates.Inc(1)
	sf.metrics.FrontierCheckpointSpanCount.Update(int64(len(frontierResolvedSpans)))

	// TODO(pbardea): Only update partitions that have changed.
	return job.FractionProgressed(ctx, nil, /* txn */
		func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			prog := details.(*jobspb.Progress_StreamIngest).StreamIngest

			prog.PartitionProgress = partitionProgress
			prog.Checkpoint.ResolvedSpans = frontierResolvedSpans
			// "FractionProgressed" isn't relevant on jobs that are streaming in changes.
			return 0.0
		},
	)
}
