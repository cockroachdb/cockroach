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

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
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

// PartitionProgressFrequency controls the frequency of partition progress checkopints.
var PartitionProgressFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"streaming.partition_progress_frequency",
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
	frontier, err := span.MakeFrontier(spec.TrackedSpans...)
	if err != nil {
		return nil, err
	}
	heartbeatSender, err := newHeartbeatSender(flowCtx, spec)
	if err != nil {
		return nil, err
	}
	sf := &streamIngestionFrontier{
		flowCtx:           flowCtx,
		spec:              spec,
		input:             input,
		highWaterAtStart:  spec.HighWaterAtStart,
		frontier:          frontier,
		partitionProgress: make(map[string]jobspb.StreamIngestionProgress_PartitionProgress),
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
	// Send signal to stopChan to stop heartbeat sender.
	stopChan chan struct{}
	// heartbeatSender closes this channel when it stops.
	stoppedChan chan struct{}
}

func newHeartbeatSender(
	flowCtx *execinfra.FlowCtx, spec execinfrapb.StreamIngestionFrontierSpec,
) (*heartbeatSender, error) {
	streamClient, err := streamclient.NewStreamClient(streamingccl.StreamAddress(spec.StreamAddress))
	if err != nil {
		return nil, err
	}
	return &heartbeatSender{
		client:          streamClient,
		streamID:        streaming.StreamID(spec.StreamID),
		flowCtx:         flowCtx,
		frontierUpdates: make(chan hlc.Timestamp),
		stopChan:        make(chan struct{}),
		stoppedChan:     make(chan struct{}),
	}, nil
}

func (h *heartbeatSender) maybeHeartbeat(ctx context.Context, frontier hlc.Timestamp) error {
	heartbeatFrequency := streamingccl.StreamReplicationConsumerHeartbeatFrequency.Get(&h.flowCtx.EvalCtx.Settings.SV)
	if h.lastSent.Add(heartbeatFrequency).After(timeutil.Now()) {
		return nil
	}
	h.lastSent = timeutil.Now()
	return h.client.Heartbeat(ctx, h.streamID, frontier)
}

func (h *heartbeatSender) startHeartbeatLoop(ctx context.Context) {
	h.cg = ctxgroup.WithContext(ctx)
	h.cg.GoCtx(func(ctx context.Context) error {
		sendHeartbeats := func() error {
			// The heartbeat thread send heartbeats when there is a frontier update,
			// and it has been a while since last time we sent it, or when we need
			// to heartbeat to keep the stream alive even if the frontier has no update.
			timer := time.NewTimer(streamingccl.StreamReplicationConsumerHeartbeatFrequency.
				Get(&h.flowCtx.EvalCtx.Settings.SV))
			defer timer.Stop()
			unknownStatusErr := log.Every(1 * time.Minute)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-h.stopChan:
					return nil
				case <-timer.C:
					timer.Reset(streamingccl.StreamReplicationConsumerHeartbeatFrequency.
						Get(&h.flowCtx.EvalCtx.Settings.SV))
				case frontier := <-h.frontierUpdates:
					h.frontier.Forward(frontier)
				}
				err := h.maybeHeartbeat(ctx, h.frontier)
				if err == nil {
					continue
				}

				var se streamingccl.StreamStatusErr
				if !errors.As(err, &se) {
					return errors.Wrap(err, "unknown stream status error")
				}

				if se.StreamStatus == streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY {
					if unknownStatusErr.ShouldLog() {
						log.Warningf(ctx, "replication stream %d has unknown status error", se.StreamID)
					}
					continue
				}
				// The replication stream is either paused or inactive.
				return err
			}
		}
		err := errors.CombineErrors(sendHeartbeats(), h.client.Close())
		close(h.stoppedChan)
		return err
	})
}

// Stop the heartbeat loop and returns any error at time of heartbeatSender's exit.
// Should be called at most once.
func (h *heartbeatSender) stop() error {
	close(h.stopChan) // Panic if closed multiple times
	return h.cg.Wait()
}

// Wait for heartbeatSender to be stopped and returns any error.
func (h *heartbeatSender) err() error {
	return h.cg.Wait()
}

// Start is part of the RowSource interface.
func (sf *streamIngestionFrontier) Start(ctx context.Context) {
	ctx = sf.StartInternal(ctx, streamIngestionFrontierProcName)
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

		if err := sf.maybeUpdatePartitionProgress(); err != nil {
			// Updating the partition progress isn't a fatal error.
			log.Errorf(sf.Ctx, "failed to update partition progress: %+v", err)
		}

		var frontierChanged bool
		var err error
		if frontierChanged, err = sf.noteResolvedTimestamps(row[0]); err != nil {
			sf.MoveToDraining(err)
			break
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
			sf.MoveToDraining(sf.heartbeatSender.err())
			return nil, sf.DrainHelper()
		}
	}
	return nil, sf.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (sf *streamIngestionFrontier) ConsumerClosed() {
	if sf.InternalClose() {
		if err := sf.heartbeatSender.stop(); err != nil {
			log.Errorf(sf.Ctx, "heartbeatSender exited with error: %s", err.Error())
		}
	}
}

// noteResolvedTimestamps processes a batch of resolved timestamp events, and
// returns whether the frontier has moved forward after processing the batch.
func (sf *streamIngestionFrontier) noteResolvedTimestamps(
	resolvedSpanDatums rowenc.EncDatum,
) (bool, error) {
	var frontierChanged bool
	if err := resolvedSpanDatums.EnsureDecoded(streamIngestionResultTypes[0], &sf.alloc); err != nil {
		return frontierChanged, err
	}
	raw, ok := resolvedSpanDatums.Datum.(*tree.DBytes)
	if !ok {
		return frontierChanged, errors.AssertionFailedf(`unexpected datum type %T: %s`,
			resolvedSpanDatums.Datum, resolvedSpanDatums.Datum)
	}
	var resolvedSpans jobspb.ResolvedSpans
	if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
		return frontierChanged, errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved timestamp: %x`, raw)
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
	updateFreq := PartitionProgressFrequency.Get(&sf.flowCtx.Cfg.Settings.SV)
	if updateFreq == 0 || timeutil.Since(sf.lastPartitionUpdate) < updateFreq {
		return nil
	}
	registry := sf.flowCtx.Cfg.JobRegistry
	jobID := jobspb.JobID(sf.spec.JobID)
	f := sf.frontier
	allSpans := roachpb.Span{
		Key:    keys.MinKey,
		EndKey: keys.MaxKey,
	}
	partitionFrontiers := sf.partitionProgress
	job, err := registry.LoadJob(ctx, jobID)
	if err != nil {
		return err
	}

	f.SpanEntries(allSpans, func(span roachpb.Span, timestamp hlc.Timestamp) (done span.OpResult) {
		partitionKey := span.Key
		partition := string(partitionKey)
		if curFrontier, ok := partitionFrontiers[partition]; !ok {
			partitionFrontiers[partition] = jobspb.StreamIngestionProgress_PartitionProgress{
				IngestedTimestamp: timestamp,
			}
		} else if curFrontier.IngestedTimestamp.Less(timestamp) {
			curFrontier.IngestedTimestamp = timestamp
		}
		return true
	})

	sf.lastPartitionUpdate = timeutil.Now()
	// TODO(pbardea): Only update partitions that have changed.
	return job.FractionProgressed(ctx, nil, /* txn */
		func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			prog := details.(*jobspb.Progress_StreamIngest).StreamIngest
			prog.PartitionProgress = partitionFrontiers
			// "FractionProgressed" isn't relevant on jobs that are streaming in
			// changes.
			return 0.0
		},
	)
}
