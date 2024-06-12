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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var logicalReplicationWriterResultType = []*types.T{
	types.Bytes, // jobspb.ResolvedSpans
}

var minimumFlushInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"logical_replication.consumer.minimum_flush_interval",
	"the minimum timestamp between flushes; flushes may still occur if internal buffers fill up",
	5*time.Second,
)

var targetKVBufferLen = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.kv_buffer_target_length",
	"the maximum length of the KV buffer allowed before a flush",
	32,
)

var maxKVBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.kv_buffer_size",
	"the maximum size of the KV buffer allowed before a flush",
	128<<20, // 128 MiB
)

var flushBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.batch_size",
	"the number of row updates to attempt in a single KV transaction",
	32,
	settings.NonNegativeInt,
)

var quantize = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"logical_replication.consumer.timestamp_granularity",
	"the granularity at which replicated times are quantized to make tracking more efficient",
	5*time.Second,
)

// logicalReplicationWriterProcessor started life as a copy/pasta fork of the
// streamIngestionProcessor.
//
// We _may_ want to refactor this to just _be_ the stream ingestion processor
// with some different dependencies injected.
type logicalReplicationWriterProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.LogicalReplicationWriterSpec

	bh BatchHandler

	buffer *ingestionBuffer

	maxFlushRateTimer timeutil.Timer

	// client is a streaming client which provides a stream of events from a given
	// address.
	forceClientForTests streamclient.Client
	// streamPartitionClients are a collection of streamclient.Client created for
	// consuming multiple partitions from a stream.
	streamPartitionClients []streamclient.Client

	// frontier keeps track of the progress for the spans tracked by this processor
	// and is used forward resolved spans
	frontier span.Frontier
	// lastFlushTime keeps track of the last time that we flushed due to a
	// checkpoint timestamp event.
	lastFlushTime     time.Time
	lastFlushFrontier hlc.Timestamp

	// workerGroup is a context group holding all goroutines
	// related to this processor.
	workerGroup ctxgroup.Group

	// subscriptionGroup is different from workerGroup since we
	// want to explicitly cancel the context related to it.
	subscriptionGroup  ctxgroup.Group
	subscriptionCancel context.CancelFunc

	// stopCh stops flush loop.
	stopCh chan struct{}

	mergedSubscription *streamingest.MergedSubscription

	flushInProgress atomic.Bool
	flushCh         chan flushableBuffer

	errCh chan error

	checkpointCh chan *jobspb.ResolvedSpans

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	logBufferEvery log.EveryN

	debug streampb.DebugLogicalConsumerStatus
}

var (
	_ execinfra.Processor = &logicalReplicationWriterProcessor{}
	_ execinfra.RowSource = &logicalReplicationWriterProcessor{}
)

const logicalReplicationWriterProcessorName = "logical-replication-writer-processor"

func newLogicalReplicationWriterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.LogicalReplicationWriterSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	trackedSpans := make([]roachpb.Span, 0)
	for _, partitionSpec := range spec.PartitionSpecs {
		trackedSpans = append(trackedSpans, partitionSpec.Spans...)
	}

	frontier, err := span.MakeFrontierAt(spec.PreviousReplicatedTimestamp, trackedSpans...)
	if err != nil {
		return nil, err
	}
	for _, resolvedSpan := range spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return nil, err
		}
	}
	const udfName = "defaultdb.public.resolve"
	// rp, err := makeSQLLastWriteWinsProcessor(ctx, flowCtx.Codec(), flowCtx.Cfg.Settings, spec.TableDescriptors)
	// if err != nil {
	// 	return nil, err
	// }
	rp, err := makeSQLUDFProcessor(ctx, flowCtx.Codec(), flowCtx.Cfg.Settings, udfName, spec.TableDescriptors)
	if err != nil {
		return nil, err
	}
	bh := &txnBatch{
		db: flowCtx.Cfg.DB,
		rp: rp,
	}
	lrw := &logicalReplicationWriterProcessor{
		flowCtx:        flowCtx,
		spec:           spec,
		bh:             bh,
		frontier:       frontier,
		buffer:         &ingestionBuffer{},
		stopCh:         make(chan struct{}),
		flushCh:        make(chan flushableBuffer),
		checkpointCh:   make(chan *jobspb.ResolvedSpans),
		errCh:          make(chan error, 1),
		logBufferEvery: log.Every(30 * time.Second),
		debug: streampb.DebugLogicalConsumerStatus{
			StreamID:    streampb.StreamID(spec.StreamID),
			ProcessorID: processorID,
		},
	}
	if err := lrw.Init(ctx, lrw, post, logicalReplicationWriterResultType, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				lrw.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	return lrw, nil
}

// Start launches a set of goroutines that read from the spans
// assigned to this processor, parses each row, and generates inserts
// or deletes to update local tables of the same name.
//
// A group of subscriptions is merged into a single event stream that
// is read by the consumeEvents loop.
//
// The consumeEvents loop builds a buffer of KVs that it then sends to
// the flushLoop. We currently allow 1 in-flight flush.
//
//	client.Subscribe -> mergedSubscription -> consumeEvents -> flushLoop -> Next()
//
// All errors are reported to Next() via errCh, with the first
// error winning.
//
// Start implements the RowSource interface.
func (lrw *logicalReplicationWriterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", lrw.spec.JobID)
	streampb.RegisterActiveLogicalConsumerStatus(&lrw.debug)

	ctx = lrw.StartInternal(ctx, logicalReplicationWriterProcessorName)

	lrw.metrics = lrw.flowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

	db := lrw.FlowCtx.Cfg.DB

	var subscriptionCtx context.Context
	subscriptionCtx, lrw.subscriptionCancel = context.WithCancel(lrw.Ctx())
	lrw.subscriptionGroup = ctxgroup.WithContext(subscriptionCtx)
	lrw.workerGroup = ctxgroup.WithContext(lrw.Ctx())

	log.Infof(ctx, "starting logical replication writer (partitions: %d)", len(lrw.spec.PartitionSpecs))

	// Initialize the event streams.
	subscriptions := make(map[string]streamclient.Subscription)
	lrw.streamPartitionClients = make([]streamclient.Client, 0)
	for _, partitionSpec := range lrw.spec.PartitionSpecs {
		id := partitionSpec.PartitionID
		token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
		addr := partitionSpec.Address
		redactedAddr, redactedErr := streamclient.RedactSourceURI(addr)
		if redactedErr != nil {
			log.Warning(lrw.Ctx(), "could not redact stream address")
		}
		var streamClient streamclient.Client
		if lrw.forceClientForTests != nil {
			streamClient = lrw.forceClientForTests
			log.Infof(ctx, "using testing client")
		} else {
			var err error
			streamClient, err = streamclient.NewStreamClient(ctx, streamingccl.StreamAddress(addr), db,
				streamclient.WithStreamID(streampb.StreamID(lrw.spec.StreamID)),
				streamclient.WithCompression(true),
			)
			if err != nil {
				lrw.MoveToDrainingAndLogError(errors.Wrapf(err, "creating client for partition spec %q from %q", token, redactedAddr))
				return
			}
			lrw.streamPartitionClients = append(lrw.streamPartitionClients, streamClient)
		}

		if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
			if streamingKnobs != nil && streamingKnobs.BeforeClientSubscribe != nil {
				streamingKnobs.BeforeClientSubscribe(addr, string(token), lrw.frontier)
			}
		}

		sub, err := streamClient.Subscribe(ctx,
			streampb.StreamID(lrw.spec.StreamID),
			int32(lrw.flowCtx.NodeID.SQLInstanceID()), lrw.ProcessorID,
			token,
			lrw.spec.InitialScanTimestamp, lrw.frontier,
			streamclient.WithFiltering(true),
		)

		if err != nil {
			lrw.MoveToDrainingAndLogError(errors.Wrapf(err, "consuming partition %v", redactedAddr))
			return
		}
		subscriptions[id] = sub
		lrw.subscriptionGroup.GoCtx(func(ctx context.Context) error {
			if err := sub.Subscribe(ctx); err != nil {
				lrw.sendError(errors.Wrap(err, "subscription"))
			}
			return nil
		})
	}

	lrw.mergedSubscription = streamingest.MergeSubscriptions(lrw.Ctx(), subscriptions)
	lrw.workerGroup.GoCtx(func(ctx context.Context) error {
		if err := lrw.mergedSubscription.Run(); err != nil {
			lrw.sendError(errors.Wrap(err, "merge subscription"))
		}
		return nil
	})
	lrw.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(lrw.flushCh)
		if err := lrw.consumeEvents(ctx); err != nil {
			lrw.sendError(errors.Wrap(err, "consume events"))
		}
		return nil
	})
	lrw.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(lrw.checkpointCh)
		if err := lrw.flushLoop(ctx); err != nil {
			lrw.sendError(errors.Wrap(err, "flush loop"))
		}
		return nil
	})
}

// Next is part of the RowSource interface.
func (lrw *logicalReplicationWriterProcessor) Next() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if lrw.State != execinfra.StateRunning {
		return nil, lrw.DrainHelper()
	}

	select {
	case progressUpdate, ok := <-lrw.checkpointCh:
		if ok {
			progressBytes, err := protoutil.Marshal(progressUpdate)
			if err != nil {
				lrw.MoveToDrainingAndLogError(err)
				return nil, lrw.DrainHelper()
			}
			row := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
			}
			return row, nil
		}
	case err := <-lrw.errCh:
		lrw.MoveToDrainingAndLogError(err)
		return nil, lrw.DrainHelper()
	}
	select {
	case err := <-lrw.errCh:
		lrw.MoveToDrainingAndLogError(err)
		return nil, lrw.DrainHelper()
	default:
		lrw.MoveToDrainingAndLogError(nil /* error */)
		return nil, lrw.DrainHelper()
	}
}

func (lrw *logicalReplicationWriterProcessor) MoveToDrainingAndLogError(err error) {
	if err != nil {
		log.Infof(lrw.Ctx(), "gracefully draining with error %s", err)
	}
	lrw.MoveToDraining(err)
}

// MustBeStreaming implements the Processor interface.
func (lrw *logicalReplicationWriterProcessor) MustBeStreaming() bool {
	return true
}

// ConsumerClosed is part of the RowSource interface.
func (lrw *logicalReplicationWriterProcessor) ConsumerClosed() {
	lrw.close()
}

func (lrw *logicalReplicationWriterProcessor) close() {
	streampb.UnregisterActiveLogicalConsumerStatus(&lrw.debug)

	if lrw.Closed {
		return
	}

	defer lrw.frontier.Release()

	// Stop the partition client and mergedSubscription. All other
	// goroutines should exit based on channel close events.
	for _, client := range lrw.streamPartitionClients {
		_ = client.Close(lrw.Ctx())
	}
	if lrw.mergedSubscription != nil {
		lrw.mergedSubscription.Close()
	}
	if lrw.stopCh != nil {
		close(lrw.stopCh)
	}

	// We shouldn't need to explicitly cancel the context for
	// members of the worker group. The mergedSubscription close
	// and stopCh close above should result in exit signals being
	// sent to all relevant goroutines.
	if err := lrw.workerGroup.Wait(); err != nil {
		log.Errorf(lrw.Ctx(), "error on close(): %s", err)
	}

	if lrw.subscriptionCancel != nil {
		lrw.subscriptionCancel()
	}
	if err := lrw.subscriptionGroup.Wait(); err != nil {
		log.Errorf(lrw.Ctx(), "error on close(): %s", err)
	}
	lrw.maxFlushRateTimer.Stop()

	lrw.InternalClose()
}

func (lrw *logicalReplicationWriterProcessor) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case lrw.errCh <- err:
	default:
		log.VInfof(lrw.Ctx(), 2, "dropping additional error: %s", err)
	}
}

func (lrw *logicalReplicationWriterProcessor) flushLoop(_ context.Context) error {
	for {
		bufferToFlush, ok := <-lrw.flushCh
		if !ok {
			// eventConsumer is done.
			return nil
		}
		lrw.flushInProgress.Store(true)
		resolvedSpan, err := lrw.flushBuffer(bufferToFlush)
		if err != nil {
			return err
		}

		// NB: The flushLoop needs to select on stopCh here
		// because the reader of checkpointCh is the caller of
		// Next(). But there might never be another Next()
		// call.
		select {
		case lrw.checkpointCh <- resolvedSpan:
		case <-lrw.stopCh:
			return nil
		}
		lrw.flushInProgress.Store(false)
	}
}

// consumeEvents handles processing events on the merged event queue and returns
// once the event channel has closed.
func (lrw *logicalReplicationWriterProcessor) consumeEvents(ctx context.Context) error {
	minFlushInterval := minimumFlushInterval.Get(&lrw.flowCtx.Cfg.Settings.SV)
	lrw.maxFlushRateTimer.Reset(minFlushInterval)
	for {
		select {
		case event, ok := <-lrw.mergedSubscription.Events():
			if !ok {
				// eventCh is closed, flush and exit.
				if err := lrw.flush(flushOnClose); err != nil {
					return err
				}
				return nil
			}
			if err := lrw.handleEvent(event); err != nil {
				return err
			}
		case <-lrw.maxFlushRateTimer.C:
			lrw.maxFlushRateTimer.Read = true
			minFlushInterval = minimumFlushInterval.Get(&lrw.flowCtx.Cfg.Settings.SV)
			if timeutil.Since(lrw.lastFlushTime) >= minFlushInterval {
				if err := lrw.maybeFlush(flushOnTime); err != nil {
					return err
				}
			}
			lrw.maxFlushRateTimer.Reset(minFlushInterval)
		}
	}
}

func (lrw *logicalReplicationWriterProcessor) handleEvent(event streamingest.PartitionEvent) error {
	sv := &lrw.FlowCtx.Cfg.Settings.SV

	if event.Type() == streamingccl.KVEvent {
		lrw.metrics.AdmitLatency.RecordValue(
			timeutil.Since(event.GetKVs()[0].Value.Timestamp.GoTime()).Nanoseconds())
	}

	if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.RunAfterReceivingEvent != nil {
			if err := streamingKnobs.RunAfterReceivingEvent(lrw.Ctx()); err != nil {
				return err
			}
		}
	}

	switch event.Type() {
	case streamingccl.KVEvent:
		if err := lrw.bufferKVs(event.GetKVs()); err != nil {
			return err
		}
	case streamingccl.CheckpointEvent:
		if err := lrw.bufferCheckpoint(event); err != nil {
			return err
		}
	case streamingccl.SSTableEvent, streamingccl.DeleteRangeEvent:
		// TODO(ssd): Handle SSTableEvent here eventually. I'm not sure
		// we'll ever want to truly handle DeleteRangeEvent since
		// currently those are only used by DROP which should be handled
		// via whatever mechanism handles schema changes.
		return errors.Newf("unexpected event for online stream: %v", event)
	case streamingccl.SplitEvent:
		log.Infof(lrw.Ctx(), "SplitEvent received on logical replication stream")
	default:
		return errors.Newf("unknown streaming event type %v", event.Type())
	}

	if lrw.logBufferEvery.ShouldLog() {
		log.Infof(lrw.Ctx(), "current KV batch size %d (%d items)", lrw.buffer.curKVBatchSize, len(lrw.buffer.curKVBatch))
	}

	shouldFlush, mustFlush := lrw.buffer.shouldFlushOnKVSize(lrw.Ctx(), sv)
	if mustFlush {
		if err := lrw.flush(flushOnSize); err != nil {
			return err
		}
	} else if shouldFlush {
		if err := lrw.maybeFlush(flushOnSize); err != nil {
			return err
		}
	}
	return nil
}

func (lrw *logicalReplicationWriterProcessor) bufferKVs(kvs []roachpb.KeyValue) error {
	if kvs == nil {
		return errors.New("kv event expected to have kv")
	}
	for _, kv := range kvs {
		lrw.buffer.addKV(kv)
	}
	return nil
}

func (lrw *logicalReplicationWriterProcessor) bufferCheckpoint(
	event streamingest.PartitionEvent,
) error {
	if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.ElideCheckpointEvent != nil {
			if streamingKnobs.ElideCheckpointEvent(lrw.FlowCtx.NodeID.SQLInstanceID(), lrw.frontier.Frontier()) {
				return nil
			}
		}
	}

	resolvedSpans := event.GetResolvedSpans()
	if resolvedSpans == nil {
		return errors.New("checkpoint event expected to have resolved spans")
	}

	d := quantize.Get(&lrw.EvalCtx.Settings.SV)
	for _, resolvedSpan := range resolvedSpans {
		// If quantizing is enabled, round the timestamp down to an even multiple of
		// the quantization amount, to maximize the number of spans that share the
		// same resolved timestamp -- even if they were individually resolved to
		// _slightly_ different/newer timestamps -- to allow them to merge into
		// fewer and larger spans in the frontier.
		if d > 0 && resolvedSpan.Timestamp.After(lrw.spec.InitialScanTimestamp) {
			resolvedSpan.Timestamp.Logical = 0
			resolvedSpan.Timestamp.WallTime -= resolvedSpan.Timestamp.WallTime % int64(d)
		}
		_, err := lrw.frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
		if err != nil {
			return errors.Wrap(err, "unable to forward checkpoint frontier")
		}
	}

	lrw.metrics.CheckpointEvents.Inc(1)
	return nil
}

func (lrw *logicalReplicationWriterProcessor) maybeFlush(reason flushReason) error {
	// TODO (ssd): This is racy but I didn't want to think about it hard yet.
	if lrw.flushInProgress.Load() {
		return nil
	}
	if len(lrw.buffer.curKVBatch) == 0 && lrw.frontier.Frontier().LessEq(lrw.lastFlushFrontier) {
		return nil
	}
	return lrw.flush(reason)
}

type flushReason int

const (
	flushOnSize flushReason = iota
	flushOnTime
	flushOnClose
)

func (lrw *logicalReplicationWriterProcessor) flush(reason flushReason) error {
	switch reason {
	case flushOnSize:
		lrw.metrics.FlushOnSize.Inc(1)
	case flushOnTime:
		lrw.metrics.FlushOnTime.Inc(1)
	}

	bufferToFlush := lrw.buffer
	lrw.buffer = getBuffer()

	checkpoint := &jobspb.ResolvedSpans{ResolvedSpans: make([]jobspb.ResolvedSpan, 0, lrw.frontier.Len())}
	lrw.frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if !ts.IsEmpty() {
			checkpoint.ResolvedSpans = append(checkpoint.ResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		}
		return span.ContinueMatch
	})
	thisFlushFrontier := lrw.frontier.Frontier()

	flushRequestStartTime := timeutil.Now()
	select {
	case lrw.flushCh <- flushableBuffer{
		buffer:     bufferToFlush,
		checkpoint: checkpoint,
	}:
		lrw.lastFlushFrontier = thisFlushFrontier
		lrw.lastFlushTime = timeutil.Now()
		lrw.metrics.FlushWaitHistNanos.RecordValue(timeutil.Since(flushRequestStartTime).Nanoseconds())
		return nil
	case <-lrw.stopCh:
		// We return on stopCh here because our flush process
		// may have been stopped or exited on error.
		return nil
	}
}

// flushBuffer flushes the given flusableBufferand returns the underlying streamIngestionBuffer to the pool.
func (lrw *logicalReplicationWriterProcessor) flushBuffer(
	b flushableBuffer,
) (*jobspb.ResolvedSpans, error) {
	ctx, sp := tracing.ChildSpan(lrw.Ctx(), "logical-replication-writer-flush")
	defer sp.Finish()

	if len(b.buffer.curKVBatch) == 0 {
		releaseBuffer(b.buffer)
		return b.checkpoint, nil
	}

	// Ensure the batcher is always reset, even on early error returns.
	preFlushTime := timeutil.Now()
	// TODO: The batching here in production would need to be much
	// smarter. Namely, we don't want to include updates to the
	// same key in the same batch. Also, it's possible batching
	// will make things much worse in practice.
	flushByteSize := 0
	batchStart := 0
	batchSize := int(flushBatchSize.Get(&lrw.EvalCtx.Settings.SV))
	batchEnd := min(batchStart+batchSize, len(b.buffer.curKVBatch))
	for batchStart < len(b.buffer.curKVBatch) && batchEnd != 0 {
		preBatchTime := timeutil.Now()
		batchStats, err := lrw.bh.HandleBatch(ctx, b.buffer.curKVBatch[batchStart:batchEnd])
		if err != nil {
			// TODO(ssd): Handle errors. We should perhaps split the batch and retry a portion of the batch.
			// If that fails, send the failed application to the dead-letter-queue.
			return nil, err
		}
		flushByteSize += batchStats.byteSize
		batchStart = batchEnd
		batchEnd = min(batchStart+batchSize, len(b.buffer.curKVBatch))
		lrw.metrics.BatchBytesHist.RecordValue(int64(batchStats.byteSize))
		lrw.metrics.BatchHistNanos.RecordValue(timeutil.Since(preBatchTime).Nanoseconds())

	}

	lrw.metrics.Flushes.Inc(1)
	lrw.metrics.FlushHistNanos.RecordValue(timeutil.Since(preFlushTime).Nanoseconds())
	lrw.metrics.FlushRowCountHist.RecordValue(int64(len(b.buffer.curKVBatch)))
	lrw.metrics.FlushBytesHist.RecordValue(int64(flushByteSize))
	lrw.metrics.IngestedLogicalBytes.Inc(int64(flushByteSize))
	lrw.metrics.CommitLatency.RecordValue(timeutil.Since(b.buffer.minTimestamp.GoTime()).Nanoseconds())
	lrw.metrics.IngestedEvents.Inc(int64(len(b.buffer.curKVBatch)))

	releaseBuffer(b.buffer)

	return b.checkpoint, nil
}

type batchStats struct {
	byteSize int
}

type BatchHandler interface {
	HandleBatch(context.Context, []roachpb.KeyValue) (batchStats, error)
}

// RowProcessor knows how to process a single row from an event stream.
type RowProcessor interface {
	ProcessRow(context.Context, isql.Txn, roachpb.KeyValue) error
}

type txnBatch struct {
	db descs.DB
	rp RowProcessor
}

func (t *txnBatch) HandleBatch(ctx context.Context, batch []roachpb.KeyValue) (batchStats, error) {
	ctx, sp := tracing.ChildSpan(ctx, "txnBatch.HandleBatch")
	defer sp.Finish()

	stats := batchStats{}
	err := t.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// TODO(ssd): For now, we SetOmitInRangefeeds to
		// prevent the data from being emitted back to the source.
		// However, I don't think we want to do this in the long run.
		// Rather, we want to store the inbound cluster ID and store that
		// in a way that allows us to choose to filter it out from or not.
		// Doing it this way means that you can't choose to run CDC just from
		// one side and not the other.
		txn.KV().SetOmitInRangefeeds()
		for _, kv := range batch {
			stats.byteSize += kv.Size()
			if err := t.rp.ProcessRow(ctx, txn, kv); err != nil {
				return err
			}

		}
		return nil
	}, isql.WithSessionData(&sessiondata.SessionData{
		SessionData: sessiondatapb.SessionData{
			Database: "defaultdb",
		},
	}))
	return stats, err
}

type flushableBuffer struct {
	buffer     *ingestionBuffer
	checkpoint *jobspb.ResolvedSpans
}

// streamIngestionBuffer is a local buffer for KVs.
//
// TODO(ssd): We want to sort curKVBatch on MVCC timestamp.

// TOOD(ssd): We may want to sort curKVBatch based on schema topology.
type ingestionBuffer struct {
	curKVBatch     []roachpb.KeyValue
	curKVBatchSize int

	// Minimum timestamp in the current batch. Used for metrics purpose.
	minTimestamp hlc.Timestamp
}

func (b *ingestionBuffer) addKV(kv roachpb.KeyValue) {
	b.curKVBatchSize += kv.Size()
	b.curKVBatch = append(b.curKVBatch, kv)
	if kv.Value.Timestamp.Less(b.minTimestamp) {
		b.minTimestamp = kv.Value.Timestamp
	}
}

func (b *ingestionBuffer) reset() {
	b.minTimestamp = hlc.MaxTimestamp
	b.curKVBatchSize = 0
	b.curKVBatch = b.curKVBatch[:0]
}

// shouldFlushOnKVSize returns two bools indicating whether the buffer
// should be flushed if possible or wether it must be flushed based on
// the overal size limit.
func (b *ingestionBuffer) shouldFlushOnKVSize(
	ctx context.Context, sv *settings.Values,
) (shouldFlush bool, mustFlush bool) {
	kvBufMax := int(maxKVBufferSize.Get(sv))
	kvBufLenTarget := int(targetKVBufferLen.Get(sv))
	if kvBufMax > 0 && b.curKVBatchSize >= kvBufMax {
		log.VInfof(ctx, 2, "flushing because current KV batch based on size %d >= %d", b.curKVBatchSize, kvBufMax)
		return true, true
	} else if len(b.curKVBatch) >= kvBufLenTarget {
		return true, false
	}
	return false, false
}

var bufferPool = sync.Pool{
	New: func() interface{} { return &ingestionBuffer{} },
}

func getBuffer() *ingestionBuffer {
	return bufferPool.Get().(*ingestionBuffer)
}

func releaseBuffer(b *ingestionBuffer) {
	b.reset()
	bufferPool.Put(b)
}

func init() {
	rowexec.NewLogicalReplicationWriterProcessor = newLogicalReplicationWriterProcessor
}
