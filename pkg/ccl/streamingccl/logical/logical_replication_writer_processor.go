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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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
	settings.WithPublic,
)

var targetKVBufferLen = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.kv_buffer_target_len",
	"the maximum size of the KV buffer allowed before a flush",
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
	"logical_repplication.consumer.batch_size",
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

	ie          isql.Executor
	decoder     cdcevent.Decoder
	queryBuffer queryBuffer
	buffer      *ingestionBuffer

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

	// stopCh stops the cutover poller and flush loop.
	stopCh chan struct{}

	mergedSubscription *streamingest.MergedSubscription

	flushInProgress atomic.Bool
	flushCh         chan flushableBuffer

	errCh chan error

	checkpointCh chan *jobspb.ResolvedSpans

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	logBufferEvery log.EveryN
}

type queryBuffer struct {
	deleteQueries map[catid.DescID]string
	insertQueries map[catid.DescID]string
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

	qb := queryBuffer{
		deleteQueries: make(map[catid.DescID]string, len(spec.TableDescriptors)),
		insertQueries: make(map[catid.DescID]string, len(spec.TableDescriptors)),
	}

	descs := make(map[catid.DescID]catalog.TableDescriptor)

	cdcEventTargets := changefeedbase.Targets{}
	for name, desc := range spec.TableDescriptors {
		td := tabledesc.NewBuilder(&desc).BuildImmutableTable()
		descs[desc.ID] = td
		qb.deleteQueries[desc.ID] = makeDeleteQuery(name, td)
		qb.insertQueries[desc.ID] = makeInsertQuery(name, td)
		cdcEventTargets.Add(changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
			TableID:           td.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(td.GetName()),
		})
	}

	rfCache, err := cdcevent.NewFixedRowFetcherCache(ctx, flowCtx.Codec(), flowCtx.Cfg.Settings, cdcEventTargets, descs)
	if err != nil {
		return nil, err
	}

	decoder := cdcevent.NewEventDecoderWithCache(ctx, rfCache, false, false)

	sip := &logicalReplicationWriterProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		ie: flowCtx.Cfg.DB.Executor(isql.WithSessionData(&sessiondata.SessionData{
			LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
				// TODO(ssd): For now, we set DisableChangefeedReplication to
				// prevent the data from being emitted back to the source.
				// However, I don't think we want to do this in the long run.
				// Rather, we want to store the inbound cluster ID and store that
				// in a way that allows us to choose to filter it out from or not.
				// Doing it this way means that you can't choose to run CDC just from
				// one side and not the other.
				DisableChangefeedReplication: true,
			},
		})),
		queryBuffer:    qb,
		decoder:        decoder,
		frontier:       frontier,
		buffer:         &ingestionBuffer{},
		stopCh:         make(chan struct{}),
		flushCh:        make(chan flushableBuffer),
		checkpointCh:   make(chan *jobspb.ResolvedSpans),
		errCh:          make(chan error, 1),
		logBufferEvery: log.Every(30 * time.Second),
	}
	if err := sip.Init(ctx, sip, post, logicalReplicationWriterResultType, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				sip.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	return sip, nil
}

// Start launches a set of goroutines that read from the spans
// assigned to this processor and ingests them until cutover is
// reached.
//
// A group of subscriptions is merged into a single event stream that
// is read by the consumeEvents loop.
//
// The consumeEvents loop builds a buffer of KVs that it then sends to
// the flushLoop. We currently allow 1 in-flight flush.
//
// A polling loop watches the cutover time and signals the
// consumeEvents loop to stop ingesting.
//
//	client.Subscribe -> mergedSubscription -> consumeEvents -> flushLoop -> Next()
//
// All errors are reported to Next() via errCh, with the first
// error winning.
//
// Start implements the RowSource interface.
func (sip *logicalReplicationWriterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", sip.spec.JobID)

	ctx = sip.StartInternal(ctx, logicalReplicationWriterProcessorName)

	sip.metrics = sip.flowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

	db := sip.FlowCtx.Cfg.DB

	var subscriptionCtx context.Context
	subscriptionCtx, sip.subscriptionCancel = context.WithCancel(sip.Ctx())
	sip.subscriptionGroup = ctxgroup.WithContext(subscriptionCtx)
	sip.workerGroup = ctxgroup.WithContext(sip.Ctx())

	log.Infof(ctx, "starting logical replication writer (partitions: %d)", len(sip.spec.PartitionSpecs))

	// Initialize the event streams.
	subscriptions := make(map[string]streamclient.Subscription)
	sip.streamPartitionClients = make([]streamclient.Client, 0)
	for _, partitionSpec := range sip.spec.PartitionSpecs {
		id := partitionSpec.PartitionID
		token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
		addr := partitionSpec.Address
		redactedAddr, redactedErr := streamclient.RedactSourceURI(addr)
		if redactedErr != nil {
			log.Warning(sip.Ctx(), "could not redact stream address")
		}
		var streamClient streamclient.Client
		if sip.forceClientForTests != nil {
			streamClient = sip.forceClientForTests
			log.Infof(ctx, "using testing client")
		} else {
			var err error
			streamClient, err = streamclient.NewStreamClient(ctx, streamingccl.StreamAddress(addr), db,
				streamclient.WithStreamID(streampb.StreamID(sip.spec.StreamID)),
				streamclient.WithCompression(true),
			)
			if err != nil {
				sip.MoveToDrainingAndLogError(errors.Wrapf(err, "creating client for partition spec %q from %q", token, redactedAddr))
				return
			}
			sip.streamPartitionClients = append(sip.streamPartitionClients, streamClient)
		}

		if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
			if streamingKnobs != nil && streamingKnobs.BeforeClientSubscribe != nil {
				streamingKnobs.BeforeClientSubscribe(addr, string(token), sip.frontier)
			}
		}

		sub, err := streamClient.Subscribe(ctx, streampb.StreamID(sip.spec.StreamID), int32(sip.flowCtx.NodeID.SQLInstanceID()), token,
			sip.spec.InitialScanTimestamp, sip.frontier, streamclient.WithFiltering(true))

		if err != nil {
			sip.MoveToDrainingAndLogError(errors.Wrapf(err, "consuming partition %v", redactedAddr))
			return
		}
		subscriptions[id] = sub
		sip.subscriptionGroup.GoCtx(func(ctx context.Context) error {
			if err := sub.Subscribe(ctx); err != nil {
				sip.sendError(errors.Wrap(err, "subscription"))
			}
			return nil
		})
	}

	sip.mergedSubscription = streamingest.MergeSubscriptions(sip.Ctx(), subscriptions)
	sip.workerGroup.GoCtx(func(ctx context.Context) error {
		if err := sip.mergedSubscription.Run(); err != nil {
			sip.sendError(errors.Wrap(err, "merge subscription"))
		}
		return nil
	})
	sip.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(sip.flushCh)
		if err := sip.consumeEvents(ctx); err != nil {
			sip.sendError(errors.Wrap(err, "consume events"))
		}
		return nil
	})
	sip.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(sip.checkpointCh)
		if err := sip.flushLoop(ctx); err != nil {
			sip.sendError(errors.Wrap(err, "flush loop"))
		}
		return nil
	})
}

// Next is part of the RowSource interface.
func (sip *logicalReplicationWriterProcessor) Next() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if sip.State != execinfra.StateRunning {
		return nil, sip.DrainHelper()
	}

	select {
	case progressUpdate, ok := <-sip.checkpointCh:
		if ok {
			progressBytes, err := protoutil.Marshal(progressUpdate)
			if err != nil {
				sip.MoveToDrainingAndLogError(err)
				return nil, sip.DrainHelper()
			}
			row := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
			}
			return row, nil
		}
	case err := <-sip.errCh:
		sip.MoveToDrainingAndLogError(err)
		return nil, sip.DrainHelper()
	}
	select {
	case err := <-sip.errCh:
		sip.MoveToDrainingAndLogError(err)
		return nil, sip.DrainHelper()
	default:
		sip.MoveToDrainingAndLogError(nil /* error */)
		return nil, sip.DrainHelper()
	}
}

func (sip *logicalReplicationWriterProcessor) MoveToDrainingAndLogError(err error) {
	if err != nil {
		log.Infof(sip.Ctx(), "gracefully draining with error %s", err)
	}
	sip.MoveToDraining(err)
}

// MustBeStreaming implements the Processor interface.
func (sip *logicalReplicationWriterProcessor) MustBeStreaming() bool {
	return true
}

// ConsumerClosed is part of the RowSource interface.
func (sip *logicalReplicationWriterProcessor) ConsumerClosed() {
	sip.close()
}

func (sip *logicalReplicationWriterProcessor) close() {
	if sip.Closed {
		return
	}

	defer sip.frontier.Release()

	// Stop the partition client, mergedSubscription, and
	// cutoverPoller. All other goroutines should exit based on
	// channel close events.
	for _, client := range sip.streamPartitionClients {
		_ = client.Close(sip.Ctx())
	}
	if sip.mergedSubscription != nil {
		sip.mergedSubscription.Close()
	}
	if sip.stopCh != nil {
		close(sip.stopCh)
	}

	// We shouldn't need to explicitly cancel the context for
	// members of the worker group. The mergedSubscription close
	// and stopCh close above should result in exit signals being
	// sent to all relevant goroutines.
	if err := sip.workerGroup.Wait(); err != nil {
		log.Errorf(sip.Ctx(), "error on close(): %s", err)
	}

	if sip.subscriptionCancel != nil {
		sip.subscriptionCancel()
	}
	if err := sip.subscriptionGroup.Wait(); err != nil {
		log.Errorf(sip.Ctx(), "error on close(): %s", err)
	}
	sip.maxFlushRateTimer.Stop()

	sip.InternalClose()
}

func (sip *logicalReplicationWriterProcessor) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case sip.errCh <- err:
	default:
		log.VInfof(sip.Ctx(), 2, "dropping additional error: %s", err)
	}
}

func (sip *logicalReplicationWriterProcessor) flushLoop(_ context.Context) error {
	for {
		bufferToFlush, ok := <-sip.flushCh
		if !ok {
			// eventConsumer is done.
			return nil
		}
		sip.flushInProgress.Store(true)
		resolvedSpan, err := sip.flushBuffer(bufferToFlush)
		if err != nil {
			return err
		}

		// NB: The flushLoop needs to select on stopCh here
		// because the reader of checkpointCh is the caller of
		// Next(). But there might never be another Next()
		// call.
		select {
		case sip.checkpointCh <- resolvedSpan:
		case <-sip.stopCh:
			return nil
		}
		sip.flushInProgress.Store(false)
	}
}

// consumeEvents handles processing events on the merged event queue and returns
// once a checkpoint event has been emitted so that it can inform the downstream
// frontier processor to consider updating the frontier.
//
// It should only make a claim that about the resolved timestamp of a partition
// increasing after it has flushed all KV events previously received by that
// partition.
func (sip *logicalReplicationWriterProcessor) consumeEvents(ctx context.Context) error {
	minFlushInterval := minimumFlushInterval.Get(&sip.flowCtx.Cfg.Settings.SV)
	sip.maxFlushRateTimer.Reset(minFlushInterval)
	for {
		select {
		case event, ok := <-sip.mergedSubscription.Events():
			if !ok {
				// eventCh is closed, flush and exit.
				if err := sip.flush(); err != nil {
					return err
				}
				return nil
			}
			if err := sip.handleEvent(event); err != nil {
				return err
			}
		case <-sip.maxFlushRateTimer.C:
			sip.maxFlushRateTimer.Read = true
			minFlushInterval = minimumFlushInterval.Get(&sip.flowCtx.Cfg.Settings.SV)
			if timeutil.Since(sip.lastFlushTime) >= minFlushInterval {
				sip.metrics.FlushOnTime.Inc(1)
				if err := sip.maybeFlush(); err != nil {
					return err
				}
			}
			sip.maxFlushRateTimer.Reset(minFlushInterval)
		}
	}
}

func (sip *logicalReplicationWriterProcessor) handleEvent(event streamingest.PartitionEvent) error {
	sv := &sip.FlowCtx.Cfg.Settings.SV

	if event.Type() == streamingccl.KVEvent {
		sip.metrics.AdmitLatency.RecordValue(
			timeutil.Since(event.GetKVs()[0].Value.Timestamp.GoTime()).Nanoseconds())
	}

	if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.RunAfterReceivingEvent != nil {
			if err := streamingKnobs.RunAfterReceivingEvent(sip.Ctx()); err != nil {
				return err
			}
		}
	}

	switch event.Type() {
	case streamingccl.KVEvent:
		if err := sip.bufferKVs(event.GetKVs()); err != nil {
			return err
		}
	case streamingccl.CheckpointEvent:
		if err := sip.bufferCheckpoint(event); err != nil {
			return err
		}
	case streamingccl.SSTableEvent, streamingccl.DeleteRangeEvent:
		// TODO(ssd): Handle SSTableEvent here eventually. I'm not sure
		// we'll ever want to truly handle DeleteRangeEvent since
		// currently those are only used by DROP which should be handled
		// via whatever mechanism handles schema changes.
		return errors.Newf("unexpected event for online stream: %v", event)
	case streamingccl.SplitEvent:
		log.Infof(sip.Ctx(), "SplitEvent received on logical replication stream")
	default:
		return errors.Newf("unknown streaming event type %v", event.Type())
	}

	if sip.logBufferEvery.ShouldLog() {
		log.Infof(sip.Ctx(), "current KV batch size %d (%d items)", sip.buffer.curKVBatchSize, len(sip.buffer.curKVBatch))
	}

	shouldFlush, mustFlush := sip.buffer.shouldFlushOnKVSize(sip.Ctx(), sv)
	if mustFlush {
		sip.metrics.FlushOnSize.Inc(1)
		if err := sip.flush(); err != nil {
			return err
		}
	} else if shouldFlush {
		sip.metrics.FlushOnSize.Inc(1)
		if err := sip.maybeFlush(); err != nil {
			return err
		}
	}
	return nil
}

func (sip *logicalReplicationWriterProcessor) bufferKVs(kvs []roachpb.KeyValue) error {
	if kvs == nil {
		return errors.New("kv event expected to have kv")
	}
	for _, kv := range kvs {
		sip.buffer.addKV(kv)
	}
	return nil
}

func (sip *logicalReplicationWriterProcessor) bufferCheckpoint(
	event streamingest.PartitionEvent,
) error {
	if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.ElideCheckpointEvent != nil {
			if streamingKnobs.ElideCheckpointEvent(sip.FlowCtx.NodeID.SQLInstanceID(), sip.frontier.Frontier()) {
				return nil
			}
		}
	}

	resolvedSpans := event.GetResolvedSpans()
	if resolvedSpans == nil {
		return errors.New("checkpoint event expected to have resolved spans")
	}

	d := quantize.Get(&sip.EvalCtx.Settings.SV)
	for _, resolvedSpan := range resolvedSpans {
		// If quantizing is enabled, round the timestamp down to an even multiple of
		// the quantization amount, to maximize the number of spans that share the
		// same resolved timestamp -- even if they were individually resolved to
		// _slightly_ different/newer timestamps -- to allow them to merge into
		// fewer and larger spans in the frontier.
		if d > 0 && resolvedSpan.Timestamp.After(sip.spec.InitialScanTimestamp) {
			resolvedSpan.Timestamp.Logical = 0
			resolvedSpan.Timestamp.WallTime -= resolvedSpan.Timestamp.WallTime % int64(d)
		}
		_, err := sip.frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
		if err != nil {
			return errors.Wrap(err, "unable to forward checkpoint frontier")
		}
	}

	sip.metrics.ResolvedEvents.Inc(1)
	return nil
}

func (sip *logicalReplicationWriterProcessor) maybeFlush() error {
	// TODO (ssd): This is racy but I didn't want to think about it hard yet.
	if sip.flushInProgress.Load() {
		return nil
	}
	if len(sip.buffer.curKVBatch) == 0 && sip.frontier.Frontier().LessEq(sip.lastFlushFrontier) {
		return nil
	}
	return sip.flush()
}

func (sip *logicalReplicationWriterProcessor) flush() error {
	log.Infof(sip.Ctx(), "flushing")
	bufferToFlush := sip.buffer
	sip.buffer = getBuffer()

	checkpoint := &jobspb.ResolvedSpans{ResolvedSpans: make([]jobspb.ResolvedSpan, 0, sip.frontier.Len())}
	sip.frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if !ts.IsEmpty() {
			checkpoint.ResolvedSpans = append(checkpoint.ResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		}
		return span.ContinueMatch
	})
	thisFlushFrontier := sip.frontier.Frontier()

	flushRequestStartTime := timeutil.Now()
	select {
	case sip.flushCh <- flushableBuffer{
		buffer:     bufferToFlush,
		checkpoint: checkpoint,
	}:
		sip.lastFlushFrontier = thisFlushFrontier
		sip.lastFlushTime = timeutil.Now()
		sip.metrics.FlushWaitHistNanos.RecordValue(timeutil.Since(flushRequestStartTime).Nanoseconds())
		return nil
	case <-sip.stopCh:
		// We return on stopCh here because our flush process
		// may have been stopped or exited on error.
		return nil
	}
}

// flushBuffer flushes the given flusableBufferand returns the underlying streamIngestionBuffer to the pool.
func (sip *logicalReplicationWriterProcessor) flushBuffer(
	b flushableBuffer,
) (*jobspb.ResolvedSpans, error) {
	ctx, sp := tracing.ChildSpan(sip.Ctx(), "logical-replication-writer-flush")
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
	batchSize := int(flushBatchSize.Get(&sip.EvalCtx.Settings.SV))
	batchEnd := min(batchStart+batchSize, len(b.buffer.curKVBatch))
	for batchStart < len(b.buffer.curKVBatch) && batchEnd != 0 {
		var batchByteSize int
		preBatchTime := timeutil.Now()
		if err := sip.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			txn.KV().SetOmitInRangefeeds()
			var err error
			batchByteSize, err = sip.flushBatch(ctx, txn, b.buffer.curKVBatch[batchStart:batchEnd])
			return err
		}); err != nil {
			// TODO: We'll want to retry this to handle
			return nil, err
		}
		flushByteSize += batchByteSize
		batchStart = batchEnd
		batchEnd = min(batchStart+batchSize, len(b.buffer.curKVBatch))

		sip.metrics.BatchBytesHist.RecordValue(int64(batchByteSize))
		sip.metrics.BatchHistNanos.RecordValue(timeutil.Since(preBatchTime).Nanoseconds())
	}

	sip.metrics.Flushes.Inc(1)
	sip.metrics.FlushHistNanos.RecordValue(timeutil.Since(preFlushTime).Nanoseconds())
	sip.metrics.FlushRowCountHist.RecordValue(int64(len(b.buffer.curKVBatch)))
	sip.metrics.FlushBytesHist.RecordValue(int64(flushByteSize))
	sip.metrics.IngestedLogicalBytes.Inc(int64(flushByteSize))
	sip.metrics.CommitLatency.RecordValue(timeutil.Since(b.buffer.minTimestamp.GoTime()).Nanoseconds())
	sip.metrics.IngestedEvents.Inc(int64(len(b.buffer.curKVBatch)))

	releaseBuffer(b.buffer)

	return b.checkpoint, nil
}

func (sip *logicalReplicationWriterProcessor) flushBatch(
	ctx context.Context, txn isql.Txn, batch []roachpb.KeyValue,
) (int, error) {
	batchBytes := 0
	for _, kv := range batch {
		batchBytes += kv.Size()
		row, err := sip.decoder.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
		if err != nil {
			return batchBytes, err
		}
		if !row.IsDeleted() {
			datums := make([]interface{}, 0, len(row.EncDatums()))
			err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
				// Ignore crdb_internal_origin_timestamp
				if col.Name == "crdb_internal_origin_timestamp" {
					if d != tree.DNull {
						// We'd only see this if we are doing an initial-scan of a table that was previously ingested into.
						log.Infof(ctx, "saw non-null crdb_internal_origin_timestamp: %v", d)
					}
					return nil
				}

				datums = append(datums, d)
				return nil
			})
			if err != nil {
				return batchBytes, err
			}
			datums = append(datums, eval.TimestampToDecimalDatum(row.MvccTimestamp))
			insertQuery := sip.queryBuffer.insertQueries[row.TableID]
			if _, err := sip.ie.Exec(ctx, "replicated-insert", txn.KV(), insertQuery, datums...); err != nil {
				log.Warningf(ctx, "replicated insert failed (query: %s): %s", insertQuery, err.Error())
				return batchBytes, err
			}
		} else {
			datums := make([]interface{}, 0, len(row.TableDescriptor().TableDesc().PrimaryIndex.KeyColumnNames))
			err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
				datums = append(datums, d)
				return nil
			})
			if err != nil {
				return batchBytes, err
			}
			deleteQuery := sip.queryBuffer.deleteQueries[row.TableID]
			if _, err := sip.ie.Exec(ctx, "replicated-delete", txn.KV(), deleteQuery, datums...); err != nil {
				log.Warningf(ctx, "replicated delete failed (query: %s): %s", deleteQuery, err.Error())
				return batchBytes, err
			}
		}
	}
	return batchBytes, nil
}

// Last-write-wins INSERT and DELETE queries.
//
// These implement partial last-write-wins semantics. We assume that the table
// has an crdb_internal_origin_timestamp column defined as:
//
//	crdb_internal_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL
//
// This row is explicitly set by the INSERT query using the MVCC timestamp of
// the inbound write.
//
// Known issues:
//
//  1. An UPDATE and a DELETE may be applied out of order because we have no way
//     from SQL of knowing the write timestamp of the deletion tombstone.
//  2. The crd_internal_origin_timestamp requires modifying the user's schema.
//
// See the design document for possible solutions to both of these problems.
func makeInsertQuery(fqTableName string, td catalog.TableDescriptor) string {
	// TODO(ssd): Column families
	var columnNames strings.Builder
	var valueStrings strings.Builder
	var onConflictUpdateClause strings.Builder
	argIdx := 1
	for _, col := range td.PublicColumns() {
		// Virtual columns are not present in the data written to disk and
		// thus not part of the rangefeed datum.
		if col.IsVirtual() {
			continue
		}
		// We will set crdb_internal_origin_timestamp ourselves from the MVCC timestamp of the incoming datum.
		// We should never see this on the rangefeed as a non-null value as that would imply we've looped data around.
		if col.GetName() == "crdb_internal_origin_timestamp" {
			continue
		}
		if argIdx == 1 {
			columnNames.WriteString(col.GetName())
			fmt.Fprintf(&valueStrings, "$%d", argIdx)
			fmt.Fprintf(&onConflictUpdateClause, "%s = $%d", col.GetName(), argIdx)
		} else {
			fmt.Fprintf(&columnNames, ", %s", col.GetName())
			fmt.Fprintf(&valueStrings, ", $%d", argIdx)
			fmt.Fprintf(&onConflictUpdateClause, ",\n%s = $%d", col.GetName(), argIdx)
		}
		argIdx++
	}
	originTSIdx := argIdx
	baseQuery := `
INSERT INTO %s (%s, crdb_internal_origin_timestamp)
VALUES (%s, %d)
ON CONFLICT ON CONSTRAINT %s
DO UPDATE SET
%s,
crdb_internal_origin_timestamp=$%[4]d
WHERE (%[1]s.crdb_internal_mvcc_timestamp < $%[4]d
       AND %[1]s.crdb_internal_origin_timestamp IS NULL)
   OR (%[1]s.crdb_internal_origin_timestamp < $%[4]d
       AND %[1]s.crdb_internal_origin_timestamp IS NOT NULL)`
	return fmt.Sprintf(baseQuery,
		fqTableName,
		columnNames.String(),
		valueStrings.String(),
		originTSIdx,
		td.GetPrimaryIndex().GetName(),
		onConflictUpdateClause.String(),
	)
}

func makeDeleteQuery(fqTableName string, td catalog.TableDescriptor) string {
	var whereClause strings.Builder
	names := td.TableDesc().PrimaryIndex.KeyColumnNames
	for i := 0; i < len(names); i++ {
		if i == 0 {
			fmt.Fprintf(&whereClause, "%s = $%d", names[i], i+1)
		} else {
			fmt.Fprintf(&whereClause, "AND %s = $%d", names[i], i+1)
		}
	}
	originTSIdx := len(names) + 1
	baseQuery := `
DELETE FROM %s WHERE %s
   AND (%[1]s.crdb_internal_mvcc_timestamp < $%[3]d
        AND %[1]s.crdb_internal_origin_timestamp IS NULL)
    OR (%[1]s.crdb_internal_origin_timestamp < $%[3]d
        AND tab.crdb_internal_origin_timestamp IS NOT NULL)`

	return fmt.Sprintf(baseQuery, fqTableName, whereClause.String(), originTSIdx)
}

type flushableBuffer struct {
	buffer     *ingestionBuffer
	checkpoint *jobspb.ResolvedSpans
}

// streamIngestionBuffer is a local buffer for KVs.
//
// TODO(ssd): We want to sort curKVBatch on MVCC timestamp.

// TOOD(ssd): We may wan tto sort curKVBatch based on schema topology.
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

func (b *ingestionBuffer) shouldFlushOnKVSize(
	ctx context.Context, sv *settings.Values,
) (bool, bool) {
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
