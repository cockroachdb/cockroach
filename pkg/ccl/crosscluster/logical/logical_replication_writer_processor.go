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
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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

var flushBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.batch_size",
	"the number of row updates to attempt in a single KV transaction",
	32,
	settings.NonNegativeInt,
)

// logicalReplicationWriterProcessor consumes a cross-cluster replication stream
// by decoding kvs in it to logical changes and applying them by executing DMLs.
type logicalReplicationWriterProcessor struct {
	execinfra.ProcessorBase

	spec execinfrapb.LogicalReplicationWriterSpec

	bh []BatchHandler

	getBatchSize func() int

	streamPartitionClient streamclient.Client

	// frontier keeps track of the progress for the spans tracked by this processor
	// and is used forward resolved spans
	frontier span.Frontier

	// workerGroup is a context group holding all goroutines
	// related to this processor.
	workerGroup ctxgroup.Group

	subscription       streamclient.Subscription
	subscriptionCancel context.CancelFunc

	// stopCh stops flush loop.
	stopCh chan struct{}

	errCh chan error

	checkpointCh chan []jobspb.ResolvedSpan

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	logBufferEvery log.EveryN

	debug streampb.DebugLogicalConsumerStatus

	dlqClient DeadLetterQueueClient

	purgatory purgatory
}

var (
	_ execinfra.Processor = &logicalReplicationWriterProcessor{}
	_ execinfra.RowSource = &logicalReplicationWriterProcessor{}
)

const useKVWriter = false

const logicalReplicationWriterProcessorName = "logical-replication-writer-processor"

func newLogicalReplicationWriterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.LogicalReplicationWriterSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	frontier, err := span.MakeFrontierAt(spec.PreviousReplicatedTimestamp, spec.PartitionSpec.Spans...)
	if err != nil {
		return nil, err
	}
	for _, resolvedSpan := range spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return nil, err
		}
	}

	tableConfigs := make(map[descpb.ID]sqlProcessorTableConfig)
	srcTableIDToName := make(map[int32]dstTableMetadata)
	for dstTableID, md := range spec.TableMetadata {
		desc := md.SourceDescriptor
		tableConfigs[descpb.ID(dstTableID)] = sqlProcessorTableConfig{
			srcDesc: tabledesc.NewBuilder(&desc).BuildImmutableTable(),
			dstOID:  md.DestinationFunctionOID,
		}

		srcTableID := int32(desc.GetID())
		srcTableIDToName[srcTableID] = dstTableMetadata{
			database: md.DestinationParentDatabaseName,
			schema:   md.DestinationParentSchemaName,
			table:    md.DestinationTableName,
			tableID:  dstTableID,
		}
	}
	bhPool := make([]BatchHandler, maxWriterWorkers)
	for i := range bhPool {
		var rp RowProcessor
		if useKVWriter {
			rp, err = newKVRowProcessor(ctx, flowCtx.Cfg, flowCtx.EvalCtx, tableConfigs)
			if err != nil {
				return nil, err
			}
		} else {
			rp, err = makeSQLProcessor(
				ctx, flowCtx.Cfg.Settings, tableConfigs,
				jobspb.JobID(spec.JobID),
				// Initialize the executor with a fresh session data - this will
				// avoid creating a new copy on each executor usage.
				flowCtx.Cfg.DB.Executor(isql.WithSessionData(sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */))),
			)
			if err != nil {
				return nil, err
			}
		}
		bhPool[i] = &txnBatch{
			db:       flowCtx.Cfg.DB,
			rp:       rp,
			settings: flowCtx.Cfg.Settings,
			sd:       sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */),
		}
	}

	dlqDbExec := flowCtx.Cfg.DB.Executor(isql.WithSessionData(sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */)))

	var numTablesWithSecondaryIndexes int
	for _, tc := range tableConfigs {
		if len(tc.srcDesc.NonPrimaryIndexes()) > 0 {
			numTablesWithSecondaryIndexes++
		}
	}

	lrw := &logicalReplicationWriterProcessor{
		spec: spec,
		getBatchSize: func() int {
			// We want to decide whether to use implicit txns or not based on
			// the schema of the target table. Benchmarking has shown that
			// implicit txns are beneficial on tables with no secondary indexes
			// whereas explicit txns are beneficial when at least one secondary
			// index is present.
			//
			// Unfortunately, if we have multiple replication pairs, we don't
			// know which tables will be affected by this batch before deciding
			// on the batch size, so we'll use a heuristic such that we'll use
			// the implicit txns if at least half of the target tables are
			// without the secondary indexes. If we only have a single
			// replication pair, then this heuristic gives us the precise
			// recommendation.
			//
			// (Here we have access to the descriptor of the source table, but
			// for now we assume that the source and the target descriptors are
			// similar.)
			if 2*numTablesWithSecondaryIndexes < len(tableConfigs) && useImplicitTxns.Get(&flowCtx.Cfg.Settings.SV) {
				return 1
			}
			return int(flushBatchSize.Get(&flowCtx.Cfg.Settings.SV))
		},
		bh:             bhPool,
		frontier:       frontier,
		stopCh:         make(chan struct{}),
		checkpointCh:   make(chan []jobspb.ResolvedSpan),
		errCh:          make(chan error, 1),
		logBufferEvery: log.Every(30 * time.Second),
		debug: streampb.DebugLogicalConsumerStatus{
			StreamID:    streampb.StreamID(spec.StreamID),
			ProcessorID: processorID,
		},
		dlqClient: InitDeadLetterQueueClient(dlqDbExec, srcTableIDToName),
		metrics:   flowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics),
	}
	lrw.purgatory = purgatory{
		deadline:    func() time.Duration { return retryQueueAgeLimit.Get(&flowCtx.Cfg.Settings.SV) },
		delay:       func() time.Duration { return retryQueueBackoff.Get(&flowCtx.Cfg.Settings.SV) },
		byteLimit:   func() int64 { return retryQueueSizeLimit.Get(&flowCtx.Cfg.Settings.SV) },
		flush:       lrw.flushBuffer,
		checkpoint:  lrw.checkpoint,
		bytesGauge:  lrw.metrics.RetryQueueBytes,
		eventsGauge: lrw.metrics.RetryQueueEvents,
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
// A subscription's event stream is read by the consumeEvents loop.
//
//	client.Subscribe -> consumeEvents -> Next()
//
// All errors are reported to Next() via errCh, with the first
// error winning.
//
// Start implements the RowSource interface.
func (lrw *logicalReplicationWriterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(logtags.AddTag(ctx, "job", lrw.spec.JobID), "part", lrw.spec.PartitionSpec.PartitionID)
	streampb.RegisterActiveLogicalConsumerStatus(&lrw.debug)

	ctx = lrw.StartInternal(ctx, logicalReplicationWriterProcessorName)

	lrw.metrics = lrw.FlowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

	db := lrw.FlowCtx.Cfg.DB

	log.Infof(ctx, "starting logical replication writer for partition %s", lrw.spec.PartitionSpec.PartitionID)

	// Start the subscription for our partition.
	partitionSpec := lrw.spec.PartitionSpec
	token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
	addr := partitionSpec.Address
	redactedAddr, redactedErr := streamclient.RedactSourceURI(addr)
	if redactedErr != nil {
		log.Warning(lrw.Ctx(), "could not redact stream address")
	}
	streamClient, err := streamclient.NewStreamClient(ctx, crosscluster.StreamAddress(addr), db,
		streamclient.WithStreamID(streampb.StreamID(lrw.spec.StreamID)),
		streamclient.WithCompression(true),
		streamclient.WithLogical(),
	)
	if err != nil {
		lrw.MoveToDrainingAndLogError(errors.Wrapf(err, "creating client for partition spec %q from %q", token, redactedAddr))
		return
	}
	lrw.streamPartitionClient = streamClient

	if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.BeforeClientSubscribe != nil {
			streamingKnobs.BeforeClientSubscribe(addr, string(token), lrw.frontier)
		}
	}
	sub, err := streamClient.Subscribe(ctx,
		streampb.StreamID(lrw.spec.StreamID),
		int32(lrw.FlowCtx.NodeID.SQLInstanceID()), lrw.ProcessorID,
		token,
		lrw.spec.InitialScanTimestamp, lrw.frontier,
		streamclient.WithFiltering(true),
		streamclient.WithDiff(true),
	)
	if err != nil {
		lrw.MoveToDrainingAndLogError(errors.Wrapf(err, "subscribing to partition from %s", redactedAddr))
		return
	}

	// We use a different context for the subscription here so
	// that we can explicitly cancel it.
	var subscriptionCtx context.Context
	subscriptionCtx, lrw.subscriptionCancel = context.WithCancel(lrw.Ctx())
	lrw.workerGroup = ctxgroup.WithContext(lrw.Ctx())
	lrw.subscription = sub
	lrw.workerGroup.GoCtx(func(_ context.Context) error {
		if err := sub.Subscribe(subscriptionCtx); err != nil {
			lrw.sendError(errors.Wrap(err, "subscription"))
		}
		return nil
	})
	lrw.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(lrw.checkpointCh)
		if err := lrw.consumeEvents(ctx); err != nil {
			lrw.sendError(errors.Wrap(err, "consume events"))
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
	case resolved, ok := <-lrw.checkpointCh:
		if ok {
			progressUpdate := &jobspb.ResolvedSpans{ResolvedSpans: resolved}
			progressBytes, err := protoutil.Marshal(progressUpdate)
			if err != nil {
				lrw.MoveToDrainingAndLogError(err)
				return nil, lrw.DrainHelper()
			}
			row := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
			}
			return row, nil
		} else {
			select {
			case err := <-lrw.errCh:
				lrw.MoveToDrainingAndLogError(err)
				return nil, lrw.DrainHelper()
			case <-time.After(10 * time.Second):
				logcrash.ReportOrPanic(lrw.Ctx(), &lrw.FlowCtx.Cfg.Settings.SV,
					"event channel closed but no error found on err channel after 10 seconds")
				lrw.MoveToDrainingAndLogError(nil /* error */)
				return nil, lrw.DrainHelper()
			}
		}
	case err := <-lrw.errCh:
		lrw.MoveToDrainingAndLogError(err)
		return nil, lrw.DrainHelper()
	}
}

func (lrw *logicalReplicationWriterProcessor) MoveToDrainingAndLogError(err error) {
	if err != nil {
		log.Infof(lrw.Ctx(), "gracefully draining with error: %s", err)
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

	for _, b := range lrw.bh {
		b.Close(lrw.Ctx())
	}

	defer lrw.frontier.Release()

	if lrw.streamPartitionClient != nil {
		_ = lrw.streamPartitionClient.Close(lrw.Ctx())
	}
	if lrw.stopCh != nil {
		close(lrw.stopCh)
	}
	if lrw.subscriptionCancel != nil {
		lrw.subscriptionCancel()
	}

	// We shouldn't need to explicitly cancel the context for members of the
	// worker group. The client close and stopCh close above should result
	// in exit signals being sent to all relevant goroutines.
	if err := lrw.workerGroup.Wait(); err != nil {
		log.Errorf(lrw.Ctx(), "error on close(): %s", err)
	}

	// Update the global retry queue gauges to reflect that this queue is going
	// away, including everything in it that is included in those gauges.
	lrw.purgatory.bytesGauge.Dec(lrw.purgatory.bytes)
	for _, i := range lrw.purgatory.levels {
		lrw.purgatory.eventsGauge.Dec(int64(len(i.events)))
	}

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

// consumeEvents handles processing events on the event queue and returns once
// the event channel has closed.
func (lrw *logicalReplicationWriterProcessor) consumeEvents(ctx context.Context) error {
	before := timeutil.Now()
	for event := range lrw.subscription.Events() {
		lrw.debug.RecordRecv(timeutil.Since(before))
		before = timeutil.Now()
		if err := lrw.handleEvent(ctx, event); err != nil {
			return err
		}
	}
	return lrw.subscription.Err()
}

func (lrw *logicalReplicationWriterProcessor) handleEvent(
	ctx context.Context, event crosscluster.Event,
) error {
	if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.RunAfterReceivingEvent != nil {
			if err := streamingKnobs.RunAfterReceivingEvent(lrw.Ctx()); err != nil {
				return err
			}
		}
	}

	switch event.Type() {
	case crosscluster.KVEvent:
		if err := lrw.handleStreamBuffer(ctx, event.GetKVs()); err != nil {
			return err
		}
	case crosscluster.CheckpointEvent:
		if err := lrw.maybeCheckpoint(ctx, event.GetResolvedSpans()); err != nil {
			return err
		}
	case crosscluster.SSTableEvent, crosscluster.DeleteRangeEvent:
		// TODO(ssd): Handle SSTableEvent here eventually. I'm not sure
		// we'll ever want to truly handle DeleteRangeEvent since
		// currently those are only used by DROP which should be handled
		// via whatever mechanism handles schema changes.
		return errors.Newf("unexpected event for online stream: %v", event)
	case crosscluster.SplitEvent:
		log.Infof(lrw.Ctx(), "SplitEvent received on logical replication stream")
	default:
		return errors.Newf("unknown streaming event type %v", event.Type())
	}
	return nil
}

func (lrw *logicalReplicationWriterProcessor) maybeCheckpoint(
	ctx context.Context, resolvedSpans []jobspb.ResolvedSpan,
) error {
	// If purgatory is non-empty, it intercepts the checkpoint and then we can try
	// to drain it.
	if !lrw.purgatory.Empty() {
		lrw.purgatory.Checkpoint(ctx, resolvedSpans)
		return lrw.purgatory.Drain(ctx)
	}

	return lrw.checkpoint(ctx, resolvedSpans)
}

func (lrw *logicalReplicationWriterProcessor) checkpoint(
	ctx context.Context, resolvedSpans []jobspb.ResolvedSpan,
) error {
	if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.ElideCheckpointEvent != nil {
			if streamingKnobs.ElideCheckpointEvent(lrw.FlowCtx.NodeID.SQLInstanceID(), lrw.frontier.Frontier()) {
				return nil
			}
		}
	}

	if resolvedSpans == nil {
		return errors.New("checkpoint event expected to have resolved spans")
	}

	for _, resolvedSpan := range resolvedSpans {
		_, err := lrw.frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
		if err != nil {
			return errors.Wrap(err, "unable to forward checkpoint frontier")
		}
	}

	select {
	case lrw.checkpointCh <- resolvedSpans:
	case <-ctx.Done():
		return ctx.Err()
	case <-lrw.stopCh:
		// we need to select on stopCh here because the reader
		// of checkpointCh is the caller of Next(). But there
		// might never be another Next() call since it may
		// have exited based on an error.
		return nil
	}
	lrw.metrics.CheckpointEvents.Inc(1)
	return nil
}

// handleStreamBuffer handles a buffer of KV events from the incoming stream.
func (lrw *logicalReplicationWriterProcessor) handleStreamBuffer(
	ctx context.Context, kvs []streampb.StreamEvent_KV,
) error {
	const notRetry = false
	unapplied, unappliedBytes, err := lrw.flushBuffer(ctx, kvs, notRetry, lrw.purgatory.Enabled())
	if err != nil {
		return err
	}
	// Put any events that failed to apply into purgatory (flushing if needed).
	if err := lrw.purgatory.Store(ctx, unapplied, unappliedBytes); err != nil {
		return err
	}

	return nil
}

func filterRemaining(kvs []streampb.StreamEvent_KV) []streampb.StreamEvent_KV {
	remaining := kvs
	var j int
	for i := range kvs {
		if len(kvs[i].KeyValue.Key) != 0 {
			remaining[j] = kvs[i]
			j++
		}
	}
	// If remaining shrunk by half or more, reallocate it to avoid aliasing.
	if j < len(kvs)/2 {
		return append(remaining[:0:0], remaining[:j]...)
	}
	return remaining[:j]
}

const maxWriterWorkers = 32

// flushBuffer processes some or all of the events in the passed buffer, and
// zeros out each event in the passed buffer for which it successfully completed
// processing either by applying it or by sending it to a DLQ. If mustProcess is
// true it must process every event in the buffer, one way or another, while if
// it is false it may elect to leave an event in the buffer to indicate that
// processing of that event did not complete, for example if application failed
// but it was not sent to the DLQ, and thus should remain buffered for a later
// retry.
func (lrw *logicalReplicationWriterProcessor) flushBuffer(
	ctx context.Context, kvs []streampb.StreamEvent_KV, isRetry bool, canRetry retryEligibility,
) (notProcessed []streampb.StreamEvent_KV, notProcessedByteSize int64, _ error) {
	ctx, sp := tracing.ChildSpan(ctx, "logical-replication-writer-flush")
	defer sp.Finish()

	if len(kvs) == 0 {
		return nil, 0, nil
	}

	preFlushTime := timeutil.Now()

	// Inform the debugging helper that a flush is starting and configure failure
	// injection if it indicates it is requested.
	testingFailPercent := lrw.debug.RecordFlushStart(preFlushTime, int64(len(kvs)))
	if testingFailPercent > 0 {
		for i := range lrw.bh {
			lrw.bh[i].SetSyntheticFailurePercent(testingFailPercent)
		}
		defer func() {
			for i := range lrw.bh {
				lrw.bh[i].SetSyntheticFailurePercent(0)
			}
		}()
	}

	// k returns the row key for some KV event, truncated if needed to the row key
	// prefix.
	k := func(kv streampb.StreamEvent_KV) roachpb.Key {
		if p, err := keys.EnsureSafeSplitKey(kv.KeyValue.Key); err == nil {
			return p
		}
		return kv.KeyValue.Key
	}

	firstKeyTS := kvs[0].KeyValue.Value.Timestamp.GoTime()

	slices.SortFunc(kvs, func(a, b streampb.StreamEvent_KV) int {
		if c := k(a).Compare(k(b)); c != 0 {
			return c
		}
		return a.KeyValue.Value.Timestamp.Compare(b.KeyValue.Value.Timestamp)
	})

	const minChunkSize = 64
	chunkSize := max((len(kvs)/len(lrw.bh))+1, minChunkSize)

	perChunkStats := make([]flushStats, len(lrw.bh))

	todo := kvs
	g := ctxgroup.WithContext(ctx)
	for worker := range lrw.bh {
		if len(todo) == 0 {
			break
		}
		// The chunk should end after the first new key after chunk size.
		chunkEnd := min(chunkSize, len(todo))
		for chunkEnd < len(todo) && k(todo[chunkEnd-1]).Equal(k(todo[chunkEnd])) {
			chunkEnd++
		}
		chunk := todo[0:chunkEnd]
		todo = todo[len(chunk):]
		bh := lrw.bh[worker]

		g.GoCtx(func(ctx context.Context) error {
			s, err := lrw.flushChunk(ctx, bh, chunk, canRetry)
			if err != nil {
				return err
			}
			perChunkStats[worker] = s
			lrw.metrics.OptimisticInsertConflictCount.Inc(s.optimisticInsertConflicts)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, 0, err
	}

	var stats flushStats
	for _, i := range perChunkStats {
		stats.Add(i)
	}

	if stats.notProcessed.count > 0 {
		notProcessed = filterRemaining(kvs)
	}

	flushTime := timeutil.Since(preFlushTime).Nanoseconds()
	lrw.debug.RecordFlushComplete(flushTime, int64(len(kvs)), stats.processed.bytes)

	lrw.metrics.AppliedRowUpdates.Inc(stats.processed.success)
	lrw.metrics.DLQedRowUpdates.Inc(stats.processed.dlq)

	lrw.metrics.CommitToCommitLatency.RecordValue(timeutil.Since(firstKeyTS).Nanoseconds())

	if isRetry {
		lrw.metrics.RetriedApplySuccesses.Inc(stats.processed.success)
		lrw.metrics.RetriedApplyFailures.Inc(stats.notProcessed.count + stats.processed.dlq)
	} else {
		lrw.metrics.InitialApplySuccesses.Inc(stats.processed.success)
		lrw.metrics.InitialApplyFailures.Inc(stats.notProcessed.count + stats.processed.dlq)
		lrw.metrics.StreamBatchNanosHist.RecordValue(flushTime)
		lrw.metrics.StreamBatchRowsHist.RecordValue(int64(len(kvs)))
		lrw.metrics.StreamBatchBytesHist.RecordValue(stats.processed.bytes + stats.notProcessed.bytes)
		lrw.metrics.ReceivedLogicalBytes.Inc(stats.processed.bytes + stats.notProcessed.bytes)
	}
	return notProcessed, stats.notProcessed.bytes, nil
}

type retryEligibility int

const (
	retryAllowed retryEligibility = iota
	noSpace
	tooOld
	errType
)

func (r retryEligibility) String() string {
	switch r {
	case retryAllowed:
		return "allowed"
	case noSpace:
		return "size limit"
	case tooOld:
		return "age limit"
	case errType:
		return "not retryable"
	}
	return "unknown"
}

type replicationMutationType int

const (
	insertMutation replicationMutationType = iota
	deleteMutation
	updateMutation
)

func (t replicationMutationType) String() string {
	switch t {
	case insertMutation:
		return "insert"
	case deleteMutation:
		return "delete"
	case updateMutation:
		return "update"
	default:
		return fmt.Sprintf("Unrecognized replicationMutationType(%d)", int(t))
	}
}

// flushChunk is the per-thread body of flushBuffer; see flushBuffer's contract.
func (lrw *logicalReplicationWriterProcessor) flushChunk(
	ctx context.Context, bh BatchHandler, chunk []streampb.StreamEvent_KV, canRetry retryEligibility,
) (flushStats, error) {
	batchSize := lrw.getBatchSize()

	var stats flushStats
	// TODO: The batching here in production would need to be much
	// smarter. Namely, we don't want to include updates to the
	// same key in the same batch. Also, it's possible batching
	// will make things much worse in practice.
	for len(chunk) > 0 {
		batch := chunk[:min(batchSize, len(chunk))]
		chunk = chunk[len(batch):]

		// Make sure we're not ingesting events with origin TS in the future.
		if lrw.FlowCtx != nil { // Some unit tests don't set this and that's fine.
			hlcNow := lrw.FlowCtx.Cfg.DB.KV().Clock().Now()
			logClock := true
			for _, kv := range batch {
				if ts := kv.KeyValue.Value.Timestamp; ts.After(hlcNow) {
					if logClock || log.V(1) {
						log.Warningf(ctx, "event timestamp %s is ahead of local clock %s; delaying batch...", ts, hlcNow)
						logClock = false
					}
					if err := lrw.FlowCtx.Cfg.DB.KV().Clock().SleepUntil(ctx, ts); err != nil {
						return flushStats{}, err
					}
				}
			}
		}

		preBatchTime := timeutil.Now()

		if s, err := bh.HandleBatch(ctx, batch); err != nil {
			// If it already failed while applying on its own, handle the failure.
			if len(batch) == 1 {
				if eligibility := lrw.shouldRetryLater(err, canRetry); eligibility != retryAllowed {
					if err := lrw.dlq(ctx, batch[0], bh.GetLastRow(), err, eligibility); err != nil {
						return flushStats{}, err
					}
					stats.processed.dlq++
				} else {
					stats.notProcessed.count++
					stats.notProcessed.bytes += int64(batch[0].Size())
				}
			} else {
				// If there were multiple events in the batch, give each its own chance
				// to apply on its own before switching to handle its failure.
				for i := range batch {
					if singleStats, err := bh.HandleBatch(ctx, batch[i:i+1]); err != nil {
						if eligibility := lrw.shouldRetryLater(err, canRetry); eligibility != retryAllowed {
							if err := lrw.dlq(ctx, batch[i], bh.GetLastRow(), err, eligibility); err != nil {
								return flushStats{}, err
							}
							stats.processed.dlq++
						} else {
							stats.notProcessed.count++
							stats.notProcessed.bytes += int64(batch[i].Size())
						}
					} else {
						stats.optimisticInsertConflicts += singleStats.optimisticInsertConflicts
						batch[i] = streampb.StreamEvent_KV{}
						stats.processed.success++
						stats.processed.bytes += int64(batch[i].Size())
					}
				}
			}
		} else {
			stats.optimisticInsertConflicts += s.optimisticInsertConflicts
			stats.processed.success += int64(len(batch))
			// Clear the event to indicate successful application.
			for i := range batch {
				stats.processed.bytes += int64(batch[i].Size())
				batch[i] = streampb.StreamEvent_KV{}
			}
		}

		batchTime := timeutil.Since(preBatchTime)
		lrw.debug.RecordBatchApplied(batchTime, int64(len(batch)))
		lrw.metrics.ApplyBatchNanosHist.RecordValue(batchTime.Nanoseconds())
	}
	return stats, nil
}

// shouldRetryLater returns true if a given error encountered by an attempt to
// process an event may be resolved if processing of that event is reattempted
// again at a later time. This could be the case, for example, if that time is
// after the parent side of an FK relationship is ingested by another processor.
func (lrw *logicalReplicationWriterProcessor) shouldRetryLater(
	err error, eligibility retryEligibility,
) retryEligibility {
	if eligibility != retryAllowed {
		return eligibility
	}
	// TODO(dt): maybe this should only be constraint violation errors?
	return retryAllowed
}

const logAllDLQs = true

// dlq handles a row update that fails to apply by durably recording it in a DLQ
// or returns an error if it cannot. The decoded row should be passed to it if
// it is available, and dlq may persist it in addition to the event if
// row.IsInitialized() is true.
//
// TODO(dt): implement something here.
// TODO(dt): plumb the cdcevent.Row to this.
func (lrw *logicalReplicationWriterProcessor) dlq(
	ctx context.Context,
	event streampb.StreamEvent_KV,
	row cdcevent.Row,
	applyErr error,
	eligibility retryEligibility,
) error {
	if log.V(1) || logAllDLQs {
		if row.IsInitialized() {
			log.Infof(ctx, "DLQ'ing row update due to %s (%s): %s", applyErr, eligibility, row.DebugString())
		} else {
			log.Infof(ctx, "DLQ'ing KV due to %s (%s): %s", applyErr, eligibility, event)
		}
	}
	// We don't inc the total DLQ'ed metric here as that is done by flushBuffer
	// instead, using a single Inc() for the total. We could accumulate these in
	// the flushStats to do the same but DLQs are rare enough not to worry about
	// an inc for each.
	switch eligibility {
	case tooOld:
		lrw.metrics.DLQedDueToAge.Inc(1)
	case noSpace:
		lrw.metrics.DLQedDueToQueueSpace.Inc(1)
	case errType:
		lrw.metrics.DLQedDueToErrType.Inc(1)
	}
	return lrw.dlqClient.Log(ctx, lrw.spec.JobID, event, row, applyErr, eligibility)
}

type batchStats struct {
	optimisticInsertConflicts int64
}
type flushStats struct {
	processed struct {
		success, dlq, bytes int64
	}
	notProcessed struct {
		count, bytes int64
	}
	optimisticInsertConflicts int64
}

func (b *flushStats) Add(o flushStats) {
	b.processed.success += o.processed.success
	b.processed.dlq += o.processed.dlq
	b.processed.bytes += o.processed.bytes
	b.notProcessed.count += o.notProcessed.count
	b.notProcessed.bytes += o.notProcessed.bytes
	b.optimisticInsertConflicts += o.optimisticInsertConflicts
}

type BatchHandler interface {
	// HandleBatch handles one batch, i.e. a set of 1 or more KVs, that should be
	// decoded to rows and committed in a single txn, i.e. that all succeed to apply
	// or are not applied as a group. If the batch is a single KV it may use an
	// implicit txn.
	HandleBatch(context.Context, []streampb.StreamEvent_KV) (batchStats, error)
	GetLastRow() cdcevent.Row
	SetSyntheticFailurePercent(uint32)
	Close(context.Context)
}

// RowProcessor knows how to process a single row from an event stream.
type RowProcessor interface {
	// ProcessRow processes a single KV update by inserting or deleting a row.
	// Txn argument can be nil. The provided value is the "previous value",
	// before the change was applied on the source.
	ProcessRow(context.Context, isql.Txn, roachpb.KeyValue, roachpb.Value) (batchStats, error)
	GetLastRow() cdcevent.Row
	SetSyntheticFailurePercent(uint32)
	Close(context.Context)
}

type txnBatch struct {
	db       descs.DB
	rp       RowProcessor
	settings *cluster.Settings
	sd       *sessiondata.SessionData
}

var useImplicitTxns = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.use_implicit_txns.enabled",
	"determines whether the consumer processes each row in a separate implicit txn",
	metamorphic.ConstantWithTestBool("logical_replication.consumer.use_implicit_txns.enabled", true),
)

func (t *txnBatch) HandleBatch(
	ctx context.Context, batch []streampb.StreamEvent_KV,
) (batchStats, error) {
	ctx, sp := tracing.ChildSpan(ctx, "txnBatch.HandleBatch")
	defer sp.Finish()

	stats := batchStats{}
	var err error
	if len(batch) == 1 {
		s, err := t.rp.ProcessRow(ctx, nil /* txn */, batch[0].KeyValue, batch[0].PrevValue)
		if err != nil {
			return stats, err
		}
		stats.optimisticInsertConflicts += s.optimisticInsertConflicts
	} else {
		err = t.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			for _, kv := range batch {
				s, err := t.rp.ProcessRow(ctx, txn, kv.KeyValue, kv.PrevValue)
				if err != nil {
					return err
				}
				stats.optimisticInsertConflicts += s.optimisticInsertConflicts
			}
			return nil
		}, isql.WithSessionData(t.sd))
	}
	return stats, err
}

func (t *txnBatch) GetLastRow() cdcevent.Row {
	return t.rp.GetLastRow()
}

func (t *txnBatch) SetSyntheticFailurePercent(rate uint32) {
	t.rp.SetSyntheticFailurePercent(rate)
}

func (t *txnBatch) Close(ctx context.Context) {
	t.rp.Close(ctx)
}

func init() {
	rowexec.NewLogicalReplicationWriterProcessor = newLogicalReplicationWriterProcessor
}
