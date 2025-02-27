// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"runtime/pprof"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
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
	pbtypes "github.com/gogo/protobuf/types"
)

var logicalReplicationWriterResultType = []*types.T{
	types.Bytes, // jobspb.ResolvedSpans
}

var useLowPriority = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.low_admission_priority.enabled",
	"determines whether the consumer sends KV work as low admission priority",
	true,
)

var flushBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.batch_size",
	"the number of row updates to attempt in a single KV transaction",
	32,
	settings.NonNegativeInt,
)

var writerWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.flush_worker_per_proc",
	"the maximum number of workers per processor to use to flush each batch",
	128,
	settings.NonNegativeInt,
)

var minChunkSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.flush_chunk_min_size",
	"minimum number of row updates to pass to a flush worker at once",
	64,
	settings.NonNegativeInt,
)
var maxChunkSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.flush_chunk_max_size",
	"maximum number of row updates to pass to a flush worker at once (repeated revisions of a row notwithstanding)",
	1000,
	settings.NonNegativeInt,
)

// logicalReplicationWriterProcessor consumes a cross-cluster replication stream
// by decoding kvs in it to logical changes and applying them by executing DMLs.
type logicalReplicationWriterProcessor struct {
	execinfra.ProcessorBase
	processorID int32

	spec execinfrapb.LogicalReplicationWriterSpec

	bh      []BatchHandler
	bhStats []flushStats

	configByTable map[descpb.ID]sqlProcessorTableConfig

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

	rangeStatsCh chan *streampb.StreamEvent_RangeStats

	agg      *tracing.TracingAggregator
	aggTimer timeutil.Timer

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	logBufferEvery log.EveryN

	debug streampb.DebugLogicalConsumerStatus

	dlqClient DeadLetterQueueClient

	purgatory purgatory

	seenKeys  map[uint64]int64
	dupeCount int64
	seenEvery log.EveryN
}

var (
	_ execinfra.Processor = &logicalReplicationWriterProcessor{}
	_ execinfra.RowSource = &logicalReplicationWriterProcessor{}
)

const logicalReplicationWriterProcessorName = "logical-replication-writer-processor"

var batchSizeSetting = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"logical_replication.stream_batch_size",
	"target batch size for logical replication stream",
	16<<20,
)

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

	procConfigByDestTableID := make(map[descpb.ID]sqlProcessorTableConfig)
	destTableBySrcID := make(map[descpb.ID]dstTableMetadata)
	for dstTableID, md := range spec.TableMetadataByDestID {
		procConfigByDestTableID[descpb.ID(dstTableID)] = sqlProcessorTableConfig{
			srcDesc: tabledesc.NewBuilder(&md.SourceDescriptor).BuildImmutableTable(),
			dstOID:  md.DestinationFunctionOID,
		}
		destTableBySrcID[md.SourceDescriptor.GetID()] = dstTableMetadata{
			database: md.DestinationParentDatabaseName,
			schema:   md.DestinationParentSchemaName,
			table:    md.DestinationTableName,
			tableID:  descpb.ID(dstTableID),
		}
	}

	dlqDbExec := flowCtx.Cfg.DB.Executor(isql.WithSessionData(sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */)))

	var numTablesWithSecondaryIndexes int
	for _, tc := range procConfigByDestTableID {
		if len(tc.srcDesc.NonPrimaryIndexes()) > 0 {
			numTablesWithSecondaryIndexes++
		}
	}

	lrw := &logicalReplicationWriterProcessor{
		configByTable: procConfigByDestTableID,
		spec:          spec,
		processorID:   processorID,
		getBatchSize: func() int {
			// TODO(ssd): We set this to 1 since putting more than 1
			// row in a KV batch using the new ConditionalPut-based
			// conflict resolution would require more complex error
			// handling and tracking that we haven't implemented
			// yet.
			if spec.Mode == jobspb.LogicalReplicationDetails_Immediate {
				return 1
			}
			// We want to decide whether to use implicit txns or not based on
			// the schema of the dest table. Benchmarking has shown that
			// implicit txns are beneficial on tables with no secondary indexes
			// whereas explicit txns are beneficial when at least one secondary
			// index is present.
			//
			// Unfortunately, if we have multiple replication pairs, we don't
			// know which tables will be affected by this batch before deciding
			// on the batch size, so we'll use a heuristic such that we'll use
			// the implicit txns if at least half of the dest tables are
			// without the secondary indexes. If we only have a single
			// replication pair, then this heuristic gives us the precise
			// recommendation.
			//
			// (Here we have access to the descriptor of the source table, but
			// for now we assume that the source and the dest descriptors are
			// similar.)
			if 2*numTablesWithSecondaryIndexes < len(procConfigByDestTableID) && useImplicitTxns.Get(&flowCtx.Cfg.Settings.SV) {
				return 1
			}
			return int(flushBatchSize.Get(&flowCtx.Cfg.Settings.SV))
		},
		frontier:       frontier,
		stopCh:         make(chan struct{}),
		checkpointCh:   make(chan []jobspb.ResolvedSpan),
		rangeStatsCh:   make(chan *streampb.StreamEvent_RangeStats),
		errCh:          make(chan error, 1),
		logBufferEvery: log.Every(30 * time.Second),
		debug: streampb.DebugLogicalConsumerStatus{
			StreamID:    streampb.StreamID(spec.StreamID),
			ProcessorID: processorID,
		},
		dlqClient: InitDeadLetterQueueClient(dlqDbExec, destTableBySrcID),
		metrics:   flowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics),
		seenEvery: log.Every(1 * time.Minute),
	}
	lrw.purgatory = purgatory{
		deadline:    func() time.Duration { return retryQueueAgeLimit.Get(&flowCtx.Cfg.Settings.SV) },
		delay:       func() time.Duration { return retryQueueBackoff.Get(&flowCtx.Cfg.Settings.SV) },
		byteLimit:   func() int64 { return retryQueueSizeLimit.Get(&flowCtx.Cfg.Settings.SV) },
		flush:       lrw.flushBuffer,
		checkpoint:  lrw.checkpoint,
		bytesGauge:  lrw.metrics.RetryQueueBytes,
		eventsGauge: lrw.metrics.RetryQueueEvents,
		debug:       &lrw.debug,
	}

	if err := lrw.Init(ctx, lrw, post, logicalReplicationWriterResultType, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				lrw.close()
				if lrw.agg != nil {
					meta := bulk.ConstructTracingAggregatorProducerMeta(ctx,
						lrw.FlowCtx.NodeID.SQLInstanceID(), lrw.FlowCtx.ID, lrw.agg)
					return []execinfrapb.ProducerMetadata{*meta}
				}
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
	ctx = logtags.AddTag(ctx, "job", lrw.spec.JobID)
	ctx = logtags.AddTag(ctx, "src-node", lrw.spec.PartitionSpec.PartitionID)
	ctx = logtags.AddTag(ctx, "proc", lrw.ProcessorID)
	lrw.agg = tracing.TracingAggregatorForContext(ctx)
	var listeners []tracing.EventListener
	if lrw.agg != nil {
		lrw.aggTimer.Reset(time.Second)
		listeners = []tracing.EventListener{lrw.agg}
	}
	streampb.RegisterActiveLogicalConsumerStatus(&lrw.debug)

	ctx = lrw.StartInternal(ctx, logicalReplicationWriterProcessorName, listeners...)

	lrw.metrics = lrw.FlowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

	db := lrw.FlowCtx.Cfg.DB

	log.Infof(ctx, "starting logical replication writer for partition %s", lrw.spec.PartitionSpec.PartitionID)

	// Start the subscription for our partition.
	partitionSpec := lrw.spec.PartitionSpec
	token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
	uri, err := streamclient.ParseClusterUri(partitionSpec.PartitionConnUri)
	if err != nil {
		lrw.MoveToDrainingAndLogError(errors.Wrap(err, "parsing partition uri uri"))
	}
	streamClient, err := streamclient.NewStreamClient(ctx, uri, db,
		streamclient.WithStreamID(streampb.StreamID(lrw.spec.StreamID)),
		streamclient.WithCompression(true),
		streamclient.WithLogical(),
	)
	if err != nil {
		lrw.MoveToDrainingAndLogError(errors.Wrapf(err, "creating client for partition spec %q from %q", token, uri.Redacted()))
		return
	}
	lrw.streamPartitionClient = streamClient

	if streamingKnobs, ok := lrw.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.BeforeClientSubscribe != nil {
			streamingKnobs.BeforeClientSubscribe(uri.Serialize(), string(token), lrw.frontier, lrw.spec.Discard == jobspb.LogicalReplicationDetails_DiscardCDCIgnoredTTLDeletes)
		}
	}
	sub, err := streamClient.Subscribe(ctx,
		streampb.StreamID(lrw.spec.StreamID),
		int32(lrw.FlowCtx.NodeID.SQLInstanceID()), lrw.ProcessorID,
		token,
		lrw.spec.InitialScanTimestamp, lrw.frontier,
		streamclient.WithFiltering(
			lrw.spec.Discard == jobspb.LogicalReplicationDetails_DiscardCDCIgnoredTTLDeletes ||
				lrw.spec.Discard == jobspb.LogicalReplicationDetails_DiscardAllDeletes),
		streamclient.WithDiff(true),
		streamclient.WithBatchSize(batchSizeSetting.Get(&lrw.FlowCtx.Cfg.Settings.SV)),
	)
	if err != nil {
		lrw.MoveToDrainingAndLogError(errors.Wrapf(err, "subscribing to partition from %s", uri.Redacted()))
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
			log.Infof(lrw.Ctx(), "subscription completed. Error: %s", err)
			lrw.sendError(errors.Wrap(err, "subscription"))
		}
		return nil
	})
	lrw.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(lrw.checkpointCh)
		pprof.Do(ctx, pprof.Labels("proc", fmt.Sprintf("%d", lrw.ProcessorID)), func(ctx context.Context) {
			if err := lrw.consumeEvents(ctx); err != nil {
				log.Infof(lrw.Ctx(), "consumer completed. Error: %s", err)
				lrw.sendError(errors.Wrap(err, "consume events"))
			}
		})
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
	case <-lrw.aggTimer.C:
		lrw.aggTimer.Read = true
		lrw.aggTimer.Reset(15 * time.Second)
		return nil, bulk.ConstructTracingAggregatorProducerMeta(lrw.Ctx(),
			lrw.FlowCtx.NodeID.SQLInstanceID(), lrw.FlowCtx.ID, lrw.agg)

	case stats := <-lrw.rangeStatsCh:
		meta, err := lrw.newRangeStatsProgressMeta(stats)
		if err != nil {
			lrw.MoveToDrainingAndLogError(err)
			return nil, lrw.DrainHelper()
		}
		return nil, meta
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
	log.Infof(lrw.Ctx(), "logical replication writer processor closing")
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

	for _, b := range lrw.bh {
		b.Close(lrw.Ctx())
	}

	// Update the global retry queue gauges to reflect that this queue is going
	// away, including everything in it that is included in those gauges.
	lrw.purgatory.bytesGauge.Dec(lrw.purgatory.bytes)
	for _, i := range lrw.purgatory.levels {
		lrw.purgatory.eventsGauge.Dec(int64(len(i.events)))
		lrw.purgatory.debug.RecordPurgatory(-int64(len(i.events)))
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
	lastLog := timeutil.Now()
	lrw.debug.RecordRecvStart()
	for event := range lrw.subscription.Events() {
		lrw.debug.RecordRecv()
		if err := lrw.handleEvent(ctx, event); err != nil {
			return err
		}
		if timeutil.Since(lastLog) > 5*time.Minute {
			lastLog = timeutil.Now()
			if !lrw.frontier.Frontier().GoTime().After(timeutil.Now().Add(-5 * time.Minute)) {
				log.Infof(lrw.Ctx(), "lagging frontier: %s with span %s", lrw.frontier.Frontier(), lrw.frontier.PeekFrontierSpan())
			}
		}
		lrw.debug.RecordRecvStart()
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
		if err := lrw.maybeCheckpoint(ctx, event.GetCheckpoint()); err != nil {
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
	ctx context.Context, checkpoint *streampb.StreamEvent_StreamCheckpoint,
) error {
	// If the checkpoint contains stats publish them to the coordinator. The
	// stats ignore purgatory because they:
	// 1. Track the status of the producer scans
	// 2. Are intended for monitoring and don't need to reflect the committed
	//    state of the write processor.
	//
	// RangeStats may be nil if the producer does not support the stats field or
	// the the producer has not finished counting the ranges.
	if checkpoint.RangeStats != nil {
		err := lrw.rangeStats(ctx, checkpoint.RangeStats)
		if err != nil {
			return err
		}
	}

	// If purgatory is non-empty, it intercepts the checkpoint and then we can try
	// to drain it.
	if !lrw.purgatory.Empty() {
		lrw.purgatory.Checkpoint(ctx, checkpoint.ResolvedSpans)
		return lrw.purgatory.Drain(ctx)
	}

	return lrw.checkpoint(ctx, checkpoint.ResolvedSpans)
}

func (lrw *logicalReplicationWriterProcessor) rangeStats(
	ctx context.Context, stats *streampb.StreamEvent_RangeStats,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case lrw.rangeStatsCh <- stats:
		return nil
	case <-lrw.stopCh:
		// We need to select on stopCh here because the reader
		// of rangestatsCh is the caller of Next(). But there
		// might never be another Next() call since it may
		// have exited based on an error.
		return nil
	}
}

func (lrw *logicalReplicationWriterProcessor) newRangeStatsProgressMeta(
	stats *streampb.StreamEvent_RangeStats,
) (*execinfrapb.ProducerMetadata, error) {
	asAny, err := pbtypes.MarshalAny(stats)
	if err != nil {
		return nil, errors.Wrap(err, "unable to convert stats into any proto")
	}
	return &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			NodeID:          lrw.FlowCtx.NodeID.SQLInstanceID(),
			FlowID:          lrw.FlowCtx.ID,
			ProcessorID:     lrw.ProcessorID,
			ProgressDetails: *asAny,
		},
	}, nil
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

	for _, p := range lrw.bh {
		p.ReportMutations(lrw.FlowCtx.Cfg.StatsRefresher)
		// We should drop our leases and re-acquire new ones at next flush, to avoid
		// holding leases continually until they expire; re-acquire is cheap when it
		// can be served from the cache so we can just stop these every checkpoint.
		p.ReleaseLeases(ctx)
	}
	lrw.metrics.CheckpointEvents.Inc(1)
	lrw.debug.RecordCheckpoint(lrw.frontier.Frontier().GoTime())
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

func (lrw *logicalReplicationWriterProcessor) setupBatchHandlers(ctx context.Context) error {
	if lrw.FlowCtx == nil {
		return nil
	}

	poolSize := writerWorkers.Get(&lrw.FlowCtx.Cfg.Settings.SV)

	if len(lrw.bh) >= int(poolSize) {
		return nil
	}

	for _, b := range lrw.bh {
		b.Close(lrw.Ctx())
	}

	flowCtx := lrw.FlowCtx
	lrw.bh = make([]BatchHandler, poolSize)
	for i := range lrw.bh {
		var rp BatchHandler
		var err error
		sd := sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */)

		if lrw.spec.Mode == jobspb.LogicalReplicationDetails_Immediate {
			rp, err = newKVRowProcessor(ctx, flowCtx.Cfg, flowCtx.EvalCtx, sd, lrw.spec, lrw.configByTable)
			if err != nil {
				return err
			}
		} else {
			rp, err = makeSQLProcessor(
				ctx, flowCtx.Cfg.Settings, lrw.configByTable,
				jobspb.JobID(lrw.spec.JobID),
				flowCtx.Cfg.DB,
				// Initialize the executor with a fresh session data - this will
				// avoid creating a new copy on each executor usage.
				flowCtx.Cfg.DB.Executor(isql.WithSessionData(sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */))),
				sd, lrw.spec,
			)
			if err != nil {
				return err
			}
		}

		if streamingKnobs, ok := flowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
			if streamingKnobs != nil && streamingKnobs.FailureRate != 0 {
				rp.SetSyntheticFailurePercent(streamingKnobs.FailureRate)
			}
		}

		lrw.bh[i] = rp
	}
	return nil
}

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

	if err := lrw.setupBatchHandlers(ctx); err != nil {
		return kvs, int64(len(kvs)), err
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

	// If the seen map is nil or has hit 2M items, reset it.
	if lrw.seenKeys == nil || len(lrw.seenKeys) > 2<<20 {
		lrw.seenKeys = make(map[uint64]int64, 2<<20)
	}

	h := fnv.New64a()
	logged := false
	for i := range kvs {
		h.Reset()
		_, _ = h.Write(kvs[i].KeyValue.Key)
		hashed := h.Sum64() + uint64(kvs[i].KeyValue.Value.Timestamp.WallTime)
		c := lrw.seenKeys[hashed]
		lrw.seenKeys[hashed] = c + 1

		if c > 0 {
			lrw.dupeCount++
			if !logged && lrw.seenEvery.ShouldLog() {
				logged = true // don't check ShouldLog again for rest of loop.
				log.Infof(ctx, "duplicate delivery of key %s@%d (%d prior times); %d total recent dupes.",
					kvs[i].KeyValue.Key, kvs[i].KeyValue.Value.Timestamp.WallTime, c, lrw.dupeCount)
			}
		}
	}

	// Aim for a chunk size that gives each worker at least 4 chunks to do so that
	// if it takes longer to process some keys in a chunk, the other 3/4 can be
	// stolen by other workers. That said, we don't want tiny chunks that are more
	// channel overhead than work, nor giant chunks, so bound it by the settings.
	minChunk, maxChunk := minChunkSize.Default(), maxChunkSize.Default()
	if lrw.FlowCtx != nil {
		minChunk, maxChunk = minChunkSize.Get(&lrw.FlowCtx.Cfg.Settings.SV), maxChunkSize.Get(&lrw.FlowCtx.Cfg.Settings.SV)
	}
	chunkSize := min(max(len(kvs)/(len(lrw.bh)*4), int(minChunk)), int(maxChunk))

	// Figure out how many workers we can utilize from the pool for the number of
	// chunks we expect (we could use fewer if chunks overshoot size target due to
	// key revisions).
	requiredWorkers := max(1, min(len(kvs)/chunkSize, len(lrw.bh)))
	if len(lrw.bhStats) < requiredWorkers {
		lrw.bhStats = make([]flushStats, requiredWorkers)
	}

	// TODO(dt): consider keeping these goroutines running for lifetime of proc
	// rather than starting new ones for each flush.
	chunks := make(chan []streampb.StreamEvent_KV)

	g := ctxgroup.WithContext(ctx)
	for worker := range lrw.bh[:requiredWorkers] {
		w := worker
		lrw.bhStats[w] = flushStats{}
		g.GoCtx(func(ctx context.Context) error {
			for chunk := range chunks {
				s, err := lrw.flushChunk(ctx, lrw.bh[w], chunk, canRetry)
				if err != nil {
					return err
				}
				lrw.bhStats[w].Add(s)
			}
			return nil
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		defer close(chunks)
		for todo := kvs; len(todo) > 0; {
			// The chunk should end after the first new key after chunk size.
			chunkEnd := min(chunkSize, len(todo))
			for chunkEnd < len(todo) && k(todo[chunkEnd-1]).Equal(k(todo[chunkEnd])) {
				chunkEnd++
			}
			chunk := todo[0:chunkEnd]
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				return ctx.Err()
			}
			todo = todo[len(chunk):]
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, 0, err
	}

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	// Collect the stats from every (possibly run) worker.
	var stats flushStats
	for i := range lrw.bhStats[:requiredWorkers] {
		stats.Add(lrw.bhStats[i])
	}

	if stats.notProcessed.count > 0 {
		notProcessed = filterRemaining(kvs)
	}

	flushTime := timeutil.Since(preFlushTime).Nanoseconds()
	lrw.debug.RecordFlushComplete(flushTime, int64(len(kvs)), stats.processed.bytes)

	lrw.metrics.KVUpdateTooOld.Inc(stats.kvWriteTooOld)
	lrw.metrics.KVValueRefreshes.Inc(stats.kvWriteValueRefreshes)
	lrw.metrics.AppliedRowUpdates.Inc(stats.processed.success)
	lrw.metrics.DLQedRowUpdates.Inc(stats.processed.dlq)
	if l := lrw.spec.MetricsLabel; l != "" {
		lrw.metrics.LabeledEventsIngested.Inc(map[string]string{"label": l}, stats.processed.success)
		lrw.metrics.LabeledEventsDLQed.Inc(map[string]string{"label": l}, stats.processed.dlq)
	}

	lrw.metrics.CommitToCommitLatency.RecordValue(timeutil.Since(firstKeyTS).Nanoseconds())

	if isRetry {
		lrw.metrics.RetriedApplySuccesses.Inc(stats.processed.success)
		lrw.metrics.RetriedApplyFailures.Inc(stats.notProcessed.count + stats.processed.dlq)
	} else {
		lrw.metrics.InitialApplySuccesses.Inc(stats.processed.success)
		lrw.metrics.InitialApplyFailures.Inc(stats.notProcessed.count + stats.processed.dlq)
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

	lrw.debug.RecordChunkStart()
	defer lrw.debug.RecordChunkComplete()

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
			if ctxErr := ctx.Err(); ctxErr != nil {
				return flushStats{}, ctxErr
			}

			// If it already failed while applying on its own, handle the failure.
			if len(batch) == 1 {
				if eligibility := lrw.shouldRetryLater(err, canRetry); eligibility != retryAllowed {
					if err := lrw.maybeDLQ(ctx, batch[0], bh.GetLastRow(), err, eligibility); err != nil {
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
						if ctxErr := ctx.Err(); ctxErr != nil {
							return flushStats{}, ctxErr
						}
						if eligibility := lrw.shouldRetryLater(err, canRetry); eligibility != retryAllowed {
							if err := lrw.maybeDLQ(ctx, batch[i], bh.GetLastRow(), err, eligibility); err != nil {
								return flushStats{}, err
							}
							stats.processed.dlq++
						} else {
							stats.notProcessed.count++
							stats.notProcessed.bytes += int64(batch[i].Size())
						}
					} else {
						stats.batchStats.Add(singleStats)
						batch[i] = streampb.StreamEvent_KV{}
						stats.processed.success++
						stats.processed.bytes += int64(batch[i].Size())
					}
				}
			}
		} else {
			stats.batchStats.Add(s)
			stats.processed.success += int64(len(batch))
			// Clear the event to indicate successful application.
			for i := range batch {
				stats.processed.bytes += int64(batch[i].Size())
				batch[i] = streampb.StreamEvent_KV{}
			}
		}

		batchTime := timeutil.Since(preBatchTime)
		lrw.debug.RecordBatchApplied(batchTime, int64(len(batch)))
		nanosPerRow := batchTime.Nanoseconds()
		if len(batch) > 0 {
			nanosPerRow /= int64(len(batch))
		}
		lrw.metrics.ApplyBatchNanosHist.RecordValue(nanosPerRow)
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

	if errors.Is(err, errInjected) {
		return tooOld
	}

	// TODO(dt): maybe this should only be constraint violation errors?
	return retryAllowed
}

const logAllDLQs = true

// dlq handles a row update that fails to apply by durably recording it in a DLQ
// or returns an error if it cannot. The decoded row should be passed to it if
// it is available, and dlq may persist it in addition to the event if
// row.IsInitialized() is true.
func (lrw *logicalReplicationWriterProcessor) maybeDLQ(
	ctx context.Context,
	event streampb.StreamEvent_KV,
	row cdcevent.Row,
	applyErr error,
	eligibility retryEligibility,
) error {
	if err := canDlqError(applyErr); err != nil {
		return errors.Wrapf(err, "dlq eligibility %s", eligibility)
	}
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

var internalPgErrors = func() *regexp.Regexp {
	codePrefixes := []string{
		// Section: Class 57 - Operator Intervention
		"57",
		// Section: Class 58 - System Error
		"58",
		// Section: Class 25 - Invalid Transaction State
		"25",
		// Section: Class 2D - Invalid Transaction Termination
		"2D",
		// Section: Class 40 - Transaction Rollback
		"40",
		// Section: Class XX - Internal Error
		"XX",
		// Section: Class 58C - System errors related to CockroachDB node problems.
		"58C",
	}
	return regexp.MustCompile(fmt.Sprintf("^(%s)", strings.Join(codePrefixes, "|")))
}()

// canDlqError returns true if the error should send a row to the DLQ. We don't
// want to DLQ rows that failed to apply because of some internal problem like
// an unavailable range. The idea is it is better to fail the processor so the
// job backs off until the system is healthy.
func canDlqError(err error) error {
	// If the error is not from the SQL layer, then we don't want to DQL it.
	if !pgerror.HasCandidateCode(err) {
		return errors.Wrap(err, "can only DLQ errors with pg codes")
	}
	code := pgerror.GetPGCode(err)
	if internalPgErrors.MatchString(code.String()) {
		return errors.Wrap(err, "unable to DLQ pgcode that indicates an internal or retryable error")
	}
	return nil
}

type batchStats struct {
	optimisticInsertConflicts int64
	kvWriteTooOld             int64
	kvWriteValueRefreshes     int64
}

func (b *batchStats) Add(o batchStats) {
	b.optimisticInsertConflicts += o.optimisticInsertConflicts
	b.kvWriteTooOld += o.kvWriteTooOld
	b.kvWriteValueRefreshes += o.kvWriteValueRefreshes
}

type flushStats struct {
	processed struct {
		success, dlq, bytes int64
	}
	notProcessed struct {
		count, bytes int64
	}

	batchStats
}

func (b *flushStats) Add(o flushStats) {
	b.processed.success += o.processed.success
	b.processed.dlq += o.processed.dlq
	b.processed.bytes += o.processed.bytes
	b.notProcessed.count += o.notProcessed.count
	b.notProcessed.bytes += o.notProcessed.bytes
	b.batchStats.Add(o.batchStats)
}

type BatchHandler interface {
	// HandleBatch handles one batch, i.e. a set of 1 or more KVs, that should be
	// decoded to rows and committed in a single txn, i.e. that all succeed to apply
	// or are not applied as a group. If the batch is a single KV it may use an
	// implicit txn.
	HandleBatch(context.Context, []streampb.StreamEvent_KV) (batchStats, error)
	GetLastRow() cdcevent.Row
	SetSyntheticFailurePercent(uint32)
	ReportMutations(*stats.Refresher)
	ReleaseLeases(context.Context)
	Close(context.Context)
}

var useImplicitTxns = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.use_implicit_txns.enabled",
	"determines whether the consumer processes each row in a separate implicit txn",
	metamorphic.ConstantWithTestBool("logical_replication.consumer.use_implicit_txns.enabled", true),
)

func init() {
	rowexec.NewLogicalReplicationWriterProcessor = newLogicalReplicationWriterProcessor
}
