// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	bulkutil "github.com/cockroachdb/cockroach/pkg/util/bulk"
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

var minimumFlushInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"bulkio.stream_ingestion.minimum_flush_interval",
	"the minimum timestamp between flushes; flushes may still occur if internal buffers fill up",
	5*time.Second,
	settings.WithPublic,
	settings.WithName("physical_replication.consumer.minimum_flush_interval"),
)

var maxKVBufferSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"bulkio.stream_ingestion.kv_buffer_size",
	"the maximum size of the KV buffer allowed before a flush",
	128<<20, // 128 MiB
	settings.WithName("physical_replication.consumer.kv_buffer_size"),
)

var maxRangeKeyBufferSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"bulkio.stream_ingestion.range_key_buffer_size",
	"the maximum size of the range key buffer allowed before a flush",
	32<<20, // 32 MiB
	settings.WithName("physical_replication.consumer.range_key_buffer_size"),
)

var tooSmallRangeKeySize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"bulkio.stream_ingestion.ingest_range_keys_as_writes",
	"size below which a range key SST will be ingested using normal writes",
	400*1<<10, // 400 KiB
	settings.WithName("physical_replication.consumer.ingest_range_keys_as_writes"),
)

// checkForCutoverSignalFrequency is the frequency at which the resumer polls
// the system.jobs table to check whether the stream ingestion job has been
// signaled to cutover.
var cutoverSignalPollInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"bulkio.stream_ingestion.failover_signal_poll_interval",
	"the interval at which the stream ingestion job checks if it has been signaled to cutover",
	10*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("physical_replication.consumer.failover_signal_poll_interval"),
)

var quantize = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"physical_replication.consumer.timestamp_granularity",
	"the granularity at which replicated times are quantized to make tracking more efficient",
	5*time.Second,
)

var ingestSplitEvent = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"physical_replication.consumer.ingest_split_event.enabled",
	"whether to ingest split events",
	false,
)

var compress = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"physical_replication.consumer.stream_compression.enabled",
	"enables requesting a compressed stream from the producer when resumed",
	true,
)

var streamIngestionResultTypes = []*types.T{
	types.Bytes, // jobspb.ResolvedSpans
}

type mvccKeyValues []storage.MVCCKeyValue
type mvccRangeKeyValues []storage.MVCCRangeKeyValue

func (s mvccKeyValues) Len() int           { return len(s) }
func (s mvccKeyValues) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s mvccKeyValues) Less(i, j int) bool { return s[i].Key.Less(s[j].Key) }

// streamIngestionBuffer is a local buffer for KVs and RangeKeys. We
// buffer them locally so that we can sort them before writing them to
// an SST Batcher.
//
// TODO: We don't yet use a buffering adder since the current
// implementation is specific to ingesting KV pairs without timestamps
// rather than MVCCKeys.
type streamIngestionBuffer struct {
	// curRangeKVBatch is the current batch of range KVs which will
	// be ingested through 'flush' later.
	curRangeKVBatch     mvccRangeKeyValues
	curRangeKVBatchSize int

	// curKVBatch temporarily batches MVCC Keys so they can be
	// sorted before ingestion.
	curKVBatch     mvccKeyValues
	curKVBatchSize int

	// Minimum timestamp in the current batch. Used for metrics purpose.
	minTimestamp hlc.Timestamp
}

func (b *streamIngestionBuffer) addKV(kv storage.MVCCKeyValue) {
	b.curKVBatchSize += len(kv.Value) + kv.Key.Len()
	b.curKVBatch = append(b.curKVBatch, kv)
	if kv.Key.Timestamp.Less(b.minTimestamp) {
		b.minTimestamp = kv.Key.Timestamp
	}
}

func (b *streamIngestionBuffer) addRangeKey(rangeKV storage.MVCCRangeKeyValue) {
	b.curRangeKVBatchSize += len(rangeKV.RangeKey.StartKey) + len(rangeKV.RangeKey.EndKey) + len(rangeKV.Value)
	b.curRangeKVBatch = append(b.curRangeKVBatch, rangeKV)
	if rangeKV.RangeKey.Timestamp.Less(b.minTimestamp) {
		b.minTimestamp = rangeKV.RangeKey.Timestamp
	}
}

func (b *streamIngestionBuffer) shouldFlushOnSize(ctx context.Context, sv *settings.Values) bool {
	kvBufMax := int(maxKVBufferSize.Get(sv))
	rkBufMax := int(maxRangeKeyBufferSize.Get(sv))
	if kvBufMax > 0 && b.curKVBatchSize >= kvBufMax {
		log.VInfof(ctx, 2, "flushing because current KV batch based on size %d >= %d", b.curKVBatchSize, kvBufMax)
		return true
	} else if rkBufMax > 0 && b.curRangeKVBatchSize >= rkBufMax {
		log.VInfof(ctx, 2, "flushing beacuse current range key batch based on size %d >= %d", b.curRangeKVBatchSize, rkBufMax)
		return true
	}
	return false
}

func (b *streamIngestionBuffer) reset() {
	b.minTimestamp = hlc.MaxTimestamp

	b.curKVBatchSize = 0
	b.curKVBatch = b.curKVBatch[:0]

	b.curRangeKVBatchSize = 0
	b.curRangeKVBatch = b.curRangeKVBatch[:0]
}

var bufferPool = sync.Pool{
	New: func() interface{} { return &streamIngestionBuffer{} },
}

func getBuffer() *streamIngestionBuffer {
	return bufferPool.Get().(*streamIngestionBuffer)
}

func releaseBuffer(b *streamIngestionBuffer) {
	b.reset()
	bufferPool.Put(b)
}

// Specialized SST batcher that is responsible for ingesting range tombstones.
type rangeKeyBatcher struct {
	db       *kv.DB
	settings *cluster.Settings

	// onFlush is the callback called after the current batch has been
	// successfully ingested.
	onFlush func(kvpb.BulkOpSummary)
}

func newRangeKeyBatcher(
	ctx context.Context, cs *cluster.Settings, db *kv.DB, onFlush func(summary kvpb.BulkOpSummary),
) *rangeKeyBatcher {
	batcher := &rangeKeyBatcher{
		db:       db,
		settings: cs,
		onFlush:  onFlush,
	}
	return batcher
}

type streamIngestionProcessor struct {
	execinfra.ProcessorBase

	spec    execinfrapb.StreamIngestionDataSpec
	rekeyer *backup.KeyRewriter
	// rewriteToDiffKey Indicates whether we are rekeying a key into a different key.
	rewriteToDiffKey bool

	buffer *streamIngestionBuffer

	// batcher is used to flush KVs into SST to the storage layer.
	batcher *bulk.SSTBatcher
	// rangeBatcher is used to flush range KVs into SST to the storage layer.
	rangeBatcher      *rangeKeyBatcher
	maxFlushRateTimer timeutil.Timer

	// client is a streaming client which provides a stream of events from a given
	// address.
	forceClientForTests streamclient.Client
	// streamPartitionClients are a collection of streamclient.Client created for
	// consuming multiple partitions from a stream.
	streamPartitionClients []streamclient.Client

	// cutoverProvider indicates when the cutover time has been reached.
	cutoverProvider cutoverProvider

	// frontier keeps track of the progress for the spans tracked by this processor
	// and is used forward resolved spans
	frontier span.Frontier
	// lastFlushTime keeps track of the last time that we flushed due to a
	// checkpoint timestamp event.
	lastFlushTime time.Time

	// workerGroup is a context group holding all goroutines
	// related to this processor.
	workerGroup ctxgroup.Group

	// subscriptionGroup is different from workerGroup since we
	// want to explicitly cancel the context related to it.
	subscriptionGroup  ctxgroup.Group
	subscriptionCancel context.CancelFunc

	// stopCh stops the cutover poller and flush loop.
	stopCh chan struct{}

	mergedSubscription *MergedSubscription

	flushCh chan flushableBuffer

	errCh chan error

	checkpointCh chan *jobspb.ResolvedSpans

	// cutoverCh is used to convey that the ingestion job has been signaled to
	// cutover.
	cutoverCh chan struct{}

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	logBufferEvery log.EveryN

	// Aggregator that aggregates StructuredEvents emitted in the
	// backupDataProcessors' trace recording.
	agg      *tracing.TracingAggregator
	aggTimer timeutil.Timer
}

// PartitionEvent augments a normal event with the partition it came from.
type PartitionEvent struct {
	crosscluster.Event
	partition string
}

var (
	_ execinfra.Processor = &streamIngestionProcessor{}
	_ execinfra.RowSource = &streamIngestionProcessor{}
)

const streamIngestionProcessorName = "stream-ingestion-processor"

func newStreamIngestionDataProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionDataSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	rekeyer, err := backup.MakeKeyRewriterFromRekeys(flowCtx.Codec(),
		nil /* tableRekeys */, []execinfrapb.TenantRekey{spec.TenantRekey},
		true /* restoreTenantFromStream */)
	if err != nil {
		return nil, err
	}
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

	sip := &streamIngestionProcessor{
		spec:     spec,
		frontier: frontier,
		cutoverProvider: &cutoverFromJobProgress{
			jobID: jobspb.JobID(spec.JobID),
			db:    flowCtx.Cfg.DB,
		},
		buffer:           &streamIngestionBuffer{},
		cutoverCh:        make(chan struct{}),
		stopCh:           make(chan struct{}),
		flushCh:          make(chan flushableBuffer),
		checkpointCh:     make(chan *jobspb.ResolvedSpans),
		errCh:            make(chan error, 1),
		rekeyer:          rekeyer,
		rewriteToDiffKey: spec.TenantRekey.NewID != spec.TenantRekey.OldID,
		logBufferEvery:   log.Every(30 * time.Second),
	}
	if err := sip.Init(ctx, sip, post, streamIngestionResultTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				sip.close()
				if sip.agg != nil {
					meta := bulkutil.ConstructTracingAggregatorProducerMeta(ctx,
						sip.FlowCtx.NodeID.SQLInstanceID(), sip.FlowCtx.ID, sip.agg)
					return []execinfrapb.ProducerMetadata{*meta}
				}
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
//	cutoverPoller ---------------------------------^
//
// All errors are reported to Next() via errCh, with the first
// error winning.
//
// Start implements the RowSource interface.
func (sip *streamIngestionProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", sip.spec.JobID)
	ctx = logtags.AddTag(ctx, "proc", sip.ProcessorID)
	log.Infof(ctx, "starting ingest proc")
	sip.agg = tracing.TracingAggregatorForContext(ctx)

	// If the aggregator is nil, we do not want the timer to fire.
	if sip.agg != nil {
		sip.aggTimer.Reset(15 * time.Second)
	}

	defer sip.FlowCtx.Cfg.JobRegistry.MarkAsIngesting(catpb.JobID(sip.spec.JobID))()

	ctx = sip.StartInternal(ctx, streamIngestionProcessorName, sip.agg)

	sip.metrics = sip.FlowCtx.Cfg.JobRegistry.MetricsStruct().StreamIngest.(*Metrics)

	st := sip.FlowCtx.Cfg.Settings
	db := sip.FlowCtx.Cfg.DB
	rc := sip.FlowCtx.Cfg.RangeCache

	var err error
	sip.batcher, err = bulk.MakeStreamSSTBatcher(
		ctx, db.KV(), rc, st, sip.FlowCtx.Cfg.BackupMonitor.MakeConcurrentBoundAccount(),
		sip.FlowCtx.Cfg.BulkSenderLimiter, sip.onFlushUpdateMetricUpdate)
	if err != nil {
		sip.MoveToDrainingAndLogError(errors.Wrap(err, "creating stream sst batcher"))
		return
	}

	sip.rangeBatcher = newRangeKeyBatcher(ctx, st, db.KV(), sip.onFlushUpdateMetricUpdate)

	var subscriptionCtx context.Context
	subscriptionCtx, sip.subscriptionCancel = context.WithCancel(sip.Ctx())
	sip.subscriptionGroup = ctxgroup.WithContext(subscriptionCtx)
	sip.workerGroup = ctxgroup.WithContext(sip.Ctx())

	log.Infof(ctx, "starting %d stream partitions", len(sip.spec.PartitionSpecs))

	// Initialize the event streams.
	subscriptions := make(map[string]streamclient.Subscription)
	sip.streamPartitionClients = make([]streamclient.Client, 0)
	for _, partitionSpec := range sip.spec.PartitionSpecs {
		id := partitionSpec.PartitionID
		token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
		uri, err := streamclient.ParseClusterUri(partitionSpec.PartitionConnUri)
		if err != nil {
			sip.MoveToDrainingAndLogError(errors.Wrap(err, "parsing partition uri"))
			return
		}
		var streamClient streamclient.Client
		if sip.forceClientForTests != nil {
			streamClient = sip.forceClientForTests
			log.Infof(ctx, "using testing client")
		} else {
			streamClient, err = streamclient.NewStreamClient(ctx, uri, db,
				streamclient.WithStreamID(streampb.StreamID(sip.spec.StreamID)),
				streamclient.WithCompression(compress.Get(&st.SV)))
			if err != nil {

				sip.MoveToDrainingAndLogError(errors.Wrapf(err, "creating client for partition spec %q from %q", token, uri.Redacted()))
				return
			}
			sip.streamPartitionClients = append(sip.streamPartitionClients, streamClient)
		}

		if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
			if streamingKnobs != nil && streamingKnobs.BeforeClientSubscribe != nil {
				streamingKnobs.BeforeClientSubscribe(uri.Serialize(), string(token), sip.frontier, false)
			}
		}

		sub, err := streamClient.Subscribe(ctx, streampb.StreamID(sip.spec.StreamID),
			int32(sip.FlowCtx.NodeID.SQLInstanceID()), sip.ProcessorID,
			token,
			sip.spec.InitialScanTimestamp, sip.frontier)

		if err != nil {
			sip.MoveToDrainingAndLogError(errors.Wrapf(err, "consuming partition %v", uri.Redacted()))
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

	sip.mergedSubscription = MergeSubscriptions(sip.Ctx(), subscriptions)
	sip.workerGroup.GoCtx(func(ctx context.Context) error {
		if err := sip.mergedSubscription.Run(); err != nil {
			sip.sendError(errors.Wrap(err, "merge subscription"))
		}
		return nil
	})
	sip.workerGroup.GoCtx(func(ctx context.Context) error {
		if err := sip.checkForCutoverSignal(ctx); err != nil {
			sip.sendError(errors.Wrap(err, "cutover signal check"))
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
func (sip *streamIngestionProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
	case <-sip.aggTimer.C:
		sip.aggTimer.Read = true
		sip.aggTimer.Reset(15 * time.Second)
		return nil, bulkutil.ConstructTracingAggregatorProducerMeta(sip.Ctx(),
			sip.FlowCtx.NodeID.SQLInstanceID(), sip.FlowCtx.ID, sip.agg)
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

func (sip *streamIngestionProcessor) MoveToDrainingAndLogError(err error) {
	if err != nil {
		log.Infof(sip.Ctx(), "gracefully draining with error %s", err)
	}
	sip.MoveToDraining(err)
}

// MustBeStreaming implements the Processor interface.
func (sip *streamIngestionProcessor) MustBeStreaming() bool {
	return true
}

// ConsumerClosed is part of the RowSource interface.
func (sip *streamIngestionProcessor) ConsumerClosed() {
	sip.close()
}

func (sip *streamIngestionProcessor) close() {
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

	if sip.batcher != nil {
		sip.batcher.Close(sip.Ctx())
	}
	sip.maxFlushRateTimer.Stop()
	sip.aggTimer.Stop()

	sip.InternalClose()
}

// checkForCutoverSignal periodically loads the job progress to check for the
// sentinel value that signals the ingestion job to complete.
func (sip *streamIngestionProcessor) checkForCutoverSignal(ctx context.Context) error {
	sv := &sip.FlowCtx.Cfg.Settings.SV
	tick := time.NewTicker(cutoverSignalPollInterval.Get(sv))
	defer tick.Stop()
	for {
		select {
		case <-sip.stopCh:
			return nil
		case <-tick.C:
			cutoverReached, err := sip.cutoverProvider.cutoverReached(ctx)
			if err != nil {
				return err
			}
			if cutoverReached {
				select {
				case sip.cutoverCh <- struct{}{}:
				case <-sip.stopCh:
				}
				return nil
			}
		}
	}
}

func (sip *streamIngestionProcessor) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case sip.errCh <- err:
	default:
		log.VInfof(sip.Ctx(), 2, "dropping additional error: %s", err)
	}
}

func (sip *streamIngestionProcessor) flushLoop(_ context.Context) error {
	for {
		bufferToFlush, ok := <-sip.flushCh
		if !ok {
			// eventConsumer is done.
			return nil
		}
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
	}
}

func (sip *streamIngestionProcessor) onFlushUpdateMetricUpdate(batchSummary kvpb.BulkOpSummary) {
	sip.metrics.IngestedLogicalBytes.Inc(batchSummary.DataSize)
}

// consumeEvents handles processing events on the merged event queue and returns
// once a checkpoint event has been emitted so that it can inform the downstream
// frontier processor to consider updating the frontier.
//
// It should only make a claim that about the resolved timestamp of a partition
// increasing after it has flushed all KV events previously received by that
// partition.
func (sip *streamIngestionProcessor) consumeEvents(ctx context.Context) error {
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
		case <-sip.cutoverCh:
			// TODO(adityamaru): Currently, the cutover time can only be <= resolved
			// ts written to the job progress and so there is no point flushing
			// buffered KVs only to be reverted. When we allow users to specify a
			// cutover ts in the future, this will need to change.
			//
			// On receiving a cutover signal, the processor must shutdown gracefully.
			log.Infof(sip.Ctx(), "received cutover signal")
			return nil
		case <-sip.maxFlushRateTimer.C:
			// This timer is used to periodically flush a
			// buffer that may have been previously
			// skipped.
			sip.maxFlushRateTimer.Read = true
			if err := sip.flush(); err != nil {
				return err
			}
		}
	}

}

func (sip *streamIngestionProcessor) handleEvent(event PartitionEvent) error {
	sv := &sip.FlowCtx.Cfg.Settings.SV

	if event.Type() == crosscluster.KVEvent {
		sip.metrics.AdmitLatency.RecordValue(
			timeutil.Since(event.GetKVs()[0].KeyValue.Value.Timestamp.GoTime()).Nanoseconds())
	}

	if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.RunAfterReceivingEvent != nil {
			if err := streamingKnobs.RunAfterReceivingEvent(sip.Ctx()); err != nil {
				return err
			}
		}
	}

	switch event.Type() {
	case crosscluster.KVEvent:
		if err := sip.bufferKVs(event.GetKVs()); err != nil {
			return err
		}
	case crosscluster.SSTableEvent:
		if err := sip.bufferSST(event.GetSSTable()); err != nil {
			return err
		}
	case crosscluster.DeleteRangeEvent:
		if err := sip.bufferDelRange(event.GetDeleteRange()); err != nil {
			return err
		}
	case crosscluster.CheckpointEvent:
		if err := sip.bufferCheckpoint(event); err != nil {
			return err
		}

		minFlushInterval := minimumFlushInterval.Get(sv)
		if timeutil.Since(sip.lastFlushTime) < minFlushInterval {
			// Not enough time has passed since the last flush. Let's set a timer
			// that will trigger a flush eventually.
			// TODO: This resets the timer every checkpoint event, but we only
			// need to reset it once.
			sip.maxFlushRateTimer.Reset(time.Until(sip.lastFlushTime.Add(minFlushInterval)))
			return nil
		}
		if err := sip.flush(); err != nil {
			return err
		}
		return nil
	case crosscluster.SplitEvent:
		if err := sip.handleSplitEvent(event.GetSplitEvent()); err != nil {
			return err
		}
	default:
		return errors.Newf("unknown streaming event type %v", event.Type())
	}

	if sip.logBufferEvery.ShouldLog() {
		log.Infof(sip.Ctx(), "current KV batch size %d (%d items)", sip.buffer.curKVBatchSize, len(sip.buffer.curKVBatch))
	}

	if sip.buffer.shouldFlushOnSize(sip.Ctx(), sv) {
		if err := sip.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (sip *streamIngestionProcessor) rekey(key roachpb.Key) ([]byte, bool, error) {
	return sip.rekeyer.RewriteTenant(key)
}

func (sip *streamIngestionProcessor) bufferSST(sst *kvpb.RangeFeedSSTable) error {
	// TODO(casper): we currently buffer all keys in an SST at once even for large SSTs.
	// If in the future we decide buffer them in separate batches, we need to be
	// careful with checkpoints: we can only send checkpoint whose TS >= SST batch TS
	// after the full SST gets ingested.

	_, sp := tracing.ChildSpan(sip.Ctx(), "stream-ingestion-buffer-sst")
	defer sp.Finish()
	return replicationutils.ScanSST(sst, sst.Span,
		func(keyVal storage.MVCCKeyValue) error {
			// TODO(ssd): We technically get MVCCValueHeaders in our
			// SSTs. But currently there are so many ways _not_ to
			// get them that writing them here would just be
			// confusing until we fix them all.
			mvccValue, err := storage.DecodeValueFromMVCCValue(keyVal.Value)
			if err != nil {
				return err
			}

			return sip.bufferKVs([]streampb.StreamEvent_KV{{
				KeyValue: roachpb.KeyValue{
					Key: keyVal.Key.Key,
					Value: roachpb.Value{
						RawBytes:  mvccValue.RawBytes,
						Timestamp: keyVal.Key.Timestamp,
					},
				}}})
		}, func(rangeKeyVal storage.MVCCRangeKeyValue) error {
			return sip.bufferRangeKeyVal(rangeKeyVal)
		})
}

func (sip *streamIngestionProcessor) bufferDelRange(delRange *kvpb.RangeFeedDeleteRange) error {
	tombstoneVal, err := storage.EncodeMVCCValue(storage.MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{
			LocalTimestamp: hlc.ClockTimestamp{
				WallTime: 0,
			}},
	})
	if err != nil {
		return err
	}
	return sip.bufferRangeKeyVal(storage.MVCCRangeKeyValue{
		RangeKey: storage.MVCCRangeKey{
			StartKey:  delRange.Span.Key,
			EndKey:    delRange.Span.EndKey,
			Timestamp: delRange.Timestamp,
		},
		Value: tombstoneVal,
	})
}

func (sip *streamIngestionProcessor) bufferRangeKeyVal(
	rangeKeyVal storage.MVCCRangeKeyValue,
) error {
	_, sp := tracing.ChildSpan(sip.Ctx(), "stream-ingestion-buffer-range-key")
	defer sp.Finish()

	var err error
	var ok bool
	rangeKeyVal.RangeKey.StartKey, ok, err = sip.rekey(rangeKeyVal.RangeKey.StartKey)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	rangeKeyVal.RangeKey.EndKey, ok, err = sip.rekey(rangeKeyVal.RangeKey.EndKey)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	sip.buffer.addRangeKey(rangeKeyVal)
	return nil
}

func (sip *streamIngestionProcessor) handleSplitEvent(key *roachpb.Key) error {
	ctx, sp := tracing.ChildSpan(sip.Ctx(), "replicated-split")
	defer sp.Finish()
	if !ingestSplitEvent.Get(&sip.FlowCtx.Cfg.Settings.SV) {
		return nil
	}
	kvDB := sip.FlowCtx.Cfg.DB.KV()
	rekey, ok, err := sip.rekey(*key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	log.Infof(ctx, "replicating split at %s", roachpb.Key(rekey).String())
	expiration := kvDB.Clock().Now().AddDuration(time.Hour)
	return kvDB.AdminSplit(ctx, rekey, expiration)
}

func (sip *streamIngestionProcessor) bufferKVs(kvs []streampb.StreamEvent_KV) error {
	// TODO: In addition to flushing when receiving a checkpoint event, we
	// should also flush when we've buffered sufficient KVs. A buffering adder
	// would save us here.
	if kvs == nil {
		return errors.New("kv event expected to have kv")
	}
	for _, ev := range kvs {
		kv := ev.KeyValue
		var err error
		var ok bool
		kv.Key, ok, err = sip.rekey(kv.Key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if sip.rewriteToDiffKey {
			kv.Value.ClearChecksum()
			kv.Value.InitChecksum(kv.Key)
		}

		sip.buffer.addKV(storage.MVCCKeyValue{
			Key: storage.MVCCKey{
				Key:       kv.Key,
				Timestamp: kv.Value.Timestamp,
			},
			Value: kv.Value.RawBytes,
		})
	}
	return nil
}

func (sip *streamIngestionProcessor) bufferCheckpoint(event PartitionEvent) error {
	if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
		if streamingKnobs != nil && streamingKnobs.ElideCheckpointEvent != nil {
			if streamingKnobs.ElideCheckpointEvent(sip.FlowCtx.NodeID.SQLInstanceID(), sip.frontier.Frontier()) {
				return nil
			}
		}
	}

	resolvedSpans := event.GetCheckpoint().ResolvedSpans
	if resolvedSpans == nil {
		return errors.New("checkpoint event expected to have resolved spans")
	}

	lowestTimestamp := hlc.MaxTimestamp
	highestTimestamp := hlc.MinTimestamp
	d := quantize.Get(&sip.FlowCtx.Cfg.Settings.SV)
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
		if resolvedSpan.Timestamp.Less(lowestTimestamp) {
			lowestTimestamp = resolvedSpan.Timestamp
		}
		if highestTimestamp.Less(resolvedSpan.Timestamp) {
			highestTimestamp = resolvedSpan.Timestamp
		}
		_, err := sip.frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
		if err != nil {
			return errors.Wrap(err, "unable to forward checkpoint frontier")
		}
	}
	sip.metrics.ResolvedEvents.Inc(1)
	return nil
}

type rangeKeySST struct {
	start roachpb.Key
	end   roachpb.Key
	data  []byte
}

// Flush all the range keys buffered so far into storage as an SST.
func (r *rangeKeyBatcher) flush(ctx context.Context, toFlush mvccRangeKeyValues) error {
	_, sp := tracing.ChildSpan(ctx, "physical.rangeKeyBatcher.flush")
	defer sp.Finish()

	if len(toFlush) == 0 {
		return nil
	}

	log.VInfof(ctx, 2, "flushing %d range keys", len(toFlush))

	sstFile := &storage.MemObject{}
	sstWriter := storage.MakeIngestionSSTWriter(ctx, r.settings, sstFile)
	defer sstWriter.Close()
	// Sort current batch as the SST writer requires a sorted order.
	sort.Slice(toFlush, func(i, j int) bool {
		return toFlush[i].RangeKey.Compare(toFlush[j].RangeKey) < 0
	})

	batchSummary := kvpb.BulkOpSummary{}
	start, end := keys.MaxKey, keys.MinKey
	for _, rangeKeyVal := range toFlush {
		if err := sstWriter.PutRawMVCCRangeKey(rangeKeyVal.RangeKey, rangeKeyVal.Value); err != nil {
			return err
		}

		if rangeKeyVal.RangeKey.StartKey.Compare(start) < 0 {
			start = rangeKeyVal.RangeKey.StartKey
		}
		if rangeKeyVal.RangeKey.EndKey.Compare(end) > 0 {
			end = rangeKeyVal.RangeKey.EndKey
		}
		batchSummary.DataSize += int64(rangeKeyVal.RangeKey.EncodedSize() + len(rangeKeyVal.Value))
	}

	// Finish the current batch.
	if err := sstWriter.Finish(); err != nil {
		return err
	}

	sstToFlush := &rangeKeySST{
		data:  sstFile.Bytes(),
		start: start,
		// NB: End is set from the range key EndKey, which is
		// already exclusive.
		end: end,
	}

	work := []*rangeKeySST{sstToFlush}
	for len(work) > 0 {
		item := work[0]
		work = work[1:]

		start := item.start
		end := item.end
		data := item.data

		ingestAsWrites := false
		asWritesMax := int(tooSmallRangeKeySize.Get(&r.settings.SV))
		if asWritesMax > 0 && len(data) <= asWritesMax {
			ingestAsWrites = true
		}

		log.Infof(ctx, "sending SSTable [%s, %s) of size %d (as write: %v)", start, end, len(data), ingestAsWrites)
		_, _, err := r.db.AddSSTable(ctx, start, end, data,
			false, /* disallowConflicts */
			hlc.Timestamp{}, nil /* stats */, ingestAsWrites,
			r.db.Clock().Now())
		if err != nil {
			if m := (*kvpb.RangeKeyMismatchError)(nil); errors.As(err, &m) {
				mr, err := m.MismatchedRange()
				if err != nil {
					return err
				}

				split := mr.Desc.EndKey.AsRawKey()
				log.Infof(ctx, "SSTable cannot be added spanning range bounds. Spliting at %v", split)
				left, right, err := splitRangeKeySSTAtKey(ctx, r.settings, start, end, split, data)
				if err != nil {
					return err
				}

				if left != nil && right != nil {
					work = append([]*rangeKeySST{left, right}, work...)
				} else if left != nil {
					log.Warningf(ctx, "RHS of split point %s was unexpectedly empty", split)
					work = append([]*rangeKeySST{left}, work...)
				} else if right != nil {
					log.Warningf(ctx, "LHS of split point %s was unexpectedly empty", split)
					work = append([]*rangeKeySST{right}, work...)
				}
			} else {
				return err
			}
		} else {
			batchSummary.SSTDataSize += int64(len(data))
		}
	}

	if r.onFlush != nil {
		r.onFlush(batchSummary)
	}

	return nil
}

// splitRangeKeySSTAtKey splits the given SST (passed as bytes) at the
// given split key.
//
// The SST is assumed to only contain range keys. The function will
// return an error if a point key is found.
//
// The caller should take care that the provided start and end key are
// correct.
//
// This is similar to createSplitSSTable in pkg/kv/bulk/sst_batcher.go
func splitRangeKeySSTAtKey(
	ctx context.Context, st *cluster.Settings, start, end, splitKey roachpb.Key, data []byte,
) (*rangeKeySST, *rangeKeySST, error) {
	// Special case: The split key less than the start key.
	if splitKey.Compare(start) < 0 {
		return nil, &rangeKeySST{start: start, end: end, data: data}, nil
	}

	// Special case: The split key is greater or equal to the
	// exclusive end key.
	if end.Compare(splitKey) <= 0 {
		return &rangeKeySST{start: start, end: end, data: data}, nil, nil
	}

	var (
		// left and right are our output SSTs.
		// Data less than the split key is written into left.
		// Data greater than or equal to the split key is written into right.
		left  = &storage.MemObject{}
		right = &storage.MemObject{}

		// We return these.
		leftRet  *rangeKeySST
		rightRet *rangeKeySST

		// We track the first and last key written into each SST.  This
		// avoids a situation where we have an SST with
		//
		//   a----c g-----h
		//
		// and a split key of d. Returning `d` as the start of the RHS
		// SST would mean then we are are risk of getting another split
		// point `f` when processing the RHS where the LHS of the split
		// would be empty. Let's avoid empty SSTs.
		first roachpb.Key
		last  roachpb.Key

		// reachedSplit tracks if we've already reached our split key.
		reachedSplit = false

		// We start writting into the left side. Eventualy
		// we'll swap in the RHS writer.
		leftWriter  = storage.MakeIngestionSSTWriter(ctx, st, left)
		rightWriter = storage.MakeIngestionSSTWriter(ctx, st, right)
		writer      = &leftWriter
	)
	defer leftWriter.Close()
	defer rightWriter.Close()

	flushLHSAndSwitchToRHSWriter := func() error {
		if err := writer.Finish(); err != nil {
			return err
		}
		if first == nil || last == nil {
			return errors.AssertionFailedf("likely prorgramming error: invalid SST bounds on RHS [%v, %v)", first, last)
		}

		leftRet = &rangeKeySST{start: first, end: last, data: left.Data()}
		writer = &rightWriter
		last = nil
		first = nil
		reachedSplit = true
		return nil
	}

	iter, err := storage.NewMemSSTIterator(data, true, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: start})
	for {
		if ok, err := iter.Valid(); err != nil {
			return nil, nil, err
		} else if !ok {
			break
		}

		if hasPoint, _ := iter.HasPointAndRange(); hasPoint {
			return nil, nil, errors.AssertionFailedf("unexpected point key in range key SST")
		}

		rangeKeys := iter.RangeKeys()
		if !reachedSplit && rangeKeys.Bounds.Key.Compare(splitKey) >= 0 {
			// The start of this range key is greater than or equal
			// to our split key -- it should be written to the right
			// side of the split.
			if err := flushLHSAndSwitchToRHSWriter(); err != nil {
				return nil, nil, err
			}
		} else if !reachedSplit && rangeKeys.Bounds.EndKey.Compare(splitKey) >= 0 {
			// The end of this range key is greater than or equal to
			// our split key. We need to write this range key to
			// both sides.
			// Truncate this range key to the split point and write
			// it to the left side.
			rangeKeys.Bounds.EndKey = splitKey
			if len(first) == 0 {
				first = append(first[:0], rangeKeys.Bounds.Key...)
			}
			// NB: We don't call Next() here because the
			// split key is exclusive already.
			last = append(last[:0], rangeKeys.Bounds.EndKey...)
			for _, rk := range rangeKeys.AsRangeKeys() {
				if err := writer.PutRawMVCCRangeKey(rk, []byte{}); err != nil {
					return nil, nil, err
				}
			}

			if err := flushLHSAndSwitchToRHSWriter(); err != nil {
				return nil, nil, err
			}

			iter.SeekGE(storage.MVCCKey{Key: splitKey})
			if ok, err := iter.Valid(); err != nil {
				return nil, nil, err
			} else if !ok {
				break
			}

			if hasPoint, _ := iter.HasPointAndRange(); hasPoint {
				return nil, nil, errors.AssertionFailedf("unexpected point key in range key SST")
			}

			rangeKeys = iter.RangeKeys()
			// The range key at this point may extend left,
			// before the start of the new SST we want to
			// build. Truncate it.
			if rangeKeys.Bounds.Key.Compare(splitKey) < 0 {
				rangeKeys.Bounds.Key = splitKey
			}
		}

		if len(first) == 0 {
			first = append(first[:0], rangeKeys.Bounds.Key...)
		}
		last = append(last[:0], rangeKeys.Bounds.EndKey...)
		last.Next()
		for _, rk := range rangeKeys.AsRangeKeys() {
			if err := writer.PutRawMVCCRangeKey(rk, []byte{}); err != nil {
				return nil, nil, err
			}
		}
		iter.Next()
	}

	if !reachedSplit {
		return nil, nil, errors.AssertionFailedf("likely programming error: split point %s not found in SST", splitKey)
	}

	if err := writer.Finish(); err != nil {
		return nil, nil, err
	}

	if first == nil || last == nil {
		return nil, nil, errors.AssertionFailedf("likely prorgramming error: invalid SST bounds on RHS [%v, %v)", first, last)
	}

	rightRet = &rangeKeySST{start: first, end: last, data: right.Data()}
	return leftRet, rightRet, nil
}

func (sip *streamIngestionProcessor) flush() error {
	bufferToFlush := sip.buffer
	sip.buffer = getBuffer()

	checkpoint := &jobspb.ResolvedSpans{ResolvedSpans: make([]jobspb.ResolvedSpan, 0, sip.frontier.Len())}
	sip.frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if !ts.IsEmpty() {
			checkpoint.ResolvedSpans = append(checkpoint.ResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		}
		return span.ContinueMatch
	})

	select {
	case sip.flushCh <- flushableBuffer{
		buffer:     bufferToFlush,
		checkpoint: checkpoint,
	}:
		sip.lastFlushTime = timeutil.Now()
		return nil
	case <-sip.stopCh:
		// We return on stopCh here because our flush process
		// may have been stopped or exited on error.
		return nil
	}
}

type flushableBuffer struct {
	buffer     *streamIngestionBuffer
	checkpoint *jobspb.ResolvedSpans
}

// flushBuffer flushes the given streamIngestionBuffer via the SST
// batchers and returns the underlying streamIngestionBuffer to the pool.
func (sip *streamIngestionProcessor) flushBuffer(b flushableBuffer) (*jobspb.ResolvedSpans, error) {
	ctx, sp := tracing.ChildSpan(sip.Ctx(), "stream-ingestion-flush")
	defer sp.Finish()
	// Ensure the batcher is always reset, even on early error returns.
	defer sip.batcher.Reset(ctx)

	// First process the point KVs.
	//
	// Ensure that the current batch is sorted.
	sort.Sort(b.buffer.curKVBatch)
	for _, keyVal := range b.buffer.curKVBatch {
		if err := sip.batcher.AddMVCCKey(ctx, keyVal.Key, keyVal.Value); err != nil {
			return nil, errors.Wrapf(err, "adding key %+v", keyVal)
		}
	}

	preFlushTime := timeutil.Now()
	if len(b.buffer.curKVBatch) > 0 {
		if err := sip.batcher.Flush(ctx); err != nil {
			return nil, errors.Wrap(err, "flushing sst batcher")
		}
	}

	// Now process the range KVs.
	if len(b.buffer.curRangeKVBatch) > 0 {
		if err := sip.rangeBatcher.flush(ctx, b.buffer.curRangeKVBatch); err != nil {
			log.Warningf(ctx, "flush error: %v", err)
			return nil, errors.Wrap(err, "flushing range key sst")
		}
	}

	// Update the flush metrics.
	sip.metrics.FlushHistNanos.RecordValue(timeutil.Since(preFlushTime).Nanoseconds())
	sip.metrics.CommitLatency.RecordValue(timeutil.Since(b.buffer.minTimestamp.GoTime()).Nanoseconds())
	sip.metrics.Flushes.Inc(1)
	sip.metrics.IngestedEvents.Inc(int64(len(b.buffer.curKVBatch)))
	sip.metrics.IngestedEvents.Inc(int64(len(b.buffer.curRangeKVBatch)))

	releaseBuffer(b.buffer)

	return b.checkpoint, nil
}

// cutoverProvider allows us to override how we decide when the job has reached
// the cutover places in tests.
type cutoverProvider interface {
	cutoverReached(context.Context) (bool, error)
}

// custoverFromJobProgress is a cutoverProvider that decides whether the cutover
// time has been reached based on the progress stored on the job record.
type cutoverFromJobProgress struct {
	db    isql.DB
	jobID jobspb.JobID
}

func (c *cutoverFromJobProgress) cutoverReached(ctx context.Context) (bool, error) {
	ingestionProgress, err := replicationutils.LoadIngestionProgress(ctx, c.db, c.jobID)
	if err != nil {
		return false, err
	}
	if ingestionProgress == nil {
		log.Warningf(ctx, "no legacy job progress recorded yet")
		return false, nil
	}

	cutoverTime := ingestionProgress.CutoverTime
	replicatedTime := ingestionProgress.ReplicatedTime
	if !cutoverTime.IsEmpty() && cutoverTime.LessEq(replicatedTime) {
		return true, nil
	}

	return false, nil
}

func init() {
	rowexec.NewStreamIngestionDataProcessor = newStreamIngestionDataProcessor
}
