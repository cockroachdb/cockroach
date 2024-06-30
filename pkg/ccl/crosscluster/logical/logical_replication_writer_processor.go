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
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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

	// purgatory is an ordered list of purgatory levels, each consisting of some
	// number of events that need to be durably processed to finish that level and
	// an optional checkpoint that can be applied when it is fully processed. If
	// purgatory is non-empty, incoming checkpoints must go to purgatory instead
	// of being emitted, and will be emitted when the purgatory level is processed
	// instead.
	purgatory []purgatory
}

type purgatory struct {
	events                  []streampb.StreamEvent_KV
	willResolve             []jobspb.ResolvedSpan
	closedAt, lastAttempted time.Time
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
	frontier, err := span.MakeFrontierAt(spec.PreviousReplicatedTimestamp, spec.PartitionSpec.Spans...)
	if err != nil {
		return nil, err
	}
	for _, resolvedSpan := range spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return nil, err
		}
	}

	bhPool := make([]BatchHandler, maxWriterWorkers)
	for i := range bhPool {
		rp, err := makeSQLLastWriteWinsHandler(
			ctx, flowCtx.Cfg.Settings, spec.TableDescriptors,
			// Initialize the executor with a fresh session data - this will
			// avoid creating a new copy on each executor usage.
			flowCtx.Cfg.DB.Executor(isql.WithSessionData(sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */))),
		)
		if err != nil {
			return nil, err
		}
		bhPool[i] = &txnBatch{
			db:       flowCtx.Cfg.DB,
			rp:       rp,
			settings: flowCtx.Cfg.Settings,
			sd:       sql.NewInternalSessionData(ctx, flowCtx.Cfg.Settings, "" /* opName */),
		}
	}

	lrw := &logicalReplicationWriterProcessor{
		spec:           spec,
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
// The consumeEvents loop builds a buffer of KVs that it then sends to
// the flushLoop. We currently allow 1 in-flight flush.
//
//	client.Subscribe -> consumeEvents -> flushLoop -> Next()
//
// All errors are reported to Next() via errCh, with the first
// error winning.
//
// Start implements the RowSource interface.
func (lrw *logicalReplicationWriterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", lrw.spec.JobID)
	streampb.RegisterActiveLogicalConsumerStatus(&lrw.debug)

	ctx = lrw.StartInternal(ctx, logicalReplicationWriterProcessorName)

	lrw.metrics = lrw.FlowCtx.Cfg.JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeLogicalReplication].(*Metrics)

	db := lrw.FlowCtx.Cfg.DB

	log.Infof(ctx, "starting logical replication writer for partitions %v", lrw.spec.PartitionSpec)

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
		// If purgatory is full give it a chance to drain before processing the next
		// incoming stream buffer.
		if lrw.purgatoryFull() {
			if err := lrw.maybeDrainPurgatory(ctx); err != nil {
				return err
			}
		}
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

	// If purgatory is empty, just emit the checkpoint now.
	if len(lrw.purgatory) == 0 {
		return lrw.checkpoint(ctx, resolvedSpans)
	}

	// If the current purgatory level is already closed, make a new one.
	if lrw.purgatory[len(lrw.purgatory)-1].willResolve != nil {
		lrw.purgatory = append(lrw.purgatory, purgatory{})
	}
	// Close the current layer and mark it as resolving this checkpoint.
	lrw.purgatory[len(lrw.purgatory)-1].willResolve = resolvedSpans
	lrw.purgatory[len(lrw.purgatory)-1].closedAt = timeutil.Now()

	// Attempt to drain purgatory.
	return lrw.maybeDrainPurgatory(ctx)
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
	}
	lrw.metrics.CheckpointEvents.Inc(1)
	return nil
}

// flushBuffer flushes a buffer using many workers if needed.
func (lrw *logicalReplicationWriterProcessor) handleStreamBuffer(
	ctx context.Context, kvs []streampb.StreamEvent_KV,
) error {
	unapplied, err := lrw.flushBuffer(ctx, kvs, false)
	if err != nil {
		return err
	}
	// Put any events that failed to apply into purgatory.
	if len(unapplied) > 0 {
		levels := len(lrw.purgatory)
		// Open a new level if there is no level or the current level is closed.
		if levels == 0 || lrw.purgatory[levels-1].willResolve != nil {
			lrw.purgatory = append(lrw.purgatory, purgatory{})
		}
		lrw.purgatory[levels-1].events = unapplied
	}

	return nil
}

func filterRemaining(kvs []streampb.StreamEvent_KV) []streampb.StreamEvent_KV {
	failures := kvs
	var j int
	for i := range kvs {
		if len(kvs[i].KeyValue.Key) != 0 {
			failures[j] = kvs[i]
			j++
		}
	}
	return failures[:j]
}

const maxWriterWorkers = 32

// flushBuffer flushes a buffer using many workers if needed.
func (lrw *logicalReplicationWriterProcessor) flushBuffer(
	ctx context.Context, kvs []streampb.StreamEvent_KV, mustProcess bool,
) (purgatory []streampb.StreamEvent_KV, _ error) {
	ctx, sp := tracing.ChildSpan(ctx, "logical-replication-writer-flush")
	defer sp.Finish()

	if len(kvs) < 1 {
		return nil, nil
	}

	// Ensure the batcher is always reset, even on early error returns.
	preFlushTime := timeutil.Now()
	lrw.debug.RecordFlushStart(preFlushTime, int64(len(kvs)))

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

	var flushByteSize, notProcessed atomic.Int64

	const minChunkSize = 64
	chunkSize := max((len(kvs)/len(lrw.bh))+1, minChunkSize)

	g := ctxgroup.WithContext(ctx)
	for worker := range lrw.bh {
		if len(kvs) == 0 {
			break
		}
		// The chunk should end after the first new key after chunk size.
		chunkEnd := min(chunkSize, len(kvs))
		for chunkEnd < len(kvs) && k(kvs[chunkEnd-1]).Equal(k(kvs[chunkEnd])) {
			chunkEnd++
		}
		chunk := kvs[0:chunkEnd]
		kvs = kvs[len(chunk):]
		bh := lrw.bh[worker]

		g.GoCtx(func(ctx context.Context) error {
			s, err := lrw.flushChunk(ctx, bh, chunk, mustProcess)
			if err != nil {
				return err
			}
			flushByteSize.Add(s.byteSize)
			notProcessed.Add(s.notProcessed)
			lrw.metrics.OptimisticInsertConflictCount.Inc(s.optimisticInsertConflicts)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return kvs, err
	}

	if notProcessed.Load() > 0 {
		purgatory = filterRemaining(kvs)
	}

	flushTime := timeutil.Since(preFlushTime).Nanoseconds()
	keyCount, byteCount := int64(len(kvs)), flushByteSize.Load()
	lrw.debug.RecordFlushComplete(flushTime, keyCount, byteCount)

	lrw.metrics.AppliedRowUpdates.Inc(int64(len(kvs)))
	lrw.metrics.AppliedLogicalBytes.Inc(byteCount)
	lrw.metrics.CommitToCommitLatency.RecordValue(timeutil.Since(firstKeyTS).Nanoseconds())

	lrw.metrics.StreamBatchNanosHist.RecordValue(flushTime)
	lrw.metrics.StreamBatchRowsHist.RecordValue(keyCount)
	lrw.metrics.StreamBatchBytesHist.RecordValue(byteCount)
	return purgatory, nil
}

func (lrw *logicalReplicationWriterProcessor) flushChunk(
	ctx context.Context, bh BatchHandler, chunk []streampb.StreamEvent_KV, mustProcess bool,
) (batchStats, error) {
	batchSize := int(flushBatchSize.Get(&lrw.FlowCtx.Cfg.Settings.SV))
	// TODO(yuzefovich): we should have a better heuristic for when to use the
	// implicit vs explicit txns (for example, depending on the presence of the
	// secondary indexes).
	if useImplicitTxns.Get(&lrw.FlowCtx.Cfg.Settings.SV) {
		batchSize = 1
	}

	var stats batchStats
	// TODO: The batching here in production would need to be much
	// smarter. Namely, we don't want to include updates to the
	// same key in the same batch. Also, it's possible batching
	// will make things much worse in practice.
	for len(chunk) > 0 {
		batch := chunk[:min(batchSize, len(chunk))]
		chunk = chunk[len(batch):]
		preBatchTime := timeutil.Now()

		if s, err := bh.HandleBatch(ctx, batch); err != nil {
			// If it already failed while applying on its own, handle the failure.
			if len(batch) == 1 {
				if mustProcess || !lrw.shouldPurgatory(err) {
					if err := lrw.dlq(ctx, batch[0], err); err != nil {
						return batchStats{}, err
					}
				}
			} else {
				// If there were multiple events in the batch, give each its own chance
				// to apply on its own before switching to handle its failure.
				for i := range batch {
					if singleStats, err := bh.HandleBatch(ctx, batch); err != nil {
						if mustProcess || !lrw.shouldPurgatory(err) {
							if err := lrw.dlq(ctx, batch[i], err); err != nil {
								return batchStats{}, err
							}
						}
					} else {
						batch[i] = streampb.StreamEvent_KV{}
						stats.Add(singleStats)
					}
				}
			}
		} else {
			// Clear the event to indicate successful application.
			for i := range batch {
				batch[i] = streampb.StreamEvent_KV{}
			}
			stats.Add(s)
		}

		batchTime := timeutil.Since(preBatchTime)
		lrw.debug.RecordBatchApplied(batchTime, int64(len(batch)))
		lrw.metrics.ApplyBatchNanosHist.RecordValue(batchTime.Nanoseconds())
	}
	return stats, nil
}

// shouldPurgatory returns true if a given error might be resolved if applying
// the event which encountered is reattempted again at a later time, for example
// after the other side of an FK relationship is ingested by another processor.
func (lrw *logicalReplicationWriterProcessor) shouldPurgatory(err error) bool {
	// TODO(dt): maybe this should only be constraint errors?
	return true
}

// dlq handles a row update that fails to apply by durably recording it in a DLQ
// or returns an error if it cannot.
//
// TODO(dt): implement something here.
// TODO(dt): plumb the cdcevent.Row to this.
func (lrw *logicalReplicationWriterProcessor) dlq(
	ctx context.Context, event streampb.StreamEvent_KV, applyErr error,
) error {
	// TODO(dt): try to DLQ it.
	return applyErr
}

func (lrw *logicalReplicationWriterProcessor) maybeDrainPurgatory(ctx context.Context) error {
	var resolved int
	for i := range lrw.purgatory {
		// If we need to make space, or if the events have been in purgatory for a
		// over a minute, then any events that still fail to apply must just DLQ.
		mustProcess := (i == 0 && lrw.purgatoryFull()) ||
			timeutil.Since(lrw.purgatory[i].closedAt) > time.Minute

		// If tried to flush this purgatory recently and it isn't required to flush
		// now, wait a until next time to try again.
		if timeutil.Since(lrw.purgatory[i].lastAttempted) < time.Second*5 && !mustProcess {
			break
		}

		lrw.purgatory[i].lastAttempted = timeutil.Now()
		remaining, err := lrw.flushBuffer(ctx, lrw.purgatory[i].events, mustProcess)
		if err != nil {
			return err
		}
		lrw.purgatory[i].events = remaining
		if len(remaining) > 0 || lrw.purgatory[i].willResolve == nil {
			break
		}
		if err := lrw.checkpoint(ctx, lrw.purgatory[i].willResolve); err != nil {
			return err
		}
		resolved++
	}
	// Remove all levels that were resolved.
	lrw.purgatory = lrw.purgatory[resolved:]
	return nil
}

func (lrw *logicalReplicationWriterProcessor) purgatoryFull() bool {
	// TODO(dt): make this smarter.
	return len(lrw.purgatory) >= 10
}

type batchStats struct {
	notProcessed              int64
	byteSize                  int64
	optimisticInsertConflicts int64
}

func (b *batchStats) Add(o batchStats) {
	b.byteSize += o.byteSize
	b.optimisticInsertConflicts += o.optimisticInsertConflicts
}

type BatchHandler interface {
	// HandleBatch handles one batch, i.e. a set of 1 or more KVs, that should be
	// decoded to rows and committed in a single txn, i.e. that all succeed to apply
	// or are not applied as a group. If the batch is a single KV it may use an
	// implicit txn.
	HandleBatch(context.Context, []streampb.StreamEvent_KV) (batchStats, error)
}

// RowProcessor knows how to process a single row from an event stream.
type RowProcessor interface {
	// ProcessRow processes a single KV update by inserting or deleting a row.
	// Txn argument can be nil. The provided value is the "previous value",
	// before the change was applied on the source.
	ProcessRow(context.Context, isql.Txn, roachpb.KeyValue, roachpb.Value) (batchStats, error)
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
		rowStats, err := t.rp.ProcessRow(ctx, nil /* txn */, batch[0].KeyValue, batch[0].PrevValue)
		if err != nil {
			return stats, err
		}
		stats.Add(rowStats)
	} else {
		var txnStats batchStats
		err = t.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			txnStats = batchStats{}
			for _, kv := range batch {
				rowStats, err := t.rp.ProcessRow(ctx, txn, kv.KeyValue, kv.PrevValue)
				if err != nil {
					return err
				}
				txnStats.Add(rowStats)
			}
			return nil
		}, isql.WithSessionData(t.sd))
		stats = txnStats
	}
	return stats, err
}

func init() {
	rowexec.NewLogicalReplicationWriterProcessor = newLogicalReplicationWriterProcessor
}
