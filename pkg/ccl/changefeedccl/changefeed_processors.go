// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeeddist"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

type changeAggregator struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ChangeAggregatorSpec
	memAcc  mon.BoundAccount

	// cancel shuts down the processor, both the `Next()` flow and the kvfeed.
	cancel func()
	// errCh contains the return values of the kvfeed.
	errCh chan error
	// kvFeedDoneCh is closed when the kvfeed exits.
	kvFeedDoneCh chan struct{}
	kvFeedMemMon *mon.BytesMonitor

	// encoder is the Encoder to use for key and value serialization.
	encoder Encoder
	// sink is the Sink to write rows to. Resolved timestamps are never written
	// by changeAggregator.
	sink Sink
	// changedRowBuf, if non-nil, contains changed rows to be emitted. Anything
	// queued in `resolvedSpanBuf` is dependent on these having been emitted, so
	// this one must be empty before moving on to that one.
	changedRowBuf *encDatumRowBuffer
	// resolvedSpanBuf contains resolved span updates to send to changeFrontier.
	// If sink is a bufferSink, it must be emptied before these are sent.
	resolvedSpanBuf encDatumRowBuffer

	// recentKVCount contains the number of emits since the last time a resolved
	// span was forwarded to the frontier
	recentKVCount uint64

	// eventProducer produces the next event from the kv feed.
	eventProducer kvevent.Reader
	// eventConsumer consumes the event.
	eventConsumer kvEventConsumer

	// lastFlush and flushFrequency keep track of the flush frequency.
	lastFlush      time.Time
	flushFrequency time.Duration

	// frontier keeps track of resolved timestamps for spans along with schema change
	// boundary information.
	frontier *schemaChangeFrontier

	metrics    *Metrics
	sliMetrics *sliMetrics
	knobs      TestingKnobs
}

type timestampLowerBoundOracle interface {
	inclusiveLowerBoundTS() hlc.Timestamp
}

type changeAggregatorLowerBoundOracle struct {
	sf                         *span.Frontier
	initialInclusiveLowerBound hlc.Timestamp
}

// inclusiveLowerBoundTs is used to generate a representative timestamp to name
// cloudStorageSink files. This timestamp is either the statement time (in case this
// changefeed job hasn't yet seen any resolved timestamps) or the successor timestamp to
// the local span frontier. This convention is chosen to preserve CDC's ordering
// guarantees. See comment on cloudStorageSink for more details.
func (o *changeAggregatorLowerBoundOracle) inclusiveLowerBoundTS() hlc.Timestamp {
	if frontier := o.sf.Frontier(); !frontier.IsEmpty() {
		// We call `Next()` here on the frontier because this allows us
		// to name files using a timestamp that is an inclusive lower bound
		// on the timestamps of the updates contained within the file.
		// Files being created at the point this method is called are guaranteed
		// to contain row updates with timestamps strictly greater than the local
		// span frontier timestamp.
		return frontier.Next()
	}
	// This should only be returned in the case where the changefeed job hasn't yet
	// seen a resolved timestamp.
	return o.initialInclusiveLowerBound
}

var _ execinfra.Processor = &changeAggregator{}
var _ execinfra.RowSource = &changeAggregator{}

func newChangeAggregatorProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeAggregatorSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changeagg-mem")
	ca := &changeAggregator{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
	}
	if err := ca.Init(
		ca,
		post,
		changefeeddist.ChangefeedResultTypes,
		flowCtx,
		processorID,
		output,
		memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				ca.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	var err error
	if ca.encoder, err = getEncoder(ca.spec.Feed.Opts, AllTargets(ca.spec.Feed)); err != nil {
		return nil, err
	}

	// MinCheckpointFrequency controls how frequently the changeAggregator flushes the sink
	// and checkpoints the local frontier to changeFrontier. It is used as a rough
	// approximation of how latency-sensitive the changefeed user is. For a high latency
	// user, such as cloud storage sink where flushes can take much longer, it is often set
	// as the sink's flush frequency so as not to negate the sink's batch config.
	//
	// If a user does not specify a 'min_checkpoint_frequency' duration, we instead default
	// to 30s, which is hopefully long enough to account for most possible sink latencies we
	// could see without falling too behind.
	//
	// NB: As long as we periodically get new span-level resolved timestamps
	// from the poller (which should always happen, even if the watched data is
	// not changing), then this is sufficient and we don't have to do anything
	// fancy with timers.
	// // TODO(casper): add test for OptMinCheckpointFrequency.
	if r, ok := ca.spec.Feed.Opts[changefeedbase.OptMinCheckpointFrequency]; ok && r != `` {
		ca.flushFrequency, err = time.ParseDuration(r)
		if err != nil {
			return nil, err
		}
	} else {
		ca.flushFrequency = changefeedbase.DefaultMinCheckpointFrequency
	}

	return ca, nil
}

// MustBeStreaming implements the execinfra.Processor interface.
func (ca *changeAggregator) MustBeStreaming() bool {
	return true
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) {
	if ca.spec.JobID != 0 {
		ctx = logtags.AddTag(ctx, "job", ca.spec.JobID)
	}
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	// Derive a separate context so that we can shutdown the poller. Note that
	// we need to update both ctx (used throughout this function) and
	// ProcessorBase.Ctx (used in all other methods) to the new context.
	ctx, ca.cancel = context.WithCancel(ctx)
	ca.Ctx = ctx

	initialHighWater, needsInitialScan := getKVFeedInitialParameters(ca.spec)

	frontierHighWater := initialHighWater
	if needsInitialScan {
		// The frontier highwater marks the latest timestamp we don't need to emit
		// spans for, and therefore should be 0 if an initial scan is needed
		frontierHighWater = hlc.Timestamp{}
	}
	spans, err := ca.setupSpansAndFrontier(frontierHighWater)

	if err != nil {
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
	timestampOracle := &changeAggregatorLowerBoundOracle{
		sf:                         ca.frontier.SpanFrontier(),
		initialInclusiveLowerBound: ca.spec.Feed.StatementTime,
	}

	if cfKnobs, ok := ca.flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		ca.knobs = *cfKnobs
	}

	// TODO(yevgeniy): Introduce separate changefeed monitor that's a parent
	// for all changefeeds to control memory allocated to all changefeeds.
	pool := ca.flowCtx.Cfg.BackfillerMonitor
	if ca.knobs.MemMonitor != nil {
		pool = ca.knobs.MemMonitor
	}
	limit := changefeedbase.PerChangefeedMemLimit.Get(&ca.flowCtx.Cfg.Settings.SV)
	kvFeedMemMon := mon.NewMonitorInheritWithLimit("kvFeed", limit, pool)
	kvFeedMemMon.Start(ctx, pool, mon.BoundAccount{})
	ca.kvFeedMemMon = kvFeedMemMon

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	ca.metrics = ca.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	ca.sliMetrics, err = ca.metrics.getSLIMetrics(ca.spec.Feed.Opts[changefeedbase.OptMetricsScope])
	if err != nil {
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	ca.sink, err = getSink(ctx, ca.flowCtx.Cfg, ca.spec.Feed, timestampOracle,
		ca.spec.User(), ca.spec.JobID, ca.sliMetrics)

	if err != nil {
		err = changefeedbase.MarkRetryableError(err)
		// Early abort in the case that there is an error creating the sink.
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	// This is the correct point to set up certain hooks depending on the sink
	// type.
	if b, ok := ca.sink.(*bufferSink); ok {
		ca.changedRowBuf = &b.buf
	}

	ca.sink = &errorWrapperSink{wrapped: ca.sink}

	ca.eventProducer, err = ca.startKVFeed(ctx, spans, initialHighWater, needsInitialScan, ca.sliMetrics)
	if err != nil {
		// Early abort in the case that there is an error creating the sink.
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	if ca.spec.Feed.Opts[changefeedbase.OptFormat] == string(changefeedbase.OptFormatNative) {
		ca.eventConsumer = newNativeKVConsumer(ca.sink)
	} else {
		ca.eventConsumer = newKVEventToRowConsumer(
			ctx, ca.flowCtx.Cfg, ca.frontier.SpanFrontier(), initialHighWater,
			ca.sink, ca.encoder, ca.spec.Feed, ca.knobs)
	}
}

func (ca *changeAggregator) startKVFeed(
	ctx context.Context,
	spans []roachpb.Span,
	initialHighWater hlc.Timestamp,
	needsInitialScan bool,
	sm *sliMetrics,
) (kvevent.Reader, error) {
	cfg := ca.flowCtx.Cfg
	buf := kvevent.NewThrottlingBuffer(
		kvevent.NewMemBuffer(ca.kvFeedMemMon.MakeBoundAccount(), &cfg.Settings.SV, &ca.metrics.KVFeedMetrics),
		cdcutils.NodeLevelThrottler(&cfg.Settings.SV, &ca.metrics.ThrottleMetrics))

	// KVFeed takes ownership of the kvevent.Writer portion of the buffer, while
	// we return the kvevent.Reader part to the caller.
	kvfeedCfg := ca.makeKVFeedCfg(ctx, spans, buf, initialHighWater, needsInitialScan, sm)

	// Give errCh enough buffer both possible errors from supporting goroutines,
	// but only the first one is ever used.
	ca.errCh = make(chan error, 2)
	ca.kvFeedDoneCh = make(chan struct{})
	if err := ca.flowCtx.Stopper().RunAsyncTask(ctx, "changefeed-poller", func(ctx context.Context) {
		defer close(ca.kvFeedDoneCh)
		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.errCh <- kvfeed.Run(ctx, kvfeedCfg)
		ca.cancel()
	}); err != nil {
		// If err != nil then the RunAsyncTask closure never ran, which means we
		// need to manually close ca.kvFeedDoneCh so `(*changeAggregator).close`
		// doesn't hang.
		close(ca.kvFeedDoneCh)
		ca.errCh <- err
		ca.cancel()
		return nil, err
	}

	return buf, nil
}

func (ca *changeAggregator) makeKVFeedCfg(
	ctx context.Context,
	spans []roachpb.Span,
	buf kvevent.Writer,
	initialHighWater hlc.Timestamp,
	needsInitialScan bool,
	sm *sliMetrics,
) kvfeed.Config {
	schemaChangeEvents := changefeedbase.SchemaChangeEventClass(
		ca.spec.Feed.Opts[changefeedbase.OptSchemaChangeEvents])
	schemaChangePolicy := changefeedbase.SchemaChangePolicy(
		ca.spec.Feed.Opts[changefeedbase.OptSchemaChangePolicy])
	_, withDiff := ca.spec.Feed.Opts[changefeedbase.OptDiff]
	cfg := ca.flowCtx.Cfg

	var sf schemafeed.SchemaFeed
	if schemaChangePolicy == changefeedbase.OptSchemaChangePolicyIgnore {
		sf = schemafeed.DoNothingSchemaFeed
	} else {
		sf = schemafeed.New(ctx, cfg, schemaChangeEvents, AllTargets(ca.spec.Feed),
			initialHighWater, &ca.metrics.SchemaFeedMetrics, ca.spec.Feed.Opts)
	}

	return kvfeed.Config{
		Writer:                  buf,
		Settings:                cfg.Settings,
		DB:                      cfg.DB,
		Codec:                   cfg.Codec,
		Clock:                   cfg.DB.Clock(),
		Gossip:                  cfg.Gossip,
		Spans:                   spans,
		BackfillCheckpoint:      ca.spec.Checkpoint.Spans,
		Targets:                 AllTargets(ca.spec.Feed),
		Metrics:                 &ca.metrics.KVFeedMetrics,
		OnBackfillCallback:      ca.sliMetrics.getBackfillCallback(),
		OnBackfillRangeCallback: ca.sliMetrics.getBackfillRangeCallback(),
		MM:                      ca.kvFeedMemMon,
		InitialHighWater:        initialHighWater,
		WithDiff:                withDiff,
		NeedsInitialScan:        needsInitialScan,
		SchemaChangeEvents:      schemaChangeEvents,
		SchemaChangePolicy:      schemaChangePolicy,
		SchemaFeed:              sf,
		Knobs:                   ca.knobs.FeedKnobs,
	}
}

// getKVFeedInitialParameters determines the starting timestamp for the kv and
// whether or not an initial scan is needed. The need for an initial scan is
// determined by whether the watched in the spec have a resolved timestamp. The
// higher layers mark each watch with the checkpointed resolved timestamp if no
// initial scan is needed.
//
// TODO(ajwerner): Utilize this partial checkpointing, especially in the face of
// of logical backfills of a single table while progress is made on others or
// get rid of it. See https://github.com/cockroachdb/cockroach/issues/43896.
func getKVFeedInitialParameters(
	spec execinfrapb.ChangeAggregatorSpec,
) (initialHighWater hlc.Timestamp, needsInitialScan bool) {
	for _, watch := range spec.Watches {
		if initialHighWater.IsEmpty() || watch.InitialResolved.Less(initialHighWater) {
			initialHighWater = watch.InitialResolved
		}
	}
	// This will be true in the case where we have no cursor and we've never
	// checkpointed a resolved timestamp or we have a cursor but we want an
	// initial scan. The higher levels will coordinate that we only have empty
	// watches when we need an initial scan.
	if needsInitialScan = initialHighWater.IsEmpty(); needsInitialScan {
		initialHighWater = spec.Feed.StatementTime
	}
	return initialHighWater, needsInitialScan
}

// setupSpans is called on start to extract the spans for this changefeed as a
// slice and creates a span frontier with the initial resolved timestampsc. This
// SpanFrontier only tracks the spans being watched on this node. There is a
// different SpanFrontier elsewhere for the entire changefeed. This object is
// used to filter out some previously emitted rows, and by the cloudStorageSink
// to name its output files in lexicographically monotonic fashion.
func (ca *changeAggregator) setupSpansAndFrontier(
	initialHighWater hlc.Timestamp,
) (spans []roachpb.Span, err error) {
	spans = make([]roachpb.Span, 0, len(ca.spec.Watches))
	for _, watch := range ca.spec.Watches {
		spans = append(spans, watch.Span)
	}

	ca.frontier, err = makeSchemaChangeFrontier(initialHighWater, spans...)
	if err != nil {
		return nil, err
	}

	// Checkpointed spans are spans that were above the highwater mark, and we
	// must preserve that information in the frontier for future checkpointing.
	// If we don't have a highwater yet (during initial scan) they must at least
	// be from StatementTime, and given an initial highwater they must all by
	// definition have been at or after initialHighWater.Next()
	var checkpointedSpanTs hlc.Timestamp
	if initialHighWater.IsEmpty() {
		checkpointedSpanTs = ca.spec.Feed.StatementTime
	} else {
		checkpointedSpanTs = initialHighWater.Next()
	}
	for _, checkpointedSpan := range ca.spec.Checkpoint.Spans {
		if _, err := ca.frontier.Forward(checkpointedSpan, checkpointedSpanTs); err != nil {
			return nil, err
		}
	}
	return spans, nil
}

// close has two purposes: to synchronize on the completion of the helper
// goroutines created by the Start method, and to clean up any resources used by
// the processor. Due to the fact that this method may be called even if the
// processor did not finish completion, there is an excessive amount of nil
// checking.
func (ca *changeAggregator) close() {
	if ca.Closed {
		return
	}
	ca.cancel()
	// Wait for the poller to finish shutting down.
	if ca.kvFeedDoneCh != nil {
		<-ca.kvFeedDoneCh
	}
	if ca.sink != nil {
		if err := ca.sink.Close(); err != nil {
			log.Warningf(ca.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
		}
	}

	ca.memAcc.Close(ca.Ctx)
	if ca.kvFeedMemMon != nil {
		ca.kvFeedMemMon.Stop(ca.Ctx)
	}
	ca.MemMonitor.Stop(ca.Ctx)
	ca.InternalClose()
}

// Next is part of the RowSource interface.
func (ca *changeAggregator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ca.State == execinfra.StateRunning {
		if !ca.changedRowBuf.IsEmpty() {
			return ca.ProcessRowHelper(ca.changedRowBuf.Pop()), nil
		} else if !ca.resolvedSpanBuf.IsEmpty() {
			return ca.ProcessRowHelper(ca.resolvedSpanBuf.Pop()), nil
		}

		if err := ca.tick(); err != nil {
			var e kvevent.ErrBufferClosed
			if errors.As(err, &e) {
				// ErrBufferClosed is a signal that
				// our kvfeed has exited expectedly.
				err = e.Unwrap()
				if errors.Is(err, kvevent.ErrNormalRestartReason) {
					err = nil
				}
			} else {

				select {
				// If the poller errored first, that's the
				// interesting one, so overwrite `err`.
				case err = <-ca.errCh:
				default:
				}
			}
			// Shut down the poller if it wasn't already.
			ca.cancel()

			ca.MoveToDraining(err)
			break
		}
	}
	return nil, ca.DrainHelper()
}

// tick is the workhorse behind Next(). It retrieves the next event from
// kvFeed, sends off this event to the event consumer, and flushes the sink
// if necessary.
func (ca *changeAggregator) tick() error {
	event, err := ca.eventProducer.Get(ca.Ctx)
	if err != nil {
		return err
	}

	queuedNanos := timeutil.Since(event.BufferAddTimestamp()).Nanoseconds()
	ca.metrics.QueueTimeNanos.Inc(queuedNanos)

	switch event.Type() {
	case kvevent.TypeKV:
		// Keep track of SLI latency for non-backfill/rangefeed KV events.
		if event.BackfillTimestamp().IsEmpty() {
			ca.sliMetrics.AdmitLatency.RecordValue(timeutil.Since(event.Timestamp().GoTime()).Nanoseconds())
		}
		ca.recentKVCount++
		return ca.eventConsumer.ConsumeEvent(ca.Ctx, event)
	case kvevent.TypeResolved:
		a := event.DetachAlloc()
		a.Release(ca.Ctx)
		resolved := event.Resolved()
		if ca.knobs.ShouldSkipResolved == nil || !ca.knobs.ShouldSkipResolved(resolved) {
			return ca.noteResolvedSpan(resolved)
		}
	case kvevent.TypeFlush:
		return ca.sink.Flush(ca.Ctx)
	}

	return nil
}

// noteResolvedSpan periodically flushes Frontier progress from the current
// changeAggregator node to the changeFrontier node to allow the changeFrontier
// to persist the overall changefeed's progress
func (ca *changeAggregator) noteResolvedSpan(resolved *jobspb.ResolvedSpan) error {
	advanced, err := ca.frontier.ForwardResolvedSpan(*resolved)
	if err != nil {
		return err
	}

	forceFlush := resolved.BoundaryType != jobspb.ResolvedSpan_NONE

	checkpointFrontier := advanced &&
		(forceFlush || timeutil.Since(ca.lastFlush) > ca.flushFrequency)

	// If backfilling we must also consider the Backfill Checkpointing frequency
	checkpointBackfill := ca.spec.JobID != 0 && /* enterprise changefeed */
		resolved.Timestamp.Equal(ca.frontier.BackfillTS()) &&
		canCheckpointBackfill(&ca.flowCtx.Cfg.Settings.SV, ca.lastFlush)

	if checkpointFrontier || checkpointBackfill {
		defer func() {
			ca.lastFlush = timeutil.Now()
		}()
		return ca.flushFrontier()
	}

	return nil
}

// flushFrontier flushes sink and emits resolved timestamp if needed.
func (ca *changeAggregator) flushFrontier() error {
	// Make sure to flush the sink before forwarding resolved spans,
	// otherwise, we could lose buffered messages and violate the
	// at-least-once guarantee. This is also true for checkpointing the
	// resolved spans in the job progress.
	if err := ca.sink.Flush(ca.Ctx); err != nil {
		return err
	}

	// Iterate frontier spans and build a list of spans to emit.
	var batch jobspb.ResolvedSpans
	ca.frontier.Entries(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		boundaryType := jobspb.ResolvedSpan_NONE
		if ca.frontier.boundaryTime.Equal(ts) {
			boundaryType = ca.frontier.boundaryType
		}

		batch.ResolvedSpans = append(batch.ResolvedSpans, jobspb.ResolvedSpan{
			Span:         s,
			Timestamp:    ts,
			BoundaryType: boundaryType,
		})
		return span.ContinueMatch
	})

	return ca.emitResolved(batch)
}

func (ca *changeAggregator) emitResolved(batch jobspb.ResolvedSpans) error {
	// TODO(smiskin): Remove post-22.2
	if !ca.flowCtx.Cfg.Settings.Version.IsActive(ca.Ctx, clusterversion.ChangefeedIdleness) {
		for _, resolved := range batch.ResolvedSpans {
			resolvedBytes, err := protoutil.Marshal(&resolved)
			if err != nil {
				return err
			}

			// Enqueue a row to be returned that indicates some span-level resolved
			// timestamp has advanced. If any rows were queued in `sink`, they must
			// be emitted first.
			ca.resolvedSpanBuf.Push(rowenc.EncDatumRow{
				rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(resolvedBytes))},
				rowenc.EncDatum{Datum: tree.DNull}, // topic
				rowenc.EncDatum{Datum: tree.DNull}, // key
				rowenc.EncDatum{Datum: tree.DNull}, // value
			})
			ca.metrics.ResolvedMessages.Inc(1)
		}

		return nil
	}

	progressUpdate := jobspb.ResolvedSpans{
		ResolvedSpans: batch.ResolvedSpans,
		Stats: jobspb.ResolvedSpans_Stats{
			RecentKvCount: ca.recentKVCount,
		},
	}
	updateBytes, err := protoutil.Marshal(&progressUpdate)
	if err != nil {
		return err
	}
	ca.resolvedSpanBuf.Push(rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(updateBytes))},
		rowenc.EncDatum{Datum: tree.DNull}, // topic
		rowenc.EncDatum{Datum: tree.DNull}, // key
		rowenc.EncDatum{Datum: tree.DNull}, // value
	})

	ca.recentKVCount = 0
	return nil
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.close()
}

type kvEventConsumer interface {
	// ConsumeEvent responsible for consuming kv event.
	ConsumeEvent(ctx context.Context, event kvevent.Event) error
}

type kvEventToRowConsumer struct {
	frontier  *span.Frontier
	encoder   Encoder
	scratch   bufalloc.ByteAllocator
	sink      Sink
	cursor    hlc.Timestamp
	knobs     TestingKnobs
	rfCache   *rowFetcherCache
	details   jobspb.ChangefeedDetails
	kvFetcher row.SpanKVFetcher
}

var _ kvEventConsumer = &kvEventToRowConsumer{}

func newKVEventToRowConsumer(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	frontier *span.Frontier,
	cursor hlc.Timestamp,
	sink Sink,
	encoder Encoder,
	details jobspb.ChangefeedDetails,
	knobs TestingKnobs,
) kvEventConsumer {
	rfCache := newRowFetcherCache(
		ctx,
		cfg.Codec,
		cfg.LeaseManager.(*lease.Manager),
		cfg.CollectionFactory,
		cfg.DB,
	)

	return &kvEventToRowConsumer{
		frontier: frontier,
		encoder:  encoder,
		sink:     sink,
		cursor:   cursor,
		rfCache:  rfCache,
		details:  details,
		knobs:    knobs,
	}
}

type tableDescriptorTopic struct {
	catalog.TableDescriptor
}

var _ TopicDescriptor = &tableDescriptorTopic{}

// ConsumeEvent implements kvEventConsumer interface
func (c *kvEventToRowConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	if ev.Type() != kvevent.TypeKV {
		return errors.AssertionFailedf("expected kv ev, got %v", ev.Type())
	}

	r, err := c.eventToRow(ctx, ev)
	if err != nil {
		return err
	}

	// Ensure that r updates are strictly newer than the least resolved timestamp
	// being tracked by the local span frontier. The poller should not be forwarding
	// r updates that have timestamps less than or equal to any resolved timestamp
	// it's forwarded before.
	// TODO(dan): This should be an assertion once we're confident this can never
	// happen under any circumstance.
	if r.updated.LessEq(c.frontier.Frontier()) && !r.updated.Equal(c.cursor) {
		log.Errorf(ctx, "cdc ux violation: detected timestamp %s that is less than "+
			"or equal to the local frontier %s.", r.updated, c.frontier.Frontier())
		return nil
	}
	var keyCopy, valueCopy []byte
	encodedKey, err := c.encoder.EncodeKey(ctx, r)
	if err != nil {
		return err
	}
	c.scratch, keyCopy = c.scratch.Copy(encodedKey, 0 /* extraCap */)
	encodedValue, err := c.encoder.EncodeValue(ctx, r)
	if err != nil {
		return err
	}
	c.scratch, valueCopy = c.scratch.Copy(encodedValue, 0 /* extraCap */)

	if c.knobs.BeforeEmitRow != nil {
		if err := c.knobs.BeforeEmitRow(ctx); err != nil {
			return err
		}
	}
	if err := c.sink.EmitRow(
		ctx, tableDescriptorTopic{r.tableDesc},
		keyCopy, valueCopy, r.updated, r.mvccTimestamp, ev.DetachAlloc(),
	); err != nil {
		return err
	}
	if log.V(3) {
		log.Infof(ctx, `r %s: %s -> %s`, r.tableDesc.GetName(), keyCopy, valueCopy)
	}
	return nil
}

func (c *kvEventToRowConsumer) eventToRow(
	ctx context.Context, event kvevent.Event,
) (encodeRow, error) {
	var r encodeRow
	schemaTimestamp := event.KV().Value.Timestamp
	prevSchemaTimestamp := schemaTimestamp
	mvccTimestamp := event.MVCCTimestamp()

	if backfillTs := event.BackfillTimestamp(); !backfillTs.IsEmpty() {
		schemaTimestamp = backfillTs
		prevSchemaTimestamp = schemaTimestamp.Prev()
	}

	desc, family, err := c.rfCache.TableDescForKey(ctx, event.KV().Key, schemaTimestamp)
	if err != nil {
		return r, err
	}

	r.tableDesc = desc
	r.familyID = family
	var rf *row.Fetcher
	rf, err = c.rfCache.RowFetcherForColumnFamily(desc, family)
	if err != nil {
		return r, err
	}

	// Get new value.
	// TODO(dan): Handle tables with multiple column families.
	// Reuse kvs to save allocations.
	c.kvFetcher.KVs = c.kvFetcher.KVs[:0]
	c.kvFetcher.KVs = append(c.kvFetcher.KVs, event.KV())
	if err := rf.StartScanFrom(ctx, &c.kvFetcher, false /* traceKV */); err != nil {
		return r, err
	}

	r.datums, err = rf.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if r.datums == nil {
		return r, errors.AssertionFailedf("unexpected empty datums")
	}
	r.datums = append(rowenc.EncDatumRow(nil), r.datums...)
	r.deleted = rf.RowIsDeleted()
	r.updated = schemaTimestamp
	r.mvccTimestamp = mvccTimestamp

	// Assert that we don't get a second row from the row.Fetcher. We
	// fed it a single KV, so that would be surprising.
	nextRow := encodeRow{
		tableDesc: desc,
	}
	nextRow.datums, err = rf.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if nextRow.datums != nil {
		return r, errors.AssertionFailedf("unexpected non-empty datums")
	}

	// Get prev value, if necessary.
	_, withDiff := c.details.Opts[changefeedbase.OptDiff]
	if withDiff {
		prevRF := rf
		r.prevTableDesc = r.tableDesc
		r.prevFamilyID = r.familyID
		if prevSchemaTimestamp != schemaTimestamp {
			// If the previous value is being interpreted under a different
			// version of the schema, fetch the correct table descriptor and
			// create a new row.Fetcher with it.
			prevDesc, family, err := c.rfCache.TableDescForKey(ctx, event.KV().Key, prevSchemaTimestamp)

			if err != nil {
				return r, err
			}
			r.prevTableDesc = prevDesc
			r.prevFamilyID = family
			prevRF, err = c.rfCache.RowFetcherForColumnFamily(prevDesc, family)

			if err != nil {
				return r, err
			}
		}

		prevKV := roachpb.KeyValue{Key: event.KV().Key, Value: event.PrevValue()}
		// TODO(dan): Handle tables with multiple column families.
		// Reuse kvs to save allocations.
		c.kvFetcher.KVs = c.kvFetcher.KVs[:0]
		c.kvFetcher.KVs = append(c.kvFetcher.KVs, prevKV)
		if err := prevRF.StartScanFrom(ctx, &c.kvFetcher, false /* traceKV */); err != nil {
			return r, err
		}
		r.prevDatums, err = prevRF.NextRow(ctx)
		if err != nil {
			return r, err
		}
		if r.prevDatums == nil {
			return r, errors.AssertionFailedf("unexpected empty datums")
		}
		r.prevDatums = append(rowenc.EncDatumRow(nil), r.prevDatums...)
		r.prevDeleted = prevRF.RowIsDeleted()

		// Assert that we don't get a second row from the row.Fetcher. We
		// fed it a single KV, so that would be surprising.
		nextRow := encodeRow{
			prevTableDesc: r.prevTableDesc,
		}
		nextRow.prevDatums, err = prevRF.NextRow(ctx)
		if err != nil {
			return r, err
		}
		if nextRow.prevDatums != nil {
			return r, errors.AssertionFailedf("unexpected non-empty datums")
		}
	}

	return r, nil
}

type nativeKVConsumer struct {
	sink Sink
}

var _ kvEventConsumer = &nativeKVConsumer{}

func newNativeKVConsumer(sink Sink) kvEventConsumer {
	return &nativeKVConsumer{sink: sink}
}

type noTopic struct{}

var _ TopicDescriptor = &noTopic{}

func (n noTopic) GetName() string {
	return ""
}

func (n noTopic) GetID() descpb.ID {
	return 0
}

func (n noTopic) GetVersion() descpb.DescriptorVersion {
	return 0
}

// ConsumeEvent implements kvEventConsumer interface.
func (c *nativeKVConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	if ev.Type() != kvevent.TypeKV {
		return errors.AssertionFailedf("expected kv ev, got %v", ev.Type())
	}
	keyBytes := []byte(ev.KV().Key)
	val := ev.KV().Value
	valBytes, err := protoutil.Marshal(&val)
	if err != nil {
		return err
	}

	return c.sink.EmitRow(
		ctx, &noTopic{}, keyBytes, valBytes, val.Timestamp, val.Timestamp, ev.DetachAlloc())
}

const (
	emitAllResolved = 0
	emitNoResolved  = -1
)

type changeFrontier struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ChangeFrontierSpec
	memAcc  mon.BoundAccount
	a       tree.DatumAlloc

	// input returns rows from one or more changeAggregator processors
	input execinfra.RowSource

	// frontier contains the current resolved timestamp high-water for the tracked
	// span set.
	frontier *schemaChangeFrontier
	// encoder is the Encoder to use for resolved timestamp serialization.
	encoder Encoder
	// sink is the Sink to write resolved timestamps to. Rows are never written
	// by changeFrontier.
	sink Sink
	// freqEmitResolved, if >= 0, is a lower bound on the duration between
	// resolved timestamp emits.
	freqEmitResolved time.Duration
	// lastEmitResolved is the last time a resolved timestamp was emitted.
	lastEmitResolved time.Time

	// slowLogEveryN rate-limits the logging of slow spans
	slowLogEveryN log.EveryN

	// lastProtectedTimestampUpdate is the last time the protected timestamp
	// record was updated to the frontier's highwater mark
	lastProtectedTimestampUpdate time.Time

	// js, if non-nil, is called to checkpoint the changefeed's
	// progress in the corresponding system job entry.
	js *jobState
	// highWaterAtStart is the greater of the job high-water and the timestamp the
	// CHANGEFEED statement was run at. It's used in an assertion that we never
	// regress the job high-water.
	highWaterAtStart hlc.Timestamp
	// passthroughBuf, in some but not all flows, contains changed row data to
	// pass through unchanged to the gateway node.
	passthroughBuf encDatumRowBuffer
	// resolvedBuf, if non-nil, contains rows indicating a changefeed-level
	// resolved timestamp to be returned. It depends on everything in
	// `passthroughBuf` being sent, so that one needs to be emptied first.
	resolvedBuf *encDatumRowBuffer
	// metrics are monitoring counters shared between all changefeeds.
	metrics    *Metrics
	sliMetrics *sliMetrics
	// metricsID is used as the unique id of this changefeed in the
	// metrics.MaxBehindNanos map.
	metricsID int
}

const (
	runStatusUpdateFrequency time.Duration = time.Minute
	slowSpanMaxFrequency                   = 10 * time.Second
)

// jobState encapsulates changefeed job state.
type jobState struct {
	job      *jobs.Job
	settings *cluster.Settings
	metrics  *Metrics
	ts       timeutil.TimeSource

	// The last time we updated job run status.
	lastRunStatusUpdate time.Time
	// The last time we updated job progress.
	lastProgressUpdate time.Time
	// How long checkpoint (job progress update) expected to take.
	checkpointDuration time.Duration
	// Flag set if we skip some updates due to rapid progress update requests.
	progressUpdatesSkipped bool
}

func newJobState(
	j *jobs.Job, st *cluster.Settings, metrics *Metrics, ts timeutil.TimeSource,
) *jobState {
	return &jobState{
		job:                j,
		settings:           st,
		metrics:            metrics,
		ts:                 ts,
		lastProgressUpdate: ts.Now(),
	}
}

func canCheckpointBackfill(sv *settings.Values, lastCheckpoint time.Time) bool {
	freq := changefeedbase.FrontierCheckpointFrequency.Get(sv)
	if freq == 0 {
		return false
	}
	return timeutil.Since(lastCheckpoint) > freq
}

func (j *jobState) canCheckpointBackfill() bool {
	return canCheckpointBackfill(&j.settings.SV, j.lastProgressUpdate)
}

// canCheckpointHighWatermark returns true if we should update job high water mark (i.e. progress).
// Normally, whenever frontier changes, we update high water mark.
// However, if the rate of frontier changes is too high, we want to slow down
// the frequency of job progress updates.  We do this by skipping some updates
// if the time to update the job progress is greater than the delta between
// previous and the current progress update time.
func (j *jobState) canCheckpointHighWatermark(frontierChanged bool) bool {
	if !(frontierChanged || j.progressUpdatesSkipped) {
		return false
	}

	minAdvance := changefeedbase.MinHighWaterMarkCheckpointAdvance.Get(&j.settings.SV)
	if j.checkpointDuration > 0 &&
		j.ts.Now().Before(j.lastProgressUpdate.Add(j.checkpointDuration+minAdvance)) {
		// Updates are too rapid; skip some.
		j.progressUpdatesSkipped = true
		return false
	}

	return true
}

// checkpointCompleted must be called when job checkpoint completes.
// checkpointDuration indicates how long the checkpoint took.
func (j *jobState) checkpointCompleted(ctx context.Context, checkpointDuration time.Duration) {
	minAdvance := changefeedbase.MinHighWaterMarkCheckpointAdvance.Get(&j.settings.SV)
	if j.progressUpdatesSkipped {
		// Log message if we skipped updates for some time.
		warnThreshold := 2 * minAdvance
		if warnThreshold < 60*time.Second {
			warnThreshold = 60 * time.Second
		}
		behind := j.ts.Now().Sub(j.lastProgressUpdate)
		if behind > warnThreshold {
			log.Warningf(ctx, "high water mark update delayed by %s; mean checkpoint duration %s",
				behind, j.checkpointDuration)
		}
	}

	j.metrics.CheckpointHistNanos.RecordValue(checkpointDuration.Nanoseconds())
	j.lastProgressUpdate = j.ts.Now()
	j.checkpointDuration = time.Duration(j.metrics.CheckpointHistNanos.Snapshot().Mean())
	j.progressUpdatesSkipped = false
}

var _ execinfra.Processor = &changeFrontier{}
var _ execinfra.RowSource = &changeFrontier{}

func newChangeFrontierProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changefntr-mem")
	sf, err := makeSchemaChangeFrontier(hlc.Timestamp{}, spec.TrackedSpans...)
	if err != nil {
		return nil, err
	}
	cf := &changeFrontier{
		flowCtx:       flowCtx,
		spec:          spec,
		memAcc:        memMonitor.MakeBoundAccount(),
		input:         input,
		frontier:      sf,
		slowLogEveryN: log.Every(slowSpanMaxFrequency),
	}
	if err := cf.Init(
		cf,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				cf.close()
				return nil
			},
			InputsToDrain: []execinfra.RowSource{cf.input},
		},
	); err != nil {
		return nil, err
	}

	if r, ok := cf.spec.Feed.Opts[changefeedbase.OptResolvedTimestamps]; ok {
		var err error
		if r == `` {
			// Empty means emit them as often as we have them.
			cf.freqEmitResolved = emitAllResolved
		} else if cf.freqEmitResolved, err = time.ParseDuration(r); err != nil {
			return nil, err
		}
	} else {
		cf.freqEmitResolved = emitNoResolved
	}

	if cf.encoder, err = getEncoder(spec.Feed.Opts, AllTargets(spec.Feed)); err != nil {
		return nil, err
	}

	return cf, nil
}

// MustBeStreaming implements the execinfra.Processor interface.
func (cf *changeFrontier) MustBeStreaming() bool {
	return true
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) {
	if cf.spec.JobID != 0 {
		ctx = logtags.AddTag(ctx, "job", cf.spec.JobID)
	}
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)
	cf.input.Start(ctx)

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	// TODO(yevgeniy): Figure out how to inject replication stream metrics.
	cf.metrics = cf.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)

	// Pass a nil oracle because this sink is only used to emit resolved timestamps
	// but the oracle is only used when emitting row updates.
	var nilOracle timestampLowerBoundOracle
	var err error
	sli, err := cf.metrics.getSLIMetrics(cf.spec.Feed.Opts[changefeedbase.OptMetricsScope])
	if err != nil {
		cf.MoveToDraining(err)
		return
	}
	cf.sliMetrics = sli
	cf.sink, err = getSink(ctx, cf.flowCtx.Cfg, cf.spec.Feed, nilOracle,
		cf.spec.User(), cf.spec.JobID, sli)

	if err != nil {
		err = changefeedbase.MarkRetryableError(err)
		cf.MoveToDraining(err)
		return
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	cf.sink = &errorWrapperSink{wrapped: cf.sink}

	cf.highWaterAtStart = cf.spec.Feed.StatementTime
	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.Cfg.JobRegistry.LoadClaimedJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return
		}
		cf.js = newJobState(job, cf.flowCtx.Cfg.Settings, cf.metrics, timeutil.DefaultTimeSource{})

		if changefeedbase.FrontierCheckpointFrequency.Get(&cf.flowCtx.Cfg.Settings.SV) == 0 {
			log.Warning(ctx,
				"Frontier checkpointing disabled; set changefeed.frontier_checkpoint_frequency to non-zero value to re-enable")
		}

		// Recover highwater information from job progress.
		// Checkpoint information from job progress will eventually be sent to the
		// changeFrontier from the changeAggregators.  Note that the changeFrontier
		// may save a new checkpoint prior to receiving all spans of the
		// aggregators' frontier, potentially missing spans that were previously
		// checkpointed, so it is still possible for job progress to regress.
		p := job.Progress()
		if ts := p.GetHighWater(); ts != nil {
			cf.highWaterAtStart.Forward(*ts)
			cf.frontier.initialHighWater = *ts
			for _, span := range cf.spec.TrackedSpans {
				if _, err := cf.frontier.Forward(span, *ts); err != nil {
					cf.MoveToDraining(err)
					return
				}
			}
		}

		if p.RunningStatus != "" {
			// If we had running status set, that means we're probably retrying
			// due to a transient error.  In that case, keep the previous
			// running status around for a while before we override it.
			cf.js.lastRunStatusUpdate = timeutil.Now()
		}
	}

	cf.metrics.mu.Lock()
	cf.metricsID = cf.metrics.mu.id
	cf.metrics.mu.id++
	sli.RunningCount.Inc(1)
	cf.metrics.mu.Unlock()
	// TODO(dan): It's very important that we de-register from the metric because
	// if we orphan an entry in there, our monitoring will lie (say the changefeed
	// is behind when it may not be). We call this in `close` but that doesn't
	// always get called when the processor is shut down (especially during crdb
	// chaos), so here's something that maybe will work some of the times that
	// close doesn't. This is all very hacky. The real answer is to fix whatever
	// bugs currently exist in processor shutdown.
	go func() {
		<-ctx.Done()
		cf.closeMetrics()
	}()
}

func (cf *changeFrontier) close() {
	if cf.InternalClose() {
		if cf.metrics != nil {
			cf.closeMetrics()
		}
		if cf.sink != nil {
			if err := cf.sink.Close(); err != nil {
				log.Warningf(cf.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		cf.memAcc.Close(cf.Ctx)
		cf.MemMonitor.Stop(cf.Ctx)
	}
}

// closeMetrics de-registers from the progress registry that powers
// `changefeed.max_behind_nanos`. This method is idempotent.
func (cf *changeFrontier) closeMetrics() {
	// Delete this feed from the MaxBehindNanos metric so it's no longer
	// considered by the gauge.
	cf.metrics.mu.Lock()
	if cf.metricsID > 0 {
		cf.sliMetrics.RunningCount.Dec(1)
	}
	delete(cf.metrics.mu.resolved, cf.metricsID)
	cf.metricsID = -1
	cf.metrics.mu.Unlock()
}

// Next is part of the RowSource interface.
func (cf *changeFrontier) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for cf.State == execinfra.StateRunning {
		if !cf.passthroughBuf.IsEmpty() {
			return cf.ProcessRowHelper(cf.passthroughBuf.Pop()), nil
		} else if !cf.resolvedBuf.IsEmpty() {
			return cf.ProcessRowHelper(cf.resolvedBuf.Pop()), nil
		}

		if cf.frontier.schemaChangeBoundaryReached() &&
			(cf.frontier.boundaryType == jobspb.ResolvedSpan_EXIT ||
				cf.frontier.boundaryType == jobspb.ResolvedSpan_RESTART) {
			err := pgerror.Newf(pgcode.SchemaChangeOccurred,
				"schema change occurred at %v", cf.frontier.boundaryTime.Next().AsOfSystemTime())

			// Detect whether this boundary should be used to kill or restart the
			// changefeed.
			if cf.frontier.boundaryType == jobspb.ResolvedSpan_RESTART {
				err = changefeedbase.MarkRetryableError(err)
			}
			// TODO(ajwerner): make this more useful by at least informing the client
			// of which tables changed.
			cf.MoveToDraining(err)
			break
		}

		row, meta := cf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				cf.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			cf.MoveToDraining(nil /* err */)
			break
		}

		if row[0].IsNull() {
			// In changefeeds with a sink, this will never happen. But in the
			// core changefeed, which returns changed rows directly via pgwire,
			// a row with a null resolved_span field is a changed row that needs
			// to be forwarded to the gateway.
			cf.passthroughBuf.Push(row)
			continue
		}

		if err := cf.noteAggregatorProgress(row[0]); err != nil {
			cf.MoveToDraining(err)
			break
		}
	}
	return nil, cf.DrainHelper()
}

func (cf *changeFrontier) noteAggregatorProgress(d rowenc.EncDatum) error {
	if err := d.EnsureDecoded(changefeeddist.ChangefeedResultTypes[0], &cf.a); err != nil {
		return err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return errors.AssertionFailedf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}

	var resolvedSpans jobspb.ResolvedSpans
	if cf.flowCtx.Cfg.Settings.Version.IsActive(cf.Ctx, clusterversion.ChangefeedIdleness) {
		if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				`unmarshalling aggregator progress update: %x`, raw)
		}

		cf.maybeMarkJobIdle(resolvedSpans.Stats.RecentKvCount)
	} else { // TODO(smiskin): Remove post-22.2
		// Progress used to be sent as individual ResolvedSpans
		var resolved jobspb.ResolvedSpan
		if err := protoutil.Unmarshal([]byte(*raw), &resolved); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				`unmarshalling resolved span: %x`, raw)
		}
		resolvedSpans = jobspb.ResolvedSpans{
			ResolvedSpans: []jobspb.ResolvedSpan{resolved},
		}
	}

	for _, resolved := range resolvedSpans.ResolvedSpans {
		// Inserting a timestamp less than the one the changefeed flow started at
		// could potentially regress the job progress. This is not expected, but it
		// was a bug at one point, so assert to prevent regressions.
		//
		// TODO(dan): This is much more naturally expressed as an assert inside the
		// job progress update closure, but it currently doesn't pass along the info
		// we'd need to do it that way.
		if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(cf.highWaterAtStart) {
			logcrash.ReportOrPanic(cf.Ctx, &cf.flowCtx.Cfg.Settings.SV,
				`got a span level timestamp %s for %s that is less than the initial high-water %s`,
				redact.Safe(resolved.Timestamp), resolved.Span, redact.Safe(cf.highWaterAtStart))
			continue
		}
		if err := cf.forwardFrontier(resolved); err != nil {
			return err
		}
	}

	return nil
}

func (cf *changeFrontier) forwardFrontier(resolved jobspb.ResolvedSpan) error {
	frontierChanged, err := cf.frontier.ForwardResolvedSpan(resolved)
	if err != nil {
		return err
	}

	cf.maybeLogBehindSpan(frontierChanged)

	// If frontier changed, we emit resolved timestamp.
	emitResolved := frontierChanged

	// Checkpoint job record progress if needed.
	// NB: Sinkless changefeeds will not have a job state (js). In fact, they
	// have no distributed state whatsoever. Because of this they also do not
	// use protected timestamps.
	if cf.js != nil {
		checkpointed, err := cf.maybeCheckpointJob(resolved, frontierChanged)
		if err != nil {
			return err
		}

		// Emit resolved timestamp only if we have checkpointed the job.
		// Usually, this happens every time frontier changes, but we can skip some updates
		// if we update frontier too rapidly.
		emitResolved = checkpointed
	}

	if emitResolved {
		// Keeping this after the checkpointJobProgress call will avoid
		// some duplicates if a restart happens.
		newResolved := cf.frontier.Frontier()
		cf.metrics.mu.Lock()
		if cf.metricsID != -1 {
			cf.metrics.mu.resolved[cf.metricsID] = newResolved
		}
		cf.metrics.mu.Unlock()

		return cf.maybeEmitResolved(newResolved)
	}

	return nil
}

func (cf *changeFrontier) maybeMarkJobIdle(recentKVCount uint64) {
	if cf.spec.JobID == 0 {
		return
	}

	if recentKVCount > 0 {
		cf.frontier.ForwardLatestKV(timeutil.Now())
	}

	idleTimeout := changefeedbase.IdleTimeout.Get(&cf.flowCtx.Cfg.Settings.SV)
	if idleTimeout == 0 {
		return
	}

	isIdle := timeutil.Since(cf.frontier.latestKV) > idleTimeout
	cf.js.job.MarkIdle(isIdle)
}

func (cf *changeFrontier) maybeCheckpointJob(
	resolvedSpan jobspb.ResolvedSpan, frontierChanged bool,
) (bool, error) {
	// When in a Backfill, the frontier remains unchanged at the backfill boundary
	// as we receive spans from the scan request at the Backfill Timestamp
	inBackfill := !frontierChanged && resolvedSpan.Timestamp.Equal(cf.frontier.BackfillTS())

	// During a backfill we store a checkpoint of completed scans at a throttled rate in the job record
	updateCheckpoint := inBackfill && cf.js.canCheckpointBackfill()

	var checkpoint jobspb.ChangefeedProgress_Checkpoint
	if updateCheckpoint {
		maxBytes := changefeedbase.FrontierCheckpointMaxBytes.Get(&cf.flowCtx.Cfg.Settings.SV)
		checkpoint.Spans = cf.frontier.getCheckpointSpans(maxBytes)
	}

	// If we're not in a backfill, highwater progress and an empty checkpoint will
	// be saved. This is throttled however we always persist progress to a schema
	// boundary.
	updateHighWater :=
		!inBackfill && (cf.frontier.schemaChangeBoundaryReached() || cf.js.canCheckpointHighWatermark(frontierChanged))

	if updateCheckpoint || updateHighWater {
		checkpointStart := timeutil.Now()
		if err := cf.checkpointJobProgress(cf.frontier.Frontier(), checkpoint); err != nil {
			return false, err
		}
		cf.js.checkpointCompleted(cf.Ctx, timeutil.Since(checkpointStart))
		return true, nil
	}

	return false, nil
}

func (cf *changeFrontier) checkpointJobProgress(
	frontier hlc.Timestamp, checkpoint jobspb.ChangefeedProgress_Checkpoint,
) (err error) {
	updateRunStatus := timeutil.Since(cf.js.lastRunStatusUpdate) > runStatusUpdateFrequency
	if updateRunStatus {
		defer func() { cf.js.lastRunStatusUpdate = timeutil.Now() }()
	}
	cf.metrics.FrontierUpdates.Inc(1)

	return cf.js.job.Update(cf.Ctx, nil, func(
		txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}

		// Advance resolved timestamp.
		progress := md.Progress
		progress.Progress = &jobspb.Progress_HighWater{
			HighWater: &frontier,
		}

		changefeedProgress := progress.Details.(*jobspb.Progress_Changefeed).Changefeed
		changefeedProgress.Checkpoint = &checkpoint

		if shouldProtectTimestamps(cf.flowCtx.Codec()) {
			if err := cf.manageProtectedTimestamps(cf.Ctx, txn, changefeedProgress); err != nil {
				log.Warningf(cf.Ctx, "error managing protected timestamp record: %v", err)
			}
		}

		if updateRunStatus {
			md.Progress.RunningStatus = fmt.Sprintf("running: resolved=%s", frontier)
		}

		ju.UpdateProgress(progress)

		// Reset RunStats.NumRuns to 1 since the changefeed is
		// now running. By resetting the NumRuns, we avoid
		// future job system level retries from having large
		// backoffs because of past failures.
		if md.RunStats != nil {
			ju.UpdateRunStats(1, md.RunStats.LastRun)
		}

		return nil
	})
}

// manageProtectedTimestamps periodically advances the protected timestamp for
// the changefeed's targets to the current highwater mark.  The record is
// cleared during changefeedResumer.OnFailOrCancel
func (cf *changeFrontier) manageProtectedTimestamps(
	ctx context.Context, txn *kv.Txn, progress *jobspb.ChangefeedProgress,
) error {
	ptsUpdateInterval := changefeedbase.ProtectTimestampInterval.Get(&cf.flowCtx.Cfg.Settings.SV)
	if timeutil.Since(cf.lastProtectedTimestampUpdate) < ptsUpdateInterval {
		return nil
	}
	cf.lastProtectedTimestampUpdate = timeutil.Now()

	pts := cf.flowCtx.Cfg.ProtectedTimestampProvider

	// Create / advance the protected timestamp record to the highwater mark
	highWater := cf.frontier.Frontier()
	if highWater.Less(cf.highWaterAtStart) {
		highWater = cf.highWaterAtStart
	}

	recordID := progress.ProtectedTimestampRecord
	if recordID == uuid.Nil {
		ptr := createProtectedTimestampRecord(ctx, cf.flowCtx.Codec(), cf.spec.JobID, AllTargets(cf.spec.Feed), highWater, progress)
		if err := pts.Protect(ctx, txn, ptr); err != nil {
			return err
		}
	} else {
		log.VEventf(ctx, 2, "updating protected timestamp %v at %v", recordID, highWater)
		if err := pts.UpdateTimestamp(ctx, txn, recordID, highWater); err != nil {
			return err
		}
	}

	return nil
}

func (cf *changeFrontier) maybeEmitResolved(newResolved hlc.Timestamp) error {
	if cf.freqEmitResolved == emitNoResolved || newResolved.IsEmpty() {
		return nil
	}
	sinceEmitted := newResolved.GoTime().Sub(cf.lastEmitResolved)
	shouldEmit := sinceEmitted >= cf.freqEmitResolved || cf.frontier.schemaChangeBoundaryReached()
	if !shouldEmit {
		return nil
	}
	if err := emitResolvedTimestamp(cf.Ctx, cf.encoder, cf.sink, newResolved); err != nil {
		return err
	}
	cf.lastEmitResolved = newResolved.GoTime()
	return nil
}

func (cf *changeFrontier) isBehind() bool {
	frontier := cf.frontier.Frontier()
	if frontier.IsEmpty() {
		// During backfills we consider ourselves "behind" for the purposes of
		// maintaining protected timestamps
		return true
	}

	return timeutil.Since(frontier.GoTime()) <= cf.slownessThreshold()
}

// Potentially log the most behind span in the frontier for debugging if the
// frontier is behind
func (cf *changeFrontier) maybeLogBehindSpan(frontierChanged bool) {
	if !cf.isBehind() {
		return
	}

	// Do not log when we're "behind" due to a backfill
	frontier := cf.frontier.Frontier()
	if frontier.IsEmpty() {
		return
	}

	now := timeutil.Now()
	resolvedBehind := now.Sub(frontier.GoTime())

	description := "sinkless feed"
	if !cf.isSinkless() {
		description = fmt.Sprintf("job %d", cf.spec.JobID)
	}
	if frontierChanged {
		log.Infof(cf.Ctx, "%s new resolved timestamp %s is behind by %s",
			description, frontier, resolvedBehind)
	}

	if cf.slowLogEveryN.ShouldProcess(now) {
		s := cf.frontier.PeekFrontierSpan()
		log.Infof(cf.Ctx, "%s span %s is behind by %s", description, s, resolvedBehind)
	}
}

func (cf *changeFrontier) slownessThreshold() time.Duration {
	clusterThreshold := changefeedbase.SlowSpanLogThreshold.Get(&cf.flowCtx.Cfg.Settings.SV)
	if clusterThreshold > 0 {
		return clusterThreshold
	}

	// These two cluster setting values represent the target
	// responsiveness of schemafeed and rangefeed.
	//
	// We add 1 second in case both these settings are set really
	// low (as they are in unit tests).
	//
	// TODO(ssd): We should probably take into account the flush
	// frequency here.
	pollInterval := changefeedbase.TableDescriptorPollInterval.Get(&cf.flowCtx.Cfg.Settings.SV)
	closedtsInterval := closedts.TargetDuration.Get(&cf.flowCtx.Cfg.Settings.SV)
	return time.Second + 10*(pollInterval+closedtsInterval)
}

// ConsumerClosed is part of the RowSource interface.
func (cf *changeFrontier) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	cf.close()
}

// isSinkless returns true if this changeFrontier is sinkless and thus does not
// have a job.
func (cf *changeFrontier) isSinkless() bool {
	return cf.spec.JobID == 0
}

// type to make embedding span.Frontier in schemaChangeFrontier convenient.
type spanFrontier struct {
	*span.Frontier
}

func (s *spanFrontier) frontierTimestamp() hlc.Timestamp {
	return s.Frontier.Frontier()
}

// schemaChangeFrontier encapsulates the span frontier, which keeps track of the
// per-span timestamps we no longer need to emit, along with information about
// the most recently observed schema change boundary.
type schemaChangeFrontier struct {
	*spanFrontier

	// schemaChangeBoundary values are communicated to the changeFrontier via
	// Resolved messages send from the changeAggregators. The policy regarding
	// which schema change events lead to a schemaChangeBoundary is controlled
	// by the KV feed based on OptSchemaChangeEvents and OptSchemaChangePolicy.
	//
	// When the changeFrontier receives a ResolvedSpan with a non-none
	// BoundaryType, it instructs the changefeed to wait for all watched spans to
	// reach the boundary and then depending on the specific BoundaryType, either
	// drain and exit, restart, or begin a backfill.
	// Upon reaching a boundary, sinkful changefeeds will save a protected
	// timestamp record in the jobs entry to ensure our ability to
	// restart/backfill from that boundary.

	// boundaryTime indicates the timestamp of the most recently observed resolved
	// span with a non-none boundary type, i.e. the most recent schema change
	// boundary.
	boundaryTime hlc.Timestamp

	// boundaryType indicates the type of the most recently observed schema change
	// boundary which corresponds to the action which should be taken when the
	// frontier reaches that boundary.
	boundaryType jobspb.ResolvedSpan_BoundaryType

	// latestTs indicates the most recent timestamp that any span in the frontier
	// has ever been forwarded to.
	latestTs hlc.Timestamp

	// initialHighWater is either the StatementTime for a new changefeed or the
	// recovered highwater mark for a resumed changefeed
	initialHighWater hlc.Timestamp

	// latestKV indicates the last time any aggregator received a kv event
	latestKV time.Time
}

func makeSchemaChangeFrontier(
	initialHighWater hlc.Timestamp, spans ...roachpb.Span,
) (*schemaChangeFrontier, error) {
	sf, err := span.MakeFrontier(spans...)
	if err != nil {
		return nil, err
	}
	for _, span := range spans {
		if _, err := sf.Forward(span, initialHighWater); err != nil {
			return nil, err
		}
	}
	return &schemaChangeFrontier{spanFrontier: &spanFrontier{sf}, initialHighWater: initialHighWater, latestTs: initialHighWater}, nil
}

// ForwardResolvedSpan advances the timestamp for a resolved span, taking care
// of updating schema change boundary information.
func (f *schemaChangeFrontier) ForwardResolvedSpan(r jobspb.ResolvedSpan) (bool, error) {
	if r.BoundaryType != jobspb.ResolvedSpan_NONE {
		if !f.boundaryTime.IsEmpty() && r.Timestamp.Less(f.boundaryTime) {
			// Boundary resolved events should be ingested from the schema feed
			// serially, where the changefeed won't even observe a new schema change
			// boundary until it has progressed past the current boundary
			return false, errors.AssertionFailedf("received boundary timestamp %v < %v "+
				"of type %v before reaching existing boundary of type %v",
				r.Timestamp, f.boundaryTime, r.BoundaryType, f.boundaryType)
		}
		f.boundaryTime = r.Timestamp
		f.boundaryType = r.BoundaryType
	}

	if f.latestTs.Less(r.Timestamp) {
		f.latestTs = r.Timestamp
	}
	return f.Forward(r.Span, r.Timestamp)
}

func (f *schemaChangeFrontier) ForwardLatestKV(ts time.Time) {
	if f.latestKV.Before(ts) {
		f.latestKV = ts
	}
}

// Frontier returns the minimum timestamp being tracked.
func (f *schemaChangeFrontier) Frontier() hlc.Timestamp {
	return f.frontierTimestamp()
}

// SpanFrontier returns underlying span.Frontier.
func (f *schemaChangeFrontier) SpanFrontier() *span.Frontier {
	return f.spanFrontier.Frontier
}

// getCheckpointSpans returns as many spans that should be checkpointed (are
// above the highwater mark) as can fit in maxBytes.
func (f *schemaChangeFrontier) getCheckpointSpans(maxBytes int64) (checkpoint []roachpb.Span) {
	var used int64
	frontier := f.frontierTimestamp()
	f.Entries(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if frontier.Less(ts) {
			used += int64(len(s.Key)) + int64(len(s.EndKey))
			if used > maxBytes {
				return span.StopMatch
			}
			checkpoint = append(checkpoint, s)
		}
		return span.ContinueMatch
	})
	return checkpoint
}

// BackfillTS returns the timestamp of the incoming spans for an ongoing
// Backfill (either an Initial Scan backfill or a Schema Change backfill).
// If no Backfill is occurring, an empty timestamp is returned.
func (f *schemaChangeFrontier) BackfillTS() hlc.Timestamp {
	frontier := f.Frontier()

	// The scan for the initial backfill results in spans sent at StatementTime,
	// which is what initialHighWater is set to when performing an initial scan.
	if frontier.IsEmpty() {
		return f.initialHighWater
	}

	// If the backfill is occurring after any initial scan (non-empty frontier),
	// then it can only be in a schema change backfill, where the scan is
	// performed immediately after the boundary timestamp.
	backfilling := f.boundaryType == jobspb.ResolvedSpan_BACKFILL && frontier.Equal(f.boundaryTime)
	// If the schema change backfill was paused and resumed, the initialHighWater
	// is read from the job progress and is equal to the old BACKFILL boundary
	restarted := frontier.Equal(f.initialHighWater)
	if backfilling || restarted {
		return frontier.Next()
	}
	return hlc.Timestamp{}
}

// schemaChangeBoundaryReached returns true at the single moment when all spans
// have reached a boundary however we have yet to receive any spans after the
// boundary
func (f *schemaChangeFrontier) schemaChangeBoundaryReached() (r bool) {
	return f.boundaryTime.Equal(f.Frontier()) &&
		f.latestTs.Equal(f.boundaryTime) &&
		f.boundaryType != jobspb.ResolvedSpan_NONE
}
