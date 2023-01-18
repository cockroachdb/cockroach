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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	// sink is the Sink to write rows to. Resolved timestamps are never written
	// by changeAggregator.
	sink EventSink
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
	eventConsumer eventConsumer

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
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeAggregatorSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, "changeagg-mem")
	ca := &changeAggregator{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
	}
	if err := ca.Init(
		ctx,
		ca,
		post,
		changefeedResultTypes,
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

	opts := changefeedbase.MakeStatementOptions(ca.spec.Feed.Opts)

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
	checkpointFreq, err := opts.GetMinCheckpointFrequency()
	if err != nil {
		return nil, err
	}
	if checkpointFreq != nil {
		ca.flushFrequency = *checkpointFreq
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
	// Derive a separate context so that we can shutdown the poller.
	ctx, ca.cancel = ca.flowCtx.Stopper().WithCancelOnQuiesce(ctx)

	if ca.spec.JobID != 0 {
		ctx = logtags.AddTag(ctx, "job", ca.spec.JobID)
	}
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	spans, err := ca.setupSpansAndFrontier()

	feed := makeChangefeedConfigFromJobDetails(ca.spec.Feed)

	opts := feed.Opts

	if err != nil {
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
	timestampOracle := &changeAggregatorLowerBoundOracle{
		sf:                         ca.frontier.SpanFrontier(),
		initialInclusiveLowerBound: feed.ScanTime,
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
	kvFeedMemMon.StartNoReserved(ctx, pool)
	ca.kvFeedMemMon = kvFeedMemMon

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	ca.metrics = ca.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	scope, _ := opts.GetMetricScope()
	ca.sliMetrics, err = ca.metrics.getSLIMetrics(scope)
	if err != nil {
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	ca.sink, err = getEventSink(ctx, ca.flowCtx.Cfg, ca.spec.Feed, timestampOracle,
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

	// If the initial scan was disabled the highwater would've already been forwarded
	needsInitialScan := ca.frontier.Frontier().IsEmpty()

	// The "HighWater" of the KVFeed is the timestamp it will begin streaming
	// change events from.  When there's an inital scan, we want the scan to cover
	// data up to the StatementTime and change events to begin from that point.
	kvFeedHighWater := ca.frontier.Frontier()
	if needsInitialScan {
		kvFeedHighWater = ca.spec.Feed.StatementTime
	}

	ca.eventProducer, err = ca.startKVFeed(ctx, spans, kvFeedHighWater, needsInitialScan, feed)
	if err != nil {
		// Early abort in the case that there is an error creating the sink.
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
	ca.sink = &errorWrapperSink{wrapped: ca.sink}
	ca.eventConsumer, ca.sink, err = newEventConsumer(
		ctx, ca.flowCtx.Cfg, ca.spec, feed, ca.frontier.SpanFrontier(), kvFeedHighWater,
		ca.sink, ca.metrics, ca.sliMetrics, ca.knobs)

	if err != nil {
		// Early abort in the case that there is an error setting up the consumption.
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
}

func (ca *changeAggregator) startKVFeed(
	ctx context.Context,
	spans []roachpb.Span,
	initialHighWater hlc.Timestamp,
	needsInitialScan bool,
	config ChangefeedConfig,
) (kvevent.Reader, error) {
	cfg := ca.flowCtx.Cfg
	buf := kvevent.NewThrottlingBuffer(
		kvevent.NewMemBuffer(ca.kvFeedMemMon.MakeBoundAccount(), &cfg.Settings.SV, &ca.metrics.KVFeedMetrics),
		cdcutils.NodeLevelThrottler(&cfg.Settings.SV, &ca.metrics.ThrottleMetrics))

	// KVFeed takes ownership of the kvevent.Writer portion of the buffer, while
	// we return the kvevent.Reader part to the caller.
	kvfeedCfg, err := ca.makeKVFeedCfg(ctx, config, spans, buf, initialHighWater, needsInitialScan)
	if err != nil {
		return nil, err
	}

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
	config ChangefeedConfig,
	spans []roachpb.Span,
	buf kvevent.Writer,
	initialHighWater hlc.Timestamp,
	needsInitialScan bool,
) (kvfeed.Config, error) {
	schemaChange, err := config.Opts.GetSchemaChangeHandlingOptions()
	if err != nil {
		return kvfeed.Config{}, err
	}
	filters := config.Opts.GetFilters()
	cfg := ca.flowCtx.Cfg

	initialScanOnly := config.EndTime.EqOrdering(initialHighWater)
	var sf schemafeed.SchemaFeed

	if schemaChange.Policy == changefeedbase.OptSchemaChangePolicyIgnore || initialScanOnly {
		sf = schemafeed.DoNothingSchemaFeed
	} else {
		sf = schemafeed.New(ctx, cfg, schemaChange.EventClass, AllTargets(ca.spec.Feed),
			initialHighWater, &ca.metrics.SchemaFeedMetrics, config.Opts.GetCanHandle())
	}

	return kvfeed.Config{
		Writer:                  buf,
		Settings:                cfg.Settings,
		DB:                      cfg.DB.KV(),
		Codec:                   cfg.Codec,
		Clock:                   cfg.DB.KV().Clock(),
		Gossip:                  cfg.Gossip,
		Spans:                   spans,
		CheckpointSpans:         ca.spec.Checkpoint.Spans,
		CheckpointTimestamp:     ca.spec.Checkpoint.Timestamp,
		Targets:                 AllTargets(ca.spec.Feed),
		Metrics:                 &ca.metrics.KVFeedMetrics,
		OnBackfillCallback:      ca.sliMetrics.getBackfillCallback(),
		OnBackfillRangeCallback: ca.sliMetrics.getBackfillRangeCallback(),
		MM:                      ca.kvFeedMemMon,
		InitialHighWater:        initialHighWater,
		EndTime:                 config.EndTime,
		WithDiff:                filters.WithDiff,
		NeedsInitialScan:        needsInitialScan,
		SchemaChangeEvents:      schemaChange.EventClass,
		SchemaChangePolicy:      schemaChange.Policy,
		SchemaFeed:              sf,
		Knobs:                   ca.knobs.FeedKnobs,
		UseMux:                  changefeedbase.UseMuxRangeFeed.Get(&cfg.Settings.SV),
	}, nil
}

// setupSpans is called on start to extract the spans for this changefeed as a
// slice and creates a span frontier with the initial resolved timestamps. This
// SpanFrontier only tracks the spans being watched on this node. There is a
// different SpanFrontier elsewhere for the entire changefeed. This object is
// used to filter out some previously emitted rows, and by the cloudStorageSink
// to name its output files in lexicographically monotonic fashion.
func (ca *changeAggregator) setupSpansAndFrontier() (spans []roachpb.Span, err error) {
	var initialHighWater hlc.Timestamp
	spans = make([]roachpb.Span, 0, len(ca.spec.Watches))
	for _, watch := range ca.spec.Watches {
		if initialHighWater.IsEmpty() || watch.InitialResolved.Less(initialHighWater) {
			initialHighWater = watch.InitialResolved
		}
		spans = append(spans, watch.Span)
	}

	ca.frontier, err = makeSchemaChangeFrontier(initialHighWater, spans...)
	if err != nil {
		return nil, err
	}

	checkpointedSpanTs := ca.spec.Checkpoint.Timestamp

	// Checkpoint records from 21.2 were used only for backfills and did not store
	// the timestamp, since in a backfill it must either be the StatementTime for
	// an initial backfill, or right after the high-water for schema backfills.
	if checkpointedSpanTs.IsEmpty() {
		if initialHighWater.IsEmpty() {
			checkpointedSpanTs = ca.spec.Feed.StatementTime
		} else {
			checkpointedSpanTs = initialHighWater.Next()
		}
	}
	// Checkpointed spans are spans that were above the highwater mark, and we
	// must preserve that information in the frontier for future checkpointing.
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
	if ca.eventConsumer != nil {
		if err := ca.eventConsumer.Close(); err != nil {
			log.Warningf(ca.Ctx(), "error closing event consumer: %s", err)
		}
	}

	if ca.sink != nil {
		// Best effort: context is often cancel by now, so we expect to see an error
		_ = ca.sink.Close()
	}
	ca.memAcc.Close(ca.Ctx())
	if ca.kvFeedMemMon != nil {
		ca.kvFeedMemMon.Stop(ca.Ctx())
	}
	ca.MemMonitor.Stop(ca.Ctx())
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
				// ErrBufferClosed is a signal that our kvfeed has exited expectedly.
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
	event, err := ca.eventProducer.Get(ca.Ctx())
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
		return ca.eventConsumer.ConsumeEvent(ca.Ctx(), event)
	case kvevent.TypeResolved:
		a := event.DetachAlloc()
		a.Release(ca.Ctx())
		resolved := event.Resolved()
		if ca.knobs.FilterSpanWithMutation == nil || !ca.knobs.FilterSpanWithMutation(&resolved) {
			return ca.noteResolvedSpan(resolved)
		}
	case kvevent.TypeFlush:
		return ca.sink.Flush(ca.Ctx())
	}

	return nil
}

// noteResolvedSpan periodically flushes Frontier progress from the current
// changeAggregator node to the changeFrontier node to allow the changeFrontier
// to persist the overall changefeed's progress
func (ca *changeAggregator) noteResolvedSpan(resolved jobspb.ResolvedSpan) error {
	advanced, err := ca.frontier.ForwardResolvedSpan(resolved)
	if err != nil {
		return err
	}

	forceFlush := resolved.BoundaryType != jobspb.ResolvedSpan_NONE

	checkpointFrontier := advanced &&
		(forceFlush || timeutil.Since(ca.lastFlush) > ca.flushFrequency)

	// At a lower frequency we checkpoint specific spans in the job progress
	// either in backfills or if the highwater mark is excessively lagging behind
	checkpointSpans := ca.spec.JobID != 0 && /* enterprise changefeed */
		(resolved.Timestamp.Equal(ca.frontier.BackfillTS()) ||
			ca.frontier.hasLaggingSpans(ca.spec.Feed.StatementTime, &ca.flowCtx.Cfg.Settings.SV)) &&
		canCheckpointSpans(&ca.flowCtx.Cfg.Settings.SV, ca.lastFlush)

	if checkpointFrontier || checkpointSpans {
		defer func() {
			ca.lastFlush = timeutil.Now()
		}()
		return ca.flushFrontier()
	}

	return nil
}

// flushFrontier flushes sink and emits resolved timestamp if needed.
func (ca *changeAggregator) flushFrontier() error {
	// Make sure to the sink before forwarding resolved spans,
	// otherwise, we could lose buffered messages and violate the
	// at-least-once guarantee. This is also true for checkpointing the
	// resolved spans in the job progress.
	if err := ca.sink.Flush(ca.Ctx()); err != nil {
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
	ca.metrics.ResolvedMessages.Inc(1)

	ca.recentKVCount = 0
	return nil
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.close()
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
	sink ResolvedTimestampSink
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

	knobs TestingKnobs
}

const (
	runStatusUpdateFrequency time.Duration = time.Minute
	slowSpanMaxFrequency                   = 10 * time.Second
)

// jobState encapsulates changefeed job state.
type jobState struct {
	// job is set for changefeeds other than core/sinkless changefeeds.
	job *jobs.Job
	// coreProgress is set for only core/sinkless changefeeds.
	coreProgress *coreChangefeedProgress

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

type coreChangefeedProgress struct {
	progress jobspb.Progress
}

// SetHighwater implements the eval.ChangefeedState interface.
func (cp *coreChangefeedProgress) SetHighwater(frontier *hlc.Timestamp) {
	cp.progress.Progress = &jobspb.Progress_HighWater{
		HighWater: frontier,
	}
}

// SetCheckpoint implements the eval.ChangefeedState interface.
func (cp *coreChangefeedProgress) SetCheckpoint(spans []roachpb.Span, timestamp hlc.Timestamp) {
	changefeedProgress := cp.progress.Details.(*jobspb.Progress_Changefeed).Changefeed
	changefeedProgress.Checkpoint = &jobspb.ChangefeedProgress_Checkpoint{
		Spans:     spans,
		Timestamp: timestamp,
	}
}

func newJobState(
	j *jobs.Job,
	coreProgress *coreChangefeedProgress,
	st *cluster.Settings,
	metrics *Metrics,
	ts timeutil.TimeSource,
) *jobState {
	return &jobState{
		job:                j,
		coreProgress:       coreProgress,
		settings:           st,
		metrics:            metrics,
		ts:                 ts,
		lastProgressUpdate: ts.Now(),
	}
}

func canCheckpointSpans(sv *settings.Values, lastCheckpoint time.Time) bool {
	freq := changefeedbase.FrontierCheckpointFrequency.Get(sv)
	if freq == 0 {
		return false
	}
	return timeutil.Since(lastCheckpoint) > freq
}

func (j *jobState) canCheckpointSpans() bool {
	return canCheckpointSpans(&j.settings.SV, j.lastProgressUpdate)
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
	j.checkpointDuration = time.Duration(j.metrics.CheckpointHistNanos.Mean())
	j.progressUpdatesSkipped = false
}

var _ execinfra.Processor = &changeFrontier{}
var _ execinfra.RowSource = &changeFrontier{}

func newChangeFrontierProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, "changefntr-mem")
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

	if cfKnobs, ok := flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		cf.knobs = *cfKnobs
	}

	if err := cf.Init(
		ctx,
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
	opts := changefeedbase.MakeStatementOptions(cf.spec.Feed.Opts)

	freq, emitResolved, err := opts.GetResolvedTimestampInterval()
	if err != nil {
		return nil, err
	}
	if emitResolved {
		if freq == nil {
			// Empty means emit them as often as we have them.
			cf.freqEmitResolved = emitAllResolved
		} else {
			cf.freqEmitResolved = *freq
		}
	} else {
		cf.freqEmitResolved = emitNoResolved
	}

	encodingOpts, err := opts.GetEncodingOptions()
	if err != nil {
		return nil, err
	}
	if cf.encoder, err = getEncoder(encodingOpts, AllTargets(spec.Feed)); err != nil {
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
	cf.sink, err = getResolvedTimestampSink(ctx, cf.flowCtx.Cfg, cf.spec.Feed, nilOracle,
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
		cf.js = newJobState(job, nil, cf.flowCtx.Cfg.Settings, cf.metrics, timeutil.DefaultTimeSource{})

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
	} else {
		cf.js = newJobState(nil,
			cf.EvalCtx.ChangefeedState.(*coreChangefeedProgress),
			cf.flowCtx.Cfg.Settings, cf.metrics, timeutil.DefaultTimeSource{})
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
			// Best effort: context is often cancel by now, so we expect to see an error
			_ = cf.sink.Close()
		}
		cf.memAcc.Close(cf.Ctx())
		cf.MemMonitor.Stop(cf.Ctx())
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
			var err error
			endTime := cf.spec.Feed.EndTime
			if endTime.IsEmpty() || endTime.Less(cf.frontier.boundaryTime.Next()) {
				err = pgerror.Newf(pgcode.SchemaChangeOccurred,
					"schema change occurred at %v", cf.frontier.boundaryTime.Next().AsOfSystemTime())

				// Detect whether this boundary should be used to kill or restart the
				// changefeed.
				if cf.frontier.boundaryType == jobspb.ResolvedSpan_EXIT {
					err = changefeedbase.WithTerminalError(errors.Wrapf(err,
						"shut down due to schema change and %s=%q",
						changefeedbase.OptSchemaChangePolicy,
						changefeedbase.OptSchemaChangePolicyStop))
				} else {
					err = changefeedbase.MarkRetryableError(err)
				}
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
	if err := d.EnsureDecoded(changefeedResultTypes[0], &cf.a); err != nil {
		return err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return errors.AssertionFailedf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}

	var resolvedSpans jobspb.ResolvedSpans
	if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling aggregator progress update: %x`, raw)
	}

	cf.maybeMarkJobIdle(resolvedSpans.Stats.RecentKvCount)

	for _, resolved := range resolvedSpans.ResolvedSpans {
		// Inserting a timestamp less than the one the changefeed flow started at
		// could potentially regress the job progress. This is not expected, but it
		// was a bug at one point, so assert to prevent regressions.
		//
		// TODO(dan): This is much more naturally expressed as an assert inside the
		// job progress update closure, but it currently doesn't pass along the info
		// we'd need to do it that way.
		if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(cf.highWaterAtStart) {
			logcrash.ReportOrPanic(cf.Ctx(), &cf.flowCtx.Cfg.Settings.SV,
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

	checkpointed, err := cf.maybeCheckpointJob(resolved, frontierChanged)
	if err != nil {
		return err
	}

	// Emit resolved timestamp only if we have checkpointed the job.
	// Usually, this happens every time frontier changes, but we can skip some updates
	// if we update frontier too rapidly.
	emitResolved = checkpointed

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

	// If we're not in a backfill, highwater progress and an empty checkpoint will
	// be saved. This is throttled however we always persist progress to a schema
	// boundary.
	updateHighWater :=
		!inBackfill && (cf.frontier.schemaChangeBoundaryReached() || cf.js.canCheckpointHighWatermark(frontierChanged))

	// During backfills or when some problematic spans stop advancing, the
	// highwater mark remains fixed while other spans may significantly outpace
	// it, therefore to avoid losing that progress on changefeed resumption we
	// also store as many of those leading spans as we can in the job progress
	updateCheckpoint :=
		(inBackfill || cf.frontier.hasLaggingSpans(cf.spec.Feed.StatementTime, &cf.js.settings.SV)) &&
			cf.js.canCheckpointSpans()

	// If the highwater has moved an empty checkpoint will be saved
	var checkpoint jobspb.ChangefeedProgress_Checkpoint
	if updateCheckpoint {
		maxBytes := changefeedbase.FrontierCheckpointMaxBytes.Get(&cf.flowCtx.Cfg.Settings.SV)
		checkpoint.Spans, checkpoint.Timestamp = cf.frontier.getCheckpointSpans(maxBytes)
	}

	if updateCheckpoint || updateHighWater {
		checkpointStart := timeutil.Now()
		updated, err := cf.checkpointJobProgress(cf.frontier.Frontier(), checkpoint)
		if err != nil {
			return false, err
		}
		cf.js.checkpointCompleted(cf.Ctx(), timeutil.Since(checkpointStart))
		return updated, nil
	}

	return false, nil
}

func (cf *changeFrontier) checkpointJobProgress(
	frontier hlc.Timestamp, checkpoint jobspb.ChangefeedProgress_Checkpoint,
) (bool, error) {
	updateRunStatus := timeutil.Since(cf.js.lastRunStatusUpdate) > runStatusUpdateFrequency
	if updateRunStatus {
		defer func() { cf.js.lastRunStatusUpdate = timeutil.Now() }()
	}
	cf.metrics.FrontierUpdates.Inc(1)
	var updateSkipped error
	if cf.js.job != nil {

		if err := cf.js.job.NoTxn().Update(cf.Ctx(), func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			// If we're unable to update the job due to the job state, such as during
			// pause-requested, simply skip the checkpoint
			if err := md.CheckRunningOrReverting(); err != nil {
				updateSkipped = err
				return nil
			}

			// Advance resolved timestamp.
			progress := md.Progress
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &frontier,
			}

			changefeedProgress := progress.Details.(*jobspb.Progress_Changefeed).Changefeed
			changefeedProgress.Checkpoint = &checkpoint

			if err := cf.manageProtectedTimestamps(cf.Ctx(), txn, changefeedProgress); err != nil {
				log.Warningf(cf.Ctx(), "error managing protected timestamp record: %v", err)
				return err
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
		}); err != nil {
			return false, err
		}
	} else {
		cf.js.coreProgress.SetHighwater(&frontier)
		cf.js.coreProgress.SetCheckpoint(checkpoint.Spans, checkpoint.Timestamp)
	}

	if updateSkipped != nil {
		log.Warningf(cf.Ctx(), "skipping changefeed checkpoint: %s", updateSkipped)
		return false, nil
	}

	if cf.knobs.RaiseRetryableError != nil {
		if err := cf.knobs.RaiseRetryableError(); err != nil {
			return false, changefeedbase.MarkRetryableError(
				errors.New("cf.knobs.RaiseRetryableError"))
		}
	}

	return true, nil
}

// manageProtectedTimestamps periodically advances the protected timestamp for
// the changefeed's targets to the current highwater mark.  The record is
// cleared during changefeedResumer.OnFailOrCancel
func (cf *changeFrontier) manageProtectedTimestamps(
	ctx context.Context, txn isql.Txn, progress *jobspb.ChangefeedProgress,
) error {
	ptsUpdateInterval := changefeedbase.ProtectTimestampInterval.Get(&cf.flowCtx.Cfg.Settings.SV)
	if timeutil.Since(cf.lastProtectedTimestampUpdate) < ptsUpdateInterval {
		return nil
	}
	cf.lastProtectedTimestampUpdate = timeutil.Now()

	pts := cf.flowCtx.Cfg.ProtectedTimestampProvider.WithTxn(txn)

	// Create / advance the protected timestamp record to the highwater mark
	highWater := cf.frontier.Frontier()
	if highWater.Less(cf.highWaterAtStart) {
		highWater = cf.highWaterAtStart
	}

	recordID := progress.ProtectedTimestampRecord
	if recordID == uuid.Nil {
		ptr := createProtectedTimestampRecord(ctx, cf.flowCtx.Codec(), cf.spec.JobID, AllTargets(cf.spec.Feed), highWater, progress)
		if err := pts.Protect(ctx, ptr); err != nil {
			return err
		}
	} else {
		log.VEventf(ctx, 2, "updating protected timestamp %v at %v", recordID, highWater)
		if err := pts.UpdateTimestamp(ctx, recordID, highWater); err != nil {
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
	if err := emitResolvedTimestamp(cf.Ctx(), cf.encoder, cf.sink, newResolved); err != nil {
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

	return timeutil.Since(frontier.GoTime()) > cf.slownessThreshold()
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
	if frontierChanged && cf.slowLogEveryN.ShouldProcess(now) {
		log.Infof(cf.Ctx(), "%s new resolved timestamp %s is behind by %s",
			description, frontier, resolvedBehind)
	}

	if cf.slowLogEveryN.ShouldProcess(now) {
		s := cf.frontier.PeekFrontierSpan()
		log.Infof(cf.Ctx(), "%s span %s is behind by %s", description, s, resolvedBehind)
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
	sf, err := span.MakeFrontierAt(initialHighWater, spans...)
	if err != nil {
		return nil, err
	}
	return &schemaChangeFrontier{
		spanFrontier:     &spanFrontier{Frontier: sf},
		initialHighWater: initialHighWater,
		latestTs:         initialHighWater,
	}, nil
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
// above the highwater mark) as can fit in maxBytes, along with the earliest
// timestamp of the checkpointed spans.  A SpanGroup is used to merge adjacent
// spans above the high-water mark.
func (f *schemaChangeFrontier) getCheckpointSpans(
	maxBytes int64,
) (spans []roachpb.Span, timestamp hlc.Timestamp) {
	frontier := f.frontierTimestamp()

	// Collect leading spans into a SpanGroup to merge adjacent spans and store
	// the lowest timestamp found
	var checkpointSpanGroup roachpb.SpanGroup
	checkpointFrontier := hlc.Timestamp{}
	f.Entries(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if frontier.Less(ts) {
			checkpointSpanGroup.Add(s)
			if checkpointFrontier.IsEmpty() || ts.Less(checkpointFrontier) {
				checkpointFrontier = ts
			}
		}
		return span.ContinueMatch
	})

	// Ensure we only return up to maxBytes spans
	var checkpointSpans []roachpb.Span
	var used int64
	for _, span := range checkpointSpanGroup.Slice() {
		used += int64(len(span.Key)) + int64(len(span.EndKey))
		if used > maxBytes {
			break
		}
		checkpointSpans = append(checkpointSpans, span)
	}

	return checkpointSpans, checkpointFrontier
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

// hasLaggingSpans returns true when the time between the earliest and latest
// resolved spans has exceeded the configured HighwaterLagCheckpointThreshold
func (f *schemaChangeFrontier) hasLaggingSpans(
	defaultIfEmpty hlc.Timestamp, sv *settings.Values,
) bool {
	lagThresholdNanos := int64(changefeedbase.FrontierHighwaterLagCheckpointThreshold.Get(sv))
	if lagThresholdNanos == 0 {
		return false
	}
	frontier := f.Frontier()
	if frontier.IsEmpty() {
		frontier = defaultIfEmpty
	}
	return frontier.Add(lagThresholdNanos, 0).Less(f.latestTs)
}
