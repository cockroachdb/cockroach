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

	// cancel cancels the context passed to all resources created while starting
	// this aggregator.
	cancel func()
	// errCh contains the return values of the kvfeed.
	errCh chan error
	// kvFeedDoneCh is closed when the kvfeed exits.
	kvFeedDoneCh chan struct{}

	// drainWatchCh is signaled if the job registry on this node is being
	// drained, which is a proxy for the node being drained. If a drain occurs,
	// it will be blocked until we allow it to proceed by calling drainDone().
	// This gives the aggregator time to checkpoint before shutting down.
	drainWatchCh <-chan struct{}
	drainDone    func()

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
	// lastPush records the time when we last pushed data to the coordinator.
	lastPush time.Time
	// shutdownCheckpointEmitted indicates if aggregator emitted checkpoint
	// information during aggregator shutdown.
	shutdownCheckpointEmitted bool

	// recentKVCount contains the number of emits since the last time a resolved
	// span was forwarded to the frontier
	recentKVCount uint64

	// eventProducer produces the next event from the kv feed.
	eventProducer kvevent.Reader
	// eventConsumer consumes the event.
	eventConsumer eventConsumer

	lastHighWaterFlush time.Time     // last time high watermark was checkpointed.
	flushFrequency     time.Duration // how often high watermark can be checkpointed.
	lastSpanFlush      time.Time     // last time expensive, span based checkpoint was written.

	// frontier keeps track of resolved timestamps for spans along with schema change
	// boundary information.
	frontier *schemaChangeFrontier

	metrics                *Metrics
	sliMetrics             *sliMetrics
	sliMetricsID           int64
	closeTelemetryRecorder func()
	knobs                  TestingKnobs
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

// wrapMetricsController wraps the supplied metricsRecorder to emit metrics to telemetry.
// This method modifies ca.cancel().
func (ca *changeAggregator) wrapMetricsController(
	ctx context.Context, recorder metricsRecorder,
) (metricsRecorder, error) {
	job, err := ca.flowCtx.Cfg.JobRegistry.LoadJob(ctx, ca.spec.JobID)
	if err != nil {
		return ca.sliMetrics, err
	}

	recorderWithTelemetry, err := wrapMetricsRecorderWithTelemetry(ctx, job, ca.flowCtx.Cfg.Settings, recorder)
	if err != nil {
		return ca.sliMetrics, err
	}
	ca.closeTelemetryRecorder = recorderWithTelemetry.close

	return recorderWithTelemetry, nil
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
	ca.sliMetricsID = ca.sliMetrics.claimId()

	// TODO(jayant): add support for sinkless changefeeds using UUID
	recorder := metricsRecorder(ca.sliMetrics)
	if !ca.isSinkless() {
		recorder, err = ca.wrapMetricsController(ctx, recorder)
		if err != nil {
			ca.MoveToDraining(err)
			ca.cancel()
			return
		}
	}

	ca.sink, err = getEventSink(ctx, ca.flowCtx.Cfg, ca.spec.Feed, timestampOracle,
		ca.spec.User(), ca.spec.JobID, recorder)
	if err != nil {
		err = changefeedbase.MarkRetryableError(err)
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

	// TODO(yevgeniy): Introduce separate changefeed monitor that's a parent
	// for all changefeeds to control memory allocated to all changefeeds.
	pool := ca.flowCtx.Cfg.BackfillerMonitor
	if ca.knobs.MemMonitor != nil {
		pool = ca.knobs.MemMonitor
	}
	limit := changefeedbase.PerChangefeedMemLimit.Get(&ca.flowCtx.Cfg.Settings.SV)
	ca.eventProducer, ca.kvFeedDoneCh, ca.errCh, err = ca.startKVFeed(ctx, spans, kvFeedHighWater, needsInitialScan, feed, pool, limit, opts)
	if err != nil {
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
	ca.sink = &errorWrapperSink{wrapped: ca.sink}
	ca.eventConsumer, ca.sink, err = newEventConsumer(
		ctx, ca.flowCtx.Cfg, ca.spec, feed, ca.frontier.SpanFrontier(), kvFeedHighWater,
		ca.sink, ca.metrics, ca.sliMetrics, ca.knobs)
	if err != nil {
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	// Init heartbeat timer.
	ca.lastPush = timeutil.Now()

	// Generate expensive checkpoint only after we ran for a while.
	ca.lastSpanFlush = timeutil.Now()

	if ca.knobs.OnDrain != nil {
		ca.drainWatchCh = ca.knobs.OnDrain()
	} else {
		ca.drainWatchCh, ca.drainDone = ca.flowCtx.Cfg.JobRegistry.OnDrain()
	}
}

// checkForNodeDrain returns an error if the node is draining.
func (ca *changeAggregator) checkForNodeDrain() error {
	if ca.drainWatchCh == nil {
		return errors.AssertionFailedf("cannot check for node drain if" +
			" watch channel is nil")
	}
	select {
	case <-ca.drainWatchCh:
		return changefeedbase.ErrNodeDraining
	default:
		return nil
	}
}

func (ca *changeAggregator) startKVFeed(
	ctx context.Context,
	spans []roachpb.Span,
	initialHighWater hlc.Timestamp,
	needsInitialScan bool,
	config ChangefeedConfig,
	parentMemMon *mon.BytesMonitor,
	memLimit int64,
	opts changefeedbase.StatementOptions,
) (kvevent.Reader, chan struct{}, chan error, error) {
	cfg := ca.flowCtx.Cfg
	kvFeedMemMon := mon.NewMonitorInheritWithLimit("kvFeed", memLimit, parentMemMon)
	kvFeedMemMon.StartNoReserved(ctx, parentMemMon)
	buf := kvevent.NewThrottlingBuffer(
		kvevent.NewMemBuffer(kvFeedMemMon.MakeBoundAccount(), &cfg.Settings.SV, &ca.metrics.KVFeedMetrics),
		cdcutils.NodeLevelThrottler(&cfg.Settings.SV, &ca.metrics.ThrottleMetrics))

	// KVFeed takes ownership of the kvevent.Writer portion of the buffer, while
	// we return the kvevent.Reader part to the caller.
	kvfeedCfg, err := ca.makeKVFeedCfg(ctx, config, spans, buf, initialHighWater, needsInitialScan, kvFeedMemMon, opts)
	if err != nil {
		return nil, nil, nil, err
	}

	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	// If RunAsyncTask immediately returns an error, the kvfeed was not run and
	// will not run.
	if err := ca.flowCtx.Stopper().RunAsyncTask(ctx, "changefeed-poller", func(ctx context.Context) {
		defer close(doneCh)
		defer kvFeedMemMon.Stop(ctx)
		errCh <- kvfeed.Run(ctx, kvfeedCfg)
	}); err != nil {
		return nil, nil, nil, err
	}

	return buf, doneCh, errCh, nil
}

func (ca *changeAggregator) waitForKVFeedDone() {
	if ca.kvFeedDoneCh != nil {
		<-ca.kvFeedDoneCh
	}
}

func (ca *changeAggregator) checkKVFeedErr() error {
	select {
	case err := <-ca.errCh:
		return err
	default:
		return nil
	}
}

func (ca *changeAggregator) makeKVFeedCfg(
	ctx context.Context,
	config ChangefeedConfig,
	spans []roachpb.Span,
	buf kvevent.Writer,
	initialHighWater hlc.Timestamp,
	needsInitialScan bool,
	memMon *mon.BytesMonitor,
	opts changefeedbase.StatementOptions,
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

	monitoringCfg, err := makeKVFeedMonitoringCfg(ca.sliMetrics, opts)
	if err != nil {
		return kvfeed.Config{}, err
	}

	return kvfeed.Config{
		Writer:              buf,
		Settings:            cfg.Settings,
		DB:                  cfg.DB.KV(),
		Codec:               cfg.Codec,
		Clock:               cfg.DB.KV().Clock(),
		Spans:               spans,
		CheckpointSpans:     ca.spec.Checkpoint.Spans,
		CheckpointTimestamp: ca.spec.Checkpoint.Timestamp,
		Targets:             AllTargets(ca.spec.Feed),
		Metrics:             &ca.metrics.KVFeedMetrics,
		MM:                  memMon,
		InitialHighWater:    initialHighWater,
		EndTime:             config.EndTime,
		WithDiff:            filters.WithDiff,
		NeedsInitialScan:    needsInitialScan,
		SchemaChangeEvents:  schemaChange.EventClass,
		SchemaChangePolicy:  schemaChange.Policy,
		SchemaFeed:          sf,
		Knobs:               ca.knobs.FeedKnobs,
		UseMux:              changefeedbase.UseMuxRangeFeed.Get(&cfg.Settings.SV),
		MonitoringCfg:       monitoringCfg,
	}, nil
}

func makeKVFeedMonitoringCfg(
	sliMetrics *sliMetrics, opts changefeedbase.StatementOptions,
) (kvfeed.MonitoringConfig, error) {
	laggingRangesThreshold, laggingRangesInterval, err := opts.GetLaggingRangesConfig()
	if err != nil {
		return kvfeed.MonitoringConfig{}, err
	}

	return kvfeed.MonitoringConfig{
		LaggingRangesCallback:        sliMetrics.getLaggingRangesCallback(),
		LaggingRangesThreshold:       laggingRangesThreshold,
		LaggingRangesPollingInterval: laggingRangesInterval,

		OnBackfillCallback:      sliMetrics.getBackfillCallback(),
		OnBackfillRangeCallback: sliMetrics.getBackfillRangeCallback(),
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
	if initialHighWater.IsEmpty() {
		// If we are performing initial scan, set frontier initialHighWater
		// to the StatementTime -- this is the time we will be scanning spans.
		// Spans that reach this time are eligible for checkpointing.
		ca.frontier.initialHighWater = ca.spec.Feed.StatementTime
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
// the processor.
//
// Due to the fact that this method may be called even if the processor did not
// finish completion, there is an excessive amount of nil checking. For example,
// (*changeAggregator) Start() may encounter an error and move the processor to
// draining before one of the fields below (ex. ca.drainDone) is set.
func (ca *changeAggregator) close() {
	if ca.Closed {
		return
	}
	ca.cancel()
	// Wait for the poller to finish shutting down.
	ca.waitForKVFeedDone()

	if ca.drainDone != nil {
		ca.drainDone()
	}
	if ca.eventConsumer != nil {
		_ = ca.eventConsumer.Close() // context cancellation expected here.
	}
	if ca.closeTelemetryRecorder != nil {
		ca.closeTelemetryRecorder()
	}

	if ca.sink != nil {
		// Best effort: context is often cancel by now, so we expect to see an error
		_ = ca.sink.Close()
	}

	ca.closeMetrics()

	ca.memAcc.Close(ca.Ctx())

	ca.MemMonitor.Stop(ca.Ctx())
	ca.InternalClose()
}

var aggregatorHeartbeatFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.aggregator.heartbeat",
	"changefeed aggregator will emit a heartbeat message to the coordinator with this frequency; 0 disables. "+
		"The setting value should be <=1/2 of server.shutdown.jobs_wait period",
	4*time.Second,
	settings.NonNegativeDuration,
)

// Next is part of the RowSource interface.
func (ca *changeAggregator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	shouldEmitHeartBeat := func() bool {
		freq := aggregatorHeartbeatFrequency.Get(&ca.FlowCtx.Cfg.Settings.SV)
		return freq > 0 && timeutil.Since(ca.lastPush) > freq
	}

	// helper to iterate frontier and return the list of changefeed frontier spans.
	getFrontierSpans := func() (spans []execinfrapb.ChangefeedMeta_FrontierSpan) {
		ca.frontier.Entries(func(r roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
			spans = append(spans,
				execinfrapb.ChangefeedMeta_FrontierSpan{
					Span:      r,
					Timestamp: ts,
				})
			return span.ContinueMatch
		})
		return spans
	}

	for ca.State == execinfra.StateRunning {
		if !ca.changedRowBuf.IsEmpty() {
			ca.lastPush = timeutil.Now()
			return ca.ProcessRowHelper(ca.changedRowBuf.Pop()), nil
		} else if !ca.resolvedSpanBuf.IsEmpty() {
			ca.lastPush = timeutil.Now()
			return ca.ProcessRowHelper(ca.resolvedSpanBuf.Pop()), nil
		} else if shouldEmitHeartBeat() {
			// heartbeat is simply an attempt to push a row into process row helper.
			// This mechanism allows coordinator to propagate shutdown information to
			// all aggregators -- that is, inability to write to this channel will
			// trigger aggregator to transition away from StateRunning.
			ca.lastPush = timeutil.Now()
			return nil, &execinfrapb.ProducerMetadata{
				Changefeed: &execinfrapb.ChangefeedMeta{Heartbeat: true},
			}
		}

		// As the last gasp before shutdown, transmit an up-to-date frontier
		// information to the coordinator. We expect to get this signal via the
		// polling below before the drain actually occurs and starts tearing
		// things down.
		if err := ca.checkForNodeDrain(); err != nil {
			nodeID, _ := ca.FlowCtx.Cfg.NodeID.OptionalNodeID()
			meta := &execinfrapb.ChangefeedMeta{
				DrainInfo:  &execinfrapb.ChangefeedMeta_DrainInfo{NodeID: nodeID},
				Checkpoint: getFrontierSpans(),
			}

			ca.AppendTrailingMeta(execinfrapb.ProducerMetadata{Changefeed: meta})
			ca.shutdownCheckpointEmitted = true
			ca.cancel()
			ca.MoveToDraining(err)
			break
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
				// If the poller errored first, that's the
				// interesting one, so overwrite `err`.
				if kvFeedErr := ca.checkKVFeedErr(); kvFeedErr != nil {
					err = kvFeedErr
				}
			}
			// Shut down the poller if it wasn't already.
			ca.cancel()
			ca.MoveToDraining(err)
			break
		}
	}

	if !ca.shutdownCheckpointEmitted {
		// Aggregator shutdown may be initiated by the coordinator.
		// Emit an up-to-date frontier information in case coordinator will restart.
		ca.shutdownCheckpointEmitted = true
		ca.AppendTrailingMeta(execinfrapb.ProducerMetadata{
			Changefeed: &execinfrapb.ChangefeedMeta{Checkpoint: getFrontierSpans()},
		})
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
		if ca.knobs.FilterSpanWithMutation != nil {
			shouldFilter, err := ca.knobs.FilterSpanWithMutation(&resolved)
			if err != nil {
				return err
			}
			if shouldFilter {
				return nil
			}
		}
		return ca.noteResolvedSpan(resolved)
	case kvevent.TypeFlush:
		return ca.flushBufferedEvents()
	}

	return nil
}

func (ca *changeAggregator) flushBufferedEvents() error {
	if err := ca.eventConsumer.Flush(ca.Ctx()); err != nil {
		return err
	}
	return ca.sink.Flush(ca.Ctx())
}

// noteResolvedSpan periodically flushes Frontier progress from the current
// changeAggregator node to the changeFrontier node to allow the changeFrontier
// to persist the overall changefeed's progress
func (ca *changeAggregator) noteResolvedSpan(resolved jobspb.ResolvedSpan) error {
	if resolved.Timestamp.IsEmpty() {
		// @0.0 resolved timestamps could come in from rangefeed checkpoint.
		// When rangefeed starts running, it emits @0.0 resolved timestamp.
		// We don't care about those as far as checkpointing concerned.
		return nil
	}

	advanced, err := ca.frontier.ForwardResolvedSpan(resolved)
	if err != nil {
		return err
	}

	// The resolved sliMetric data backs the aggregator_progress metric
	if advanced {
		ca.sliMetrics.setResolved(ca.sliMetricsID, ca.frontier.Frontier())
	}

	forceFlush := resolved.BoundaryType != jobspb.ResolvedSpan_NONE

	checkpointFrontier := advanced &&
		(forceFlush || timeutil.Since(ca.lastHighWaterFlush) > ca.flushFrequency)

	if checkpointFrontier {
		defer func() {
			ca.lastHighWaterFlush = timeutil.Now()
		}()
		return ca.flushFrontier()
	}

	// At a lower frequency we checkpoint specific spans in the job progress
	// either in backfills or if the highwater mark is excessively lagging behind
	checkpointSpans := ca.spec.JobID != 0 && /* enterprise changefeed */
		(resolved.Timestamp.Equal(ca.frontier.BackfillTS()) ||
			ca.frontier.hasLaggingSpans(ca.spec.Feed.StatementTime, &ca.flowCtx.Cfg.Settings.SV)) &&
		canCheckpointSpans(&ca.flowCtx.Cfg.Settings.SV, ca.lastSpanFlush)

	if checkpointSpans {
		defer func() {
			ca.lastSpanFlush = timeutil.Now()
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
	if err := ca.flushBufferedEvents(); err != nil {
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

// closeMetrics de-registers the aggregator from the sliMetrics registry so that
// it's no longer considered by the aggregator_progress gauge
func (ca *changeAggregator) closeMetrics() {
	ca.sliMetrics.closeId(ca.sliMetricsID)
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.close()
}

func (ca *changeAggregator) isSinkless() bool {
	return ca.spec.JobID == 0
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

	// localState contains an in memory cache of progress updates.
	// Used by core style changefeeds as well as regular changefeeds to make
	// restarts more efficient with respects to duplicates.
	// localState reflects an up-to-date information *after* the checkpoint.
	localState *cachedState

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

	// sliMetricsID and metricsID uniquely identify the changefeed in the metrics's
	// map (a shared struct across all changefeeds on the node) and the sliMetrics's
	// map (shared structure between all feeds within the same scope on the node).
	metricsID    int
	sliMetricsID int64

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

// cachedState is a changefeed progress stored in memory.
// It is used to reduce the number of duplicate events emitted during retries.
type cachedState struct {
	progress jobspb.Progress

	// set of spans for this changefeed.
	trackedSpans roachpb.Spans
	// aggregatorFrontier contains the list of frontier spans
	// emitted by aggregators when they are being shut down.
	aggregatorFrontier []execinfrapb.ChangefeedMeta_FrontierSpan
	// drainingNodes is the list of nodes that are draining.
	drainingNodes []roachpb.NodeID
}

// SetHighwater implements the eval.ChangefeedState interface.
func (cs *cachedState) SetHighwater(frontier hlc.Timestamp) {
	cs.progress.Progress = &jobspb.Progress_HighWater{
		HighWater: &frontier,
	}
}

// SetCheckpoint implements the eval.ChangefeedState interface.
func (cs *cachedState) SetCheckpoint(spans []roachpb.Span, timestamp hlc.Timestamp) {
	changefeedProgress := cs.progress.Details.(*jobspb.Progress_Changefeed).Changefeed
	changefeedProgress.Checkpoint = &jobspb.ChangefeedProgress_Checkpoint{
		Spans:     spans,
		Timestamp: timestamp,
	}
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

	sliMertics, err := flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(cf.spec.Feed.Opts[changefeedbase.OptMetricsScope])
	if err != nil {
		return nil, err
	}

	if cf.encoder, err = getEncoder(
		encodingOpts, AllTargets(spec.Feed), spec.Feed.Select != "",
		makeExternalConnectionProvider(ctx, flowCtx.Cfg.DB), sliMertics,
	); err != nil {
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
	if cf.EvalCtx.ChangefeedState == nil {
		cf.MoveToDraining(errors.AssertionFailedf("expected initialized local state"))
		return
	}

	cf.localState = cf.EvalCtx.ChangefeedState.(*cachedState)
	cf.js = newJobState(nil, cf.flowCtx.Cfg.Settings, cf.metrics, timeutil.DefaultTimeSource{})

	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.Cfg.JobRegistry.LoadClaimedJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return
		}
		cf.js.job = job
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

	func() {
		cf.metrics.mu.Lock()
		defer cf.metrics.mu.Unlock()
		cf.metricsID = cf.metrics.mu.id
		cf.metrics.mu.id++
		sli.RunningCount.Inc(1)
	}()

	cf.sliMetricsID = cf.sliMetrics.claimId()

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
	func() {
		cf.metrics.mu.Lock()
		defer cf.metrics.mu.Unlock()
		if cf.metricsID > 0 {
			cf.sliMetrics.RunningCount.Dec(1)
		}
		delete(cf.metrics.mu.resolved, cf.metricsID)
		cf.metricsID = -1
	}()

	cf.sliMetrics.closeId(cf.sliMetricsID)
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
			if endTime.IsEmpty() || endTime.Less(cf.frontier.boundaryTime) {
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
			if meta.Changefeed != nil && meta.Changefeed.DrainInfo != nil {
				// Seeing changefeed drain info metadata from the aggregator means
				// that the aggregator exited due to node shutdown.  Transition to
				// draining so that the remaining aggregators will shut down and
				// transmit their up-to-date frontier.
				cf.MoveToDraining(changefeedbase.ErrNodeDraining)
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

		// The feed's checkpoint is tracked in a map which is used to inform the
		// checkpoint_progress metric which will return the lowest timestamp across
		// all feeds in the scope.
		cf.sliMetrics.setCheckpoint(cf.sliMetricsID, newResolved)

		// This backs max_behind_nanos which is deprecated in favor of checkpoint_progress
		func() {
			cf.metrics.mu.Lock()
			defer cf.metrics.mu.Unlock()
			if cf.metricsID != -1 {
				cf.metrics.mu.resolved[cf.metricsID] = newResolved
			}
		}()

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
		if cf.knobs.ShouldCheckpointToJobRecord != nil && !cf.knobs.ShouldCheckpointToJobRecord(cf.frontier.Frontier()) {
			return false, nil
		}
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
	if cf.knobs.RaiseRetryableError != nil {
		if err := cf.knobs.RaiseRetryableError(); err != nil {
			return false, changefeedbase.MarkRetryableError(
				errors.New("cf.knobs.RaiseRetryableError"))
		}
	}

	updateRunStatus := timeutil.Since(cf.js.lastRunStatusUpdate) > runStatusUpdateFrequency
	if updateRunStatus {
		defer func() { cf.js.lastRunStatusUpdate = timeutil.Now() }()
	}
	cf.metrics.FrontierUpdates.Inc(1)
	if cf.js.job != nil {
		if err := cf.js.job.NoTxn().Update(cf.Ctx(), func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
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
	}

	cf.localState.SetHighwater(frontier)
	cf.localState.SetCheckpoint(checkpoint.Spans, checkpoint.Timestamp)

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
		ptr := createProtectedTimestampRecord(
			ctx, cf.flowCtx.Codec(), cf.spec.JobID, AllTargets(cf.spec.Feed), highWater,
		)
		progress.ProtectedTimestampRecord = ptr.ID.GetUUID()
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

		// latestKV timestamp is set to the current time to make
		// sure that even if the target table does not have any
		// KV events coming in, that the changefeed will remain
		// running for at least changefeedbase.IdleTimeout time
		// to ensure that sql pod in serverless deployment does
		// not get shutdown immediately after changefeed starts.
		latestKV: timeutil.Now(),
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

type spanIter func(forEachSpan span.Operation)

// getCheckpointSpans returns as many spans that should be checkpointed (are
// above the highwater mark) as can fit in maxBytes, along with the earliest
// timestamp of the checkpointed spans.  A SpanGroup is used to merge adjacent
// spans above the high-water mark.
func getCheckpointSpans(
	frontier hlc.Timestamp, forEachSpan spanIter, maxBytes int64,
) (spans []roachpb.Span, timestamp hlc.Timestamp) {
	// Collect leading spans into a SpanGroup to merge adjacent spans and store
	// the lowest timestamp found
	var checkpointSpanGroup roachpb.SpanGroup
	checkpointFrontier := hlc.Timestamp{}
	forEachSpan(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
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

func (f *schemaChangeFrontier) getCheckpointSpans(
	maxBytes int64,
) (spans []roachpb.Span, timestamp hlc.Timestamp) {
	return getCheckpointSpans(f.frontierTimestamp(), f.Entries, maxBytes)
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
