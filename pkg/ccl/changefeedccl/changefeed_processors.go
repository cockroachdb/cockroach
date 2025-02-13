// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/resolvedspan"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const EnableCloudBillingAccountingEnvVar = "COCKROACH_ENABLE_CLOUD_BILLING_ACCOUNTING"

var EnableCloudBillingAccounting = envutil.EnvOrDefaultBool(EnableCloudBillingAccountingEnvVar, false)

type changeAggregator struct {
	execinfra.ProcessorBase

	spec   execinfrapb.ChangeAggregatorSpec
	memAcc mon.BoundAccount

	// checkForNodeDrain is a callback that returns an error
	// if this node is being drained.
	checkForNodeDrain func() error

	// cancel cancels the context passed to all resources created while starting
	// this aggregator.
	cancel func()
	// errCh contains the return values of the kvfeed.
	errCh chan error
	// kvFeedDoneCh is closed when the kvfeed exits.
	kvFeedDoneCh chan struct{}

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

	// recentKVCount contains the number of emits since the last time a resolved
	// span was forwarded to the frontier
	recentKVCount uint64

	// eventProducer produces the next event from the kv feed.
	eventProducer kvevent.Reader
	// eventConsumer consumes the event.
	eventConsumer eventConsumer

	nextHighWaterFlush time.Time     // next time high watermark may be flushed.
	flushFrequency     time.Duration // how often high watermark can be checkpointed.
	lastSpanFlush      time.Time     // last time expensive, span based checkpoint was written.

	// frontier keeps track of resolved timestamps for spans along with schema change
	// boundary information.
	frontier *resolvedspan.AggregatorFrontier

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
	sf                         frontier
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

var aggregatorEmitsShutdownCheckpoint = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.shutdown_checkpoint.enabled",
	"upon shutdown aggregator attempts to emit an up-to-date checkpoint",
	false,
)

type drainWatcher <-chan struct{}

func (w drainWatcher) checkForNodeDrain() error {
	select {
	case <-w:
		return changefeedbase.ErrNodeDraining
	default:
		return nil
	}
}

func (w drainWatcher) enabled() bool {
	return w != nil
}

func makeDrainWatcher(flowCtx *execinfra.FlowCtx) (w drainWatcher, _ func()) {
	if !aggregatorEmitsShutdownCheckpoint.Get(&flowCtx.Cfg.Settings.SV) {
		// Drain watcher disabled
		return nil, func() {}
	}

	if cfKnobs, ok := flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok && cfKnobs != nil && cfKnobs.OnDrain != nil {
		return cfKnobs.OnDrain(), func() {}
	}
	return flowCtx.Cfg.JobRegistry.OnDrain()
}

func newChangeAggregatorProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeAggregatorSpec,
	post *execinfrapb.PostProcessSpec,
) (_ execinfra.Processor, retErr error) {
	// Setup monitoring for this node drain.
	drainWatcher, drainDone := makeDrainWatcher(flowCtx)
	defer func() {
		if retErr != nil {
			drainDone()
		}
	}()

	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, "changeagg-mem")
	ca := &changeAggregator{
		spec:              spec,
		memAcc:            memMonitor.MakeBoundAccount(),
		checkForNodeDrain: drainWatcher.checkForNodeDrain,
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
			TrailingMetaCallback: func() (producerMeta []execinfrapb.ProducerMetadata) {
				defer drainDone()

				if drainWatcher.enabled() {
					var meta execinfrapb.ChangefeedMeta
					if err := drainWatcher.checkForNodeDrain(); err != nil {
						// This node is draining.  Indicate so in the trailing metadata.
						nodeID, _ := flowCtx.Cfg.NodeID.OptionalNodeID()
						meta.DrainInfo = &execinfrapb.ChangefeedMeta_DrainInfo{NodeID: nodeID}
					}
					ca.computeTrailingMetadata(&meta)
					producerMeta = []execinfrapb.ProducerMetadata{{Changefeed: &meta}}
				}

				ca.close()
				return producerMeta
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

// wrapMetricsRecorderWithTelemetry wraps the supplied metricsRecorder
// so it periodically emits metrics to telemetry.
func (ca *changeAggregator) wrapMetricsRecorderWithTelemetry(
	ctx context.Context, recorder metricsRecorder,
) (metricsRecorder, error) {
	details := ca.spec.Feed
	jobID := ca.spec.JobID
	description := ca.spec.Description
	// This code exists so that the old behavior is preserved if the spec is created
	// in a mixed-version cluster on a node without the new Description field.
	// It can be deleted once we no longer need to interoperate with binaries that
	// are version 24.3 or earlier.
	if description == "" {
		// Don't emit telemetry messages for core changefeeds without a description.
		if ca.isSinkless() {
			return recorder, nil
		}
		job, err := ca.FlowCtx.Cfg.JobRegistry.LoadJob(ctx, jobID)
		if err != nil {
			return nil, err
		}
		description = job.Payload().Description
	}

	recorderWithTelemetry, err := wrapMetricsRecorderWithTelemetry(ctx, details, description, jobID, ca.FlowCtx.Cfg.Settings, recorder, ca.knobs)
	if err != nil {
		return ca.sliMetrics, err
	}
	ca.closeTelemetryRecorder = recorderWithTelemetry.close

	return recorderWithTelemetry, nil
}

const (
	changeAggregatorLogTag = "change-aggregator"
	changeFrontierLogTag   = "change-frontier"
)

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) {
	// Derive a separate context so that we can shutdown the poller.
	ctx, ca.cancel = ca.FlowCtx.Stopper().WithCancelOnQuiesce(ctx)

	if ca.spec.JobID != 0 {
		ctx = logtags.AddTag(ctx, "job", ca.spec.JobID)
	}
	ctx = logtags.RemoveTag(ctx, changeFrontierLogTag)
	ctx = logtags.AddTag(ctx, changeAggregatorLogTag, nil /* value */)
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	spans, err := ca.setupSpansAndFrontier()
	if err != nil {
		if log.V(2) {
			log.Infof(ca.Ctx(), "change aggregator moving to draining due to error setting up spans and frontier: %v", err)
		}
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	feed := makeChangefeedConfigFromJobDetails(ca.spec.Feed)

	opts := feed.Opts

	timestampOracle := &changeAggregatorLowerBoundOracle{
		sf:                         ca.frontier,
		initialInclusiveLowerBound: feed.ScanTime,
	}

	if cfKnobs, ok := ca.FlowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		ca.knobs = *cfKnobs
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	ca.metrics = ca.FlowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	scope, _ := opts.GetMetricScope()
	ca.sliMetrics, err = ca.metrics.getSLIMetrics(scope)
	if err != nil {
		if log.V(2) {
			log.Infof(ca.Ctx(), "change aggregator moving to draining due to error getting sli metrics: %v", err)
		}
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
	ca.sliMetricsID = ca.sliMetrics.claimId()

	recorder := metricsRecorder(ca.sliMetrics)
	recorder, err = ca.wrapMetricsRecorderWithTelemetry(ctx, recorder)
	if err != nil {
		if log.V(2) {
			log.Infof(ca.Ctx(), "change aggregator moving to draining due to error wrapping metrics controller: %v", err)
		}
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	ca.sink, err = getEventSink(ctx, ca.FlowCtx.Cfg, ca.spec.Feed, timestampOracle,
		ca.spec.User(), ca.spec.JobID, recorder)
	if err != nil {
		err = changefeedbase.MarkRetryableError(err)
		if log.V(2) {
			log.Infof(ca.Ctx(), "change aggregator moving to draining due to error getting sink: %v", err)
		}
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
	pool := ca.FlowCtx.Cfg.BackfillerMonitor
	if ca.knobs.MemMonitor != nil {
		pool = ca.knobs.MemMonitor
	}
	limit := changefeedbase.PerChangefeedMemLimit.Get(&ca.FlowCtx.Cfg.Settings.SV)
	ca.eventProducer, ca.kvFeedDoneCh, ca.errCh, err = ca.startKVFeed(ctx, spans, kvFeedHighWater, needsInitialScan, feed, pool, limit, opts)
	if err != nil {
		if log.V(2) {
			log.Infof(ca.Ctx(), "change aggregator moving to draining due to error starting kv feed: %v", err)
		}
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}
	ca.sink = &errorWrapperSink{wrapped: ca.sink}
	ca.eventConsumer, ca.sink, err = newEventConsumer(
		ctx, ca.FlowCtx.Cfg, ca.spec, feed, ca.frontier, kvFeedHighWater,
		ca.sink, ca.metrics, ca.sliMetrics, ca.knobs)
	if err != nil {
		if log.V(2) {
			log.Infof(ca.Ctx(), "change aggregator moving to draining due to error creating event consumer: %v", err)
		}
		ca.MoveToDraining(err)
		ca.cancel()
		return
	}

	// Init heartbeat timer.
	ca.lastPush = timeutil.Now()

	// Generate expensive checkpoint only after we ran for a while.
	ca.lastSpanFlush = timeutil.Now()
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
	cfg := ca.FlowCtx.Cfg
	kvFeedMemMon := mon.NewMonitorInheritWithLimit("kvFeed", memLimit, parentMemMon, false /* longLiving */)
	kvFeedMemMon.StartNoReserved(ctx, parentMemMon)
	buf := kvevent.NewThrottlingBuffer(
		kvevent.NewMemBuffer(kvFeedMemMon.MakeBoundAccount(), &cfg.Settings.SV, &ca.metrics.KVFeedMetrics.AggregatorBufferMetricsWithCompat),
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
	if err := ca.FlowCtx.Stopper().RunAsyncTask(ctx, "changefeed-poller", func(ctx context.Context) {
		defer close(doneCh)
		defer kvFeedMemMon.Stop(ctx)
		errCh <- kvfeed.Run(ctx, kvfeedCfg)
	}); err != nil {
		// Ensure that the memory monitor is closed properly.
		kvFeedMemMon.Stop(ctx)
		return nil, nil, nil, err
	}

	return buf, doneCh, errCh, nil
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
	cfg := ca.FlowCtx.Cfg

	initialScanOnly := config.EndTime == initialHighWater
	var sf schemafeed.SchemaFeed

	if schemaChange.Policy == changefeedbase.OptSchemaChangePolicyIgnore || initialScanOnly {
		sf = schemafeed.DoNothingSchemaFeed
	} else {
		sf = schemafeed.New(ctx, cfg, schemaChange.EventClass, AllTargets(ca.spec.Feed),
			initialHighWater, &ca.metrics.SchemaFeedMetrics, config.Opts.GetCanHandle())
	}

	monitoringCfg, err := makeKVFeedMonitoringCfg(ctx, ca.sliMetrics, opts, ca.FlowCtx.Cfg.Settings)
	if err != nil {
		return kvfeed.Config{}, err
	}

	return kvfeed.Config{
		Writer:               buf,
		Settings:             cfg.Settings,
		DB:                   cfg.DB.KV(),
		Codec:                cfg.Codec,
		Clock:                cfg.DB.KV().Clock(),
		Spans:                spans,
		SpanLevelCheckpoint:  ca.spec.SpanLevelCheckpoint,
		Targets:              AllTargets(ca.spec.Feed),
		Metrics:              &ca.metrics.KVFeedMetrics,
		MM:                   memMon,
		InitialHighWater:     initialHighWater,
		EndTime:              config.EndTime,
		WithDiff:             filters.WithDiff,
		WithFiltering:        filters.WithFiltering,
		WithFrontierQuantize: changefeedbase.Quantize.Get(&cfg.Settings.SV),
		NeedsInitialScan:     needsInitialScan,
		SchemaChangeEvents:   schemaChange.EventClass,
		SchemaChangePolicy:   schemaChange.Policy,
		SchemaFeed:           sf,
		Knobs:                ca.knobs.FeedKnobs,
		ScopedTimers:         ca.sliMetrics.Timers,
		MonitoringCfg:        monitoringCfg,
		ConsumerID:           int64(ca.spec.JobID),
	}, nil
}

func makeKVFeedMonitoringCfg(
	ctx context.Context,
	sliMetrics *sliMetrics,
	opts changefeedbase.StatementOptions,
	settings *cluster.Settings,
) (kvfeed.MonitoringConfig, error) {
	laggingRangesThreshold, laggingRangesInterval, err := opts.GetLaggingRangesConfig(ctx, settings)
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

// getInitialHighWaterAndSpans returns the initial highwater and spans the
// aggregator is responsible for watching based on ca.spec. InitialHighWater is
// the minimal resolved timestamps of all InitialResolved timestamps.
func (ca *changeAggregator) getInitialHighWaterAndSpans() (hlc.Timestamp, []roachpb.Span) {
	if ca.spec.InitialHighWater != nil {
		spans := make([]roachpb.Span, 0, len(ca.spec.Watches))
		for _, watch := range ca.spec.Watches {
			spans = append(spans, watch.Span)
		}
		return *ca.spec.InitialHighWater, spans
	} else {
		// Keep initialHighWater as the minimum of all InitialResolved timestamps.
		// If there are any zero InitialResolved timestamps, initial scan is
		// ongoing. If there are no zero InitialResolved timestamps, initial scan
		// is not required.
		var initialHighWater hlc.Timestamp
		spans := make([]roachpb.Span, 0, len(ca.spec.Watches))
		for i, watch := range ca.spec.Watches {
			spans = append(spans, watch.Span)
			if i == 0 {
				initialHighWater = watch.InitialResolved
				continue
			}
			if watch.InitialResolved.Less(initialHighWater) {
				initialHighWater = watch.InitialResolved
			}
		}
		return initialHighWater, spans
	}
}

// setupSpans is called on start to extract the spans for this changefeed as a
// slice and creates a span frontier with the initial resolved timestamps. This
// SpanFrontier only tracks the spans being watched on this node. There is a
// different SpanFrontier elsewhere for the entire changefeed. This object is
// used to filter out some previously emitted rows, and by the cloudStorageSink
// to name its output files in lexicographically monotonic fashion.
func (ca *changeAggregator) setupSpansAndFrontier() (spans []roachpb.Span, err error) {
	initialHighWater, spans := ca.getInitialHighWaterAndSpans()
	ca.frontier, err = resolvedspan.NewAggregatorFrontier(ca.spec.Feed.StatementTime, initialHighWater, spans...)
	if err != nil {
		return nil, err
	}

	// Convert the legacy checkpoint to the new span-level checkpoint so that we
	// can ignore it from this point on.
	if !ca.spec.Checkpoint.IsEmpty() {
		if ca.spec.SpanLevelCheckpoint != nil {
			return nil, errors.New("both legacy and current checkpoint set on change aggregator spec")
		}

		// This conversion undoes an unnecessary conversion when the spec
		// was created in the first place.
		//lint:ignore SA1019 deprecated usage
		legacyCheckpoint := &jobspb.ChangefeedProgress_Checkpoint{
			Spans:     ca.spec.Checkpoint.Spans,
			Timestamp: ca.spec.Checkpoint.Timestamp,
		}
		statementTime := ca.spec.Feed.StatementTime
		ca.spec.SpanLevelCheckpoint = checkpoint.ConvertFromLegacyCheckpoint(legacyCheckpoint, statementTime, initialHighWater)
		ca.spec.Checkpoint.Reset()
	}

	// Checkpointed spans are spans that were above the highwater mark, and we
	// must preserve that information in the frontier for future checkpointing.
	if err := checkpoint.Restore(ca.frontier, ca.spec.SpanLevelCheckpoint); err != nil {
		return nil, err
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
	if ca.cancel != nil {
		// consumer close may be called even before Start is called. If that's
		// the case, cancel is not initialized. We still need to perform the
		// remainder of the cleanup though.
		ca.cancel()
	}
	// Wait for the poller to finish shutting down if it was started.
	if ca.kvFeedDoneCh != nil {
		<-ca.kvFeedDoneCh
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

	// The sliMetrics registry may hold on to some state for each aggregator
	// (ex. last known resolved timestamp). De-register the aggregator so this
	// data is deleted and not included in metrics.
	if ca.sliMetrics != nil {
		ca.sliMetrics.closeId(ca.sliMetricsID)
	}

	ca.memAcc.Close(ca.Ctx())
	ca.MemMonitor.Stop(ca.Ctx())
	ca.InternalClose()
}

var aggregatorHeartbeatFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.aggregator.heartbeat",
	"changefeed aggregator will emit a heartbeat message to the coordinator with this frequency; 0 disables. "+
		"The setting value should be <=1/2 of server.shutdown.jobs.timeout period",
	4*time.Second,
	settings.NonNegativeDuration,
)

var aggregatorFlushJitter = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"changefeed.aggregator.flush_jitter",
	"jitter aggregator flushes as a fraction of min_checkpoint_frequency. This "+
		"setting has no effect if min_checkpoint_frequency is set to 0.",
	0.1, /* 10% */
	settings.NonNegativeFloat,
	settings.WithPublic,
)

func nextFlushWithJitter(s timeutil.TimeSource, d time.Duration, j float64) (time.Time, error) {
	if j < 0 || d < 0 {
		return s.Now(), errors.AssertionFailedf("invalid jitter value: %f, duration: %s", j, d)
	}
	if j == 0 || d == 0 {
		return s.Now().Add(d), nil
	}
	nextFlush := d + time.Duration(rand.Int63n(int64(j*float64(d))))
	return s.Now().Add(nextFlush), nil
}

// Next is part of the RowSource interface.
func (ca *changeAggregator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	shouldEmitHeartBeat := func() bool {
		freq := aggregatorHeartbeatFrequency.Get(&ca.FlowCtx.Cfg.Settings.SV)
		return freq > 0 && timeutil.Since(ca.lastPush) > freq
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
			// NB: we do not invoke ca.cancel here -- just merely moving
			// to drain state so that the trailing metadata callback
			// has a chance to produce shutdown checkpoint.
			if log.V(2) {
				log.Infof(ca.Ctx(), "change aggregator moving to draining due to error while checking for node drain: %v", err)
			}
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
			if log.V(2) {
				log.Infof(ca.Ctx(), "change aggregator moving to draining due to error from tick: %v", err)
			}
			ca.MoveToDraining(err)
			break
		}
	}

	return nil, ca.DrainHelper()
}

func (ca *changeAggregator) computeTrailingMetadata(meta *execinfrapb.ChangefeedMeta) {
	if ca.eventConsumer == nil || ca.sink == nil {
		// Shutdown may be called even if Start didn't succeed.
		return
	}

	// Before emitting trailing metadata, we must flush any buffered events.
	// Note: we are not flushing KV feed -- blocking buffer may still have buffered
	// elements; but we are not interested in flushing potentially large number of events;
	// all we want to ensure is that any previously observed event (such as resolved timestamp)
	// has been fully processed.
	if err := ca.flushBufferedEvents(); err != nil {
		// This method may be invoked during shutdown when the context already canceled.
		// Regardless for the cause of this error, there is nothing we can do with it anyway.
		// All we want to ensure is that if any error occurs we still return correct checkpoint,
		// which in this case is nothing.
		return
	}

	// Build out the list of frontier spans.
	ca.frontier.Entries(func(r roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		meta.Checkpoint = append(meta.Checkpoint,
			execinfrapb.ChangefeedMeta_FrontierSpan{
				Span:      r,
				Timestamp: ts,
			})
		return span.ContinueMatch
	})
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
func (ca *changeAggregator) noteResolvedSpan(resolved jobspb.ResolvedSpan) (returnErr error) {
	if resolved.Timestamp.IsEmpty() {
		// @0.0 resolved timestamps could come in from rangefeed checkpoint.
		// When rangefeed starts running, it emits @0.0 resolved timestamp.
		// We don't care about those as far as checkpointing concerned.
		return nil
	}

	advanced, err := ca.frontier.ForwardResolvedSpan(ca.Ctx(), resolved)
	if err != nil {
		return err
	}
	sv := &ca.FlowCtx.Cfg.Settings.SV

	defer func(ctx context.Context) {
		maybeLogBehindSpan(ctx, "aggregator", ca.frontier, advanced, sv)
	}(ca.Ctx())

	// The resolved sliMetric data backs the aggregator_progress metric
	if advanced {
		ca.sliMetrics.setResolved(ca.sliMetricsID, ca.frontier.Frontier())
	}

	forceFlush := resolved.BoundaryType != jobspb.ResolvedSpan_NONE

	// NB: if we miss flush window, and the flush frequency is fairly high (minutes),
	// it might be a while before frontier advances again (particularly if
	// the number of ranges and closed timestamp settings are high).
	// TODO(yevgeniy): Consider doing something similar to how job checkpointing
	//  works in the frontier where if we missed the window to checkpoint, we will attempt
	//  the checkpoint at the next opportune moment.
	checkpointFrontier := advanced &&
		(forceFlush || timeutil.Now().After(ca.nextHighWaterFlush))

	if checkpointFrontier {
		defer func() {
			ca.nextHighWaterFlush, err = nextFlushWithJitter(
				timeutil.DefaultTimeSource{}, ca.flushFrequency, aggregatorFlushJitter.Get(sv))
			if err != nil {
				returnErr = errors.CombineErrors(returnErr, err)
			}
		}()
		return ca.flushFrontier()
	}

	// At a lower frequency, we checkpoint specific spans in the job progress
	// either in backfills or if the highwater mark is excessively lagging behind.
	checkpointSpans := ca.spec.JobID != 0 && /* enterprise changefeed */
		(ca.frontier.InBackfill(resolved) || ca.frontier.HasLaggingSpans(sv)) &&
		canCheckpointSpans(sv, ca.lastSpanFlush)

	if checkpointSpans {
		defer func() {
			ca.lastSpanFlush = timeutil.Now()
		}()
		return ca.flushFrontier()
	}
	return returnErr
}

// flushFrontier flushes sink and emits resolved spans to the change frontier.
func (ca *changeAggregator) flushFrontier() error {
	// Make sure to flush the sink before forwarding resolved spans,
	// otherwise, we could lose buffered messages and violate the
	// at-least-once guarantee. This is also true for checkpointing the
	// resolved spans in the job progress.
	if err := ca.flushBufferedEvents(); err != nil {
		return err
	}

	// Iterate frontier spans and build a list of spans to emit.
	var batch jobspb.ResolvedSpans
	ca.frontier.EntriesWithBoundaryType(func(s roachpb.Span, ts hlc.Timestamp, boundaryType jobspb.ResolvedSpan_BoundaryType) (done span.OpResult) {
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

func (ca *changeAggregator) isSinkless() bool {
	return ca.spec.JobID == 0
}

const (
	emitAllResolved = 0
	emitNoResolved  = -1
)

type changeFrontier struct {
	execinfra.ProcessorBase

	evalCtx *eval.Context
	spec    execinfrapb.ChangeFrontierSpec
	memAcc  mon.BoundAccount
	a       tree.DatumAlloc

	// input returns rows from one or more changeAggregator processors
	input execinfra.RowSource

	// frontier contains the current resolved timestamp high-water for the tracked
	// span set.
	frontier *resolvedspan.CoordinatorFrontier

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

	// latestResolvedKV indicates the last time the frontier received a resolved
	// span from an aggregator that has recently emitted kv events.
	latestResolvedKV time.Time

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

	usageWg       sync.WaitGroup
	usageWgCancel context.CancelFunc
}

const (
	runStatusUpdateFrequency time.Duration = time.Minute
	slowSpanMaxFrequency                   = 10 * time.Second
)

// slowLogEveryN rate-limits the logging of slow spans
var slowLogEveryN = log.Every(slowSpanMaxFrequency)

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
func (cs *cachedState) SetCheckpoint(checkpoint *jobspb.TimestampSpansMap) {
	// NB: It's not necessary to set the legacy checkpoint field because this
	// copy of the checkpoint is only used in-memory on a coordinator node that
	// knows about the new field.
	cs.progress.Details.(*jobspb.Progress_Changefeed).Changefeed.SpanLevelCheckpoint = checkpoint
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
	interval := changefeedbase.SpanCheckpointInterval.Get(sv)
	if interval == 0 {
		return false
	}
	return timeutil.Since(lastCheckpoint) > interval
}

func (j *jobState) canCheckpointSpans() bool {
	return canCheckpointSpans(&j.settings.SV, j.lastProgressUpdate)
}

// canCheckpointHighWatermark returns true if we should update job high water mark (i.e. progress).
// Normally, whenever the frontier changes, we update the high water mark.
// However, if the rate of frontier changes is too high, we want to slow down
// the frequency of job progress updates. We do this by enforcing a minimum
// interval between high water updates, which is the greater of the average time
// it takes to update the job progress and changefeed.resolved_timestamp.min_update_interval.
func (j *jobState) canCheckpointHighWatermark(frontierChanged bool) bool {
	if !(frontierChanged || j.progressUpdatesSkipped) {
		return false
	}

	minInterval := max(
		j.checkpointDuration,
		changefeedbase.ResolvedTimestampMinUpdateInterval.Get(&j.settings.SV),
	)
	if j.ts.Now().Before(j.lastProgressUpdate.Add(minInterval)) {
		// Updates are too rapid; skip some.
		j.progressUpdatesSkipped = true
		return false
	}

	return true
}

// checkpointCompleted must be called when job checkpoint completes.
// checkpointDuration indicates how long the checkpoint took.
func (j *jobState) checkpointCompleted(ctx context.Context, checkpointDuration time.Duration) {
	if j.progressUpdatesSkipped {
		// Log message if we skipped updates for some time.
		warnThreshold := 2 * changefeedbase.ResolvedTimestampMinUpdateInterval.Get(&j.settings.SV)
		if warnThreshold < 60*time.Second {
			warnThreshold = 60 * time.Second
		}
		behind := j.ts.Now().Sub(j.lastProgressUpdate)
		if behind > warnThreshold {
			log.Warningf(ctx, "high water mark update was delayed by %s; mean checkpoint duration %s",
				behind, j.checkpointDuration)
		}
	}

	j.metrics.CheckpointHistNanos.RecordValue(checkpointDuration.Nanoseconds())
	j.lastProgressUpdate = j.ts.Now()
	j.checkpointDuration = time.Duration(j.metrics.CheckpointHistNanos.CumulativeSnapshot().Mean())
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

	cf := &changeFrontier{
		// We might modify the ChangefeedState field in the eval.Context, so we
		// need to make a copy.
		evalCtx:       flowCtx.NewEvalCtx(),
		spec:          spec,
		memAcc:        memMonitor.MakeBoundAccount(),
		input:         input,
		usageWgCancel: func() {},
	}

	if cfKnobs, ok := flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		cf.knobs = *cfKnobs
	}

	if err := cf.InitWithEvalCtx(
		ctx,
		cf,
		post,
		input.OutputTypes(),
		flowCtx,
		cf.evalCtx,
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

	sliMetrics, err := flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(cf.spec.Feed.Opts[changefeedbase.OptMetricsScope])
	if err != nil {
		return nil, err
	}

	sourceProvider := newEnrichedSourceProvider(encodingOpts, enrichedSourceData{jobId: cf.spec.JobID.String()})
	if cf.encoder, err = getEncoder(
		ctx, encodingOpts, AllTargets(spec.Feed), spec.Feed.Select != "",
		makeExternalConnectionProvider(ctx, flowCtx.Cfg.DB), sliMetrics,
		sourceProvider,
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
	ctx = logtags.AddTag(ctx, changeFrontierLogTag, nil /* value */)
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)
	cf.input.Start(ctx)

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	// TODO(yevgeniy): Figure out how to inject replication stream metrics.
	cf.metrics = cf.FlowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)

	// Pass a nil oracle because this sink is only used to emit resolved timestamps
	// but the oracle is only used when emitting row updates.
	var nilOracle timestampLowerBoundOracle
	var err error
	scope := cf.spec.Feed.Opts[changefeedbase.OptMetricsScope]
	sli, err := cf.metrics.getSLIMetrics(scope)
	if err != nil {
		if log.V(2) {
			log.Infof(cf.Ctx(), "change frontier moving to draining due to error getting sli metrics: %v", err)
		}
		cf.MoveToDraining(err)
		return
	}
	cf.sliMetrics = sli

	cf.sink, err = getResolvedTimestampSink(ctx, cf.FlowCtx.Cfg, cf.spec.Feed, nilOracle,
		cf.spec.User(), cf.spec.JobID, sli)
	if err != nil {
		err = changefeedbase.MarkRetryableError(err)
		if log.V(2) {
			log.Infof(cf.Ctx(), "change frontier moving to draining due to error getting sink: %v", err)
		}
		cf.MoveToDraining(err)
		return
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	cf.sink = &errorWrapperSink{wrapped: cf.sink}

	cf.highWaterAtStart = cf.spec.Feed.StatementTime
	if cf.evalCtx.ChangefeedState == nil {
		if log.V(2) {
			log.Infof(cf.Ctx(), "change frontier moving to draining due to missing changefeed state")
		}
		cf.MoveToDraining(errors.AssertionFailedf("expected initialized local state"))
		return
	}

	cf.localState = cf.evalCtx.ChangefeedState.(*cachedState)
	cf.js = newJobState(nil, cf.FlowCtx.Cfg.Settings, cf.metrics, timeutil.DefaultTimeSource{})

	var initialHighwater hlc.Timestamp
	if cf.spec.JobID != 0 {
		job, err := cf.FlowCtx.Cfg.JobRegistry.LoadClaimedJob(ctx, cf.spec.JobID)
		if err != nil {
			if log.V(2) {
				log.Infof(cf.Ctx(), "change frontier moving to draining due to error loading claimed job: %v", err)
			}
			cf.MoveToDraining(err)
			return
		}
		cf.js.job = job
		if changefeedbase.SpanCheckpointInterval.Get(&cf.FlowCtx.Cfg.Settings.SV) == 0 {
			log.Warning(ctx,
				"span-level checkpointing disabled; set changefeed.span_checkpoint.interval to positive duration to re-enable")
		}

		// Recover highwater information from job progress.
		p := job.Progress()
		if ts := p.GetHighWater(); ts != nil {
			cf.highWaterAtStart.Forward(*ts)
			initialHighwater = *ts
		}

		// latestResolvedKV timestamp is set to the current time to make
		// sure that even if the target table does not have any
		// KV events coming in, the changefeed will remain
		// running for at least changefeedbase.IdleTimeout time
		// to ensure that sql pod in serverless deployment does
		// not get shutdown immediately after the changefeed starts.
		cf.latestResolvedKV = timeutil.Now()

		if p.StatusMessage != "" {
			// If we had running status set, that means we're probably retrying
			// due to a transient error.  In that case, keep the previous
			// running status around for a while before we override it.
			cf.js.lastRunStatusUpdate = timeutil.Now()
		}

		// Start the usage metric reporting goroutine.
		usageCtx, usageCancel := context.WithCancel(ctx)
		cf.usageWgCancel = usageCancel
		cf.usageWg.Add(1)
		go func() {
			defer cf.usageWg.Done()
			cf.runUsageMetricReporting(usageCtx)
		}()
	}

	// Set up the resolved span frontier.
	cf.frontier, err = resolvedspan.NewCoordinatorFrontier(cf.spec.Feed.StatementTime, initialHighwater, cf.spec.TrackedSpans...)
	if err != nil {
		log.Infof(cf.Ctx(), "change frontier moving to draining due to error setting up frontier: %v", err)
		cf.MoveToDraining(err)
		return
	}

	if err := checkpoint.Restore(cf.frontier, cf.spec.SpanLevelCheckpoint); err != nil {
		if log.V(2) {
			log.Infof(cf.Ctx(), "change frontier encountered error on checkpoint restore: %v", err)
		}
	}

	if cf.knobs.AfterCoordinatorFrontierRestore != nil {
		cf.knobs.AfterCoordinatorFrontierRestore(cf.frontier)
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

func (cf *changeFrontier) runUsageMetricReporting(ctx context.Context) {
	if cf.spec.JobID == 0 { // don't report for core (non-enterprise) changefeeds
		return
	}

	// If cloud billing accounting is not enabled via an env var, don't do this work.
	if !EnableCloudBillingAccounting {
		return
	}

	// Deregister the usage metric when the goroutine exits so we don't over-report.
	defer cf.metrics.UsageMetrics.DeregisterJobMetrics(cf.spec.JobID)

	var ts timeutil.TimeSource = timeutil.DefaultTimeSource{}
	if cf.knobs.TimeSource != nil {
		ts = cf.knobs.TimeSource
	}

	t := ts.NewTimer()
	defer t.Stop()
	var lastDuration time.Duration
	for ctx.Err() == nil {
		reportingInterval := jitter(changefeedbase.UsageMetricsReportingInterval.Get(&cf.FlowCtx.Cfg.Settings.SV))
		var start time.Time

		t.Reset(reportingInterval - lastDuration)
		select {
		case start = <-t.Ch():
			t.MarkRead()
		case <-ctx.Done():
			return
		}

		execCfg := cf.FlowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)
		if cf.knobs.OverrideExecCfg != nil {
			execCfg = cf.knobs.OverrideExecCfg(execCfg)
		}

		// Set a timeout of some % of the interval so we still have a duty cycle in degenerate scenarios.
		var bytes int64
		percent := changefeedbase.UsageMetricsReportingTimeoutPercent.Get(&cf.FlowCtx.Cfg.Settings.SV)
		err := timeutil.RunWithTimeout(ctx, "fetch usage bytes", time.Duration(float64(reportingInterval)*(float64(percent)/100.)), func(reqCtx context.Context) (err error) {
			bytes, err = FetchChangefeedUsageBytes(reqCtx, execCfg, cf.js.job.Payload())
			return err
		})
		if err != nil {
			// Don't increment the error count if it's due to us being shut down, or due to a backing table being dropped (since that will result in us shutting down also).
			if shouldCountUsageError(err) {
				log.Warningf(ctx, "failed to fetch usage bytes: %v", err)
				cf.metrics.UsageMetrics.RecordError()
			}
			continue
		}
		lastDuration = timeutil.Since(start)
		cf.metrics.UsageMetrics.UpdateTableBytes(cf.spec.JobID, bytes, lastDuration)
	}
}

func (cf *changeFrontier) close() {
	// Shut down the usage metric reporting goroutine first, since otherwise
	// we can use a span after it's finished.
	cf.usageWgCancel()
	cf.usageWg.Wait()

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

func (cf *changeFrontier) closeMetrics() {
	func() {
		cf.metrics.mu.Lock()
		defer cf.metrics.mu.Unlock()
		if cf.metricsID > 0 {
			cf.sliMetrics.RunningCount.Dec(1)
		}
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

		if ok, boundaryType, boundaryTime := cf.frontier.AtBoundary(); ok &&
			(boundaryType == jobspb.ResolvedSpan_EXIT || boundaryType == jobspb.ResolvedSpan_RESTART) {
			var err error
			endTime := cf.spec.Feed.EndTime
			if endTime.IsEmpty() || endTime.Less(boundaryTime) {
				err = pgerror.Newf(pgcode.SchemaChangeOccurred,
					"schema change occurred at %v", boundaryTime.Next().AsOfSystemTime())

				// Detect whether this boundary should be used to kill or restart the
				// changefeed.
				if boundaryType == jobspb.ResolvedSpan_EXIT {
					err = changefeedbase.WithTerminalError(errors.Wrapf(err,
						"shut down due to schema change and %s=%q",
						changefeedbase.OptSchemaChangePolicy,
						changefeedbase.OptSchemaChangePolicyStop))
				} else {
					err = changefeedbase.MarkRetryableError(err)
				}
			}

			if log.V(2) {
				log.Infof(cf.Ctx(),
					"change frontier moving to draining after reaching resolved span boundary (%s): %v",
					boundaryType, err)
			}
			cf.MoveToDraining(err)
			break
		}

		row, meta := cf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				if log.V(2) {
					log.Infof(cf.Ctx(), "change frontier moving to draining after getting error from aggregator: %v", meta.Err)
				}
				cf.MoveToDraining(nil /* err */)
			}
			if meta.Changefeed != nil && meta.Changefeed.DrainInfo != nil {
				// Seeing changefeed drain info metadata from the aggregator means
				// that the aggregator exited due to node shutdown.  Transition to
				// draining so that the remaining aggregators will shut down and
				// transmit their up-to-date frontier.
				if log.V(2) {
					log.Infof(cf.Ctx(), "change frontier moving to draining due to aggregator shutdown: %s", meta.Changefeed)
				}
				cf.MoveToDraining(changefeedbase.ErrNodeDraining)
			}
			return nil, meta
		}
		if row == nil {
			if log.V(2) {
				log.Infof(cf.Ctx(), "change frontier moving to draining after getting nil row from aggregator")
			}
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
			if log.V(2) {
				log.Infof(cf.Ctx(), "change frontier moving to draining after error while processing aggregator progress: %v", err)
			}
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
	if log.V(2) {
		log.Infof(cf.Ctx(), "progress update from aggregator: %#v", resolvedSpans)
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
			logcrash.ReportOrPanic(cf.Ctx(), &cf.FlowCtx.Cfg.Settings.SV,
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
	frontierChanged, err := cf.frontier.ForwardResolvedSpan(cf.Ctx(), resolved)
	if err != nil {
		return err
	}

	maybeLogBehindSpan(cf.Ctx(), "coordinator", cf.frontier, frontierChanged, &cf.FlowCtx.Cfg.Settings.SV)

	checkpointed, err := cf.maybeCheckpointJob(resolved, frontierChanged)
	if err != nil {
		return err
	}

	// Emit resolved timestamp only if we have checkpointed the job.
	// Usually, this happens every time frontier changes, but we can skip some updates
	// if we update frontier too rapidly.
	if checkpointed {
		// Keeping this after the checkpointJobProgress call will avoid
		// some duplicates if a restart happens.
		newResolved := cf.frontier.Frontier()

		// The feed's checkpoint is tracked in a map which is used to inform the
		// checkpoint_progress metric which will return the lowest timestamp across
		// all feeds in the scope.
		cf.sliMetrics.setCheckpoint(cf.sliMetricsID, newResolved)

		return cf.maybeEmitResolved(newResolved)
	}

	return nil
}

func (cf *changeFrontier) maybeMarkJobIdle(recentKVCount uint64) {
	if cf.spec.JobID == 0 {
		return
	}

	if recentKVCount > 0 {
		cf.latestResolvedKV = timeutil.Now()
	}

	idleTimeout := changefeedbase.IdleTimeout.Get(&cf.FlowCtx.Cfg.Settings.SV)
	if idleTimeout == 0 {
		return
	}

	isIdle := timeutil.Since(cf.latestResolvedKV) > idleTimeout
	cf.js.job.MarkIdle(isIdle)
}

func (cf *changeFrontier) maybeCheckpointJob(
	resolvedSpan jobspb.ResolvedSpan, frontierChanged bool,
) (bool, error) {
	// When in a Backfill, the frontier remains unchanged at the backfill boundary
	// as we receive spans from the scan request at the Backfill Timestamp
	inBackfill := !frontierChanged && cf.frontier.InBackfill(resolvedSpan)

	// If we're not in a backfill, highwater progress and an empty checkpoint will
	// be saved. This is throttled however we always persist progress to a schema
	// boundary.
	atBoundary, _, _ := cf.frontier.AtBoundary()
	updateHighWater :=
		!inBackfill && (atBoundary || cf.js.canCheckpointHighWatermark(frontierChanged))

	// During backfills or when some problematic spans stop advancing, the
	// highwater mark remains fixed while other spans may significantly outpace
	// it, therefore to avoid losing that progress on changefeed resumption we
	// also store as many of those leading spans as we can in the job progress
	updateCheckpoint := (inBackfill || cf.frontier.HasLaggingSpans(&cf.js.settings.SV)) && cf.js.canCheckpointSpans()

	// If the highwater has moved an empty checkpoint will be saved
	var checkpoint *jobspb.TimestampSpansMap
	if updateCheckpoint {
		maxBytes := changefeedbase.SpanCheckpointMaxBytes.Get(&cf.FlowCtx.Cfg.Settings.SV)
		checkpoint = cf.frontier.MakeCheckpoint(maxBytes, cf.sliMetrics.CheckpointMetrics)
	}

	if updateCheckpoint || updateHighWater {
		if cf.knobs.ShouldCheckpointToJobRecord != nil && !cf.knobs.ShouldCheckpointToJobRecord(cf.frontier.Frontier()) {
			return false, nil
		}
		checkpointStart := timeutil.Now()
		updated, err := cf.checkpointJobProgress(cf.frontier.Frontier(), checkpoint, cf.evalCtx.Settings.Version)
		if err != nil {
			return false, err
		}
		cf.js.checkpointCompleted(cf.Ctx(), timeutil.Since(checkpointStart))
		return updated, nil
	}

	return false, nil
}

const changefeedJobProgressTxnName = "changefeed job progress"

func (cf *changeFrontier) checkpointJobProgress(
	frontier hlc.Timestamp, spanLevelCheckpoint *jobspb.TimestampSpansMap, cv clusterversion.Handle,
) (bool, error) {
	defer cf.sliMetrics.Timers.CheckpointJobProgress.Start()()

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
		var ptsUpdated bool
		var checkpointStr string
		if err := cf.js.job.DebugNameNoTxn(changefeedJobProgressTxnName).Update(cf.Ctx(), func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			var err error
			if err = md.CheckRunningOrReverting(); err != nil {
				return err
			}

			// Advance resolved timestamp.
			progress := md.Progress
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &frontier,
			}

			changefeedProgress := progress.Details.(*jobspb.Progress_Changefeed).Changefeed
			if cv.IsActive(cf.Ctx(), clusterversion.V25_2) {
				changefeedProgress.SpanLevelCheckpoint = spanLevelCheckpoint
				checkpointStr = spanLevelCheckpoint.String()
			} else {
				legacyCheckpoint := checkpoint.ConvertToLegacyCheckpoint(spanLevelCheckpoint)
				changefeedProgress.Checkpoint = legacyCheckpoint
				checkpointStr = legacyCheckpoint.String()
			}

			if ptsUpdated, err = cf.manageProtectedTimestamps(cf.Ctx(), txn, changefeedProgress); err != nil {
				log.Warningf(cf.Ctx(), "error managing protected timestamp record: %v", err)
				return err
			}

			if updateRunStatus {
				progress.StatusMessage = fmt.Sprintf("running: resolved=%s", frontier)
			}

			ju.UpdateProgress(progress)

			return nil
		}); err != nil {
			return false, err
		}
		if ptsUpdated {
			cf.lastProtectedTimestampUpdate = timeutil.Now()
		}
		if log.V(2) {
			log.Infof(cf.Ctx(), "change frontier persisted highwater=%s and checkpoint=%s",
				frontier, checkpointStr)
		}
	}

	cf.localState.SetHighwater(frontier)
	cf.localState.SetCheckpoint(spanLevelCheckpoint)

	return true, nil
}

// manageProtectedTimestamps periodically advances the protected timestamp for
// the changefeed's targets to the current highwater mark.  The record is
// cleared during changefeedResumer.OnFailOrCancel
//
// NOTE: this method may be retried by `txn`, so don't mutate any state that
// would interfere with that.
func (cf *changeFrontier) manageProtectedTimestamps(
	ctx context.Context, txn isql.Txn, progress *jobspb.ChangefeedProgress,
) (updated bool, err error) {
	ptsUpdateInterval := changefeedbase.ProtectTimestampInterval.Get(&cf.FlowCtx.Cfg.Settings.SV)
	ptsUpdateLag := changefeedbase.ProtectTimestampLag.Get(&cf.FlowCtx.Cfg.Settings.SV)
	if timeutil.Since(cf.lastProtectedTimestampUpdate) < ptsUpdateInterval {
		return false, nil
	}

	pts := cf.FlowCtx.Cfg.ProtectedTimestampProvider.WithTxn(txn)

	// Create / advance the protected timestamp record to the highwater mark
	highWater := cf.frontier.Frontier()
	if highWater.Less(cf.highWaterAtStart) {
		highWater = cf.highWaterAtStart
	}

	if progress.ProtectedTimestampRecord == uuid.Nil {
		ptr := createProtectedTimestampRecord(
			ctx, cf.FlowCtx.Codec(), cf.spec.JobID, AllTargets(cf.spec.Feed), highWater,
		)
		progress.ProtectedTimestampRecord = ptr.ID.GetUUID()
		return true, pts.Protect(ctx, ptr)
	}

	rec, err := pts.GetRecord(ctx, progress.ProtectedTimestampRecord)
	if err != nil {
		return false, err
	}

	// If this changefeed was created in 22.1 or earlier, it may be using a
	// deprecated pts record in which the target field is nil. If so, we
	// "migrate" it to use the new style of pts records and delete the old one.
	if rec.Target == nil {
		if preserveDeprecatedPts := cf.knobs.PreserveDeprecatedPts != nil && cf.knobs.PreserveDeprecatedPts(); preserveDeprecatedPts {
			return false, nil
		}
		if err := cf.remakePTSRecord(ctx, pts, progress, highWater); err != nil {
			return false, err
		}
		return true, nil
	}

	// If we've identified more tables that need to be protected since this
	// changefeed was created, it will be missing here. If so, we "migrate" it
	// to include all the appropriate targets.
	if targets := AllTargets(cf.spec.Feed); !makeTargetToProtect(targets).Equal(rec.Target) {
		if preservePTSTargets := cf.knobs.PreservePTSTargets != nil && cf.knobs.PreservePTSTargets(); preservePTSTargets {
			return false, nil
		}
		if err := cf.remakePTSRecord(ctx, pts, progress, highWater); err != nil {
			return false, err
		}
		return true, nil
	}

	// Only update the PTS timestamp if it is lagging behind the high
	// watermark. This is to prevent a rush of updates to the PTS if the
	// changefeed restarts, which can cause contention and second order effects
	// on system tables.
	if !rec.Timestamp.AddDuration(ptsUpdateLag).Less(highWater) {
		return false, nil
	}

	log.VEventf(ctx, 2, "updating protected timestamp %v at %v", progress.ProtectedTimestampRecord, highWater)
	return true, pts.UpdateTimestamp(ctx, progress.ProtectedTimestampRecord, highWater)
}

func (cf *changeFrontier) remakePTSRecord(
	ctx context.Context,
	pts protectedts.Storage,
	progress *jobspb.ChangefeedProgress,
	resolved hlc.Timestamp,
) error {
	prevRecordId := progress.ProtectedTimestampRecord
	ptr := createProtectedTimestampRecord(
		ctx, cf.FlowCtx.Codec(), cf.spec.JobID, AllTargets(cf.spec.Feed), resolved,
	)
	if err := pts.Protect(ctx, ptr); err != nil {
		return err
	}
	progress.ProtectedTimestampRecord = ptr.ID.GetUUID()
	if err := pts.Release(ctx, prevRecordId); err != nil {
		return err
	}

	log.Eventf(ctx, "created new pts record %v to replace old pts record %v at %v",
		progress.ProtectedTimestampRecord, prevRecordId, resolved)

	return nil
}

func (cf *changeFrontier) maybeEmitResolved(newResolved hlc.Timestamp) error {
	if cf.freqEmitResolved == emitNoResolved || newResolved.IsEmpty() {
		return nil
	}
	sinceEmitted := newResolved.GoTime().Sub(cf.lastEmitResolved)
	atBoundary, _, _ := cf.frontier.AtBoundary()
	shouldEmit := sinceEmitted >= cf.freqEmitResolved || atBoundary
	if !shouldEmit {
		return nil
	}
	if err := emitResolvedTimestamp(cf.Ctx(), cf.encoder, cf.sink, newResolved); err != nil {
		return err
	}
	cf.lastEmitResolved = newResolved.GoTime()
	return nil
}

func frontierIsBehind(frontier hlc.Timestamp, sv *settings.Values) bool {
	if frontier.IsEmpty() {
		// During backfills we consider ourselves "behind" for the purposes of
		// maintaining protected timestamps
		return true
	}

	return timeutil.Since(frontier.GoTime()) > slownessThreshold(sv)
}

// Potentially log the most behind span in the frontier for debugging if the
// frontier is behind
func maybeLogBehindSpan(
	ctx context.Context,
	description string,
	frontier span.Frontier,
	frontierChanged bool,
	sv *settings.Values,
) {
	// Do not log when we're "behind" due to a backfill
	frontierTS := frontier.Frontier()
	if frontierTS.IsEmpty() {
		return
	}

	if !frontierIsBehind(frontierTS, sv) {
		return
	}

	now := timeutil.Now()
	resolvedBehind := now.Sub(frontierTS.GoTime())

	if frontierChanged && slowLogEveryN.ShouldProcess(now) {
		log.Infof(ctx, "%s new resolved timestamp %s is behind by %s",
			description, frontierTS, resolvedBehind)
	}

	if slowLogEveryN.ShouldProcess(now) {
		s := frontier.PeekFrontierSpan()
		log.Infof(ctx, "%s span %s is behind by %s", description, s, resolvedBehind)
	}
}

func slownessThreshold(sv *settings.Values) time.Duration {
	clusterThreshold := changefeedbase.SlowSpanLogThreshold.Get(sv)
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
	pollInterval := changefeedbase.TableDescriptorPollInterval.Get(sv)
	closedtsInterval := closedts.TargetDuration.Get(sv)
	return time.Second + 10*(pollInterval+closedtsInterval)
}

// ConsumerClosed is part of the RowSource interface.
func (cf *changeFrontier) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	cf.close()
}

// jitter returns a new duration with +-10% jitter applied to the given duration.
func jitter(d time.Duration) time.Duration {
	if d == 0 {
		return 0
	}
	start, end := int64(d-d/10), int64(d+d/10)
	return time.Duration(start + rand.Int63n(end-start))
}

func shouldCountUsageError(err error) bool {
	return !errors.Is(err, context.Canceled) &&
		!errors.Is(err, cancelchecker.QueryCanceledError) &&
		pgerror.GetPGCode(err) != pgcode.UndefinedTable &&
		status.Code(err) != codes.Canceled
}
