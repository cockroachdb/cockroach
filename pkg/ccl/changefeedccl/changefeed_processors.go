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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// eventProducer produces the next event from the kv feed.
	eventProducer kvEventProducer
	// eventConsumer consumes the event.
	eventConsumer kvEventConsumer

	// flush related fields: clock to obtain current hlc time,
	// lastFlush and flushFrequency keep track of the flush frequency.
	clock *hlc.Clock

	lastFlush      hlc.Timestamp
	flushFrequency time.Duration

	// frontier keeps track of resolved timestamps for spans along with schema change
	// boundary information.
	frontier *schemaChangeFrontier

	// number of frontier updates since the last flush.
	numFrontierUpdates int

	metrics *Metrics
	knobs   TestingKnobs
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
	clock := flowCtx.Cfg.DB.Clock()
	ca := &changeAggregator{
		flowCtx:   flowCtx,
		spec:      spec,
		memAcc:    memMonitor.MakeBoundAccount(),
		clock:     clock,
		lastFlush: clock.Now(),
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
	if ca.encoder, err = getEncoder(ctx, ca.spec.Feed.Opts, ca.spec.Feed.Targets); err != nil {
		return nil, err
	}

	// If the resolved timestamp frequency is specified, use it as a rough
	// approximation of how latency-sensitive the changefeed user is. If it's
	// not, fall back to a default of 5s
	//
	// With timeBetweenFlushes and changefeedPollInterval both set to 1s, TPCC
	// was seeing about 100x more time spent emitting than flushing when tested
	// with low-latency sinks like Kafka. However when using cloud-storage
	// sinks, flushes can take much longer and trying to flush too often can
	// thus end up spending too much time flushing and not enough in emitting to
	// keep up with the feed. If a user does not specify a 'resolved' time, we
	// instead default to 5s, which is hopefully long enough to account for most
	// possible sink latencies we could see without falling behind.
	//
	// NB: As long as we periodically get new span-level resolved timestamps
	// from the poller (which should always happen, even if the watched data is
	// not changing), then this is sufficient and we don't have to do anything
	// fancy with timers.
	if r, ok := ca.spec.Feed.Opts[changefeedbase.OptResolvedTimestamps]; ok && r != `` {
		ca.flushFrequency, err = time.ParseDuration(r)
		if err != nil {
			return nil, err
		}
	} else {
		ca.flushFrequency = changefeedbase.DefaultFlushFrequency
	}
	return ca, nil
}

// MustBeStreaming implements the execinfra.Processor interface.
func (ca *changeAggregator) MustBeStreaming() bool {
	return true
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) {
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	// Derive a separate context so that we can shutdown the poller. Note that
	// we need to update both ctx (used throughout this function) and
	// ProcessorBase.Ctx (used in all other methods) to the new context.
	ctx, ca.cancel = context.WithCancel(ctx)
	ca.Ctx = ctx

	spans, err := ca.setupSpansAndFrontier()
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

	ca.sink, err = getSink(
		ctx, ca.flowCtx.Cfg, ca.spec.Feed, timestampOracle,
		ca.spec.User(), kvFeedMemMon.MakeBoundAccount(), ca.spec.JobID)

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

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	ca.metrics = ca.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	ca.sink = makeMetricsSink(ca.metrics, ca.sink)
	ca.sink = &errorWrapperSink{wrapped: ca.sink}

	buf := kvevent.MakeChanBuffer()
	kvfeedCfg := makeKVFeedCfg(ctx, ca.flowCtx.Cfg, ca.kvFeedMemMon,
		ca.spec, spans, buf, ca.metrics, ca.knobs.FeedKnobs)
	cfg := ca.flowCtx.Cfg

	ca.eventProducer = &bufEventProducer{buf}

	if ca.spec.Feed.Opts[changefeedbase.OptFormat] == string(changefeedbase.OptFormatNative) {
		ca.eventConsumer = newNativeKVConsumer(ca.sink)
	} else {
		ca.eventConsumer = newKVEventToRowConsumer(
			ctx, cfg, ca.frontier.SpanFrontier(), kvfeedCfg.InitialHighWater,
			ca.sink, ca.encoder, ca.spec.Feed, ca.knobs)
	}

	ca.startKVFeed(ctx, kvfeedCfg)
}

func (ca *changeAggregator) startKVFeed(ctx context.Context, kvfeedCfg kvfeed.Config) {
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
	}
}

func newSchemaFeed(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	spec execinfrapb.ChangeAggregatorSpec,
	metrics *Metrics,
) schemafeed.SchemaFeed {
	schemaChangePolicy := changefeedbase.SchemaChangePolicy(
		spec.Feed.Opts[changefeedbase.OptSchemaChangePolicy])
	if schemaChangePolicy == changefeedbase.OptSchemaChangePolicyIgnore {
		return schemafeed.DoNothingSchemaFeed
	}
	schemaChangeEvents := changefeedbase.SchemaChangeEventClass(
		spec.Feed.Opts[changefeedbase.OptSchemaChangeEvents])
	initialHighWater, _ := getKVFeedInitialParameters(spec)
	return schemafeed.New(ctx, cfg, schemaChangeEvents,
		spec.Feed.Targets, initialHighWater, &metrics.SchemaFeedMetrics)
}

func makeKVFeedCfg(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	mm *mon.BytesMonitor,
	spec execinfrapb.ChangeAggregatorSpec,
	spans []roachpb.Span,
	buf kvevent.Buffer,
	metrics *Metrics,
	knobs kvfeed.TestingKnobs,
) kvfeed.Config {
	schemaChangeEvents := changefeedbase.SchemaChangeEventClass(
		spec.Feed.Opts[changefeedbase.OptSchemaChangeEvents])
	schemaChangePolicy := changefeedbase.SchemaChangePolicy(
		spec.Feed.Opts[changefeedbase.OptSchemaChangePolicy])
	_, withDiff := spec.Feed.Opts[changefeedbase.OptDiff]
	initialHighWater, needsInitialScan := getKVFeedInitialParameters(spec)

	return kvfeed.Config{
		Sink:               buf,
		Settings:           cfg.Settings,
		DB:                 cfg.DB,
		Codec:              cfg.Codec,
		Clock:              cfg.DB.Clock(),
		Gossip:             cfg.Gossip,
		Spans:              spans,
		BackfillCheckpoint: spec.Checkpoint.Spans,
		Targets:            spec.Feed.Targets,
		Metrics:            &metrics.KVFeedMetrics,
		MM:                 mm,
		InitialHighWater:   initialHighWater,
		WithDiff:           withDiff,
		NeedsInitialScan:   needsInitialScan,
		SchemaChangeEvents: schemaChangeEvents,
		SchemaChangePolicy: schemaChangePolicy,
		SchemaFeed:         newSchemaFeed(ctx, cfg, spec, metrics),
		Knobs:              knobs,
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
func (ca *changeAggregator) setupSpansAndFrontier() (spans []roachpb.Span, err error) {
	spans = make([]roachpb.Span, 0, len(ca.spec.Watches))
	for _, watch := range ca.spec.Watches {
		spans = append(spans, watch.Span)
	}

	ca.frontier, err = makeSchemaChangeFrontier(ca.clock, spans...)
	if err != nil {
		return nil, err
	}
	for _, watch := range ca.spec.Watches {
		if _, err := ca.frontier.Forward(watch.Span, watch.InitialResolved); err != nil {
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
	if ca.InternalClose() {
		// Shut down the poller if it wasn't already. Note that it will cancel
		// the context used by all components, but ca.Ctx has been updated by
		// InternalClose() to the "original" context (the one passed into
		// StartInternal()).
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
	}
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
			select {
			// If the poller errored first, that's the
			// interesting one, so overwrite `err`.
			case err = <-ca.errCh:
			default:
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
	// TODO(yevgeniy): Getting an event from producer decreases the amount of
	//  memory tracked by kvFeedMonitor.  We should "transfer" that memory
	//  to the consumer below, and keep track of changes to the memory usage when
	//  we convert feed event to datums; and then when we encode those datums.
	event, err := ca.eventProducer.GetEvent(ca.Ctx)
	if err != nil {
		return err
	}

	if event.BufferGetTimestamp() == (time.Time{}) {
		// We could gracefully handle this instead of panic'ing, but
		// we'd really like to be able to reason about this data, so
		// instead we're defensive. If this is ever seen in prod without
		// breaking a unit test, then we have a pretty severe test
		// coverage issue.
		panic(`unreachable: bufferGetTimestamp is set by all codepaths`)
	}
	processingNanos := timeutil.Since(event.BufferGetTimestamp()).Nanoseconds()
	ca.metrics.ProcessingNanos.Inc(processingNanos)

	forceFlush := false
	switch event.Type() {
	case kvevent.TypeKV:
		if err := ca.eventConsumer.ConsumeEvent(ca.Ctx, event); err != nil {
			return err
		}
	case kvevent.TypeResolved:
		resolved := event.Resolved()
		if ca.knobs.ShouldSkipResolved == nil || !ca.knobs.ShouldSkipResolved(resolved) {
			if _, err := ca.frontier.ForwardResolvedSpan(*resolved); err != nil {
				return err
			}
			ca.numFrontierUpdates++
			forceFlush = resolved.BoundaryType != jobspb.ResolvedSpan_NONE
		}
	}

	return ca.maybeFlush(forceFlush)
}

// maybeFlush flushes sink and emits resolved timestamp if needed.
func (ca *changeAggregator) maybeFlush(force bool) error {
	if ca.numFrontierUpdates == 0 || (timeutil.Since(ca.lastFlush.GoTime()) < ca.flushFrequency && !force) {
		return nil
	}

	// Make sure to flush the sink before forwarding resolved spans,
	// otherwise, we could lose buffered messages and violate the
	// at-least-once guarantee. This is also true for checkpointing the
	// resolved spans in the job progress.
	if err := ca.sink.Flush(ca.Ctx); err != nil {
		return err
	}

	// Iterate spans that have updated timestamp ahead of the last flush timestamp and
	// emit resolved span records.
	var err error
	ca.frontier.UpdatedEntries(ca.lastFlush, func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		err = ca.emitResolved(s, ts, ca.frontier.boundaryTypeAt(ts))
		if err != nil {
			return span.StopMatch
		}
		return span.ContinueMatch
	})

	if err != nil {
		return err
	}

	ca.lastFlush = ca.clock.Now()
	ca.numFrontierUpdates = 0
	return nil
}

func (ca *changeAggregator) emitResolved(
	s roachpb.Span, ts hlc.Timestamp, boundary jobspb.ResolvedSpan_BoundaryType,
) error {
	var resolvedSpan jobspb.ResolvedSpan
	resolvedSpan.Span = s
	resolvedSpan.Timestamp = ts
	resolvedSpan.BoundaryType = boundary

	resolvedBytes, err := protoutil.Marshal(&resolvedSpan)
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
	return nil
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.close()
}

type kvEventProducer interface {
	// GetEvent returns the next kv event.
	GetEvent(ctx context.Context) (kvevent.Event, error)
}

type bufEventProducer struct {
	kvevent.Reader
}

var _ kvEventProducer = &bufEventProducer{}

// GetEvent implements kvEventProducer interface
func (p *bufEventProducer) GetEvent(ctx context.Context) (kvevent.Event, error) {
	return p.Get(ctx)
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
	rfCache := newRowFetcherCache(ctx, cfg.Codec, cfg.Settings,
		cfg.LeaseManager.(*lease.Manager), cfg.HydratedTables, cfg.DB)

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
			"or equal to the local frontier %s.", cloudStorageFormatTime(r.updated),
			cloudStorageFormatTime(c.frontier.Frontier()))
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
		ctx, tableDescriptorTopic{r.tableDesc}, keyCopy, valueCopy, r.updated,
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

	desc, err := c.rfCache.TableDescForKey(ctx, event.KV().Key, schemaTimestamp)
	if err != nil {
		return r, err
	}

	if _, ok := c.details.Targets[desc.GetID()]; !ok {
		// This kv is for an interleaved table that we're not watching.
		if log.V(3) {
			log.Infof(ctx, `skipping key from unwatched table %s: %s`, desc.GetName(), event.KV().Key)
		}
		return r, nil
	}

	rf, err := c.rfCache.RowFetcherForTableDesc(desc)
	if err != nil {
		return r, err
	}

	// Get new value.
	// TODO(dan): Handle tables with multiple column families.
	// Reuse kvs to save allocations.
	c.kvFetcher.KVs = c.kvFetcher.KVs[:0]
	c.kvFetcher.KVs = append(c.kvFetcher.KVs, event.KV())
	if err := rf.StartScanFrom(ctx, &c.kvFetcher); err != nil {
		return r, err
	}

	r.datums, r.tableDesc, _, err = rf.NextRow(ctx)
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
	var nextRow encodeRow
	nextRow.datums, nextRow.tableDesc, _, err = rf.NextRow(ctx)
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
		if prevSchemaTimestamp != schemaTimestamp {
			// If the previous value is being interpreted under a different
			// version of the schema, fetch the correct table descriptor and
			// create a new row.Fetcher with it.
			prevDesc, err := c.rfCache.TableDescForKey(ctx, event.KV().Key, prevSchemaTimestamp)
			if err != nil {
				return r, err
			}

			prevRF, err = c.rfCache.RowFetcherForTableDesc(prevDesc)
			if err != nil {
				return r, err
			}
		}

		prevKV := roachpb.KeyValue{Key: event.KV().Key, Value: event.PrevValue()}
		// TODO(dan): Handle tables with multiple column families.
		// Reuse kvs to save allocations.
		c.kvFetcher.KVs = c.kvFetcher.KVs[:0]
		c.kvFetcher.KVs = append(c.kvFetcher.KVs, prevKV)
		if err := prevRF.StartScanFrom(ctx, &c.kvFetcher); err != nil {
			return r, err
		}
		r.prevDatums, r.prevTableDesc, _, err = prevRF.NextRow(ctx)
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
		var nextRow encodeRow
		nextRow.prevDatums, nextRow.prevTableDesc, _, err = prevRF.NextRow(ctx)
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

	return c.sink.EmitRow(ctx, &noTopic{}, keyBytes, valBytes, val.Timestamp)
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
	a       rowenc.DatumAlloc

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
	metrics *Metrics
	// metricsID is used as the unique id of this changefeed in the
	// metrics.MaxBehindNanos map.
	metricsID int
}

const (
	runStatusUpdateFrequency time.Duration = time.Minute
	slowSpanMaxFrequency                   = 10 * time.Second
)

type jobState struct {
	job                    *jobs.Job
	lastRunStatusUpdate    time.Time
	lastFrontierCheckpoint time.Time
	settings               *cluster.Settings
}

func (j *jobState) canCheckpointFrontier() bool {
	freq := changefeedbase.FrontierCheckpointFrequency.Get(&j.settings.SV)
	if freq == 0 {
		return false
	}
	return timeutil.Since(j.lastFrontierCheckpoint) > freq
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
	sf, err := makeSchemaChangeFrontier(flowCtx.Cfg.DB.Clock(), spec.TrackedSpans...)
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

	if cf.encoder, err = getEncoder(ctx, spec.Feed.Opts, spec.Feed.Targets); err != nil {
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
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)
	cf.input.Start(ctx)

	// Pass a nil oracle because this sink is only used to emit resolved timestamps
	// but the oracle is only used when emitting row updates.
	var nilOracle timestampLowerBoundOracle
	var err error
	// TODO(yevgeniy): Evaluate if we should introduce changefeed specific monitor.
	mm := cf.flowCtx.Cfg.BackfillerMonitor
	cf.sink, err = getSink(ctx, cf.flowCtx.Cfg, cf.spec.Feed, nilOracle,
		cf.spec.User(), mm.MakeBoundAccount(), cf.spec.JobID)

	if err != nil {
		err = changefeedbase.MarkRetryableError(err)
		cf.MoveToDraining(err)
		return
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	// TODO(yevgeniy): Figure out how to inject replication stream metrics.
	cf.metrics = cf.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	cf.sink = makeMetricsSink(cf.metrics, cf.sink)
	cf.sink = &errorWrapperSink{wrapped: cf.sink}

	cf.highWaterAtStart = cf.spec.Feed.StatementTime
	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return
		}
		cf.js = &jobState{
			job:                    job,
			lastFrontierCheckpoint: timeutil.Now(),
			settings:               cf.flowCtx.Cfg.Settings,
		}

		if changefeedbase.FrontierCheckpointFrequency.Get(&cf.flowCtx.Cfg.Settings.SV) == 0 {
			log.Warning(ctx,
				"Frontier checkpointing disabled; set changefeed.frontier_checkpoint_frequency to non-zero value to re-enable")
		}

		p := job.Progress()
		if ts := p.GetHighWater(); ts != nil {
			cf.highWaterAtStart.Forward(*ts)
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
	cf.metrics.Running.Inc(1)
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
		cf.metrics.Running.Dec(1)
	}
	delete(cf.metrics.mu.resolved, cf.metricsID)
	cf.metricsID = -1
	cf.metrics.mu.Unlock()
}

// shouldProtectBoundaries checks the job's spec to determine whether it should
// install protected timestamps when encountering scan boundaries.
func (cf *changeFrontier) shouldProtectBoundaries() bool {
	policy := changefeedbase.SchemaChangePolicy(cf.spec.Feed.Opts[changefeedbase.OptSchemaChangePolicy])
	return policy == changefeedbase.OptSchemaChangePolicyBackfill
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
				// The code to restart the changefeed is only supported once 21.1 is
				// activated.
				//
				// TODO(ajwerner): Remove this gate in 21.2.
				if cf.EvalCtx.Settings.Version.IsActive(
					cf.Ctx, clusterversion.ChangefeedsSupportPrimaryIndexChanges,
				) {
					err = changefeedbase.MarkRetryableError(err)
				} else {
					err = errors.Wrap(err, "primary key change occurred")
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

		if err := cf.noteResolvedSpan(row[0]); err != nil {
			cf.MoveToDraining(err)
			break
		}
	}
	return nil, cf.DrainHelper()
}

func (cf *changeFrontier) noteResolvedSpan(d rowenc.EncDatum) error {
	if err := d.EnsureDecoded(changefeeddist.ChangefeedResultTypes[0], &cf.a); err != nil {
		return err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return errors.AssertionFailedf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}
	var resolved jobspb.ResolvedSpan
	if err := protoutil.Unmarshal([]byte(*raw), &resolved); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved span: %x`, raw)
	}

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
			log.Safe(resolved.Timestamp), resolved.Span, log.Safe(cf.highWaterAtStart))
		return nil
	}

	return cf.forwardFrontier(resolved)
}

func (cf *changeFrontier) forwardFrontier(resolved jobspb.ResolvedSpan) error {
	frontierChanged, err := cf.frontier.ForwardResolvedSpan(resolved)
	if err != nil {
		return err
	}

	isBehind := cf.maybeLogBehindSpan(frontierChanged)

	// Checkpoint job record progress if needed.
	// NB: Sinkless changefeeds will not have a job state (js). In fact, they
	// have no distributed state whatsoever. Because of this they also do not
	// use protected timestamps.
	if cf.js != nil {
		if err := cf.maybeCheckpointJob(frontierChanged, isBehind); err != nil {
			return err
		}
	}

	if frontierChanged {
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

func (cf *changeFrontier) maybeCheckpointJob(frontierChanged, isBehind bool) error {
	// Update checkpoint if the frontier has not changed, but it is time to checkpoint.
	// If the frontier has changed, we want to write an empty checkpoint record indicating
	// that all spans have reached the frontier.
	updateCheckpoint := !frontierChanged && cf.js.canCheckpointFrontier()

	var checkpoint jobspb.ChangefeedProgress_Checkpoint
	if updateCheckpoint {
		maxBytes := changefeedbase.FrontierCheckpointMaxBytes.Get(&cf.flowCtx.Cfg.Settings.SV)
		checkpoint.Spans = cf.frontier.getCheckpointSpans(maxBytes)
	}

	if frontierChanged || updateCheckpoint {
		cf.js.lastFrontierCheckpoint = timeutil.Now()
		checkpointStart := timeutil.Now()
		if err := cf.checkpointJobProgress(
			cf.frontier.Frontier(), frontierChanged, checkpoint, isBehind,
		); err != nil {
			return err
		}
		cf.metrics.CheckpointHistNanos.RecordValue(timeutil.Since(checkpointStart).Nanoseconds())
	}

	return nil
}

// checkpointJobProgress checkpoints a changefeed-level job information.
// In addition, if 'manageProtected' is true, which only happens when frontier advanced,
// this method manages the protected timestamp state.
// The isBehind argument is used to determine whether an existing protected timestamp
// should be released.
func (cf *changeFrontier) checkpointJobProgress(
	frontier hlc.Timestamp,
	manageProtected bool,
	checkpoint jobspb.ChangefeedProgress_Checkpoint,
	isBehind bool,
) (err error) {
	updateRunStatus := timeutil.Since(cf.js.lastRunStatusUpdate) > runStatusUpdateFrequency
	if updateRunStatus {
		defer func() { cf.js.lastRunStatusUpdate = timeutil.Now() }()
	}

	updateStart := timeutil.Now()
	defer func() {
		elapsed := timeutil.Since(updateStart)
		if elapsed > 5*time.Millisecond {
			log.Warningf(cf.Ctx, "slow job progress update took %s", elapsed)
		}
	}()

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

		// Manage protected timestamps.
		changefeedProgress := progress.Details.(*jobspb.Progress_Changefeed).Changefeed
		if manageProtected {
			if err := cf.manageProtectedTimestamps(cf.Ctx, changefeedProgress, txn, frontier, isBehind); err != nil {
				return err
			}
		}

		changefeedProgress.Checkpoint = &checkpoint

		if updateRunStatus {
			md.Progress.RunningStatus = fmt.Sprintf("running: resolved=%s", frontier)
		}

		ju.UpdateProgress(progress)
		return nil
	})
}

// manageProtectedTimestamps is called when the resolved timestamp is being
// checkpointed. The changeFrontier always checkpoints resolved timestamps
// which occur at scan boundaries. It releases previously protected timestamps
// if the changefeed is not behind. See maybeLogBehindSpan for details on the
// behind calculation.
//
// Note that this function is never called for sinkless changefeeds as they have
// no corresponding job and thus no corresponding distributed state on which to
// attach protected timestamp information.
//
// TODO(ajwerner): Adopt protected timestamps for sinkless changefeeds,
// perhaps by using whatever mechanism is eventually built to protect
// data for long-running SQL transactions. There's some discussion of this
// use case in the protected timestamps RFC.
func (cf *changeFrontier) manageProtectedTimestamps(
	ctx context.Context,
	progress *jobspb.ChangefeedProgress,
	txn *kv.Txn,
	resolved hlc.Timestamp,
	isBehind bool,
) error {
	pts := cf.flowCtx.Cfg.ProtectedTimestampProvider
	if err := cf.maybeReleaseProtectedTimestamp(ctx, progress, pts, txn, isBehind); err != nil {
		return err
	}
	return cf.maybeProtectTimestamp(ctx, progress, pts, txn, resolved)
}

// maybeReleaseProtectedTimestamp will release the current protected timestamp
// if either the resolved timestamp is close to the present or we've reached
// a new schemaChangeBoundary which will be protected.
func (cf *changeFrontier) maybeReleaseProtectedTimestamp(
	ctx context.Context,
	progress *jobspb.ChangefeedProgress,
	pts protectedts.Storage,
	txn *kv.Txn,
	isBehind bool,
) error {
	if progress.ProtectedTimestampRecord == uuid.Nil {
		return nil
	}
	if !cf.frontier.schemaChangeBoundaryReached() && isBehind {
		log.VEventf(ctx, 2, "not releasing protected timestamp because changefeed is behind")
		return nil
	}
	log.VEventf(ctx, 2, "releasing protected timestamp %v",
		progress.ProtectedTimestampRecord)
	if err := pts.Release(ctx, txn, progress.ProtectedTimestampRecord); err != nil {
		return err
	}
	progress.ProtectedTimestampRecord = uuid.Nil
	return nil
}

// maybeProtectTimestamp creates a new protected timestamp when the
// changeFrontier reaches a scanBoundary and the schemaChangePolicy indicates
// that we should perform a backfill (see cf.shouldProtectBoundaries()).
func (cf *changeFrontier) maybeProtectTimestamp(
	ctx context.Context,
	progress *jobspb.ChangefeedProgress,
	pts protectedts.Storage,
	txn *kv.Txn,
	resolved hlc.Timestamp,
) error {
	if cf.isSinkless() || cf.isTenant() || !cf.frontier.schemaChangeBoundaryReached() || !cf.shouldProtectBoundaries() {
		return nil
	}

	jobID := cf.spec.JobID
	targets := cf.spec.Feed.Targets
	return createProtectedTimestampRecord(ctx, cf.flowCtx.Codec(), pts, txn, jobID, targets, resolved, progress)
}

func (cf *changeFrontier) maybeEmitResolved(newResolved hlc.Timestamp) error {
	if cf.freqEmitResolved == emitNoResolved {
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

// Potentially log the most behind span in the frontier for debugging. The
// returned boolean will be true if the resolved timestamp lags far behind the
// present as defined by the current configuration.
func (cf *changeFrontier) maybeLogBehindSpan(frontierChanged bool) (isBehind bool) {
	frontier := cf.frontier.Frontier()
	now := timeutil.Now()
	resolvedBehind := now.Sub(frontier.GoTime())
	if resolvedBehind <= cf.slownessThreshold() {
		return false
	}

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
	return true
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

// isTenant() bool returns true if this changeFrontier is running on a
// tenant.
func (cf *changeFrontier) isTenant() bool {
	return !cf.flowCtx.Codec().ForSystemTenant()
}

// type to make embedding span.Frontier in schemaChangeFrontier convenient.
type spanFrontier struct {
	*span.Frontier
}

func (s *spanFrontier) frontierTimestamp() hlc.Timestamp {
	return s.Frontier.Frontier()
}

// schemaChangeFrontier encapsulates span frontier, keeping track of span resolved time,
// along with the schema change boundary information.
type schemaChangeFrontier struct {
	*spanFrontier

	// boundaryTime represents an hlc timestamp at which a schema change
	// event occurred to a target watched by this frontier. If the changefeed is
	// configured to stop on schema change then the changeFrontier will wait for
	// the span frontier to reach the schemaChangeBoundary, will drain, and then
	// will exit. If the changefeed is configured to backfill on schema changes,
	// the changeFrontier will protect the scan timestamp in order to ensure that
	// the scan complete. The protected timestamp will be released when a new scan
	// schemaChangeBoundary is created or the changefeed reaches a timestamp that
	// is near the present.
	//
	// schemaChangeBoundary values are communicated to the changeFrontier via
	// Resolved messages send from the changeAggregators. The policy regarding
	// which schema change events lead to a schemaChangeBoundary is controlled
	// by the KV feed based on OptSchemaChangeEvents and OptSchemaChangePolicy.
	boundaryTime hlc.Timestamp

	// boundaryType indicates the type of the schemaChangeBoundary and thus the
	// action which should be taken when the frontier reaches that boundary.
	boundaryType jobspb.ResolvedSpan_BoundaryType
}

func makeSchemaChangeFrontier(c *hlc.Clock, spans ...roachpb.Span) (*schemaChangeFrontier, error) {
	sf, err := span.MakeFrontier(spans...)
	if err != nil {
		return nil, err
	}
	f := &schemaChangeFrontier{spanFrontier: &spanFrontier{sf}}
	f.spanFrontier.TrackUpdateTimestamp(func() hlc.Timestamp { return c.Now() })
	return f, nil
}

// ForwardResolvedSpan advances the timestamp for a resolved span.
// Takes care of updating schema change boundary information.
func (f *schemaChangeFrontier) ForwardResolvedSpan(r jobspb.ResolvedSpan) (bool, error) {
	// We want to ensure that we mark the schemaChangeBoundary and then we want
	// to detect when the frontier reaches to or past the schemaChangeBoundary.
	// The behavior when the boundary is reached is controlled by the
	// boundary type.
	// NB: boundaryType and time update machinery is tricky.  In particular,
	// we never go back to ResolvedSpan_None if we have seen newer boundary other than none.
	switch r.BoundaryType {
	case jobspb.ResolvedSpan_NONE:
		if !f.boundaryTime.IsEmpty() && f.boundaryTime.Less(r.Timestamp) {
			f.boundaryTime = hlc.Timestamp{}
			f.boundaryType = jobspb.ResolvedSpan_NONE
		}
	case jobspb.ResolvedSpan_BACKFILL, jobspb.ResolvedSpan_EXIT, jobspb.ResolvedSpan_RESTART:
		if !f.boundaryTime.IsEmpty() && r.Timestamp.Less(f.boundaryTime) {
			return false, errors.AssertionFailedf("received boundary timestamp %v < %v "+
				"of type %v before reaching existing boundary of type %v",
				r.Timestamp, f.boundaryTime, r.BoundaryType, f.boundaryType)
		}
		f.boundaryTime = r.Timestamp
		f.boundaryType = r.BoundaryType
	}
	return f.Forward(r.Span, r.Timestamp)
}

// Frontier returns the minimum timestamp being tracked.
func (f *schemaChangeFrontier) Frontier() hlc.Timestamp {
	return f.frontierTimestamp()
}

// SpanFrontier returns underlying span.Frontier.
func (f *schemaChangeFrontier) SpanFrontier() *span.Frontier {
	return f.spanFrontier.Frontier
}

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

// schemaChangeBoundaryReached returns true if the schema change boundary has been reached.
func (f *schemaChangeFrontier) schemaChangeBoundaryReached() (r bool) {
	return !f.boundaryTime.IsEmpty() && f.boundaryTime.Equal(f.Frontier())
}

// boundaryTypeAt returns boundary type applicable at the specified timestamp.
func (f *schemaChangeFrontier) boundaryTypeAt(ts hlc.Timestamp) jobspb.ResolvedSpan_BoundaryType {
	if f.boundaryTime.IsEmpty() || ts.Less(f.boundaryTime) {
		return jobspb.ResolvedSpan_NONE
	}
	return f.boundaryType
}
