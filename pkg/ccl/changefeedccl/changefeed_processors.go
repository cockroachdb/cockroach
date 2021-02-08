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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	execinfra.StreamingProcessor

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
	// lastFlush and flushFrequency keep track of the flush frequency.
	lastFlush      time.Time
	flushFrequency time.Duration
	// spansToFlush keeps track of resolved spans that have not been flushed yet.
	spansToFlush []*jobspb.ResolvedSpan
	// spanFrontier keeps track of resolved timestamps for spans.
	spanFrontier *span.Frontier

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

// Default frequency to flush sink.
// See comment in newChangeAggregatorProcessor for explanation on the value.
var defaultFlushFrequency = 5 * time.Second

// TestingSetDefaultFlushFrequency changes defaultFlushFrequency for tests.
// Returns function to restore flush frequency to its original value.
func TestingSetDefaultFlushFrequency(f time.Duration) func() {
	defaultFlushFrequency = f
	return func() { defaultFlushFrequency = 5 * time.Second }
}

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
		flowCtx:   flowCtx,
		spec:      spec,
		memAcc:    memMonitor.MakeBoundAccount(),
		lastFlush: timeutil.Now(),
	}
	if err := ca.Init(
		ca,
		post,
		changefeedResultTypes,
		flowCtx,
		processorID,
		output,
		memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				ca.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	var err error
	if ca.encoder, err = getEncoder(ca.spec.Feed.Opts, ca.spec.Feed.Targets); err != nil {
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
		ca.flushFrequency = defaultFlushFrequency
	}
	return ca, nil
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) context.Context {
	ctx, ca.cancel = context.WithCancel(ctx)
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	spans := ca.setupSpansAndFrontier()
	timestampOracle := &changeAggregatorLowerBoundOracle{sf: ca.spanFrontier, initialInclusiveLowerBound: ca.spec.Feed.StatementTime}

	var err error
	ca.sink, err = getSink(
		ctx, ca.spec.Feed.SinkURI, ca.flowCtx.EvalCtx.NodeID.SQLInstanceID(), ca.spec.Feed.Opts, ca.spec.Feed.Targets,
		ca.flowCtx.Cfg.Settings, timestampOracle, ca.flowCtx.Cfg.ExternalStorageFromURI, ca.spec.User(),
	)
	if err != nil {
		err = MarkRetryableError(err)
		// Early abort in the case that there is an error creating the sink.
		ca.MoveToDraining(err)
		ca.cancel()
		return ctx
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

	if cfKnobs, ok := ca.flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		ca.knobs = *cfKnobs
	}

	// It seems like we should also be able to use `ca.ProcessorBase.MemMonitor`
	// for the poller, but there is a race between the flow's MemoryMonitor
	// getting Stopped and `changeAggregator.Close`, which causes panics. Not sure
	// what to do about this yet.
	kvFeedMemMonCapacity := kvfeed.MemBufferDefaultCapacity
	if ca.knobs.MemBufferCapacity != 0 {
		kvFeedMemMonCapacity = ca.knobs.MemBufferCapacity
	}
	kvFeedMemMon := mon.NewMonitorInheritWithLimit("kvFeed", math.MaxInt64, ca.ProcessorBase.MemMonitor)
	kvFeedMemMon.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(kvFeedMemMonCapacity))
	ca.kvFeedMemMon = kvFeedMemMon

	buf := kvfeed.MakeChanBuffer()
	leaseMgr := ca.flowCtx.Cfg.LeaseManager.(*lease.Manager)
	_, withDiff := ca.spec.Feed.Opts[changefeedbase.OptDiff]
	kvfeedCfg := makeKVFeedCfg(ca.flowCtx.Cfg, leaseMgr, ca.kvFeedMemMon, ca.spec,
		spans, withDiff, buf, ca.metrics)
	cfg := ca.flowCtx.Cfg

	ca.eventProducer = &bufEventProducer{buf}
	ca.eventConsumer = newKVEventToRowConsumer(ctx, cfg, ca.spanFrontier, kvfeedCfg.InitialHighWater,
		ca.sink, ca.encoder, ca.spec.Feed, ca.knobs)
	ca.startKVFeed(ctx, kvfeedCfg)

	return ctx
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

func makeKVFeedCfg(
	cfg *execinfra.ServerConfig,
	leaseMgr *lease.Manager,
	mm *mon.BytesMonitor,
	spec execinfrapb.ChangeAggregatorSpec,
	spans []roachpb.Span,
	withDiff bool,
	buf kvfeed.EventBuffer,
	metrics *Metrics,
) kvfeed.Config {
	schemaChangeEvents := changefeedbase.SchemaChangeEventClass(
		spec.Feed.Opts[changefeedbase.OptSchemaChangeEvents])
	schemaChangePolicy := changefeedbase.SchemaChangePolicy(
		spec.Feed.Opts[changefeedbase.OptSchemaChangePolicy])
	initialHighWater, needsInitialScan := getKVFeedInitialParameters(spec)
	kvfeedCfg := kvfeed.Config{
		Sink:               buf,
		Settings:           cfg.Settings,
		DB:                 cfg.DB,
		Codec:              cfg.Codec,
		Clock:              cfg.DB.Clock(),
		Gossip:             cfg.Gossip,
		Spans:              spans,
		Targets:            spec.Feed.Targets,
		LeaseMgr:           leaseMgr,
		Metrics:            &metrics.KVFeedMetrics,
		MM:                 mm,
		InitialHighWater:   initialHighWater,
		WithDiff:           withDiff,
		NeedsInitialScan:   needsInitialScan,
		SchemaChangeEvents: schemaChangeEvents,
		SchemaChangePolicy: schemaChangePolicy,
	}
	return kvfeedCfg
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
func (ca *changeAggregator) setupSpansAndFrontier() []roachpb.Span {
	spans := make([]roachpb.Span, 0, len(ca.spec.Watches))
	for _, watch := range ca.spec.Watches {
		spans = append(spans, watch.Span)
	}
	ca.spanFrontier = span.MakeFrontier(spans...)
	for _, watch := range ca.spec.Watches {
		ca.spanFrontier.Forward(watch.Span, watch.InitialResolved)
	}
	return spans
}

// close has two purposes: to synchronize on the completion of the helper
// goroutines created by the Start method, and to clean up any resources used by
// the processor. Due to the fact that this method may be called even if the
// processor did not finish completion, there is an excessive amount of nil
// checking.
func (ca *changeAggregator) close() {
	if ca.InternalClose() {
		// Shut down the poller if it wasn't already.
		if ca.cancel != nil {
			ca.cancel()
		}
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

	if event.Type() == kvfeed.KVEvent {
		if err := ca.eventConsumer.ConsumeEvent(ca.Ctx, event); err != nil {
			return err
		}
	}

	return ca.maybeFlush(event.Resolved())
}

// maybeFlush flushes sink and emits resolved timestamp if needed.
func (ca *changeAggregator) maybeFlush(resolvedSpan *jobspb.ResolvedSpan) error {
	if resolvedSpan != nil {
		ca.spanFrontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
		ca.spansToFlush = append(ca.spansToFlush, resolvedSpan)
	}

	boundaryReached := resolvedSpan != nil && resolvedSpan.BoundaryReached
	if len(ca.spansToFlush) == 0 ||
		(timeutil.Since(ca.lastFlush) < ca.flushFrequency && !boundaryReached) {
		return nil
	}

	// Make sure to flush the sink before forwarding resolved spans,
	// otherwise, we could lose buffered messages and violate the
	// at-least-once guarantee. This is also true for checkpointing the
	// resolved spans in the job progress.
	if err := ca.sink.Flush(ca.Ctx); err != nil {
		return err
	}
	ca.lastFlush = timeutil.Now()
	if ca.knobs.AfterSinkFlush != nil {
		if err := ca.knobs.AfterSinkFlush(); err != nil {
			return err
		}
	}

	for _, resolvedSpan := range ca.spansToFlush {
		resolvedBytes, err := protoutil.Marshal(resolvedSpan)
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
	}
	ca.spansToFlush = ca.spansToFlush[:0]
	return nil
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.InternalClose()
}

type kvEventProducer interface {
	// GetEvent returns the next kv event.
	GetEvent(ctx context.Context) (kvfeed.Event, error)
}

type bufEventProducer struct {
	kvfeed.EventBufferReader
}

var _ kvEventProducer = &bufEventProducer{}

// GetEvent implements kvEventProducer interface
func (p *bufEventProducer) GetEvent(ctx context.Context) (kvfeed.Event, error) {
	return p.Get(ctx)
}

type kvEventConsumer interface {
	// ConsumeEvent responsible for consuming kv event.
	ConsumeEvent(ctx context.Context, event kvfeed.Event) error
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

// ConsumeEvent implements kvEventConsumer interface
func (c *kvEventToRowConsumer) ConsumeEvent(ctx context.Context, event kvfeed.Event) error {
	if event.Type() != kvfeed.KVEvent {
		return errors.AssertionFailedf("expected kv event, got %v", event.Type())
	}

	r, err := c.eventToRow(ctx, event)
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
		ctx, r.tableDesc, keyCopy, valueCopy, r.updated,
	); err != nil {
		return err
	}
	if log.V(3) {
		log.Infof(ctx, `r %s: %s -> %s`, r.tableDesc.GetName(), keyCopy, valueCopy)
	}
	return nil
}

func (c *kvEventToRowConsumer) eventToRow(
	ctx context.Context, event kvfeed.Event,
) (encodeRow, error) {
	var r encodeRow
	schemaTimestamp := event.KV().Value.Timestamp
	prevSchemaTimestamp := schemaTimestamp

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

const (
	emitAllResolved = 0
	emitNoResolved  = -1
)

type changeFrontier struct {
	execinfra.ProcessorBase
	execinfra.StreamingProcessor

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ChangeFrontierSpec
	memAcc  mon.BoundAccount
	a       rowenc.DatumAlloc

	// input returns rows from one or more changeAggregator processors
	input execinfra.RowSource

	// sf contains the current resolved timestamp high-water for the tracked
	// span set.
	sf *span.Frontier
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
	// lastSlowSpanLog is the last time a slow span from `sf` was logged.
	lastSlowSpanLog time.Time

	// schemaChangeBoundary represents an hlc timestamp at which a schema change
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
	schemaChangeBoundary hlc.Timestamp

	// jobProgressedFn, if non-nil, is called to checkpoint the changefeed's
	// progress in the corresponding system job entry.
	jobProgressedFn func(context.Context, jobs.HighWaterProgressedFn) error
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
	cf := &changeFrontier{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
		input:   input,
		sf:      span.MakeFrontier(spec.TrackedSpans...),
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
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
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

	var err error
	if cf.encoder, err = getEncoder(spec.Feed.Opts, spec.Feed.Targets); err != nil {
		return nil, err
	}

	return cf, nil
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) context.Context {
	cf.input.Start(ctx)

	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)

	// Pass a nil oracle because this sink is only used to emit resolved timestamps
	// but the oracle is only used when emitting row updates.
	var nilOracle timestampLowerBoundOracle
	var err error
	cf.sink, err = getSink(
		ctx, cf.spec.Feed.SinkURI, cf.flowCtx.EvalCtx.NodeID.SQLInstanceID(), cf.spec.Feed.Opts, cf.spec.Feed.Targets,
		cf.flowCtx.Cfg.Settings, nilOracle, cf.flowCtx.Cfg.ExternalStorageFromURI, cf.spec.User(),
	)
	if err != nil {
		err = MarkRetryableError(err)
		cf.MoveToDraining(err)
		return ctx
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	cf.metrics = cf.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	cf.sink = makeMetricsSink(cf.metrics, cf.sink)
	cf.sink = &errorWrapperSink{wrapped: cf.sink}

	cf.highWaterAtStart = cf.spec.Feed.StatementTime
	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return ctx
		}
		cf.jobProgressedFn = job.HighWaterProgressed

		p := job.Progress()
		if ts := p.GetHighWater(); ts != nil {
			cf.highWaterAtStart.Forward(*ts)
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

	return ctx
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

// schemaChangeBoundaryReached returns true if the spanFrontier is at the
// current schemaChangeBoundary.
func (cf *changeFrontier) schemaChangeBoundaryReached() (r bool) {
	return !cf.schemaChangeBoundary.IsEmpty() && cf.schemaChangeBoundary.Equal(cf.sf.Frontier())
}

// shouldFailOnSchemaChange checks the job's spec to determine whether it should
// failed on schema change events after all spans have been resolved.
func (cf *changeFrontier) shouldFailOnSchemaChange() bool {
	policy := changefeedbase.SchemaChangePolicy(cf.spec.Feed.Opts[changefeedbase.OptSchemaChangePolicy])
	return policy == changefeedbase.OptSchemaChangePolicyStop
}

// shouldFailOnSchemaChange checks the job's spec to determine whether it should
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

		if cf.schemaChangeBoundaryReached() && cf.shouldFailOnSchemaChange() {
			// TODO(ajwerner): make this more useful by at least informing the client
			// of which tables changed.
			cf.MoveToDraining(pgerror.Newf(pgcode.SchemaChangeOccurred,
				"schema change occurred at %v", cf.schemaChangeBoundary.Next().AsOfSystemTime()))
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
	if err := d.EnsureDecoded(changefeedResultTypes[0], &cf.a); err != nil {
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

	// We want to ensure that we mark the schemaChangeBoundary and then we want to detect when
	// the frontier reaches to or past the schemaChangeBoundary.
	if resolved.BoundaryReached && (cf.schemaChangeBoundary.IsEmpty() || resolved.Timestamp.Less(cf.schemaChangeBoundary)) {
		cf.schemaChangeBoundary = resolved.Timestamp
	}
	// If we've moved past a schemaChangeBoundary, make sure to clear it.
	if !resolved.BoundaryReached && !cf.schemaChangeBoundary.IsEmpty() && cf.schemaChangeBoundary.Less(resolved.Timestamp) {
		cf.schemaChangeBoundary = hlc.Timestamp{}
	}

	frontierChanged := cf.sf.Forward(resolved.Span, resolved.Timestamp)
	isBehind := cf.maybeLogBehindSpan(frontierChanged)
	if frontierChanged {
		if err := cf.handleFrontierChanged(isBehind); err != nil {
			return err
		}
	}
	return nil
}

func (cf *changeFrontier) handleFrontierChanged(isBehind bool) error {
	newResolved := cf.sf.Frontier()
	cf.metrics.mu.Lock()
	if cf.metricsID != -1 {
		cf.metrics.mu.resolved[cf.metricsID] = newResolved
	}
	cf.metrics.mu.Unlock()
	if err := cf.checkpointResolvedTimestamp(newResolved, isBehind); err != nil {
		return err
	}
	if err := cf.maybeEmitResolved(newResolved); err != nil {
		return err
	}
	return nil
}

// checkpointResolvedTimestamp checkpoints a changefeed-level resolved timestamp
// to the jobs record. It additionally manages the protected timestamp state
// which is stored in the job progress details. It is only called if the new
// resolved timestamp is later than the current one. The isBehind argument is
// used to determine whether an existing protected timestamp should be released.
func (cf *changeFrontier) checkpointResolvedTimestamp(
	resolved hlc.Timestamp, isBehind bool,
) (err error) {
	// NB: Sinkless changefeeds will not have a jobProgressedFn. In fact, they
	// have no distributed state whatsoever. Because of this they also do not
	// use protected timestamps.
	if cf.jobProgressedFn == nil {
		return nil
	}
	return cf.jobProgressedFn(cf.Ctx, func(
		ctx context.Context, txn *kv.Txn, details jobspb.ProgressDetails,
	) (hlc.Timestamp, error) {
		progress := details.(*jobspb.Progress_Changefeed).Changefeed
		if err := cf.manageProtectedTimestamps(ctx, progress, txn, resolved, isBehind); err != nil {
			return hlc.Timestamp{}, err
		}
		return resolved, nil
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
	if !cf.schemaChangeBoundaryReached() && isBehind {
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
	if cf.isSinkless() || !cf.schemaChangeBoundaryReached() || !cf.shouldProtectBoundaries() {
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
	shouldEmit := sinceEmitted >= cf.freqEmitResolved || cf.schemaChangeBoundaryReached()
	if !shouldEmit {
		return nil
	}
	// Keeping this after the checkpointResolvedTimestamp call will avoid
	// some duplicates if a restart happens.
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
	// These two cluster setting values represent the target responsiveness of
	// poller and range feed. The cluster setting for switching between poller and
	// rangefeed is only checked at changefeed start/resume, so instead of
	// switching on it here, just add them. Also add 1 second in case both these
	// settings are set really low (as they are in unit tests).
	pollInterval := changefeedbase.TableDescriptorPollInterval.Get(&cf.flowCtx.Cfg.Settings.SV)
	closedtsInterval := closedts.TargetDuration.Get(&cf.flowCtx.Cfg.Settings.SV)
	slownessThreshold := time.Second + 10*(pollInterval+closedtsInterval)
	frontier := cf.sf.Frontier()
	now := timeutil.Now()
	resolvedBehind := now.Sub(frontier.GoTime())
	if resolvedBehind <= slownessThreshold {
		return false
	}

	description := `sinkless feed`
	if !cf.isSinkless() {
		description = fmt.Sprintf("job %d", cf.spec.JobID)
	}
	if frontierChanged {
		log.Infof(cf.Ctx, "%s new resolved timestamp %s is behind by %s",
			description, frontier, resolvedBehind)
	}
	const slowSpanMaxFrequency = 10 * time.Second
	if now.Sub(cf.lastSlowSpanLog) > slowSpanMaxFrequency {
		cf.lastSlowSpanLog = now
		s := cf.sf.PeekFrontierSpan()
		log.Infof(cf.Ctx, "%s span %s is behind by %s", description, s, resolvedBehind)
	}
	return true
}

// ConsumerClosed is part of the RowSource interface.
func (cf *changeFrontier) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	cf.InternalClose()
}

// isSinkless returns true if this changeFrontier is sinkless and thus does not
// have a job.
func (cf *changeFrontier) isSinkless() bool {
	return cf.spec.JobID == 0
}
