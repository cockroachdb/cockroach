// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"hash"
	"hash/crc32"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// eventContext holds metadata pertaining to event.
type eventContext struct {
	updated, mvcc hlc.Timestamp
	// topic is set to the string to be included if TopicInValue is true
	topic string
}

type eventConsumer interface {
	ConsumeEvent(ctx context.Context, ev kvevent.Event) error
	Close() error
	Flush(ctx context.Context) error
}

type frontier interface{ Frontier() hlc.Timestamp }

type kvEventToRowConsumer struct {
	frontier
	encoder      Encoder
	scratch      bufalloc.ByteAllocator
	sink         EventSink
	cursor       hlc.Timestamp
	knobs        TestingKnobs
	decoder      cdcevent.Decoder
	details      ChangefeedConfig
	evaluator    *cdceval.Evaluator
	encodingOpts changefeedbase.EncodingOptions

	topicDescriptorCache map[TopicIdentifier]TopicDescriptor
	topicNamer           *TopicNamer

	metrics *sliMetrics
	sv      *settings.Values

	// This pacer is used to incorporate event consumption to elastic CPU
	// control. This helps ensure that event encoding/decoding does not throttle
	// foreground SQL traffic.
	//
	// Note that for pacer to function correctly,
	// kvEventToRowConsumer.ConsumeEvent must be called by the same goroutine in a
	// tight loop.
	//
	// The pacer is closed by kvEventToRowConsumer.Close.
	pacer *admission.Pacer
}

func newEventConsumer(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	spec execinfrapb.ChangeAggregatorSpec,
	feed ChangefeedConfig,
	spanFrontier frontier,
	cursor hlc.Timestamp,
	sink EventSink,
	metrics *Metrics,
	sliMetrics *sliMetrics,
	knobs TestingKnobs,
) (eventConsumer, EventSink, error) {
	encodingOpts, err := feed.Opts.GetEncodingOptions()
	if err != nil {
		return nil, nil, err
	}

	pacerRequestUnit := changefeedbase.EventConsumerPacerRequestSize.Get(&cfg.Settings.SV)
	enablePacer := changefeedbase.PerEventElasticCPUControlEnabled.Get(&cfg.Settings.SV)

	makeConsumer := func(s EventSink, frontier frontier) (eventConsumer, error) {
		var err error
		encoder, err := getEncoder(ctx, encodingOpts, feed.Targets, spec.Select.Expr != "",
			makeExternalConnectionProvider(ctx, cfg.DB), sliMetrics, newEnrichedSourceProvider(
				encodingOpts, enrichedSourceData{
					jobId: spec.JobID.String(),
				}),
		)
		if err != nil {
			return nil, err
		}

		var topicNamer *TopicNamer
		if encodingOpts.TopicInValue {
			topicNamer, err = MakeTopicNamer(feed.Targets)
			if err != nil {
				return nil, err
			}
		}

		// Passing a nil Pacer is effectively a noop Pacer if
		// CPU control is disabled.
		var pacer *admission.Pacer = nil
		if enablePacer {
			tenantID, ok := roachpb.ClientTenantFromContext(ctx)
			if !ok {
				tenantID = roachpb.SystemTenantID
			}

			pacer = cfg.AdmissionPacerFactory.NewPacer(
				pacerRequestUnit,
				admission.WorkInfo{
					TenantID:        tenantID,
					Priority:        admissionpb.BulkNormalPri,
					CreateTime:      timeutil.Now().UnixNano(),
					BypassAdmission: false,
				},
			)
		}

		execCfg := cfg.ExecutorConfig.(*sql.ExecutorConfig)
		return newKVEventToRowConsumer(ctx, execCfg, frontier, cursor, s,
			encoder, feed, spec, knobs, topicNamer, sliMetrics, pacer)
	}

	numWorkers := changefeedbase.EventConsumerWorkers.Get(&cfg.Settings.SV)
	if numWorkers == 0 {
		// Pick a reasonable default.
		numWorkers = defaultNumWorkers()
	}

	// The descriptions for event_consumer_worker settings should also be updated
	// when these TODOs are completed.
	//
	// TODO (ganeshb) Add support for parallel encoding when using parquet.
	// We cannot have a separate encoder and sink for parquet format (see
	// parquet_sink_cloudstorage.go). Because of this the current nprox solution
	// does not work for parquet format.
	//
	// TODO (jayshrivastava) enable parallel consumers for sinkless changefeeds.
	isSinkless := spec.JobID == 0
	if numWorkers <= 1 || isSinkless || encodingOpts.Format == changefeedbase.OptFormatParquet {
		c, err := makeConsumer(sink, spanFrontier)
		if err != nil {
			return nil, nil, err
		}
		return c, sink, err
	}

	c := &parallelEventConsumer{
		g:            ctxgroup.WithContext(ctx),
		hasher:       makeHasher(),
		metrics:      metrics,
		termCh:       make(chan struct{}),
		flushCh:      make(chan struct{}, 1),
		doneCh:       make(chan struct{}),
		numWorkers:   numWorkers,
		workerCh:     make([]chan kvevent.Event, numWorkers),
		workerChSize: changefeedbase.EventConsumerWorkerQueueSize.Get(&cfg.Settings.SV),
		spanFrontier: spanFrontier,
	}
	ss := sink
	if !sinkSupportsConcurrentEmits(sink) {
		ss = &safeSink{wrapped: sink}
	}
	c.makeConsumer = func() (eventConsumer, error) {
		return makeConsumer(ss, c)
	}

	if err := c.startWorkers(); err != nil {
		return nil, nil, err
	}
	return c, ss, nil
}

func defaultNumWorkers() int64 {
	idealNumber := runtime.GOMAXPROCS(0) >> 2
	if idealNumber < 1 {
		return 1
	}
	if idealNumber > 8 {
		return 8
	}
	return int64(idealNumber)
}

func makeHasher() hash.Hash32 {
	return crc32.New(crc32.MakeTable(crc32.IEEE))
}

func newKVEventToRowConsumer(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	frontier frontier,
	cursor hlc.Timestamp,
	sink EventSink,
	encoder Encoder,
	details ChangefeedConfig,
	spec execinfrapb.ChangeAggregatorSpec,
	knobs TestingKnobs,
	topicNamer *TopicNamer,
	metrics *sliMetrics,
	pacer *admission.Pacer,
) (_ *kvEventToRowConsumer, err error) {
	includeVirtual := details.Opts.IncludeVirtual()
	keyOnly := details.Opts.KeyOnly()
	decoder, err := cdcevent.NewEventDecoder(ctx, cfg, details.Targets, includeVirtual, keyOnly)
	if err != nil {
		return nil, err
	}

	var evaluator *cdceval.Evaluator
	if spec.Select.Expr != "" {
		evaluator, err = newEvaluator(ctx, cfg, spec, details.Opts.GetFilters().WithDiff)
		if err != nil {
			return nil, err
		}
	}

	encodingOpts, err := details.Opts.GetEncodingOptions()
	if err != nil {
		return nil, err
	}

	return &kvEventToRowConsumer{
		frontier:             frontier,
		encoder:              encoder,
		decoder:              decoder,
		sink:                 sink,
		cursor:               cursor,
		details:              details,
		knobs:                knobs,
		topicDescriptorCache: make(map[TopicIdentifier]TopicDescriptor),
		topicNamer:           topicNamer,
		evaluator:            evaluator,
		encodingOpts:         encodingOpts,
		metrics:              metrics,
		pacer:                pacer,
		sv:                   cfg.SV(),
	}, nil
}

func newEvaluator(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	spec execinfrapb.ChangeAggregatorSpec,
	withDiff bool,
) (*cdceval.Evaluator, error) {
	sc, err := cdceval.ParseChangefeedExpression(spec.Select.Expr)
	if err != nil {
		return nil, err
	}

	sd := sql.NewInternalSessionData(ctx, cfg.Settings, "changefeed-evaluator")
	if spec.Feed.SessionData == nil {
		// This changefeed was created prior to 23.1; thus we must
		// rewrite expression to comply with current cluster version.
		newExpr, err := cdceval.RewritePreviewExpression(sc)
		if err != nil {
			// This is very surprising and fatal.
			return nil, changefeedbase.WithTerminalError(errors.WithHint(err,
				"error upgrading changefeed expression.  Please recreate changefeed manually"))
		}
		if newExpr != sc {
			log.Warningf(ctx,
				"changefeed expression %s (job %d) created prior to 22.2-30 rewritten as %s",
				tree.AsString(sc), spec.JobID,
				tree.AsString(newExpr))
			sc = newExpr
		}
	} else {
		sd.SessionData = *spec.Feed.SessionData
	}

	return cdceval.NewEvaluator(sc, cfg, spec.User(), sd, spec.Feed.StatementTime, withDiff), nil
}

func (c *kvEventToRowConsumer) topicForEvent(eventMeta cdcevent.Metadata) (TopicDescriptor, error) {
	if topic, ok := c.topicDescriptorCache[TopicIdentifier{TableID: eventMeta.TableID, FamilyID: eventMeta.FamilyID}]; ok {
		if topic.GetVersion() == eventMeta.Version {
			return topic, nil
		}
	}
	t, found := c.details.Targets.FindByTableIDAndFamilyName(eventMeta.TableID, eventMeta.FamilyName)
	if found {
		topic, err := makeTopicDescriptorFromSpec(t, eventMeta)
		if err != nil {
			return noTopic{}, err
		}
		c.topicDescriptorCache[topic.GetTopicIdentifier()] = topic
		return topic, nil
	}
	return noTopic{}, errors.AssertionFailedf("no TargetSpecification for row %s",
		eventMeta.DebugString())
}

// ConsumeEvent manages kv event lifetime: parsing, encoding and event being emitted to the sink.
func (c *kvEventToRowConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	if ev.Type() != kvevent.TypeKV {
		return errors.AssertionFailedf("expected kv ev, got %v", ev.Type())
	}

	// Request CPU time to use for event consumption, block if this time is
	// unavailable. If there is unused CPU time left from the last call to
	// Pace, then use that time instead of blocking.
	if err := c.pacer.Pace(ctx); err != nil {
		return err
	}

	schemaTimestamp := ev.KV().Value.Timestamp
	prevSchemaTimestamp := schemaTimestamp
	keyOnly := c.details.Opts.KeyOnly()

	if backfillTs := ev.BackfillTimestamp(); !backfillTs.IsEmpty() {
		schemaTimestamp = backfillTs
		prevSchemaTimestamp = schemaTimestamp.Prev()
	}

	updatedRow, err := c.decoder.DecodeKV(ctx, ev.KV(), cdcevent.CurrentRow, schemaTimestamp, keyOnly)
	if err != nil {
		// Column families are stored contiguously, so we'll get
		// events for each one even if we're not watching them all.
		if errors.Is(err, cdcevent.ErrUnwatchedFamily) {
			return nil
		}
		return err
	}

	// Get prev value, if necessary.
	prevRow, err := func() (cdcevent.Row, error) {
		if !c.details.Opts.GetFilters().WithDiff {
			return cdcevent.Row{}, nil
		}
		return c.decoder.DecodeKV(ctx, ev.PrevKeyValue(), cdcevent.PrevRow, prevSchemaTimestamp, keyOnly)
	}()
	if err != nil {
		// Column families are stored contiguously, so we'll get
		// events for each one even if we're not watching them all.
		if errors.Is(err, cdcevent.ErrUnwatchedFamily) {
			return nil
		}
		return err
	}

	if c.evaluator != nil {
		updatedRow, err = c.evaluator.Eval(ctx, updatedRow, prevRow)
		if err != nil {
			return err
		}

		if !updatedRow.IsInitialized() {
			// Filter did not match.
			c.metrics.FilteredMessages.Inc(1)
			a := ev.DetachAlloc()
			a.Release(ctx)
			return nil
		}
	}

	return c.encodeAndEmit(ctx, updatedRow, prevRow, schemaTimestamp, ev.DetachAlloc())
}

func (c *kvEventToRowConsumer) encodeAndEmit(
	ctx context.Context,
	updatedRow cdcevent.Row,
	prevRow cdcevent.Row,
	schemaTS hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	topic, err := c.topicForEvent(updatedRow.Metadata)
	if err != nil {
		return err
	}

	// Ensure that r updates are strictly newer than the least resolved timestamp
	// being tracked by the local span frontier. The poller should not be forwarding
	// r updates that have timestamps less than or equal to any resolved timestamp
	// it's forwarded before.
	if schemaTS.LessEq(c.frontier.Frontier()) && !schemaTS.Equal(c.cursor) {
		logcrash.ReportOrPanic(ctx, c.sv,
			"cdc ux violation: detected timestamp %s that is less than or equal to the local frontier %s.",
			schemaTS, c.frontier.Frontier())
		return nil
	}

	evCtx := eventContext{
		updated: schemaTS,
		mvcc:    updatedRow.MvccTimestamp,
	}

	if c.topicNamer != nil {
		topic, err := c.topicNamer.Name(topic)
		if err != nil {
			return err
		}
		evCtx.topic = topic
	}

	if c.knobs.BeforeEmitRow != nil {
		if err := c.knobs.BeforeEmitRow(ctx); err != nil {
			return err
		}
	}

	stop := c.metrics.Timers.Encode.Start()
	if c.encodingOpts.Format == changefeedbase.OptFormatParquet {
		return c.encodeForParquet(
			ctx, updatedRow, prevRow, topic, schemaTS, updatedRow.MvccTimestamp,
			c.encodingOpts, alloc,
		)
	}
	var keyCopy, valueCopy []byte
	encodedKey, err := c.encoder.EncodeKey(ctx, updatedRow)
	if err != nil {
		return err
	}
	c.scratch, keyCopy = c.scratch.Copy(encodedKey, 0 /* extraCap */)
	// TODO(yevgeniy): Some refactoring is needed in the encoder: namely, prevRow
	// might not be available at all when working with changefeed expressions.
	encodedValue, err := c.encoder.EncodeValue(ctx, evCtx, updatedRow, prevRow)
	if err != nil {
		return err
	}
	c.scratch, valueCopy = c.scratch.Copy(encodedValue, 0 /* extraCap */)

	// Since we're done processing/converting this event, and will not use much more
	// than len(key)+len(bytes) worth of resources, adjust allocation to match.
	alloc.AdjustBytesToTarget(ctx, int64(len(keyCopy)+len(valueCopy)))
	stop()

	c.metrics.Timers.EmitRow.Time(func() {
		err = c.sink.EmitRow(
			ctx, topic, keyCopy, valueCopy, schemaTS, updatedRow.MvccTimestamp, alloc,
		)
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Warningf(ctx, `sink failed to emit row: %v`, err)
			c.metrics.SinkErrors.Inc(1)
		}
		return err
	}
	if log.V(3) {
		log.Infof(ctx, `r %s: %s -> %s`, updatedRow.TableName, keyCopy, valueCopy)
	}
	return nil
}

// Close closes this consumer.
func (c *kvEventToRowConsumer) Close() error {
	c.pacer.Close()
	if c.evaluator != nil {
		c.evaluator.Close()
	}
	return nil
}

func (c *kvEventToRowConsumer) encodeForParquet(
	ctx context.Context,
	updatedRow cdcevent.Row,
	prevRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	encodingOpts changefeedbase.EncodingOptions,
	alloc kvevent.Alloc,
) error {
	sinkWithEncoder, ok := c.sink.(SinkWithEncoder)
	if !ok {
		return errors.AssertionFailedf("Expected a SinkWithEncoder for parquet format, found %T", c.sink)
	}
	if err := sinkWithEncoder.EncodeAndEmitRow(
		ctx, updatedRow, prevRow, topic, updated, mvcc, encodingOpts, alloc,
	); err != nil {
		return err
	}
	return nil
}

// Flush is a noop for the kvEventToRowConsumer because it does not buffer any events.
func (c *kvEventToRowConsumer) Flush(ctx context.Context) error {
	return nil
}

type parallelEventConsumer struct {
	// g is a group used to manage worker goroutines.
	g ctxgroup.Group

	// hasher is used to shard keys into worker queues.
	hasher hash.Hash32

	metrics *Metrics

	// doneCh is used to shut down all workers when
	// parallelEventConsumer.Close() is called.
	doneCh chan struct{}

	// makeConsumer creates a single-threaded consumer
	// which encodes and emits events.
	makeConsumer func() (eventConsumer, error)

	// numWorkers is the number of worker threads.
	numWorkers int64
	// workerCh stores the event buffer for each worker.
	// It is a fixed size array with length numWorkers.
	workerCh []chan kvevent.Event
	// workerChSize is the maximum number of events a worker can buffer.
	workerChSize int64

	// spanFrontier stores the frontier for the aggregator
	// that spawned this event consumer.
	spanFrontier frontier

	// termErr and termCh are used to save the first error that occurs
	// in any worker and signal all workers to stop.
	//
	// inFlight, waiting, and flushCh are used to flush the sink.
	//
	// flushFrontier tracks the local frontier. It is set to the
	// spanFrontier upon flushing. This guarantees that workers are
	// not buffering events which have timestamps lower than the frontier.
	mu struct {
		syncutil.Mutex
		termErr error

		inFlight      int
		waiting       bool
		flushFrontier hlc.Timestamp
	}
	termCh  chan struct{}
	flushCh chan struct{}
}

var _ eventConsumer = (*parallelEventConsumer)(nil)

func (c *parallelEventConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	startTime := timeutil.Now().UnixNano()
	defer func() {
		time := timeutil.Now().UnixNano()
		c.metrics.ParallelConsumerConsumeNanos.RecordValue(time - startTime)
	}()

	bucket := c.getBucketForEvent(ev)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.termCh:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.mu.termErr
	case c.workerCh[bucket] <- ev:
		c.incInFlight()
		return nil
	}
}

// getBucketForEvent returns a deterministic value  between [0,
// parallelEventConsumer.numWorkers).
//
// This is used to ensure events of the same key get sent to the same worker.
// Events of the same key are sent to the same worker so per-key ordering is
// maintained.
func (c *parallelEventConsumer) getBucketForEvent(ev kvevent.Event) int64 {
	c.hasher.Reset()
	c.hasher.Write(ev.KV().Key)
	return int64(c.hasher.Sum32()) % c.numWorkers
}

func (c *parallelEventConsumer) startWorkers() error {
	// Create the consumers in a separate loop so that returning
	// in case of an error is simple and does not require
	// shutting down goroutines.
	consumers := make([]eventConsumer, c.numWorkers)
	for i := int64(0); i < c.numWorkers; i++ {
		consumer, err := c.makeConsumer()
		if err != nil {
			return err
		}
		consumers[i] = consumer
	}

	for i := int64(0); i < c.numWorkers; i++ {
		c.workerCh[i] = make(chan kvevent.Event, c.workerChSize)

		// c.g.GoCtx requires a func(context.Context), so
		// a closure is used to pass the consumer and id to
		// the worker.
		id := i
		consumer := consumers[i]
		workerClosure := func(ctx2 context.Context) error {
			return c.workerLoop(ctx2, consumer, id)
		}
		c.g.GoCtx(workerClosure)
	}
	return nil
}

func (c *parallelEventConsumer) workerLoop(
	ctx context.Context, consumer eventConsumer, id int64,
) error {
	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Errorf(ctx, "closing consumer: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.doneCh:
			return nil
		case <-c.termCh:
			c.mu.Lock()
			//nolint:deferloop TODO(#137605)
			defer c.mu.Unlock()
			return c.mu.termErr
		case e := <-c.workerCh[id]:
			if err := consumer.ConsumeEvent(ctx, e); err != nil {
				return c.setWorkerError(err)
			}
			c.decInFlight()
		}
	}
}

func (c *parallelEventConsumer) incInFlight() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.inFlight++
	c.metrics.ParallelConsumerInFlightEvents.Update(int64(c.mu.inFlight))
}

func (c *parallelEventConsumer) decInFlight() {
	c.mu.Lock()
	c.mu.inFlight--
	notifyFlush := c.mu.waiting && c.mu.inFlight == 0
	c.mu.Unlock()

	// If someone is waiting on a flush, signal to them
	// that there are no more events.
	if notifyFlush {
		c.flushCh <- struct{}{}
	}
}

func (c *parallelEventConsumer) setWorkerError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.termErr == nil {
		c.mu.termErr = err
		close(c.termCh)
	}

	return err
}

// Flush flushes the consumer by blocking until all events are consumed,
// or until there is an error.
func (c *parallelEventConsumer) Flush(ctx context.Context) error {
	startTime := timeutil.Now().UnixNano()
	defer func() {
		time := timeutil.Now().UnixNano()
		c.metrics.ParallelConsumerFlushNanos.RecordValue(time - startTime)
	}()

	needFlush := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.mu.inFlight > 0 {
			c.mu.waiting = true
		}
		return c.mu.waiting
	}

	if !needFlush() {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.termCh:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.mu.termErr
	case <-c.flushCh:
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.waiting = false
		c.mu.flushFrontier = c.spanFrontier.Frontier()
		return nil
	}
}

func (c *parallelEventConsumer) Frontier() hlc.Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.flushFrontier
}

func (c *parallelEventConsumer) Close() error {
	// Signal the done channel and wait for all workers to finish.
	// If an error occurred, at least one worker will return an error, so
	// it will be returned by c.g.Wait().
	close(c.doneCh)
	return c.g.Wait()
}
