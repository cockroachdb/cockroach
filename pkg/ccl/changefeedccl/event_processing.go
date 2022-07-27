// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"hash"
	"hash/crc32"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// pacerLogEvery is used for logging errors instead of returning terminal
// errors when pacer.Pace returns an error.
var pacerLogEvery log.EveryN = log.Every(100 * time.Millisecond)

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
	encoder        Encoder
	scratch        bufalloc.ByteAllocator
	sink           EventSink
	cursor         hlc.Timestamp
	knobs          TestingKnobs
	decoder        cdcevent.Decoder
	details        ChangefeedConfig
	evaluator      *cdceval.Evaluator
	encodingFormat changefeedbase.FormatType

	topicDescriptorCache map[TopicIdentifier]TopicDescriptor
	topicNamer           *TopicNamer

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
	spanFrontier *span.Frontier,
	cursor hlc.Timestamp,
	sink EventSink,
	metrics *Metrics,
	knobs TestingKnobs,
) (eventConsumer, EventSink, error) {
	encodingOpts, err := feed.Opts.GetEncodingOptions()
	if err != nil {
		return nil, nil, err
	}

	pacerRequestUnit := changefeedbase.EventConsumerPacerRequestSize.Get(&cfg.Settings.SV)
	enablePacer := changefeedbase.EventConsumerElasticCPUControlEnabled.Get(&cfg.Settings.SV)

	makeConsumer := func(s EventSink, frontier frontier) (eventConsumer, error) {
		var err error
		encoder, err := getEncoder(encodingOpts, feed.Targets)
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
			tenantID, ok := roachpb.TenantFromContext(ctx)
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
			encoder, feed, spec.Select, spec.User(), knobs, topicNamer, pacer)
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
	ss := &safeSink{wrapped: sink, beforeFlush: c.Flush}
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
	expr execinfrapb.Expression,
	userName username.SQLUsername,
	knobs TestingKnobs,
	topicNamer *TopicNamer,
	pacer *admission.Pacer,
) (_ *kvEventToRowConsumer, err error) {
	includeVirtual := details.Opts.IncludeVirtual()
	keyOnly := details.Opts.KeyOnly()
	decoder, err := cdcevent.NewEventDecoder(ctx, cfg, details.Targets, includeVirtual, keyOnly)
	if err != nil {
		return nil, err
	}

	var evaluator *cdceval.Evaluator
	if expr.Expr != "" {
		evaluator, err = newEvaluator(cfg, userName, expr)
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
		encodingFormat:       encodingOpts.Format,
		pacer:                pacer,
	}, nil
}

func newEvaluator(
	cfg *sql.ExecutorConfig, user username.SQLUsername, expr execinfrapb.Expression,
) (*cdceval.Evaluator, error) {
	sc, err := cdceval.ParseChangefeedExpression(expr.Expr)
	if err != nil {
		return nil, err
	}

	return cdceval.NewEvaluator(sc, cfg, user)
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
		if pacerLogEvery.ShouldLog() {
			log.Errorf(ctx, "automatic pacing: %v", err)
		}
	}

	schemaTimestamp := ev.KV().Value.Timestamp
	prevSchemaTimestamp := schemaTimestamp
	keyOnly := c.details.Opts.KeyOnly()

	if backfillTs := ev.BackfillTimestamp(); !backfillTs.IsEmpty() {
		schemaTimestamp = backfillTs
		prevSchemaTimestamp = schemaTimestamp.Prev()
	}

	updatedRow, err := c.decoder.DecodeKV(ctx, ev.KV(), schemaTimestamp, keyOnly)
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
		return c.decoder.DecodeKV(ctx, ev.PrevKeyValue(), prevSchemaTimestamp, keyOnly)
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
		projection, err := c.evaluator.Eval(ctx, updatedRow, prevRow)
		if err != nil {
			return err
		}

		if !projection.IsInitialized() {
			// Filter did not match.
			// TODO(yevgeniy): Add metrics.
			a := ev.DetachAlloc()
			a.Release(ctx)
			return nil
		}

		// Clear out prevRow.  Projection can already emit previous row; thus
		// it would be superfluous to also encode prevRow.
		updatedRow, prevRow = projection, cdcevent.Row{}
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
	// TODO(dan): This should be an assertion once we're confident this can never
	// happen under any circumstance.
	if schemaTS.LessEq(c.frontier.Frontier()) && !schemaTS.Equal(c.cursor) {
		log.Errorf(ctx, "cdc ux violation: detected timestamp %s that is less than "+
			"or equal to the local frontier %s.", schemaTS, c.frontier.Frontier())
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

	if c.encodingFormat == changefeedbase.OptFormatParquet {
		return c.encodeForParquet(
			ctx, updatedRow, prevRow, topic, schemaTS, updatedRow.MvccTimestamp, alloc,
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

	if err := c.sink.EmitRow(
		ctx, topic, keyCopy, valueCopy, schemaTS, updatedRow.MvccTimestamp, alloc,
	); err != nil {
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
	alloc kvevent.Alloc,
) error {
	sinkWithEncoder, ok := c.sink.(SinkWithEncoder)
	if !ok {
		return errors.AssertionFailedf("Expected a SinkWithEncoder for parquet format, found %T", c.sink)
	}
	if err := sinkWithEncoder.EncodeAndEmitRow(
		ctx, updatedRow, prevRow, topic, updated, mvcc, alloc,
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
	spanFrontier *span.Frontier

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
	c.mu.inFlight++
	c.metrics.ParallelConsumerInFlightEvents.Update(int64(c.mu.inFlight))
	c.mu.Unlock()
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
		c.mu.waiting = false
		c.mu.flushFrontier = c.spanFrontier.Frontier()
		c.mu.Unlock()
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
