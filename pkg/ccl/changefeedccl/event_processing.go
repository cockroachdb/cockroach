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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
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
	encoder   Encoder
	scratch   bufalloc.ByteAllocator
	sink      EventSink
	cursor    hlc.Timestamp
	knobs     TestingKnobs
	decoder   cdcevent.Decoder
	details   ChangefeedConfig
	evaluator *cdceval.Evaluator
	safeExpr  string

	topicDescriptorCache map[TopicIdentifier]TopicDescriptor
	topicNamer           *TopicNamer
}

func newEventConsumer(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	feed ChangefeedConfig,
	spanFrontier *span.Frontier,
	cursor hlc.Timestamp,
	sink EventSink,
	details ChangefeedConfig,
	expr execinfrapb.Expression,
	knobs TestingKnobs,
	metrics *Metrics,
	isSinkless bool,
) (eventConsumer, EventSink, error) {
	cfg := flowCtx.Cfg
	evalCtx := flowCtx.EvalCtx

	makeConsumer := func(s EventSink, frontier frontier) (eventConsumer, error) {
		var err error
		encodingOpts, err := feed.Opts.GetEncodingOptions()
		if err != nil {
			return nil, err
		}
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

		return newKVEventToRowConsumer(ctx, cfg, evalCtx, frontier, cursor, s,
			encoder, details, expr, knobs, topicNamer)
	}

	// TODO (jayshrivastava) enable parallel consumers for sinkless changefeeds
	numWorkers := changefeedbase.EventConsumerWorkers.Get(&cfg.Settings.SV)
	if numWorkers == 0 {
		// Pick a reasonable default.
		numWorkers = defaultNumWorkers()
	}
	if numWorkers <= 1 || isSinkless {
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
	cfg *execinfra.ServerConfig,
	evalCtx *eval.Context,
	frontier frontier,
	cursor hlc.Timestamp,
	sink EventSink,
	encoder Encoder,
	details ChangefeedConfig,
	expr execinfrapb.Expression,
	knobs TestingKnobs,
	topicNamer *TopicNamer,
) (*kvEventToRowConsumer, error) {
	includeVirtual := details.Opts.IncludeVirtual()
	decoder, err := cdcevent.NewEventDecoder(ctx, cfg, details.Targets, includeVirtual)

	if err != nil {
		return nil, err
	}

	var evaluator *cdceval.Evaluator
	var safeExpr string
	if expr.Expr != "" {
		expr, err := cdceval.ParseChangefeedExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		safeExpr = tree.AsString(expr)
		evaluator, err = cdceval.NewEvaluator(evalCtx, expr)
		if err != nil {
			return nil, err
		}
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
		safeExpr:             safeExpr,
	}, nil
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
	return noTopic{}, errors.AssertionFailedf("no TargetSpecification for row %s", eventMeta)
}

// ConsumeEvent manages kv event lifetime: parsing, encoding and event being emitted to the sink.
func (c *kvEventToRowConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	if ev.Type() != kvevent.TypeKV {
		return errors.AssertionFailedf("expected kv ev, got %v", ev.Type())
	}

	schemaTimestamp := ev.KV().Value.Timestamp
	prevSchemaTimestamp := schemaTimestamp
	mvccTimestamp := ev.MVCCTimestamp()

	if backfillTs := ev.BackfillTimestamp(); !backfillTs.IsEmpty() {
		schemaTimestamp = backfillTs
		prevSchemaTimestamp = schemaTimestamp.Prev()
	}

	updatedRow, err := c.decoder.DecodeKV(ctx, ev.KV(), schemaTimestamp)
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
		return c.decoder.DecodeKV(ctx, ev.PrevKeyValue(), prevSchemaTimestamp)
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
		matches, err := c.evaluator.MatchesFilter(ctx, updatedRow, mvccTimestamp, prevRow)
		if err != nil {
			return errors.Wrapf(err, "while matching filter: %s", c.safeExpr)
		}

		if !matches {
			// TODO(yevgeniy): Add metrics
			a := ev.DetachAlloc()
			a.Release(ctx)
			return nil
		}

		projection, err := c.evaluator.Projection(ctx, updatedRow, mvccTimestamp, prevRow)
		if err != nil {
			return errors.Wrapf(err, "while evaluating projection: %s", c.safeExpr)
		}
		updatedRow = projection

		// Clear out prevRow.  Projection can already emit previous row; thus
		// it would be superfluous to also encode prevRow.
		prevRow = cdcevent.Row{}
	}

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
	if schemaTimestamp.LessEq(c.frontier.Frontier()) && !schemaTimestamp.Equal(c.cursor) {
		log.Errorf(ctx, "cdc ux violation: detected timestamp %s that is less than "+
			"or equal to the local frontier %s.", schemaTimestamp, c.frontier.Frontier())
		return nil
	}

	evCtx := eventContext{
		updated: schemaTimestamp,
		mvcc:    mvccTimestamp,
	}

	if c.topicNamer != nil {
		topic, err := c.topicNamer.Name(topic)
		if err != nil {
			return err
		}
		evCtx.topic = topic
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

	if c.knobs.BeforeEmitRow != nil {
		if err := c.knobs.BeforeEmitRow(ctx); err != nil {
			return err
		}
	}

	// Since we're done processing/converting this event, and will not use much more
	// than len(key)+len(bytes) worth of resources, adjust allocation to match.
	a := ev.DetachAlloc()
	a.AdjustBytesToTarget(ctx, int64(len(keyCopy)+len(valueCopy)))

	if err := c.sink.EmitRow(
		ctx, topic, keyCopy, valueCopy, schemaTimestamp, mvccTimestamp, a,
	); err != nil {
		return err
	}
	if log.V(3) {
		log.Infof(ctx, `r %s: %s -> %s`, updatedRow.TableName, keyCopy, valueCopy)
	}
	return nil
}

// Close is a noop for the kvEventToRowConsumer because it
// has no goroutines in flight.
func (c *kvEventToRowConsumer) Close() error {
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
		c.metrics.ParallelConsumerConsumeNanos.Inc(time - startTime)
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
	c.mu.Unlock()
	c.metrics.ParallelConsumerInFlightEvents.Inc(1)
}

func (c *parallelEventConsumer) decInFlight() {
	c.mu.Lock()
	c.mu.inFlight--
	c.metrics.ParallelConsumerInFlightEvents.Dec(1)
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
		c.metrics.ParallelConsumerFlushNanos.Inc(time - startTime)
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
