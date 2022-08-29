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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
}

type kvEventToRowConsumer struct {
	frontier  *span.Frontier
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
	frontier *span.Frontier,
	cursor hlc.Timestamp,
	sink EventSink,
	details ChangefeedConfig,
	expr execinfrapb.Expression,
	knobs TestingKnobs,
	errCh chan error,
	cancel func(),
) (eventConsumer, EventSink, error) {
	cfg := flowCtx.Cfg
	evalCtx := flowCtx.EvalCtx

	makeConsumer := func(s EventSink) (eventConsumer, error) {
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

	if changefeedbase.EventConsumerMaxWorkers.Get(&cfg.Settings.SV) == 0 {
		c, err := makeConsumer(sink)
		if err != nil {
			return nil, nil, err
		}
		return c, sink, err
	}

	ss := &safeSink{wrapped: sink}
	c := &parallelEventConsumer{
		g:          ctxgroup.WithContext(ctx),
		spawnAfter: timeutil.NewTimer(),
		eventCh:    make(chan kvevent.Event, 128),
		doneCh:     make(chan struct{}),
		makeConsumer: func() (eventConsumer, error) {
			return makeConsumer(ss)
		},
		maxWorkers: func() int64 {
			return changefeedbase.EventConsumerMaxWorkers.Get(&cfg.Settings.SV)
		},

		// Use to bubble up errors asynchronously to changefeed processors.
		errCh:  errCh,
		cancel: cancel,
	}

	c.trySpawn()

	return c, ss, nil
}

func newKVEventToRowConsumer(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	evalCtx *eval.Context,
	frontier *span.Frontier,
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
		prevKV := roachpb.KeyValue{Key: ev.KV().Key, Value: ev.PrevValue()}
		return c.decoder.DecodeKV(ctx, prevKV, prevSchemaTimestamp)
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
	if err := c.sink.EmitRow(
		ctx, topic,
		keyCopy, valueCopy, schemaTimestamp, mvccTimestamp, ev.DetachAlloc(),
	); err != nil {
		return err
	}
	if log.V(3) {
		log.Infof(ctx, `r %s: %s -> %s`, updatedRow.TableName, keyCopy, valueCopy)
	}
	return nil
}

func (c *kvEventToRowConsumer) Close() error {
	return nil
}

type parallelEventConsumer struct {
	g            ctxgroup.Group
	spawnAfter   *timeutil.Timer
	eventCh      chan kvevent.Event
	doneCh       chan struct{}
	maxWorkers   func() int64
	makeConsumer func() (eventConsumer, error)
	errCh        chan error
	cancel       func()

	mu struct {
		syncutil.Mutex
		numWorkers int64
	}
}

var _ eventConsumer = (*parallelEventConsumer)(nil)

const spawnWorkerDelay = 50 * time.Millisecond

func (c *parallelEventConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	c.spawnAfter.Reset(spawnWorkerDelay)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.eventCh <- ev:
			return nil
		case <-c.spawnAfter.C:
			c.spawnAfter.Read = true
			c.trySpawn()
			c.spawnAfter.Reset(spawnWorkerDelay)
		}
	}
}

func (c *parallelEventConsumer) trySpawn() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.numWorkers > c.maxWorkers() {
		return
	}

	c.mu.numWorkers++
	c.g.GoCtx(c.workerLoop)
}

// jitter adds a small jitter in the given duration.
func idleAfter() time.Duration {
	const idleAfter = 2 * time.Minute
	const jitter = 1.0 / 6.0
	jitterFraction := 1 + (2*rand.Float64()-1)*jitter // 1 + [-1/6, +1/6)
	return time.Duration(float64(idleAfter) * jitterFraction)
}

func (c *parallelEventConsumer) workerLoop(ctx context.Context) error {
	consumer, err := c.makeConsumer()
	if err != nil {
		c.setWorkerError(err)
		return nil
	}

	idleTimer := timeutil.NewTimer()
	defer idleTimer.Stop()

	for {
		idleTimer.Reset(idleAfter())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.doneCh:
			return nil
		case e := <-c.eventCh:
			if err := consumer.ConsumeEvent(ctx, e); err != nil {
				c.setWorkerError(err)
				return err
			}
		case <-idleTimer.C:
			idleTimer.Read = true

			c.mu.Lock()
			shutDown := c.mu.numWorkers > 1
			if shutDown {
				c.mu.numWorkers--
			}
			c.mu.Unlock()
			if shutDown {
				return nil
			}
		}
	}
}

func (c *parallelEventConsumer) setWorkerError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.errCh <- err
	c.cancel()
}

func (c *parallelEventConsumer) Close() error {
	defer c.spawnAfter.Stop()
	close(c.doneCh)
	return c.g.Wait()
}
