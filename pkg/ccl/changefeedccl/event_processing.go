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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// eventContext holds metadata pertaining to event.
type eventContext struct {
	updated, mvcc hlc.Timestamp
	// topic is set to the string to be included if TopicInValue is true
	topic string
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
