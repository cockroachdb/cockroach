// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// physicalFeedFactory constructs a physical feed which writes into sink and
// runs until the group's context expires.
type physicalFeedFactory interface {
	Run(ctx context.Context, sink kvevent.Writer, cfg physicalConfig) error
}

type physicalConfig struct {
	Spans     []roachpb.Span
	Timestamp hlc.Timestamp
	WithDiff  bool
	Knobs     TestingKnobs
}

type rangefeedFactory func(
	ctx context.Context,
	span roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventC chan<- *roachpb.RangeFeedEvent,
) error

type rangefeed struct {
	memBuf kvevent.Writer
	cfg    physicalConfig
	eventC chan *roachpb.RangeFeedEvent
}

func (p rangefeedFactory) Run(ctx context.Context, sink kvevent.Writer, cfg physicalConfig) error {
	// To avoid blocking raft, RangeFeed puts all entries in a server side
	// buffer. But to keep things simple, it's a small fixed-sized buffer. This
	// means we need to ingest everything we get back as quickly as possible, so
	// we throw it in a buffer here to pick up the slack between RangeFeed and
	// the sink.
	//
	// TODO(dan): Right now, there are two buffers in the changefeed flow when
	// using RangeFeeds, one here and the usual one between the KVFeed and the
	// rest of the changefeed (the latter of which is implemented with an
	// unbuffered channel, and so doesn't actually buffer). Ideally, we'd have
	// one, but the structure of the KVFeed code right now makes this hard.
	// Specifically, when a schema change happens, we need a barrier where we
	// flush out every change before the schema change timestamp before we start
	// emitting any changes from after the schema change. The KVFeed's
	// `SchemaFeed` is responsible for detecting and enforcing these , but the
	// after-KVFeed buffer doesn't have access to any of this state. A cleanup is
	// in order.
	feed := rangefeed{
		memBuf: sink,
		cfg:    cfg,
		eventC: make(chan *roachpb.RangeFeedEvent, 128),
	}
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(feed.addEventsToBuffer)
	for _, span := range cfg.Spans {
		span := span
		g.GoCtx(func(ctx context.Context) error {
			return p(ctx, span, cfg.Timestamp, cfg.WithDiff, feed.eventC)
		})
	}
	return g.Wait()
}

func (p *rangefeed) addEventsToBuffer(ctx context.Context) error {
	var backfillTimestamp hlc.Timestamp
	for {
		select {
		case e := <-p.eventC:
			switch t := e.GetValue().(type) {
			case *roachpb.RangeFeedValue:
				kv := roachpb.KeyValue{Key: t.Key, Value: t.Value}
				var prevVal roachpb.Value
				if p.cfg.WithDiff {
					prevVal = t.PrevValue
				}
				if err := p.memBuf.AddKV(ctx, kv, prevVal, backfillTimestamp); err != nil {
					return err
				}
			case *roachpb.RangeFeedCheckpoint:
				if !t.ResolvedTS.IsEmpty() && t.ResolvedTS.Less(p.cfg.Timestamp) {
					// RangeFeed happily forwards any closed timestamps it receives as
					// soon as there are no outstanding intents under them.
					// Changefeeds don't care about these at all, so throw them out.
					continue
				}
				if err := p.memBuf.AddResolved(ctx, t.Span, t.ResolvedTS, jobspb.ResolvedSpan_NONE); err != nil {
					return err
				}
			default:
				log.Fatalf(ctx, "unexpected RangeFeedEvent variant %v", t)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
