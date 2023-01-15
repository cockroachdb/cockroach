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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// physicalFeedFactory constructs a physical feed which writes into sink and
// runs until the group's context expires.
type physicalFeedFactory interface {
	Run(ctx context.Context, sink kvevent.Writer, cfg rangeFeedConfig) error
}

type rangeFeedConfig struct {
	Frontier hlc.Timestamp
	Spans    []kvcoord.SpanTimePair
	WithDiff bool
	Knobs    TestingKnobs
	UseMux   bool
}

type rangefeedFactory func(
	ctx context.Context,
	spans []kvcoord.SpanTimePair,
	withDiff bool,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error

type rangefeed struct {
	memBuf kvevent.Writer
	cfg    rangeFeedConfig
	eventC chan kvcoord.RangeFeedMessage
	knobs  TestingKnobs
}

func (p rangefeedFactory) Run(ctx context.Context, sink kvevent.Writer, cfg rangeFeedConfig) error {
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
		eventC: make(chan kvcoord.RangeFeedMessage, 128),
		knobs:  cfg.Knobs,
	}
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(feed.addEventsToBuffer)
	var rfOpts []kvcoord.RangeFeedOption
	if cfg.UseMux {
		rfOpts = append(rfOpts, kvcoord.WithMuxRangeFeed())
	}

	g.GoCtx(func(ctx context.Context) error {
		return p(ctx, cfg.Spans, cfg.WithDiff, feed.eventC, rfOpts...)
	})
	return g.Wait()
}

func (p *rangefeed) addEventsToBuffer(ctx context.Context) error {
	for {
		select {
		case e := <-p.eventC:
			switch t := e.GetValue().(type) {
			case *roachpb.RangeFeedValue:
				if p.cfg.Knobs.OnRangeFeedValue != nil {
					if err := p.cfg.Knobs.OnRangeFeedValue(); err != nil {
						return err
					}
				}
				if p.knobs.ModifyTimestamps != nil {
					e = kvcoord.RangeFeedMessage{RangeFeedEvent: e.ShallowCopy(), RegisteredSpan: e.RegisteredSpan}
					p.knobs.ModifyTimestamps(&e.Val.Value.Timestamp)
				}
				if err := p.memBuf.Add(
					ctx, kvevent.MakeKVEvent(e.RangeFeedEvent),
				); err != nil {
					return err
				}
			case *roachpb.RangeFeedCheckpoint:
				if p.knobs.ModifyTimestamps != nil {
					p.knobs.ModifyTimestamps(&t.ResolvedTS)
				}
				if !t.ResolvedTS.IsEmpty() && t.ResolvedTS.Less(p.cfg.Frontier) {
					// RangeFeed happily forwards any closed timestamps it receives as
					// soon as there are no outstanding intents under them.
					// Changefeeds don't care about these at all, so throw them out.
					continue
				}
				if p.knobs.ShouldSkipCheckpoint != nil && p.knobs.ShouldSkipCheckpoint(t) {
					continue
				}
				if err := p.memBuf.Add(
					ctx, kvevent.MakeResolvedEvent(e.RangeFeedEvent, jobspb.ResolvedSpan_NONE),
				); err != nil {
					return err
				}
			case *roachpb.RangeFeedSSTable:
				// For now, we just error on SST ingestion, since we currently don't
				// expect SST ingestion into spans with active changefeeds.
				return errors.Errorf("unexpected SST ingestion: %v", t)

			case *roachpb.RangeFeedDeleteRange:
				// For now, we just ignore on MVCC range tombstones. These are currently
				// only expected to be used by schema GC and IMPORT INTO, and such spans
				// should not have active changefeeds across them, at least at the times
				// of interest. A case where one will show up in a changefeed is when
				// the primary index changes while we're watching it and then the old
				// primary index is dropped. In this case, we'll get a schema event to
				// restart into the new primary index, but the DeleteRange may come
				// through before the schema event.
				//
				// TODO(erikgrinaker): Write an end-to-end test which verifies that an
				// IMPORT INTO which gets rolled back using MVCC range tombstones will
				// not be visible to a changefeed, neither when it was started before
				// the import or when resuming from a timestamp before the import. The
				// table decriptor should be marked as offline during the import, and
				// catchup scans should detect that this happened and prevent reading
				// anything in that timespan. See:
				// https://github.com/cockroachdb/cockroach/issues/70433
				continue

			default:
				return errors.Errorf("unexpected RangeFeedEvent variant %v", t)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
