// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// FileIDSource hands out unique file IDs. Production: a wrapper over
// the cluster's unique-int generator. Tests: an atomic counter.
type FileIDSource interface {
	Next() int64
}

// TickSink is the producer's output side. After each round of work,
// the producer calls Flush exactly once with two pieces of info:
// any data files that were just durably written (each tagged with
// the tick they belong to) and any rangefeed checkpoints observed
// since the previous Flush. The two are coupled — every reported
// checkpoint's covered events have already been flushed — so the
// coordinator can apply both atomically.
//
// TickManager (in-process) implements this directly. In the
// distsql split, a metadata-emitting wrapper implements it on the
// producer side and the real TickManager lives on the coordinator.
type TickSink interface {
	Flush(ctx context.Context, msg *revlogpb.Flush) error
}

// Producer buffers KV events received via push (OnValue) into per-tick
// in-memory buckets, then on each checkpoint (OnCheckpoint) flushes
// every tick whose end has been crossed by the rangefeed frontier.
// Producers are wired to a TickSink that receives the resulting file
// descriptors and frontier advances.
//
// Producer is the per-node piece of the revlog write path: in
// production its OnValue / OnCheckpoint are bound directly to
// rangefeed.WithOnValue / WithOnCheckpoint via a tiny adapter that
// unpacks the kvpb wire types into the plain-args API here.
type Producer struct {
	es        cloud.ExternalStorage
	tickWidth time.Duration
	startHLC  hlc.Timestamp
	fileIDs   FileIDSource
	sink      TickSink

	// frontier tracks the resolved time of every covered span. It
	// is internal: the producer uses it only to decide which ticks
	// are safe to flush (all events for the tick have been seen).
	// The coordinator builds its own frontier from the raw
	// checkpoints we forward in each Flush.
	frontier span.Frontier

	// open holds buffered KV events keyed by the tick_end the event
	// belongs to. An entry is removed when its tick is flushed.
	open map[hlc.Timestamp]*tickBuffer

	// flushedThrough is the highest tick_end the producer has
	// already flushed. Subsequent flushes only consider ticks with
	// end > flushedThrough, so re-entering OnCheckpoint with the
	// same frontier is idempotent.
	flushedThrough hlc.Timestamp

	// pendingFiles and pendingCheckpoints accumulate between
	// Flushes. Both are reset on each call to sink.Flush.
	pendingFiles       []revlogpb.FlushedFile
	pendingCheckpoints []kvpb.RangeFeedCheckpoint
}

// tickBuffer holds the events for a single open tick.
type tickBuffer struct {
	// TODO(dt): track total byte size for size-triggered
	// flushes (multi-file per tick, flush_order > 0).
	events []bufferedEvent
}

// bufferedEvent is the in-memory shape of a KV awaiting flush.
type bufferedEvent struct {
	key       roachpb.Key
	timestamp hlc.Timestamp
	value     []byte
	prevValue []byte
}

// NewProducer constructs a Producer. spans must be non-empty;
// tickWidth must be positive; sink must be non-nil. The producer's
// internal frontier is initialized at startHLC, so the first
// closeable tick is the one whose tick_end is the smallest boundary
// > startHLC.
func NewProducer(
	es cloud.ExternalStorage,
	spans []roachpb.Span,
	startHLC hlc.Timestamp,
	tickWidth time.Duration,
	fileIDs FileIDSource,
	sink TickSink,
) (*Producer, error) {
	if len(spans) == 0 {
		return nil, errors.AssertionFailedf("revlogjob: NewProducer requires at least one span")
	}
	if tickWidth <= 0 {
		return nil, errors.AssertionFailedf("revlogjob: tickWidth must be positive (got %s)", tickWidth)
	}
	if sink == nil {
		return nil, errors.AssertionFailedf("revlogjob: sink must be non-nil")
	}
	// TODO(dt): support replan / dynamic re-partitioning rather than
	// taking a static span set at construction.
	f, err := span.MakeFrontierAt(startHLC, spans...)
	if err != nil {
		return nil, errors.Wrap(err, "creating span frontier")
	}
	return &Producer{
		es:             es,
		tickWidth:      tickWidth,
		startHLC:       startHLC,
		fileIDs:        fileIDs,
		sink:           sink,
		frontier:       f,
		open:           make(map[hlc.Timestamp]*tickBuffer),
		flushedThrough: startHLC,
	}, nil
}

// OnValue records one KV event. Designed to be called from the
// rangefeed.OnValue callback (via an adapter that unpacks
// *kvpb.RangeFeedValue into these plain args).
//
// Events with ts <= startHLC are dropped defensively — the rangefeed
// subscription should already filter them out.
func (p *Producer) OnValue(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp, value, prevValue []byte,
) {
	if !p.startHLC.Less(ts) {
		return
	}
	te := tickEndFor(ts, p.tickWidth)
	buf, ok := p.open[te]
	if !ok {
		buf = &tickBuffer{}
		p.open[te] = buf
	}
	buf.events = append(buf.events, bufferedEvent{
		key:       key,
		timestamp: ts,
		value:     value,
		prevValue: prevValue,
	})
}

// OnCheckpoint records a rangefeed checkpoint, advances the
// internal frontier, flushes any tick whose end has now been crossed,
// then emits a single Flush carrying the just-flushed files and the
// just-observed checkpoint. The coordinator forwards each
// checkpoint into its own multi-span frontier; once that aggregate
// crosses a tick boundary, the manifest is written using the
// accumulated files.
//
// Designed to be called from the rangefeed.OnCheckpoint callback
// (via an adapter that unpacks *kvpb.RangeFeedCheckpoint).
func (p *Producer) OnCheckpoint(ctx context.Context, sp roachpb.Span, ts hlc.Timestamp) error {
	p.pendingCheckpoints = append(p.pendingCheckpoints,
		kvpb.RangeFeedCheckpoint{Span: sp, ResolvedTS: ts})
	if _, err := p.frontier.Forward(sp, ts); err != nil {
		return errors.Wrap(err, "advancing frontier")
	}
	frontier := p.frontier.Frontier()
	for {
		next := nextTickEndAfter(p.flushedThrough, p.tickWidth)
		if frontier.Less(next) {
			break
		}
		if err := p.flushTick(ctx, next); err != nil {
			return err
		}
		p.flushedThrough = next
	}
	msg := &revlogpb.Flush{
		Files:       p.pendingFiles,
		Checkpoints: p.pendingCheckpoints,
	}
	p.pendingFiles = nil
	p.pendingCheckpoints = nil
	return p.sink.Flush(ctx, msg)
}

// flushTick writes any buffered events for tickEnd as one SST data file
// and appends a FlushedFile entry to pendingFiles for the next Flush
// emission. Empty ticks (no buffered events) flush nothing — the
// coordinator's TickManager will still write a manifest for them when
// its aggregate frontier crosses the boundary.
//
// TODO(dt): support size/time-triggered intra-tick flushes producing
// multiple files per tick (flush_order > 0).
func (p *Producer) flushTick(ctx context.Context, tickEnd hlc.Timestamp) error {
	buf, ok := p.open[tickEnd]
	delete(p.open, tickEnd)
	if !ok || len(buf.events) == 0 {
		return nil
	}
	sort.Slice(buf.events, func(i, j int) bool {
		if c := bytes.Compare(buf.events[i].key, buf.events[j].key); c != 0 {
			return c < 0
		}
		return buf.events[i].timestamp.Less(buf.events[j].timestamp)
	})
	// Drop adjacent (key, ts) duplicates. Rangefeed may replay
	// already-delivered events after a reconnect; (key, ts)
	// uniquely identifies an MVCC revision, so any such duplicates
	// carry the same value and dropping them is safe. Without this
	// the SSTable writer would reject the second occurrence ("keys
	// must be added in strictly increasing order").
	deduped := buf.events[:0]
	for _, ev := range buf.events {
		if n := len(deduped); n > 0 {
			last := deduped[n-1]
			if last.timestamp.Equal(ev.timestamp) && bytes.Equal(last.key, ev.key) {
				continue
			}
		}
		deduped = append(deduped, ev)
	}
	buf.events = deduped

	tw, err := revlog.NewTickWriter(ctx, p.es, tickEnd, p.fileIDs.Next(), 0 /* flushOrder */)
	if err != nil {
		return errors.Wrapf(err, "opening tick writer for %s", tickEnd)
	}
	for _, ev := range buf.events {
		if err := tw.Add(ev.key, ev.timestamp, ev.value, ev.prevValue); err != nil {
			_, _, _ = tw.Close()
			return errors.Wrapf(err, "adding event to tick %s", tickEnd)
		}
	}
	f, stats, err := tw.Close()
	if err != nil {
		return errors.Wrapf(err, "closing tick writer for %s", tickEnd)
	}
	p.pendingFiles = append(p.pendingFiles, revlogpb.FlushedFile{
		TickEnd: tickEnd, File: f, Stats: stats,
	})
	return nil
}

// tickEndFor returns the tick boundary that an event timestamp
// belongs to: the smallest hlc.Timestamp >= ts whose WallTime is a
// multiple of tickWidth and whose Logical is zero.
func tickEndFor(ts hlc.Timestamp, tickWidth time.Duration) hlc.Timestamp {
	widthNanos := int64(tickWidth)
	w := ts.WallTime
	boundary := ((w + widthNanos - 1) / widthNanos) * widthNanos
	if boundary == w && ts.Logical > 0 {
		boundary += widthNanos
	}
	return hlc.Timestamp{WallTime: boundary}
}

// nextTickEndAfter returns the smallest tick boundary strictly after
// prev (HLC-wise).
func nextTickEndAfter(prev hlc.Timestamp, tickWidth time.Duration) hlc.Timestamp {
	widthNanos := int64(tickWidth)
	return hlc.Timestamp{WallTime: (prev.WallTime/widthNanos + 1) * widthNanos}
}
