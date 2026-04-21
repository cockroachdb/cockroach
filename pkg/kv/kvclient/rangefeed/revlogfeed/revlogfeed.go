// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package revlogfeed provides a rangefeed.DB implementation that serves
// the catch-up portion of a rangefeed from a continuous-backup revision
// log on external storage, then hands off to a live KV rangefeed.
//
// See docs/RFCS/20260420_continuous_backup.md §3 for the design.
package revlogfeed

import (
	"context"
	"iter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// tickSource is the subset of *revlog.LogReader that the wrapper
// depends on. It exists so tests can supply a fake without standing up
// real external storage. *revlog.LogReader satisfies it directly; in
// production the constructor is called with one.
type tickSource interface {
	Ticks(ctx context.Context, start, end hlc.Timestamp) iter.Seq2[revlog.Tick, error]
}

// defaultHandoffThreshold is the residual catch-up window below which
// the wrapper opens the live KV rangefeed. Chosen to leave comfortable
// headroom over the live rangefeed's catch-up scan against KV's GC TTL.
const defaultHandoffThreshold = 20 * time.Minute

// Options configures a revlogfeed-backed DB.
type Options struct {
	// HandoffThreshold is the residual catch-up window
	// (now - latestDrainedTick) at or below which the wrapper stops
	// draining revlog and opens the live KV rangefeed. Zero means use
	// defaultHandoffThreshold.
	HandoffThreshold time.Duration

	// FreshnessBudget is the maximum allowed gap between the freshest
	// closed tick the source advertises and "now" at the moment of
	// pre-flight. If the gap exceeds it, RangeFeed fails fast rather
	// than entering a drain loop that would spin until its
	// max-iteration guard. Settable per-call so tests can drive
	// staleness scenarios deterministically.
	//
	// TODO(aerin): pick a principled default. For now, fall back to
	// HandoffThreshold; the two answer related questions ("is the log
	// keeping up?") and using the same value avoids surprising
	// asymmetry between pre-flight and the drain loop.
	FreshnessBudget time.Duration
}

// DB is a rangefeed.DB implementation that serves catch-up reads from a
// revlog and then delegates to a live KV rangefeed for the tail.
//
// Lifecycle: each call to RangeFeed (or RangeFeedFromFrontier) drains
// closed ticks from src that fall in (consumer.startFrom, T*] and emits
// them on eventC as RangeFeedValue + per-tick RangeFeedCheckpoint, then
// invokes live.RangeFeed with startFrom = T* to deliver the live tail.
//
// Coverage gaps are surfaced as a hard error before any events are
// emitted; this package never silently falls back to live KV.
type DB struct {
	src  tickSource
	live rangefeed.DB
	opts Options
}

var _ rangefeed.DB = (*DB)(nil)

// New constructs a revlog-backed rangefeed.DB. live is the underlying
// live KV adapter (typically the one returned by rangefeed.NewFactory)
// and is used for the tail phase after revlog drain completes. src is
// typically a *revlog.LogReader.
func New(src tickSource, live rangefeed.DB, opts Options) *DB {
	if opts.HandoffThreshold == 0 {
		opts.HandoffThreshold = defaultHandoffThreshold
	}
	if opts.FreshnessBudget == 0 {
		opts.FreshnessBudget = opts.HandoffThreshold
	}
	return &DB{src: src, live: live, opts: opts}
}

// checkCoverage verifies that the configured tickSource can serve a
// catch-up read from startFrom up to (close to) now. Returns nil on
// success; otherwise returns an error identifying the failure.
//
// The check has two parts:
//
//  1. Contiguity. The yielded ticks must form a chain starting at or
//     before startFrom (no gap at the front) and with no holes between
//     adjacent ticks. A hole is reported as "(prev.TickEnd, next.TickStart]".
//  2. Freshness. The freshest tick's TickEnd must be within
//     opts.FreshnessBudget of now. Without this check, a producer that
//     stopped writing hours ago would silently pass pre-flight, then
//     the drain loop would spin until its max-iteration guard
//     eventually fired — a slow, opaque failure for what is
//     structurally diagnosable up front.
//
// "now" is a parameter (not read from a clock) so tests can drive
// staleness scenarios deterministically and so the caller controls
// what "the present" means in the surrounding RangeFeed call.
func (d *DB) checkCoverage(ctx context.Context, startFrom, now hlc.Timestamp) error {
	prevEnd := startFrom
	any := false
	for tick, err := range d.src.Ticks(ctx, startFrom, now) {
		if err != nil {
			return errors.Wrap(err, "enumerating revlog ticks")
		}
		if tick.Manifest.TickStart.LessEq(prevEnd) {
			// Contiguous with the previous tick (or covers startFrom on
			// the first iteration). Advance the cursor.
			prevEnd = tick.Manifest.TickEnd
			any = true
			continue
		}
		// Gap: previous tick ended at prevEnd, next begins later.
		return errors.Newf(
			"revlog missing coverage for window (%s, %s]",
			prevEnd, tick.Manifest.TickStart,
		)
	}
	if !any {
		return errors.Newf(
			"revlog missing coverage for window (%s, %s]",
			startFrom, now,
		)
	}
	residual := now.WallTime - prevEnd.WallTime
	if residual > d.opts.FreshnessBudget.Nanoseconds() {
		return errors.Newf(
			"revlog freshest tick %s is %s behind now (%s); exceeds budget %s",
			prevEnd, time.Duration(residual), now, d.opts.FreshnessBudget,
		)
	}
	return nil
}

// RangeFeed implements rangefeed.DB.
func (d *DB) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	return errors.AssertionFailedf("revlogfeed.DB.RangeFeed: not implemented")
}

// RangeFeedFromFrontier implements rangefeed.DB.
func (d *DB) RangeFeedFromFrontier(
	ctx context.Context,
	frontier span.Frontier,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	return errors.AssertionFailedf("revlogfeed.DB.RangeFeedFromFrontier: not implemented")
}

// Scan implements rangefeed.DB. The revlog format does not include base
// snapshots, so initial scans are delegated to the wrapped live DB.
func (d *DB) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func([]kvpb.RangeFeedValue),
	cfg rangefeed.ScanConfig,
) error {
	return d.live.Scan(ctx, spans, asOf, rowFn, rowsFn, cfg)
}
