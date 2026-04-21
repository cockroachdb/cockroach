// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvfeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/timers"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// physicalFeedFactory constructs a physical feed which writes into sink and
// runs until the group's context expires.
type physicalFeedFactory interface {
	Run(ctx context.Context, sink kvevent.Writer, cfg rangeFeedConfig) error
}

// rangeFeedConfig contains configuration options for creating a rangefeed.
// It provides an abstraction over the actual rangefeed API.
type rangeFeedConfig struct {
	Frontier             hlc.Timestamp
	Spans                []kvcoord.SpanTimePair
	WithDiff             bool
	WithFiltering        bool
	WithFrontierQuantize time.Duration
	ConsumerID           int64
	Knobs                TestingKnobs
	Timers               *timers.ScopedTimers
}

// rangefeedClientFactory implements physicalFeedFactory using the high-level
// rangefeed.Factory client with callback-based event processing. The Factory
// handles automatic retries on transient errors, so callers only see permanent
// failures.
type rangefeedClientFactory struct {
	factory *rangefeed.Factory
}

// quantizeTS returns a new timestamp with the walltime rounded down to the
// nearest multiple of the quantization granularity. If the granularity is 0, it
// returns the original timestamp.
func quantizeTS(ts hlc.Timestamp, granularity time.Duration) hlc.Timestamp {
	if granularity == 0 {
		return ts
	}
	return hlc.Timestamp{
		WallTime: ts.WallTime - ts.WallTime%int64(granularity),
		Logical:  0,
	}
}

func spansFromPairs(pairs []kvcoord.SpanTimePair) []roachpb.Span {
	spans := make([]roachpb.Span, len(pairs))
	for i, p := range pairs {
		spans[i] = p.Span
	}
	return spans
}

// Run implements the physicalFeedFactory interface.
func (f *rangefeedClientFactory) Run(
	ctx context.Context, sink kvevent.Writer, cfg rangeFeedConfig,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.physical_kv_feed.run")
	defer sp.Finish()

	frontier, err := span.MakeFrontier(spansFromPairs(cfg.Spans)...)
	if err != nil {
		return err
	}
	for _, stp := range cfg.Spans {
		if _, err := frontier.Forward(stp.Span, stp.StartAfter); err != nil {
			frontier.Release()
			return err
		}
	}

	// setErr records the first error reported from any callback and cancels
	// ctx so the rangefeed shuts down. errCh is buffered so the first sender
	// never blocks; subsequent senders drop their error (the first one wins).
	// The trailing select on this function reads from errCh so we can return
	// the original cause rather than ctx.Err()'s generic "context canceled".
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errCh := make(chan error, 1)
	setErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		cancel(err)
	}

	onValue := func(ctx context.Context, value *kvpb.RangeFeedValue) {
		if cfg.Knobs.OnRangeFeedValue != nil {
			if err := cfg.Knobs.OnRangeFeedValue(); err != nil {
				setErr(err)
				return
			}
		}
		timer := cfg.Timers.RangefeedBufferValue.Start()
		ev := &kvpb.RangeFeedEvent{Val: value}
		if err := sink.Add(ctx, kvevent.MakeKVEvent(ev)); err != nil {
			setErr(err)
			return
		}
		timer.End()
	}

	onCheckpoint := func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
		// The Factory quantizes internally for its own frontier but passes
		// original timestamps to callbacks, so we must re-quantize here.
		resolvedTS := quantizeTS(checkpoint.ResolvedTS, cfg.WithFrontierQuantize)
		if !resolvedTS.IsEmpty() && resolvedTS.Less(cfg.Frontier) {
			return
		}
		if cfg.Knobs.ShouldSkipCheckpoint != nil && cfg.Knobs.ShouldSkipCheckpoint(checkpoint) {
			return
		}
		timer := cfg.Timers.RangefeedBufferCheckpoint.Start()
		ev := &kvpb.RangeFeedEvent{
			Checkpoint: &kvpb.RangeFeedCheckpoint{
				Span:       checkpoint.Span,
				ResolvedTS: resolvedTS,
			},
		}
		if err := sink.Add(ctx, kvevent.MakeResolvedEvent(ev, jobspb.ResolvedSpan_NONE)); err != nil {
			setErr(err)
			return
		}
		timer.End()
	}

	opts := []rangefeed.Option{
		rangefeed.WithDiff(cfg.WithDiff),
		rangefeed.WithFiltering(cfg.WithFiltering),
		rangefeed.WithFrontierQuantized(cfg.WithFrontierQuantize),
		rangefeed.WithConsumerID(cfg.ConsumerID),
		rangefeed.WithOnCheckpoint(onCheckpoint),
		// We do not expect SST ingestion into spans with active changefeeds.
		// If one occurs, treat it as a permanent error rather than letting the
		// rangefeed client silently retry on the assertion failure it would
		// otherwise raise.
		rangefeed.WithOnSSTable(func(
			ctx context.Context, sst *kvpb.RangeFeedSSTable, _ roachpb.Span,
		) {
			setErr(errors.AssertionFailedf("unexpected SST ingestion: %v", sst))
		}),
		// We ignore MVCC range tombstones. These are currently only expected to
		// be used by schema GC and IMPORT INTO, and such spans should not have
		// active changefeeds across them, at least at the times of interest. A
		// case where one will show up in a changefeed is when the primary index
		// changes while we're watching it and then the old primary index is
		// dropped. In this case, we'll get a schema event to restart into the
		// new primary index, but the DeleteRange may come through before the
		// schema event.
		//
		// Without an OnDeleteRange handler the rangefeed client treats these
		// events as an assertion failure, so register an explicit no-op.
		//
		// TODO(erikgrinaker): Write an end-to-end test which verifies that an
		// IMPORT INTO which gets rolled back using MVCC range tombstones will
		// not be visible to a changefeed, neither when it was started before
		// the import or when resuming from a timestamp before the import. The
		// table descriptor should be marked as offline during the import, and
		// catchup scans should detect that this happened and prevent reading
		// anything in that timespan. See:
		// https://github.com/cockroachdb/cockroach/issues/70433
		rangefeed.WithOnDeleteRange(func(
			ctx context.Context, value *kvpb.RangeFeedDeleteRange,
		) {
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			setErr(err)
		}),
	}

	if cfg.Knobs.OnRangeFeedStart != nil {
		cfg.Knobs.OnRangeFeedStart(cfg.Spans)
	}

	// TODO(aerfrei): rangefeed.Factory.New takes an initialTimestamp, but it
	// is only consulted by RangeFeed.Start (resumeWithFrontier=false).
	// StartFromFrontier reads per-span timestamps from the frontier and
	// ignores it. We pass an empty timestamp to make that explicit at the
	// callsite, but the underlying API would be cleaner if initialTimestamp
	// lived on Start instead of New, or if New had two variants matching the
	// two start methods.
	rf := f.factory.New("changefeed", hlc.Timestamp{}, onValue, opts...)
	if err := rf.StartFromFrontier(ctx, frontier); err != nil {
		frontier.Release()
		return err
	}
	// Close blocks until the rangefeed goroutine exits; releasing the
	// frontier first would invalidate it underneath an in-flight callback.
	defer func() {
		rf.Close()
		frontier.Release()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		// Prefer the causal error from errCh if setErr triggered the
		// cancellation, since ctx.Err() would only say "context canceled".
		select {
		case err := <-errCh:
			return err
		default:
			return ctx.Err()
		}
	}
}
