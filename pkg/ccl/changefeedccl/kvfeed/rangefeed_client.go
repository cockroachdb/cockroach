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

// rangeFeedConfig contains configuration options for creating a rangefeed.
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

// rangefeedSink adapts callbacks from the rangefeed.Factory client into
// kvevent.Writer additions. It bundles the per-run state — destination
// sink, run-scoped config, and a setErr closure that fans the first
// callback error back to runRangeFeed — so the callbacks can be defined
// as top-level methods rather than closures inside runRangeFeed.
type rangefeedSink struct {
	sink   kvevent.Writer
	cfg    rangeFeedConfig
	setErr func(error)
}

func (s *rangefeedSink) onValue(ctx context.Context, value *kvpb.RangeFeedValue) {
	if s.cfg.Knobs.OnRangeFeedValue != nil {
		if err := s.cfg.Knobs.OnRangeFeedValue(); err != nil {
			s.setErr(err)
			return
		}
	}
	timer := s.cfg.Timers.RangefeedBufferValue.Start()
	ev := &kvpb.RangeFeedEvent{Val: value}
	if err := s.sink.Add(ctx, kvevent.MakeKVEvent(ev)); err != nil {
		s.setErr(err)
		return
	}
	timer.End()
}

func (s *rangefeedSink) onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
	// The Factory quantizes internally for its own frontier but passes
	// original timestamps to callbacks, so we must re-quantize here.
	resolvedTS := quantizeTS(checkpoint.ResolvedTS, s.cfg.WithFrontierQuantize)
	if !resolvedTS.IsEmpty() && resolvedTS.Less(s.cfg.Frontier) {
		return
	}
	if s.cfg.Knobs.ShouldSkipCheckpoint != nil && s.cfg.Knobs.ShouldSkipCheckpoint(checkpoint) {
		return
	}
	timer := s.cfg.Timers.RangefeedBufferCheckpoint.Start()
	ev := &kvpb.RangeFeedEvent{
		Checkpoint: &kvpb.RangeFeedCheckpoint{
			Span:       checkpoint.Span,
			ResolvedTS: resolvedTS,
		},
	}
	if err := s.sink.Add(ctx, kvevent.MakeResolvedEvent(ev, jobspb.ResolvedSpan_NONE)); err != nil {
		s.setErr(err)
		return
	}
	timer.End()
}

// onSSTable handles SST ingestion events. We do not expect SST ingestion into
// spans with active changefeeds. If one occurs, treat it as a permanent error
// rather than letting the rangefeed client silently retry on the assertion
// failure it would otherwise raise.
func (s *rangefeedSink) onSSTable(_ context.Context, sst *kvpb.RangeFeedSSTable, _ roachpb.Span) {
	s.setErr(errors.AssertionFailedf("unexpected SST ingestion: %v", sst))
}

// onDeleteRange handles MVCC range tombstone events. We ignore these. They
// are currently only expected to be used by schema GC and IMPORT INTO, and
// such spans should not have active changefeeds across them, at least at the
// times of interest. A case where one will show up in a changefeed is when
// the primary index changes while we're watching it and then the old primary
// index is dropped. In this case, we'll get a schema event to restart into
// the new primary index, but the DeleteRange may come through before the
// schema event.
//
// Without an OnDeleteRange handler the rangefeed client treats these events
// as an assertion failure, so this no-op is registered explicitly.
//
// TODO(erikgrinaker): Write an end-to-end test which verifies that an IMPORT
// INTO which gets rolled back using MVCC range tombstones will not be visible
// to a changefeed, neither when it was started before the import or when
// resuming from a timestamp before the import. The table descriptor should be
// marked as offline during the import, and catchup scans should detect that
// this happened and prevent reading anything in that timespan. See:
// https://github.com/cockroachdb/cockroach/issues/70433
func (s *rangefeedSink) onDeleteRange(_ context.Context, _ *kvpb.RangeFeedDeleteRange) {
}

// onInternalError handles non-retryable failures surfaced by the rangefeed
// client.
func (s *rangefeedSink) onInternalError(_ context.Context, err error) {
	s.setErr(err)
}

// runRangeFeed starts a rangefeed via the high-level rangefeed.Factory client
// and copies its events into sink. It blocks until the rangefeed terminates
// with a permanent error or ctx is canceled. The Factory handles transient
// retries internally; only permanent failures surface here.
func (f *kvFeed) runRangeFeed(ctx context.Context, sink kvevent.Writer, cfg rangeFeedConfig) error {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.kvfeed.run_rangefeed")
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

	rs := &rangefeedSink{sink: sink, cfg: cfg, setErr: setErr}

	opts := []rangefeed.Option{
		rangefeed.WithDiff(cfg.WithDiff),
		rangefeed.WithFiltering(cfg.WithFiltering),
		rangefeed.WithFrontierQuantized(cfg.WithFrontierQuantize),
		rangefeed.WithConsumerID(cfg.ConsumerID),
		rangefeed.WithOnCheckpoint(rs.onCheckpoint),
		rangefeed.WithOnSSTable(rs.onSSTable),
		rangefeed.WithOnDeleteRange(rs.onDeleteRange),
		rangefeed.WithOnInternalError(rs.onInternalError),
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
	rf := f.factory.New("changefeed", hlc.Timestamp{}, rs.onValue, opts...)
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
