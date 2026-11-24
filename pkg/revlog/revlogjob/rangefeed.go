// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// startRangeFeed subscribes a rangefeed for the given spans and
// dispatches its events to a Producer running in a single dedicated
// goroutine fed by eventsCh.
//
// The rangefeed is opened with WithInitialScan and started via
// StartFromFrontier with a pre-populated per-span frontier built
// from resume:
//
//   - Spans listed in resume.SpanResumes are forwarded to the
//     resumed ts. The rangefeed library's per-span init-scan gate
//     (runInitialScan in pkg/kv/kvclient/rangefeed/scanner.go) skips
//     these — they're already past the rangefeed's start ts.
//   - Spans assigned to this producer but absent from
//     resume.SpanResumes (i.e. newly-introduced spans from a
//     widening, see runDescFeed) sit at zero in the frontier, so
//     the initial scan picks them up at the rangefeed's start ts.
//
// On the very first flow (no widening yet, no persisted resume
// state) ResumeStateForPartition forwards every assigned span to
// startHLC, so the initial scan is a no-op and we don't accidentally
// re-export the entire backed-up keyspace. See ResumeStateForPartition.
//
// The returned RangeFeed is owned by the caller; Close it to stop
// the subscription. The caller is responsible for running the event
// dispatcher (runDispatcher) on a goroutine and draining errCh.
//
// Decoupling rangefeed callbacks from Producer via eventsCh lets the
// callbacks return immediately (the rangefeed library invokes them
// from a shared goroutine pool that should not be blocked on slow
// downstream work like writing data files). The dispatcher
// goroutine serializes Producer access — Producer is single-writer.
func startRangeFeed(
	ctx context.Context,
	factory *rangefeed.Factory,
	name string,
	spans []roachpb.Span,
	startHLC hlc.Timestamp,
	resume ResumeState,
	eventsCh chan<- rangefeedEvent,
	errCh chan<- error,
) (*rangefeed.RangeFeed, error) {
	onValue := func(ctx context.Context, v *kvpb.RangeFeedValue) {
		select {
		case eventsCh <- rangefeedEvent{value: v}:
		case <-ctx.Done():
		}
	}
	onCheckpoint := func(ctx context.Context, cp *kvpb.RangeFeedCheckpoint) {
		select {
		case eventsCh <- rangefeedEvent{checkpoint: cp}:
		case <-ctx.Done():
		}
	}
	onInternalErr := func(ctx context.Context, err error) {
		select {
		case errCh <- err:
		case <-ctx.Done():
		}
	}

	frontier, err := buildRangefeedFrontier(spans, startHLC, resume)
	if err != nil {
		return nil, err
	}
	rf := factory.New(name, startHLC, onValue,
		rangefeed.WithDiff(true),
		rangefeed.WithInitialScan(nil /* onInitialScanDone */),
		rangefeed.WithOnCheckpoint(onCheckpoint),
		rangefeed.WithOnInternalError(onInternalErr),
	)
	if err := rf.StartFromFrontier(ctx, frontier); err != nil {
		frontier.Release()
		return nil, errors.Wrap(err, "starting rangefeed")
	}
	return rf, nil
}

// buildRangefeedFrontier constructs the per-span frontier passed to
// RangeFeed.StartFromFrontier. Spans start at zero unless
// resume.SpanResumes lists them with a resolved ts >= startHLC, in
// which case they are forwarded to that ts so the rangefeed
// library's init-scan gate (runInitialScan) skips them.
//
// Spans left at zero get an initial scan at startHLC; this is the
// pre-existing-state capture for a span newly added by a widening
// (see runDescFeed). On the first flow, ResumeStateForPartition
// supplies an entry at startHLC for every assigned span, so the
// initial scan is a no-op for everything — without that, we'd
// re-export the entire backed-up keyspace on every job start.
//
// The rangefeed takes ownership of the returned frontier (see
// RangeFeed.StartFromFrontier); the caller releases it on error
// before StartFromFrontier returns.
func buildRangefeedFrontier(
	spans []roachpb.Span, startHLC hlc.Timestamp, resume ResumeState,
) (span.Frontier, error) {
	f, err := span.MakeFrontier(spans...)
	if err != nil {
		return nil, errors.Wrap(err, "creating rangefeed frontier")
	}
	for _, sr := range resume.SpanResumes {
		// Forward is a no-op for ts <= the current entry's ts;
		// resume entries below startHLC (e.g. a newly-introduced
		// span carried at zero) are silently ignored, leaving the
		// span at zero so init-scan picks it up.
		if _, err := f.Forward(sr.Span, sr.Resolved); err != nil {
			f.Release()
			return nil, errors.Wrapf(err,
				"forwarding rangefeed frontier for span %s to %s", sr.Span, sr.Resolved)
		}
	}
	return f, nil
}

// rangefeedEvent carries one rangefeed delivery. Exactly one of
// value or checkpoint is set.
type rangefeedEvent struct {
	value      *kvpb.RangeFeedValue
	checkpoint *kvpb.RangeFeedCheckpoint
}

// runDispatcher pumps events from eventsCh into the Producer until
// ctx is cancelled or eventsCh is closed. Returns nil on clean
// shutdown, the first error from Producer.OnCheckpoint otherwise.
func runDispatcher(ctx context.Context, p *Producer, eventsCh <-chan rangefeedEvent) error {
	for {
		select {
		case ev, ok := <-eventsCh:
			if !ok {
				return nil
			}
			switch {
			case ev.value != nil:
				p.OnValue(ctx, ev.value.Key, ev.value.Value.Timestamp,
					ev.value.Value.RawBytes, ev.value.PrevValue.RawBytes)
			case ev.checkpoint != nil:
				if err := p.OnCheckpoint(ctx, ev.checkpoint.Span, ev.checkpoint.ResolvedTS); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
