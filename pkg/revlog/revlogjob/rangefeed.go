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
	"github.com/cockroachdb/errors"
)

// startRangeFeed subscribes a rangefeed for the given spans starting
// after startHLC and dispatches its events to a Producer running in
// a single dedicated goroutine fed by eventsCh.
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
	rf, err := factory.RangeFeed(
		ctx, name, spans, startHLC, onValue,
		rangefeed.WithDiff(true),
		rangefeed.WithOnCheckpoint(onCheckpoint),
		rangefeed.WithOnInternalError(onInternalErr),
	)
	if err != nil {
		return nil, errors.Wrap(err, "starting rangefeed")
	}
	return rf, nil
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
