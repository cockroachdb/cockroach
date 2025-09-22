// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

//            ┌─────────────────┐
//            │Node.MuxRangefeed│
//            └──────┬───┬──────┘
//  Sender.AddStream │   │LockedMuxStream.Send ───────────────────┐────────────────────────────────┐
//        ┌──────────┴─▼─┴────────────┐                           │                                │
//        │ Buffered/Unbuffered Sender├───────────┐               │                                │
//        └────────────┬──────────────┘           │               │                                │
//                     │                          │               │                                │
//            ┌────────▼─────────┐                │               │                                │
//            │ Stores.Rangefeed │                │               │                                │
//            └────────┬─────────┘                │               │                                │
//                     │                          │               │                                │
//             ┌───────▼─────────┐         BufferedSender      BufferedSender                      │
//             │ Store.Rangefeed │ SendUnbuffered/SendBuffered SendBufferedError ─────► BufferedSender.run
//             └───────┬─────────┘ (catch-up scan)(live raft)     ▲
//                     │                          ▲               │
//            ┌────────▼──────────┐               │               │
//            │ Replica.Rangefeed │               │               │
//            └────────┬──────────┘               │               │
//                     │                          │               │
//             ┌───────▼──────┐                   │               │
//             │ Registration │                   │               │
//             └──────┬───────┘                   │               │
//                    │                           │               │
//                    │                           │               │
//                    └───────────────────────────┘───────────────┘
//               BufferedPerRangeEventSink.Send    BufferedPerRangeEventSink.SendError
//

// RangefeedSingleBufferedSenderQueueMaxSize is the maximum number of events
// that the buffered sender will buffer before it starts returning capacity
// exceeded errors. Updates to this setting are only applied to new
// MuxRangefeedCalls. Existing streams will use the previous value until
// restarted.
//
// The default here has been arbitrarily chosen. Ideally,
//
//   - We want to avoid capacity exceeded errors that wouldn't have occurred
//     when the buffered registrations were in use.
//
//   - We don't want to drastically increase the amount of queueing allowed for a
//     single registration.
//
// A small buffer may be justified given that:
//
//   - One buffered sender is feeding a single gRPC client, so scaling based on
//     registrations doesn't necessarily make sense. If the consumer is behind, it
//     is behind.
//
//   - Events emitted during catchup scans have their own per-registration buffer
//     still.
//
// TODO(ssd): This is a bit of a stop-gap so that we have a knob to turn if we
// need to. We probably want each buffered sender (or each consumerID) to be
// able to hold up to some fraction of the total rangefeed budget. But we are
// starting here for now.
var RangefeedSingleBufferedSenderQueueMaxSize = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.rangefeed.buffered_sender.queue_max_size",
	"max size of a buffered senders event queue (0 for no max)",
	kvserverbase.DefaultRangefeedEventCap*8,
)

// BufferedSender is embedded in every rangefeed.BufferedPerRangeEventSink,
// serving as a helper which buffers events before forwarding events to the
// underlying gRPC stream.
type BufferedSender struct {
	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// queueMu protects the buffer queue.
	queueMu struct {
		syncutil.Mutex
		stopped bool
		buffer  *eventQueue
		// capacity is the maximum number of events that can be buffered.
		capacity   int64
		overflowed bool
	}

	// notifyDataC is used to notify the BufferedSender.run goroutine that there
	// are events to send. Channel is initialised with a buffer of 1 and all writes to it
	// are non-blocking.
	notifyDataC chan struct{}

	// metrics is used to track the set of BufferedSender related metrics for a
	// given node. Note that there could be multiple buffered senders in a node,
	// sharing the metrics.
	metrics *BufferedSenderMetrics
}

func NewBufferedSender(
	sender ServerStreamSender, settings *cluster.Settings, bsMetrics *BufferedSenderMetrics,
) *BufferedSender {
	bs := &BufferedSender{
		sender:  sender,
		metrics: bsMetrics,
	}
	bs.queueMu.buffer = newEventQueue()
	bs.notifyDataC = make(chan struct{}, 1)
	bs.queueMu.buffer = newEventQueue()
	bs.queueMu.capacity = RangefeedSingleBufferedSenderQueueMaxSize.Get(&settings.SV)
	return bs
}

// sendBuffered buffers the event before sending it to the underlying
// gRPC stream. It does not block. sendBuffered will take the
// ownership of the alloc and release it if the returned error is
// non-nil. It only errors in the case of an already stopped stream.
func (bs *BufferedSender) sendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		return errors.New("stream sender is stopped")
	}
	if bs.queueMu.overflowed {
		return newRetryErrBufferCapacityExceeded()
	}
	if bs.queueMu.capacity > 0 && bs.queueMu.buffer.len() >= bs.queueMu.capacity {
		bs.queueMu.overflowed = true
		return newRetryErrBufferCapacityExceeded()
	}
	// TODO(wenyihu6): pass an actual context here
	alloc.Use(context.Background())
	bs.queueMu.buffer.pushBack(sharedMuxEvent{ev, alloc})
	bs.metrics.BufferedSenderQueueSize.Inc(1)
	select {
	case bs.notifyDataC <- struct{}{}:
	default:
	}
	return nil
}

// sendUnbuffered sends the event directly to the underlying
// ServerStreamSender.  It bypasses the buffer and thus may block.
func (bs *BufferedSender) sendUnbuffered(ev *kvpb.MuxRangeFeedEvent) error {
	return bs.sender.Send(ev)
}

// run loops until the sender or stopper signal teardown. In each
// iteration, it waits for events to enter the buffer and moves them
// to the sender.
func (bs *BufferedSender) run(
	ctx context.Context, stopper *stop.Stopper, onError func(streamID int64),
) error {
	const bufSize = 64
	eventsBuf := make([]sharedMuxEvent, 0, bufSize)

	for {
		select {
		case <-ctx.Done():
			// Top level goroutine will receive the context cancellation and handle
			// ctx.Err().
			return nil
		case <-stopper.ShouldQuiesce():
			// Top level goroutine will receive the stopper quiesce signal and handle
			// error.
			return nil
		case <-bs.notifyDataC:
			for {
				var remains int64
				var overflowed bool
				eventsBuf, remains, overflowed = bs.popEvents(eventsBuf[:0], bufSize)
				if len(eventsBuf) == 0 {
					break
				}
				bs.metrics.BufferedSenderQueueSize.Dec(int64(len(eventsBuf)))

				for _, evt := range eventsBuf {
					// TODO(ssd): This might be another location where we could transform
					// multiple events into BulkEvents. We can't just throw them all in a
					// bulk event though since we are processing events for different
					// streams here.
					err := bs.sender.Send(evt.ev)
					evt.alloc.Release(ctx)
					if evt.ev.Error != nil {
						onError(evt.ev.StreamID)
					}
					if err != nil {
						return err
					}
				}
				clear(eventsBuf)
				if overflowed && remains == int64(0) {
					return newRetryErrBufferCapacityExceeded()
				}
			}
		}
	}
}

// popEvents appends up to eventsToPop events into dest, returning the new
// slice, the remaining queue length, and a bool indicating whether the queue is
// in an overflow state.
func (bs *BufferedSender) popEvents(
	dest []sharedMuxEvent, eventsToPop int,
) ([]sharedMuxEvent, int64, bool) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()

	dest = bs.queueMu.buffer.popFrontInto(dest, eventsToPop)
	return dest, bs.queueMu.buffer.len(), bs.queueMu.overflowed
}

// cleanup is called when the sender is stopped. It is expected to free up
// buffer queue and no new events should be buffered after this.
func (bs *BufferedSender) cleanup(ctx context.Context) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	remaining := bs.queueMu.buffer.len()
	bs.queueMu.buffer.drain(ctx)
	bs.metrics.BufferedSenderQueueSize.Dec(remaining)
}

func (bs *BufferedSender) len() int {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	return int(bs.queueMu.buffer.len())
}

func (bs *BufferedSender) overflowed() bool {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	return bs.queueMu.overflowed
}

// Used for testing only.
func (bs *BufferedSender) waitForEmptyBuffer(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		bs.queueMu.Lock()
		caughtUp := bs.queueMu.buffer.len() == 0 // nolint:deferunlockcheck
		bs.queueMu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("buffered sender failed to send in time")
}
