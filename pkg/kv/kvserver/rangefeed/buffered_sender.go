// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	}

	// notifyDataC is used to notify the BufferedSender.run goroutine that there
	// are events to send. Channel is initialised with a buffer of 1 and all writes to it
	// are non-blocking.
	notifyDataC chan struct{}
}

func NewBufferedSender(sender ServerStreamSender) *BufferedSender {
	bs := &BufferedSender{
		sender: sender,
	}
	bs.queueMu.buffer = newEventQueue()
	bs.notifyDataC = make(chan struct{}, 1)
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
	// TODO(wenyihu6): pass an actual context here
	alloc.Use(context.Background())
	bs.queueMu.buffer.pushBack(sharedMuxEvent{ev, alloc})
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
				e, success := bs.popFront()
				if !success {
					break
				}
				err := bs.sender.Send(e.ev)
				e.alloc.Release(ctx)
				if e.ev.Error != nil {
					onError(e.ev.StreamID)
				}
				if err != nil {
					return err
				}
			}
		}
	}
}

// popFront pops the front event from the buffer queue. It returns the event and
// a boolean indicating if the event was successfully popped.
func (bs *BufferedSender) popFront() (e sharedMuxEvent, success bool) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.popFront()
	return event, ok
}

// cleanup is called when the sender is stopped. It is expected to free up
// buffer queue and no new events should be buffered after this.
func (bs *BufferedSender) cleanup(ctx context.Context) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	bs.queueMu.buffer.drain(ctx)
}

// Used for testing only.
func (bs *BufferedSender) len() int {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	return int(bs.queueMu.buffer.len())
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
