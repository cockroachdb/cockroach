// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

// RangefeedSingleBufferedSenderQueueMaxPerReg is the maximum number of events
// that the buffered sender will buffer for a single registration (identified by
// streamID). Existing MuxRangefeeds will use the previous value until
// restarted.
//
// TODO(ssd): This is a bit of a stop-gap so that we have a knob to turn if we
// need to. We probably want each buffered sender (or each consumerID) to be
// able to hold up to some fraction of the total rangefeed budget. But we are
// starting here for now.
var RangefeedSingleBufferedSenderQueueMaxPerReg = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.rangefeed.buffered_sender.per_registration_max_queue_size",
	"maximum number of events a single registration can have queued in the event queue (0 for no max)",
	kvserverbase.DefaultRangefeedEventCap*2,
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
		// perStreamCapacity is the maximum number buffered events allowed per
		// stream.
		perStreamCapacity int64
		byStream          map[int64]streamStatus
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

type streamState int64

const (
	// streamActive is the default state of the stream.
	streamActive streamState = iota
	// streamOverflowing is the state we are in when the stream has reached its
	// limit and is waiting to deliver an error.
	streamOverflowing streamState = iota
	// streamOverflowed means the stream has overflowed and the error has been
	// placed in the queue.
	streamOverflowed streamState = iota
)

func (s streamState) String() string {
	switch s {
	case streamActive:
		return "active"
	case streamOverflowing:
		return "overflowing"
	case streamOverflowed:
		return "overflowed"
	default:
		return "unknown"
	}
}

func (s streamState) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s", redact.SafeString(s.String()))
}

type streamStatus struct {
	// queueItems is the number of items for a given stream in the event queue.
	queueItems int64
	state      streamState
}

func (s streamStatus) String() string {
	return fmt.Sprintf("%s [queue_len:%d]", s.state, s.queueItems)
}

func (s streamStatus) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s", redact.SafeString(s.String()))
}

func NewBufferedSender(
	sender ServerStreamSender, settings *cluster.Settings, bsMetrics *BufferedSenderMetrics,
) *BufferedSender {
	bs := &BufferedSender{
		sender:  sender,
		metrics: bsMetrics,
	}
	bs.notifyDataC = make(chan struct{}, 1)
	bs.queueMu.buffer = newEventQueue()
	bs.queueMu.perStreamCapacity = RangefeedSingleBufferedSenderQueueMaxPerReg.Get(&settings.SV)
	bs.queueMu.byStream = make(map[int64]streamStatus)
	return bs
}

// sendBuffered buffers the event before sending it to the underlying gRPC
// stream. It does not block.
//
// It errors in the case of a stopped sender of if the registration has exceeded
// its capacity. sendBuffered with rangefeed events for streams that have
// already encountered an error will be dropped without error.
func (bs *BufferedSender) sendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		return errors.New("stream sender is stopped")
	}

	// Per-stream capacity limits. If the stream is already overflowed we drop the
	// request. If the stream has hit its limit, we return an error to the
	// registration. This error should be the next event that is sent to
	// stream.
	status, ok := bs.queueMu.byStream[ev.StreamID]
	if !ok {
		// We don't error if the stream status is not found as this may be an
		// event for an already closed stream. Such events are possible while the
		// registration publishes the catch up scan buffer.
		//
		// The client ignores such events, but we drop it here regardless.
		return nil
	}
	nextState, shouldAdmit, err := bs.nextPerQueueStateLocked(status, ev)
	status.state = nextState
	if shouldAdmit {
		status.queueItems++
	}
	bs.queueMu.byStream[ev.StreamID] = status
	if err != nil || !shouldAdmit {
		return err
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

// nextPerQueueStateLocked returns the next state that should be stored for the
// stream related to the given rangefeed event and a bool that indicates whether
// the event should be admitted to the buffer. Any error returned should
// returned to the caller of sendBuffered.
func (bs *BufferedSender) nextPerQueueStateLocked(
	status streamStatus, ev *kvpb.MuxRangeFeedEvent,
) (streamState, bool, error) {
	switch status.state {
	case streamActive:
		if bs.queueMu.perStreamCapacity > 0 && status.queueItems == bs.queueMu.perStreamCapacity {
			if ev.Error != nil {
				// If _this_ event is an error, no use sending another error. This stream
				// is going down. Admit this error and mark the stream as overflowed.
				return streamOverflowed, true, nil
			} else {
				// This stream is at capacity, return an error to the registration that it
				// should send back to us after cleaning up.
				return streamOverflowing, false, newRetryErrBufferCapacityExceeded()
			}
		}
		// Happy path.
		return streamActive, true, nil
	case streamOverflowing:
		// The unbufferedRegistration is the only component that sends non-error
		// events to our stream. In response to the error we return when moving to
		// stateOverflowing, it should immediately send us an error and mark itself
		// as disconnected.
		//
		// The only unfortunate exception is if we get disconnected while flushing
		// the catch-up scan buffer. In this case we admit the event and stay in
		// state overflowing until we actually receive the error.
		//
		// TODO(ssd): Given the above exception, we should perhaps just move
		// directly to streamOverflowed. But, I think instead we want to remove
		// that exception if possible.
		if ev.Error != nil {
			return streamOverflowed, true, nil
		} else {
			// Drop everything but error events. We didn't admit the event that put
			// us into the overflowing state so we don't want to admit any later
			// non-error events.
			return streamOverflowing, false, nil
		}
	case streamOverflowed:
		// TODO(ssd): It would be nice to be able to put this assertion here:
		//
		//    assumedUnreachable("event on overflowed stream")
		//
		// But, it isn't yet possible because of the following:
		//
		// 1. Unbuffered Registration is publishing the catch-up buffer while not
		//    holding its lock.
		//
		// 2. An another error comes in and that error moves us to the overflowed
		//    state.
		//
		// 3. The unbuffered registration publishes another message from the
		//    catch-up buffer.
		//
		// One way to fix this is to remove the stream from the stream status map
		// when we admit an error. We leave that to a future PR.
		return streamOverflowed, false, nil
	default:
		panic(fmt.Sprintf("unhandled stream state: %v", status.state))
	}
}

// sendUnbuffered sends the event directly to the underlying
// ServerStreamSender. It bypasses the buffer and thus may block.
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

				bs.metrics.BufferedSenderQueueSize.Dec(1)
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
	if ok {
		state, streamFound := bs.queueMu.byStream[event.ev.StreamID]
		if streamFound {
			state.queueItems--
			bs.queueMu.byStream[event.ev.StreamID] = state
		}
	}
	return event, ok
}

// addStream initializes the per-stream tracking for the given streamID.
func (bs *BufferedSender) addStream(streamID int64) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if _, ok := bs.queueMu.byStream[streamID]; !ok {
		bs.queueMu.byStream[streamID] = streamStatus{}
	} else {
		assumedUnreachable(fmt.Sprintf("stream %d already exists in buffered sender", streamID))
	}
}

// removeStream removes the per-stream state tracking from the sender.
//
// TODO(ssd): There may be items still in the queue when removeStream is called.
// We'd like to solve this by removing this as a possibility. But this is OK
// since we will eventually process the events and the client knows to ignore
// them.
func (bs *BufferedSender) removeStream(streamID int64) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	delete(bs.queueMu.byStream, streamID)
}

// cleanup is called when the sender is stopped. It is expected to free up
// buffer queue and no new events should be buffered after this.
func (bs *BufferedSender) cleanup(ctx context.Context) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	remaining := bs.queueMu.buffer.len()
	bs.queueMu.buffer.drain(ctx)
	bs.queueMu.byStream = nil
	bs.metrics.BufferedSenderQueueSize.Dec(remaining)
}

func (bs *BufferedSender) len() int {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	return int(bs.queueMu.buffer.len())
}

func (bs *BufferedSender) TestingBufferSummary() string {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()

	summary := &strings.Builder{}
	fmt.Fprintf(summary, "buffered sender: queue_len=%d streams=%d", bs.queueMu.buffer.len(), len(bs.queueMu.byStream))
	for id, stream := range bs.queueMu.byStream {
		fmt.Fprintf(summary, "\n    %d: %s", id, stream)
	}
	return summary.String()
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
		if bs.len() == 0 {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("buffered sender failed to send in time")
}
