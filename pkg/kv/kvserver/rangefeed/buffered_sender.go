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
	// streamErrored means an error has been enqueued for this stream and further
	// buffered sends will be ignored. Streams in this state will not be found in
	// the status map.
	streamErrored streamState = iota
)

func (s streamState) String() string {
	switch s {
	case streamActive:
		return "active"
	case streamOverflowing:
		return "overflowing"
	case streamErrored:
		return "errored"
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

var errNoSuchStream = errors.New("stream already encountered an error or has not be added to buffered sender")

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
// already enqueued an error event or have not been added via addStream will
// return an error.
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
		return errNoSuchStream
	}
	nextState, err := bs.nextPerStreamStateLocked(status, ev)
	if nextState == streamErrored {
		// We will be admitting this event but no events after this.
		assertTrue(err == nil, "expected error event to be admitted")
		delete(bs.queueMu.byStream, ev.StreamID)
	} else {
		if err == nil {
			status.queueItems++
		}
		status.state = nextState
		bs.queueMu.byStream[ev.StreamID] = status
	}
	if err != nil {
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

// nextPerStreamStateLocked returns the next state that should be stored for the
// stream related to the given rangefeed event. If an error is returned, the
// event should not be admitted and the given error should be returned to the
// client.
func (bs *BufferedSender) nextPerStreamStateLocked(
	status streamStatus, ev *kvpb.MuxRangeFeedEvent,
) (streamState, error) {
	// An error should always put us into stateErrored, so let's do that first.
	if ev.Error != nil {
		if status.state == streamErrored {
			assumedUnreachable("unexpected buffered event on stream in state streamErrored")
		}
		return streamErrored, nil
	}

	switch status.state {
	case streamActive:
		if bs.queueMu.perStreamCapacity > 0 && status.queueItems == bs.queueMu.perStreamCapacity {
			// This stream is at capacity, return an error to the registration that it
			// should send back to us after cleaning up.
			return streamOverflowing, newRetryErrBufferCapacityExceeded()
		}
		// Happy path.
		return streamActive, nil
	case streamOverflowing:
		// The only place we do concurrent buffered sends is during catch-up scan
		// publishing which may be concurrent with a disconnect. The catch-up scan
		// will stop publishing if it receives an error and try to send an error
		// back. A disconnect only sends an error. This path exclusively handles
		// non-errors.
		assumedUnreachable("unexpected buffered event on stream in state streamOverflowing")
		return streamOverflowing, newRetryErrBufferCapacityExceeded()
	case streamErrored:
		// This is unexpected because streamErrored streams are removed from the
		// status map and thus should be handled in sendBuffered before this
		// function is called.
		assumedUnreachable("unexpected buffered event on stream in state streamErrored")
		return streamErrored, errNoSuchStream
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
