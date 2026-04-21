// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ServerStreamSender forwards MuxTxnFeedEvents to the underlying gRPC stream.
type ServerStreamSender interface {
	// Send must be thread-safe to be called concurrently.
	Send(*kvpb.MuxTxnFeedEvent) error
	// SendIsThreadSafe is a no-op declaration method. It is a contract that the
	// interface has a thread-safe Send method.
	SendIsThreadSafe()
}

// BufferedSender buffers MuxTxnFeedEvents before forwarding them to the
// underlying gRPC stream. It maintains a shared event queue across all streams
// with per-stream capacity limits. A single background goroutine (run) drains
// the queue and sends events to the gRPC stream.
//
// This is the txnfeed equivalent of rangefeed.BufferedSender.
type BufferedSender struct {
	// sender wraps the underlying gRPC server stream, ensuring thread safety.
	sender      ServerStreamSender
	sendBufSize int

	// queueMu protects the buffer queue.
	queueMu struct {
		syncutil.Mutex
		stopped bool
		buffer  *eventQueue
		// perStreamCapacity is the maximum number of buffered events allowed per
		// stream. 0 means no limit.
		perStreamCapacity int64
		byStream          map[int64]streamStatus
	}

	// notifyDataC is used to notify the BufferedSender.run goroutine that there
	// are events to send. Channel is initialized with a buffer of 1 and all
	// writes to it are non-blocking.
	notifyDataC chan struct{}
}

const defaultSendBufSize = 64

type streamState int64

const (
	// streamActive is the default state of the stream.
	streamActive streamState = iota
	// streamOverflowing is the state we are in when the stream has reached its
	// limit and is waiting to deliver an error.
	streamOverflowing
	// streamErrored means an error has been enqueued for this stream and further
	// buffered sends will be ignored. Streams in this state will not be found in
	// the status map.
	streamErrored
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

// SafeFormat implements redact.SafeFormatter.
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

// SafeFormat implements redact.SafeFormatter.
func (s streamStatus) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s", redact.SafeString(s.String()))
}

var errNoSuchStream = errors.New(
	"stream already encountered an error or has not been added to buffered sender")

// NewBufferedSender creates a new BufferedSender wrapping the given thread-safe
// gRPC stream sender. perStreamCapacity sets the maximum number of buffered
// events per stream (0 for no limit).
func NewBufferedSender(sender ServerStreamSender, perStreamCapacity int64) *BufferedSender {
	bs := &BufferedSender{
		sendBufSize: defaultSendBufSize,
		sender:      sender,
	}
	bs.notifyDataC = make(chan struct{}, 1)
	bs.queueMu.buffer = newEventQueue()
	bs.queueMu.perStreamCapacity = perStreamCapacity
	bs.queueMu.byStream = make(map[int64]streamStatus)
	return bs
}

// sendBuffered buffers the event before sending it to the underlying gRPC
// stream. It does not block.
//
// It errors if the sender is stopped, the registration has exceeded its
// capacity, or the stream has already enqueued an error event or has not been
// added via addStream.
func (bs *BufferedSender) sendBuffered(ev *kvpb.MuxTxnFeedEvent) error {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		return errors.New("stream sender is stopped")
	}

	// Per-stream capacity limits. If the stream is already overflowed we drop
	// the request. If the stream has hit its limit, we return an error to the
	// registration.
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

	bs.queueMu.buffer.pushBack(ev)
	select {
	case bs.notifyDataC <- struct{}{}:
	default:
	}
	return nil
}

// nextPerStreamStateLocked returns the next state that should be stored for
// the stream related to the given event. If an error is returned, the event
// should not be admitted.
func (bs *BufferedSender) nextPerStreamStateLocked(
	status streamStatus, ev *kvpb.MuxTxnFeedEvent,
) (streamState, error) {
	// An error should always put us into stateErrored.
	if ev.Error != nil {
		if status.state == streamErrored {
			assumedUnreachable(
				"unexpected buffered event on stream in state streamErrored")
		}
		return streamErrored, nil
	}

	switch status.state {
	case streamActive:
		if bs.queueMu.perStreamCapacity > 0 &&
			status.queueItems == bs.queueMu.perStreamCapacity {
			return streamOverflowing, errBufferCapacityExceeded
		}
		return streamActive, nil
	case streamOverflowing:
		assumedUnreachable(
			"unexpected buffered event on stream in state streamOverflowing")
		return streamOverflowing, errBufferCapacityExceeded
	case streamErrored:
		assumedUnreachable(
			"unexpected buffered event on stream in state streamErrored")
		return streamErrored, errNoSuchStream
	default:
		panic(fmt.Sprintf("unhandled stream state: %v", status.state))
	}
}

// sendUnbuffered sends the event directly to the underlying
// ServerStreamSender. It bypasses the buffer and thus may block.
func (bs *BufferedSender) sendUnbuffered(ev *kvpb.MuxTxnFeedEvent) error {
	return bs.sender.Send(ev)
}

// run loops until the sender or stopper signal teardown. In each iteration, it
// waits for events to enter the buffer and moves them to the sender.
func (bs *BufferedSender) run(
	ctx context.Context, stopper *stop.Stopper, onError func(streamID int64),
) error {
	eventsBuf := make([]*kvpb.MuxTxnFeedEvent, 0, bs.sendBufSize)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		case <-bs.notifyDataC:
			for {
				eventsBuf = bs.popEvents(eventsBuf[:0], bs.sendBufSize)
				if len(eventsBuf) == 0 {
					break
				}

				for _, evt := range eventsBuf {
					err := bs.sender.Send(evt)
					if evt.Error != nil {
						onError(evt.StreamID)
					}
					if err != nil {
						return err
					}
				}
				clear(eventsBuf)
			}
		}
	}
}

// popEvents appends up to eventsToPop events into dest, returning the
// appended slice.
func (bs *BufferedSender) popEvents(
	dest []*kvpb.MuxTxnFeedEvent, eventsToPop int,
) []*kvpb.MuxTxnFeedEvent {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	dest = bs.queueMu.buffer.popFrontInto(dest, eventsToPop)

	// Update accounting for everything we popped.
	for _, event := range dest {
		state, streamFound := bs.queueMu.byStream[event.StreamID]
		if streamFound {
			state.queueItems--
			bs.queueMu.byStream[event.StreamID] = state
		}
	}
	return dest
}

// addStream initializes the per-stream tracking for the given streamID.
func (bs *BufferedSender) addStream(streamID int64) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if _, ok := bs.queueMu.byStream[streamID]; !ok {
		bs.queueMu.byStream[streamID] = streamStatus{}
	} else {
		assumedUnreachable(
			fmt.Sprintf("stream %d already exists in buffered sender", streamID))
	}
}

// cleanup is called when the sender is stopped. It frees the buffer queue.
// No new events should be buffered after this.
func (bs *BufferedSender) cleanup(_ context.Context) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	bs.queueMu.buffer.drain()
	bs.queueMu.byStream = nil
}

func (bs *BufferedSender) len() int {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	return int(bs.queueMu.buffer.len())
}

// TestingBufferSummary returns a human-readable summary of the buffer state.
func (bs *BufferedSender) TestingBufferSummary() string {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()

	summary := &strings.Builder{}
	fmt.Fprintf(summary, "buffered sender: queue_len=%d streams=%d",
		bs.queueMu.buffer.len(), len(bs.queueMu.byStream))
	for id, stream := range bs.queueMu.byStream {
		fmt.Fprintf(summary, "\n    %d: %s", id, stream)
	}
	return summary.String()
}

var errBufferCapacityExceeded = errors.New("txnfeed buffer capacity exceeded")

func assertTrue(cond bool, msg string) {
	if buildutil.CrdbTestBuild && !cond {
		panic(msg)
	}
}

func assumedUnreachable(msg string) {
	if buildutil.CrdbTestBuild {
		panic(fmt.Sprintf("assumed unreachable code reached: %s", msg))
	}
}

// waitForEmptyBuffer waits until the buffer is empty. Used for testing only.
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
