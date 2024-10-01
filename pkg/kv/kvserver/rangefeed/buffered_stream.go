// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// BufferedStream is a Stream that can buffer events before sending them to the
// underlying Stream. Note that the caller may still choose to bypass the buffer
// and send to the underlying Stream directly by calling Send directly. Doing so
// can cause event re-ordering. Caller is responsible for ensuring that events
// are sent in order.
type BufferedStream interface {
	Stream
	// SendBuffered buffers the event before sending it to the underlying Stream.
	SendBuffered(*kvpb.RangeFeedEvent, *SharedBudgetAllocation) error
}

// BufferedPerRangeEventSink is an implementation of BufferedStream which is
// similar to PerRangeEventSink but buffers events in BufferedSender before
// forwarding events to the underlying grpc stream.
type BufferedPerRangeEventSink struct {
	ctx      context.Context
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *BufferedSender
}

func NewBufferedPerRangeEventSink(
	ctx context.Context, rangeID roachpb.RangeID, streamID int64, wrapped *BufferedSender,
) *BufferedPerRangeEventSink {
	return &BufferedPerRangeEventSink{
		ctx:      ctx,
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
	}
}

var _ kvpb.RangeFeedEventSink = (*BufferedPerRangeEventSink)(nil)
var _ Stream = (*BufferedPerRangeEventSink)(nil)
var _ BufferedStream = (*BufferedPerRangeEventSink)(nil)

func (s *BufferedPerRangeEventSink) Context() context.Context {
	return s.ctx
}

// SendUnbufferedIsThreadSafe is a no-op declaration method. It is a contract
// that the SendUnbuffered method is thread-safe. Note that
// BufferedSender.SendBuffered and BufferedSender.SendUnbuffered are both
// thread-safe.
func (s *BufferedPerRangeEventSink) SendUnbufferedIsThreadSafe() {}

// SendBuffered buffers the event in BufferedSender and transfers the ownership
// of SharedBudgetAllocation to BufferedSender. BufferedSender is responsible
// for properly using and releasing it when an error occurs or when the event is
// sent. The event is guaranteed to be sent unless BufferedSender terminates
// before sending (such as due to broken grpc stream).
//
// If the function returns an error, it is safe to disconnect the stream and
// assume that all future SendBuffered on this stream will return an error.
func (s *BufferedPerRangeEventSink) SendBuffered(
	event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.SendBuffered(response, alloc)
}

// SendUnbuffered bypass the buffer and sends the event to the underlying grpc
// stream directly. It blocks until the event is sent or an error occurs.
func (s *BufferedPerRangeEventSink) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.SendUnbuffered(response, nil)
}

// Disconnect implements the Stream interface. BufferedSender is then
// responsible for canceling the context of the stream. The actual rangefeed
// disconnection from processor happens late when the error event popped from
// the queue and about to be sent to the grpc stream. So caller should not rely
// on immediate disconnection as cleanup takes place async.
func (s *BufferedPerRangeEventSink) Disconnect(err *kvpb.Error) {
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: s.streamID,
		RangeID:  s.rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	s.wrapped.SendBufferedError(ev)
}
