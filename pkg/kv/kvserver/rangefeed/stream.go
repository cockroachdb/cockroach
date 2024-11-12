// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Stream is an object capable of transmitting RangeFeedEvents from a server
// rangefeed to a client.
type Stream interface {
	kvpb.RangeFeedEventSink
	// SendError sends an error to the stream. Since this function can be called by
	// the processor worker while holding raftMu as part of
	// registration.Disconnect(), it is important that this function doesn't block
	// IO or try acquiring locks that could lead to deadlocks.
	SendError(err *kvpb.Error)
}

// BufferedStream is a Stream that can buffer events before sending them to the
// underlying Stream. Note that the caller may still choose to bypass the buffer
// and send to the underlying Stream directly by calling Send directly. Doing so
// can cause event re-ordering. Caller is responsible for ensuring that events
// are sent in order.
type BufferedStream interface {
	Stream
	// SendBuffered buffers the event before sending it to the underlying Stream.
	// It should not block if ev.Error != nil.
	SendBuffered(*kvpb.RangeFeedEvent, *SharedBudgetAllocation) error
}

// PerRangeEventSink is an implementation of Stream which annotates each
// response with rangeID and streamID. It is used by MuxRangeFeed.
type PerRangeEventSink struct {
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  sender
}

func NewPerRangeEventSink(
	rangeID roachpb.RangeID, streamID int64, wrapped sender,
) *PerRangeEventSink {
	return &PerRangeEventSink{
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
	}
}

var _ kvpb.RangeFeedEventSink = (*PerRangeEventSink)(nil)
var _ Stream = (*PerRangeEventSink)(nil)

// SendUnbufferedIsThreadSafe is a no-op declaration method. It is a contract
// that the SendUnbuffered method is thread-safe. Note that
// UnbufferedSender.SendUnbuffered is thread-safe.
func (s *PerRangeEventSink) SendUnbufferedIsThreadSafe() {}

// SendUnbuffered implements the Stream interface. It sends a RangeFeedEvent to
// the underlying grpc stream directly.
func (s *PerRangeEventSink) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.sendUnbuffered(response)
}

// SendError implements the Stream interface. It sends a rangefeed completion
// error back to the client without blocking. Note that this may be called by
// the processor worker while holding raftMu, so it is important that this
// function doesn't block on IO.
func (s *PerRangeEventSink) SendError(err *kvpb.Error) {
	ev := &kvpb.MuxRangeFeedEvent{
		RangeID:  s.rangeID,
		StreamID: s.streamID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	if ev.Error == nil {
		log.Fatalf(context.Background(),
			"unexpected: SendWithoutBlocking called with non-error event")
	}
	if err := s.wrapped.sendBuffered(ev, nil); err != nil {
		log.Infof(context.Background(),
			"failed to send rangefeed error to client: %v", err)
	}
}

// transformRangefeedErrToClientError converts a rangefeed error to a client
// error to be sent back to client. This also handles nil values, preventing nil
// pointer dereference.
//
// NB: when processor.Stop() is called (stopped when it no longer has any
// registrations, it would attempt to close all feeds again with a nil error).
// Theoretically, this should never happen as processor would always stop with a
// reason if feeds are active.
func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	if err == nil {
		return kvpb.NewError(
			kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}
	return err
}

// BufferedPerRangeEventSink is an implementation of BufferedStream which is
// similar to PerRangeEventSink but buffers events in BufferedSender before
// forwarding events to the underlying grpc stream.
type BufferedPerRangeEventSink struct {
	*PerRangeEventSink
}

var _ kvpb.RangeFeedEventSink = (*BufferedPerRangeEventSink)(nil)
var _ Stream = (*BufferedPerRangeEventSink)(nil)
var _ BufferedStream = (*BufferedPerRangeEventSink)(nil)

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
	return s.wrapped.sendBuffered(response, alloc)
}
