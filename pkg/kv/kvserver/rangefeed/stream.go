// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Stream is an object capable of transmitting RangeFeedEvents from a server
// rangefeed to a client.
type Stream interface {
	kvpb.RangeFeedEventSink
	// Disconnect disconnects the stream with the provided error. Note that this
	// function can be called by the processor worker while holding raftMu, so it
	// is important that this function doesn't block IO or try acquiring locks
	// that could lead to deadlocks.
	Disconnect(err *kvpb.Error)
}

// BufferedStream is a Stream that can buffer events before sending them to the
// underlying Stream. Note that the caller may still choose to bypass the buffer
// and send to the underlying Stream directly by calling Send directly.
type BufferedStream interface {
	Stream
	// SendBuffered buffers the event before sending it to the underlying Stream.
	SendBuffered(*kvpb.RangeFeedEvent, *SharedBudgetAllocation) error
}

// PerRangeEventSink is an implementation of Stream which annotates each
// response with rangeID and streamID. It is used by MuxRangeFeed.
type PerRangeEventSink struct {
	ctx      context.Context
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *StreamMuxer
}

func NewPerRangeEventSink(
	ctx context.Context, rangeID roachpb.RangeID, streamID int64, wrapped *StreamMuxer,
) *PerRangeEventSink {
	return &PerRangeEventSink{
		ctx:      ctx,
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
	}
}

var _ kvpb.RangeFeedEventSink = (*PerRangeEventSink)(nil)
var _ Stream = (*PerRangeEventSink)(nil)

func (s *PerRangeEventSink) Context() context.Context {
	return s.ctx
}

// SendIsThreadSafe is a no-op declaration method. It is a contract that the
// Send method is thread-safe. Note that Send wraps StreamMuxer which declares
// its Send method to be thread-safe.
func (s *PerRangeEventSink) SendIsThreadSafe() {}

func (s *PerRangeEventSink) Send(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.Send(response)
}

// Disconnect implements the Stream interface. It requests the StreamMuxer to
// detach the stream. The StreamMuxer is then responsible for handling the
// actual disconnection and additional cleanup. Note that Caller should not rely
// on immediate disconnection as cleanup takes place async.
func (s *PerRangeEventSink) Disconnect(err *kvpb.Error) {
	s.wrapped.DisconnectStreamWithError(s.streamID, s.rangeID, err)
}

// BufferedPerRangeEventSink is an implementation of rangefeed.BufferedStream
// which buffers events before sending them to the underlying grpc stream.
type BufferedPerRangeEventSink struct {
	*PerRangeEventSink
}

var _ kvpb.RangeFeedEventSink = (*BufferedPerRangeEventSink)(nil)
var _ Stream = (*BufferedPerRangeEventSink)(nil)
var _ BufferedStream = (*BufferedPerRangeEventSink)(nil)

// SendBuffered buffers the event in StreamMuxer.BufferedStreamSender,
// transferring the ownership of the allocated SharedBudgetAllocation to
// bufferedPerRangeEventSink. The underlying streamMuxer is responsible for
// properly using and releasing it when an error occurs or when the event is
// sent. The event is guaranteed to be sent unless the buffered stream
// terminates before sending (e.g. broken grpc stream).
//
// Note that this should only be called if the StreamMuxer has a
// BufferedStreamSender as the sender. Panics otherwise.
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
