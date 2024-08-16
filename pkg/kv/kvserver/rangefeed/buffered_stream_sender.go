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

// PerRangeEventSink is an implementation of rangefeed.Stream which annotates
// each response with rangeID and streamID. It is used by MuxRangeFeed.
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
// Send method is thread-safe. Note that Send wraps rangefeed.StreamMuxer which
// declares its Send method to be thread-safe.
func (s *PerRangeEventSink) SendIsThreadSafe() {}

func (s *PerRangeEventSink) Send(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.Send(response)
}

// Disconnect implements the rangefeed.Stream interface. It requests the
// StreamMuxer to detach the stream. The StreamMuxer is then responsible for
// handling the actual disconnection and additional cleanup. Note that Caller
// should not rely on immediate disconnection as cleanup takes place async.
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

// RegisterRangefeedCleanUp registers a cleanup callback to be called in a
// background async job when the stream is disconnected. Note that the callback will
// not be invoked immediately during DisconnectStreamWithError and may not be
// called if the StreamMuxer.Stop has been called. It is up to the caller to
// ensure that this is not called after StreamMuxer.Stop. For p.Register, it is
// currently done by waiting for runRequest to complete for each
// stores.RangeFeed call.
func (s *BufferedPerRangeEventSink) RegisterRangefeedCleanUp(f func()) {
	s.wrapped.RegisterRangefeedCleanUp(s.streamID, f)
}

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

// BufferedStreamSender is a StreamSender that buffers events before sending
// them to the underlying rpc ServerStreamSender stream.
type BufferedStreamSender struct {
	ServerStreamSender
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender. It returns an error if the buffer is full or has been
// stopped. BufferedStreamSender is responsible for properly releasing it from
// now on. The event is guaranteed to be sent unless the buffered stream
// terminates before sending (e.g. broken grpc stream).
func (bs *BufferedStreamSender) SendBuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	return bs.Send(event)
}
