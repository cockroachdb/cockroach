// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	*PerRangeEventSink
	sender
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
	return s.send(response, alloc)
}
