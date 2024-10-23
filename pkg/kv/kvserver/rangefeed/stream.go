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

func (s *PerRangeEventSink) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.send(response, nil)
}

// Disconnect implements the Stream interface.
func (s *PerRangeEventSink) SendError(err *kvpb.Error) {
	ev := &kvpb.MuxRangeFeedEvent{
		RangeID:  s.rangeID,
		StreamID: s.streamID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	if err := s.wrapped.send(ev, nil); err != nil {
		log.Errorf(context.Background(),
			"failed to send rangefeed completion error back to client due to broken stream: %v", err)
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
