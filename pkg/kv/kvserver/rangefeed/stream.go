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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// StreamManager manages one or more streams.
//
// Implemented by BufferedSender and UnbufferedSender.
type StreamManager interface {
	// SendBufferedError sends an error back to client. This call is
	// non-blocking.
	SendBufferedError(ev *kvpb.MuxRangeFeedEvent)

	// Disconnect disconnects the stream with the ev.StreamID. This call is
	// non-blocking, and additional clean-up takes place async. Caller cannot
	// expect immediate disconnection. The returned bool indicates whether the
	// given stream was previously registered with this manager.
	Disconnect(ev *kvpb.MuxRangeFeedEvent) bool

	// AddStream adds a new per-range stream for the streamManager to manage.
	AddStream(streamID int64, r Disconnector)

	// Start starts the streamManager background job to manage all active streams.
	// It continues until it errors or Stop is called. It is not valid to call
	// Start multiple times or restart after Stop.
	Start(ctx context.Context, stopper *stop.Stopper) error

	// Stop streamManager background job if it is still running.
	Stop()

	// Error returns a channel that will be non-empty if the streamManager
	// encounters an error and a node level shutdown is required.
	Error() chan error
}

// Stream is an object capable of transmitting RangeFeedEvents from a server
// rangefeed to a client.
type Stream interface {
	kvpb.RangeFeedEventSink
	// SendError sends an error to the stream. Since this function can be called by
	// the processor worker while holding raftMu as part of
	// registration.Disconnect(), it is important that this function doesn't block
	// IO or try acquiring locks that could lead to deadlocks.
	SendError(err *kvpb.Error)

	AddRegistration(Disconnector)
}

// PerRangeEventSink is an implementation of Stream which annotates each
// response with rangeID and streamID. It is used by MuxRangeFeed.
type PerRangeEventSink struct {
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *UnbufferedSender
	manager  StreamManager
}

func NewPerRangeEventSink(
	rangeID roachpb.RangeID, streamID int64, wrapped *UnbufferedSender,
) *PerRangeEventSink {
	return &PerRangeEventSink{
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
		manager:  wrapped,
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
	return s.wrapped.SendUnbuffered(response)
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
	log.Infof(context.Background(), "s.wrapped.SendBufferedErr(%v)", ev)
	s.wrapped.SendBufferedError(ev)
}

func (s *PerRangeEventSink) AddRegistration(r Disconnector) {
	s.manager.AddStream(s.streamID, r)
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
