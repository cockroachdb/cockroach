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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Stream is an object capable of transmitting RangeFeedEvents from a server
// rangefeed to a client.
type Stream interface {
	kvpb.RangeFeedEventSink

	// AddRegistration associate the given registration to this stream.
	AddRegistration(r registration, cleanup func(registration) bool)
	// Disconnect disconnects the stream with the provided error. Note that this
	// function can be called by the processor worker while holding raftMu, so it
	// is important that this function doesn't block IO or try acquiring locks
	// that could lead to deadlocks.
	Disconnect(err *kvpb.Error)
}

// StreamManager is an interface that defines the methods required to manage a
// rangefeed.Stream at the node level. Implemented by BufferedSender
// and UnbufferedSender.
type StreamManager interface {
	// SendBufferedError disconnects the stream with the ev.StreamID and sends
	// error back to client. This call is un-blocking, and additional clean-up
	// takes place async. Caller cannot expect immediate disconnection.
	Disconnect(streamID int64, err *kvpb.Error)
	// AddStream adds a new per-range stream for the streamManager to manage.
	//AddStream(streamID int64, cancel context.CancelFunc)
	// Start starts the streamManager background job to manage all active streams.
	// It continues until it errors or Stop is called. It is not valid to call
	// Start multiple times or restart after Stop.
	Start(ctx context.Context, stopper *stop.Stopper) error
	// Stop streamManager background job if it is still running.
	Stop()
	// Error returns a channel that will be non-empty if the streamManager
	// encounters an error and a node level shutdown is required.
	Error() chan error

	// AddRegistration associate the given registration to the given streamID.
	AddRegistration(streamID int64, r registration, cleanup func(registration) bool)
}

// PerRangeEventSink is an implementation of Stream which annotates each
// response with rangeID and streamID. It is used by MuxRangeFeed.
type PerRangeEventSink struct {
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *UnbufferedSender
	// TODO(ssd): Think about the overlap between this and UnbufferedSender above.
	manager StreamManager
}

func NewPerRangeEventSink(
	rangeID roachpb.RangeID, streamID int64, wrapped *UnbufferedSender, manager StreamManager,
) *PerRangeEventSink {
	return &PerRangeEventSink{
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
		manager:  manager,
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

// Disconnect implements the Stream interface. It requests the UnbufferedSender
// to detach the stream. The UnbufferedSender is then responsible for handling
// the actual disconnection and additional cleanup. Note that Caller should not
// rely on immediate disconnection as cleanup takes place async.
func (s *PerRangeEventSink) Disconnect(err *kvpb.Error) {
	ev := &kvpb.MuxRangeFeedEvent{
		RangeID:  s.rangeID,
		StreamID: s.streamID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	s.wrapped.sendBufferedError(ev)
}

func (s *PerRangeEventSink) AddRegistration(r registration, cleanup func(registration) bool) {
	s.manager.AddRegistration(s.streamID, r, cleanup)
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
