// Copyright 2018 The Cockroach Authors.
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

// Stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	// Context returns the context for this stream.
	Context() context.Context
	// Send blocks until it sends m, the stream is done, or the stream breaks.
	// Send must be safe to call on the same stream in different goroutines.
	Send(*kvpb.RangeFeedEvent) error
}

type MuxFeedStream struct {
	ctx      context.Context
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  kvpb.MuxRangeFeedEventSink
	// muxer    *StreamMuxer
}

func NewMuxFeedStream(
	ctx context.Context,
	streamID int64,
	rangeID roachpb.RangeID,
	muxStream kvpb.MuxRangeFeedEventSink,
) *MuxFeedStream {
	return &MuxFeedStream{
		ctx:      ctx,
		streamID: streamID,
		rangeID:  rangeID,
		wrapped:  muxStream,
	}
}

func (s *MuxFeedStream) Context() context.Context {
	return s.ctx
}

func (s *MuxFeedStream) Send(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.Send(response)
}

var _ kvpb.RangeFeedEventSink = (*MuxFeedStream)(nil)
var _ Stream = (*MuxFeedStream)(nil)
