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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	muxer    *StreamMuxer
}

func NewMuxFeedStream(
	ctx context.Context, streamID int64, rangeID roachpb.RangeID, streamMuxer *StreamMuxer,
) *MuxFeedStream {
	return &MuxFeedStream{
		ctx:      ctx,
		streamID: streamID,
		rangeID:  rangeID,
		muxer:    streamMuxer,
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
	return s.muxer.publish(response)
}

var _ kvpb.RangeFeedEventSink = (*MuxFeedStream)(nil)
var _ Stream = (*MuxFeedStream)(nil)

type sharedMuxEvent struct {
	streamID int64
	rangeID  roachpb.RangeID
	event    *kvpb.RangeFeedEvent
	alloc    *SharedBudgetAllocation
	err      *kvpb.Error
}

type producer struct {
	syncutil.Mutex
	streamId     int64
	rangeID      roachpb.RangeID
	disconnected bool
}

type StreamMuxer struct {
	wrapped    kvpb.MuxRangeFeedEventSink
	capacity   int
	notifyData chan struct{}
	cleanup    chan int

	// queue
	queueMu struct {
		syncutil.Mutex
		buffer muxEventQueue
	}

	prodsMu struct {
		syncutil.RWMutex
		prods map[int64]*producer
	}
}

const defaultEventBufferCapacity = 4096 * 2

func NewStreamMuxer(wrapped kvpb.MuxRangeFeedEventSink) *StreamMuxer {
	muxer := &StreamMuxer{
		wrapped:    wrapped,
		capacity:   defaultEventBufferCapacity,
		notifyData: make(chan struct{}, 1),
		cleanup:    make(chan int, 10),
	}
	muxer.queueMu.buffer = newMuxEventQueue()
	muxer.prodsMu.prods = make(map[int64]*producer)
	return muxer
}

func (m *StreamMuxer) publish(e *kvpb.MuxRangeFeedEvent) error {
	return m.wrapped.Send(e)
}
