// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

//	┌─────────────────────────────────────────┐                                      MuxRangefeedEvent
//	│            Node.MuxRangeFeed            │◄──────────────────────────────────────────────────┐
//	└─────────────────┬───▲───────────────────┘                  ▲                                │
//	 Sender.AddStream │   │LockedMuxStream.Send                  │                                │
//			 ┌────────────▼───┴──────────┐                           │                                │
//			 │ Buffered/Unbuffered Sender├───────────┐               │                                │
//			 └────────────┬──────────────┘           │               │                                │
//			 	   				  │                          │               │                                │
//			 	   ┌────────▼─────────┐                │               │                                │
//			 	   │ Stores.Rangefeed │                │               │                                │
//			 	   └────────┬─────────┘                │               │                                │
//			 	   				  │                          │               │                                │
//			 	    ┌───────▼─────────┐         BufferedSender      BufferedSender                      │
//			 	    │ Store.Rangefeed │ SendUnbuffered/SendBuffered SendBufferedError ─────► BufferedSender.run
//			 	    └───────┬─────────┘ (catch-up scan)(live raft)     ▲
//			 	   				  │                        ▲                 │
//			 	   ┌────────▼──────────┐             │                 │
//			 	   │ Replica.Rangefeed │             │                 │
//			 	   └────────┬──────────┘             │                 │
//			 	   				  │                        │                 │
//			 	    ┌───────▼──────┐                 │                 │
//			 	    │ Registration │                 │                 │
//			 	    └──────┬───────┘                 │                 │
//			 	    			 │      								   │					  		 │
//			 	    			 │                         │                 │
//			 	    			 └─────────────────────────┘─────────────────┘
//			 	    		BufferedPerRangeEventSink.Send    BufferedPerRangeEventSink.Disconnect
//
// BufferedSender is embedded in every rangefeed.BufferedPerRangeEventSink,
// serving as a helper which buffers events before forwarding events to the
// underlying gRPC stream.
//
// Refer to the comments above UnbufferedSender for more details on the role of
// senders in the entire rangefeed architecture.
type BufferedSender struct {
	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder
}

func NewBufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *BufferedSender {
	return &BufferedSender{
		sender:  sender,
		metrics: metrics,
	}
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender.
func (bs *BufferedSender) SendBuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// SendUnbuffered bypasses the buffer and sends the event to the underlying
// ServerStreamSender directly. Note that this can cause event re-ordering.
// Caller is responsible for ensuring that events are sent in order.
func (bs *BufferedSender) SendUnbuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) SendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	// Disconnect stream and cancel context. Then call SendBuffered with the error
	// event.
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) AddStream(streamID int64, cancel context.CancelFunc) {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Stop() {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Error() chan error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}
