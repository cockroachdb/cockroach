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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	// streamID -> context cancellation
	streams syncutil.Map[int64, context.CancelFunc]

	// streamID -> cleanup callback
	rangefeedCleanup syncutil.Map[int64, func()]

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
// ServerStreamSender. Currently, this function is only used for testing
// purposes, so it just mimics the behavior we’d expect from BufferedSender when
// an event is ready to be sent to the underlying gRPC stream. We plan to
// implement this fully by buffering the event in a queue in the future.
func (bs *BufferedSender) SendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	err := bs.sender.Send(ev)
	alloc.Release(context.Background())
	if ev.Error != nil {
		// Add metrics here
		if cleanUp, ok := bs.rangefeedCleanup.LoadAndDelete(ev.StreamID); ok {
			// TODO(wenyihu6): add more observability metrics into how long the
			// clean up call is taking
			(*cleanUp)()
		}
	}
	return err
}

// SendUnbuffered bypasses the buffer and sends the event to the underlying
// ServerStreamSender directly. Note that this can cause event re-ordering.
// Caller is responsible for ensuring that events are sent in order.
func (bs *BufferedSender) SendUnbuffered(event *kvpb.MuxRangeFeedEvent) error {
	if event.Error != nil {
		log.Fatalf(context.Background(), "unexpected: SendUnbuffered called with error event")
	}
	return bs.sender.Send(event)
}

func (bs *BufferedSender) SendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	if ev.Error == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
	}

	if cancel, ok := bs.streams.LoadAndDelete(ev.StreamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		(*cancel)()
		bs.metrics.UpdateMetricsOnRangefeedDisconnect()
		// Ignore error since the stream is already disconnecting. There is nothing
		// else that could be done. When SendBuffered is returning an error, a node
		// level shutdown from node.MuxRangefeed is happening soon to let clients
		// know that the rangefeed is shutting down.
		log.Infof(context.Background(),
			"failed to buffer rangefeed complete event for stream %d due to %s, "+
				"but a node level shutdown should be happening", ev.StreamID, ev.Error)
		_ = bs.SendBuffered(ev, nil)
	}
}

// RegisterRangefeedCleanUp registers a cleanup callback for unbuffered
// registrations The callback associated with the streamID will be invoked when
// BufferedSender 1. receives an error event with the streamID from
// SendBufferedError 2. the error event has been popped from the queue and is
// about to be sent to grpc stream. Caller cannot expect immediate rangefeed
// cleanup after SendBufferedError, and it may not be called if
// BufferedSender.run stopped. It is not valid to RegisterRangefeedCleanUp on an
// already disconnected stream and expect clean up to be invoked after
// SendBufferedError. TODO(wenyihu6): check with Nathan and see if this is okay?
// Shouldn't be possible since RegisterRangefeedCleanUp is blocking until
// stores.Rangefeed returns.
func (bs *BufferedSender) RegisterRangefeedCleanUp(streamID int64, cleanUp func()) {
	bs.rangefeedCleanup.Store(streamID, &cleanUp)
}

// disconnectAll disconnects all active streams and invokes all rangefeed clean
// up callbacks. It is expected to be called during StreamMuxer.Stop.
func (bs *BufferedSender) disconnectAll() {
	bs.streams.Range(func(streamID int64, cancel *context.CancelFunc) bool {
		(*cancel)()
		// Remove the stream from the activeStreams map.
		bs.streams.Delete(streamID)
		bs.metrics.UpdateMetricsOnRangefeedDisconnect()
		return true
	})

	bs.rangefeedCleanup.Range(func(streamID int64, cleanUp *func()) bool {
		(*cleanUp)()
		bs.rangefeedCleanup.Delete(streamID)
		return true
	})
}

func (bs *BufferedSender) AddStream(streamID int64, cancel context.CancelFunc) {
	if _, loaded := bs.streams.LoadOrStore(streamID, &cancel); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	bs.metrics.UpdateMetricsOnRangefeedConnect()
}

func (bs *BufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// Stop cancels the BufferedSender.run task, waits for it to complete, and
// handles any cleanups for active streams. It is expected to be called after
// BufferedSender.Start. After this function returns, the caller is not expected
// to call RegisterRangefeedCleanUp, assume that the registered cleanup callback
// will be executed after calling SendBufferedError, or assume that
// SendBufferedError would send an error back to the client.
// TODO(wenyihu6): add observability into when this goes wrong
//
// Note that Stop does not send any errors back to notify clients since the grpc
// stream is being torn down, and the client will decide whether to restart all
// rangefeeds again based on the returned error from grpc server stream.
//
// TODO(wenyihu6): add tests to make sure client treats node out of budget
// errors as retryable and will restart all rangefeeds
func (bs *BufferedSender) Stop() {
	bs.disconnectAll()
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Error() chan error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}
