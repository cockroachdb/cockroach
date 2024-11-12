// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RangefeedMetricsRecorder is an interface for recording rangefeed metrics.
type RangefeedMetricsRecorder interface {
	UpdateMetricsOnRangefeedConnect()
	UpdateMetricsOnRangefeedDisconnect()
	UpdateMetricsOnRangefeedDisconnectBy(num int64)
}

// ServerStreamSender forwards MuxRangefeedEvents from UnbufferedSender to the
// underlying grpc stream.
type ServerStreamSender interface {
	// Send must be thread-safe to be called concurrently.
	Send(*kvpb.MuxRangeFeedEvent) error
	// SendIsThreadSafe is a no-op declaration method. It is a contract that the
	// interface has a thread-safe Send method.
	SendIsThreadSafe()
}

//			                            ┌───────────────────────────┐
//			                            │ DistSender.RangefeedSpans │  rangefeedMuxer
//			                            └────────────▲──────────────┘
//			                                         │ divideAllSpansOnRangeBoundaries
//		            ┌───────────────────────────┬───────────────────────────┐
//		            │                           │                           │
//		  ┌────────────────────┐     ┌────────────────────┐      ┌────────────────────┐
//		  │   rangefeedMuxer   │     │   rangefeedMuxer   │      │   rangefeedMuxer   │   (client: rangefeedMuxer.
//		  │startSingleRangefeed│     │startSingleRangefeed│      │startSingleRangefeed│   restartActiveRangeFeed)
//		  └─────────▲──────────┘     └──────────▲─────────┘      └──────────▲─────────┘                  ▲
//		            │                           │                           │                            │
//		       new streamID               new streamID                 new streamID                      │
//		    ┌────────────────┐         ┌────────────────┐           ┌────────────────┐                   │
//		    │RangefeedRequest│         │RangefeedRequest│           │RangefeedRequest│                   │
//		    └────────────────┘         └────────────────┘           └────────────────┘                   │
//		      rangefeedMuxer             rangefeedMuxer               rangefeedMuxer                     │
//		  establishMuxConnection     establishMuxConnection       establishMuxConnection                 │
//		            ▲                           ▲                            ▲                           │
//		            │                           │                            │                           │
//		          rangefeedMuxer.startNodeMuxRangefeed           rangefeedMuxer.startNodeMuxRangefeed    │
//		          rangefeedMuxer.receiveEventsFromNode           rangefeedMuxer.receiveEventsFromNode    │
//		       ┌─────────────────────────────────────────┐    ┌─────────────────────────────────────────┐│
//		       │rpc.RestrictedInternalClient.MuxRangeFeed│    │rpc.RestrictedInternalClient.MuxRangeFeed││
//		       └─────────────┬────────────▲──────────────┘    └─────────────────────────────────────────┘│
//	 kvpb.RangefeedRequest │            │ kvpb.MuxRangefeedEvent                                       │
//			   ┌───────────────▼────────────┴────────────┐                                      MuxRangefeedEvent
//			   │            Node.MuxRangeFeed            │◄─────── MuxRangefeedEvent ────────── with kvpb.RangeFeedError
//			   └─────────────────┬───▲───────────────────┘                ▲                                ▲
//			    Sender.AddStream │   │LockedMuxStream.Send                │                                │
//			        ┌────────────▼───┴──────────┐                         │                                │
//			        │ Buffered/Unbuffered Sender├───────────┐             │                                │
//			        └────────────▲──────────────┘           │             │                                │
//			                     │                          │             │                                │
//			            ┌────────┴──────────┐        UnbufferedSender  UnbufferedSender                    │
//			            │ Replica.Rangefeed │        sendUnbuffered    sendBuffered ─────► UnbufferedSender.run
//					        └────────▲──────────┘              ▲               ▲                               ▲
//			 		                 │                         │               │                               │
//			 		          ┌──────┴───────┐                 │               │                               │
//			 		          │ Registration │                 │               │                               │
//			 		          └──────┬───────┘                 │               │                               │
//			  	                 │                         │               │                               │
//			         	 	         └─────────────────────────┘───────────────┘───────────────────────────────┘
//			          			                  PerRangeEventSink.Send   PerRangeEventSink.SendError

// UnbufferedSender is embedded in every rangefeed.PerRangeEventSink, serving as
// a helper to forward events to the underlying gRPC stream.
// - For non-error events, SendUnbuffered is blocking until the event is sent.
// - For error events, SendBufferedError is non-blocking and ensures
// 1) stream context is canceled
// 2) exactly one error is sent back to the client on behalf of the stream
// 3) metrics updates.
// It makes sure SendBufferedError is non-blocking by delegating the
// responsibility of sending mux error to UnbufferedSender.run (in a separate
// goroutine). There should only be one UnbufferedSender per Node.MuxRangefeed.
type UnbufferedSender struct {
	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// notifyMuxError is a buffered channel of size 1 used to signal the presence
	// of muxErrors. Additional signals are dropped if the channel is already full
	// so that it's non-blocking.
	notifyMuxError chan struct{}

	mu struct {
		syncutil.Mutex
		// muxErrors is a slice of mux rangefeed completion errors to be sent back
		// to the client. Upon receiving the error, the client restart rangefeed
		// when possible.
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func NewUnbufferedSender(sender ServerStreamSender) *UnbufferedSender {
	return &UnbufferedSender{
		notifyMuxError: make(chan struct{}, 1),
		sender:         sender,
	}
}

// sendBuffered sends MuxRangeFeedEvent to client gRPC without blocking. It does
// so by delegating the responsibility of sending events to
// UnbufferedSender.run.
//
// Note that ev may not be sent successfully if UnbufferedSender.run stops
// early. In that case, Node.MuxRangefeed would return, allowing clients to know
// rangefeeds termination. Safe to call with nil alloc.
func (ubs *UnbufferedSender) sendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	ubs.appendMuxError(ev)
	alloc.Release(context.Background())
	return nil
}

// sendUnbuffered blocks until the event is sent to the underlying grpc stream.
// It should be only called for non-error events. If this function returns an
// error, caller must ensure that no further events are sent from
// rangefeed.Stream to avoid potential event loss. (NB: While subsequent Send
// should also return an error if one is encountered, let's play safe.)
// Important to be thread-safe.
func (ubs *UnbufferedSender) sendUnbuffered(event *kvpb.MuxRangeFeedEvent) error {
	if event.Error != nil {
		log.Fatalf(context.Background(), "unexpected: SendUnbuffered called with error event")
	}
	return ubs.sender.Send(event)
}

// run forwards rangefeed completion errors back to the client. run is expected
// to be called in a goroutine and will block until the context is done or the
// stopper is quiesced. UnbufferedSender will stop forward rangefeed completion
// errors after run completes, but a node level shutdown from Node.MuxRangefeed
// should happen soon.
func (ubs *UnbufferedSender) run(
	ctx context.Context, stopper *stop.Stopper, onError func(streamID int64),
) error {
	for {
		select {
		case <-ubs.notifyMuxError:
			toSend := ubs.detachMuxErrors()
			for _, clientErr := range toSend {
				onError(clientErr.StreamID)
				if err := ubs.sender.Send(clientErr); err != nil {
					log.Infof(ctx, "failed to send rangefeed error to client: %v", err)
					return err
				}
			}
		case <-ctx.Done():
			// Top level goroutine will receive the context cancellation and handle
			// ctx.Err().
			return nil
		case <-stopper.ShouldQuiesce():
			// Top level goroutine will receive the stopper quiesce signal and handle
			// error.
			return nil
		}
	}
}

// appendMuxError appends a mux rangefeed completion error to be sent back to
// the client. Note that this method cannot block on IO.
func (ubs *UnbufferedSender) appendMuxError(e *kvpb.MuxRangeFeedEvent) {
	ubs.mu.Lock()
	defer ubs.mu.Unlock()
	ubs.mu.muxErrors = append(ubs.mu.muxErrors, e)
	// Note that notifyMuxError is non-blocking.
	select {
	case ubs.notifyMuxError <- struct{}{}:
	default:
	}
}

// detachMuxErrors returns muxErrors and clears the slice. Caller must ensure
// the returned errors are sent back to the client.
func (ubs *UnbufferedSender) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	ubs.mu.Lock()
	defer ubs.mu.Unlock()
	toSend := ubs.mu.muxErrors
	ubs.mu.muxErrors = nil
	return toSend
}

func (ubs *UnbufferedSender) cleanup(context.Context) {}
