// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RangefeedMetricsRecorder is an interface for recording rangefeed metrics.
type RangefeedMetricsRecorder interface {
	UpdateMetricsOnRangefeedConnect()
	UpdateMetricsOnRangefeedDisconnect()
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
//			                            └───────────────────────────┘
//			                                         │ divideAllSpansOnRangeBoundaries
//		            ┌───────────────────────────┬───────────────────────────┐
//		            ▼                           ▼                           ▼
//		  ┌────────────────────┐     ┌────────────────────┐      ┌────────────────────┐
//		  │   rangefeedMuxer   │     │   rangefeedMuxer   │      │   rangefeedMuxer   │   (client: rangefeedMuxer.
//		  │startSingleRangefeed│     │startSingleRangefeed│      │startSingleRangefeed│   restartActiveRangeFeed)
//		  └─────────┬──────────┘     └──────────┬─────────┘      └──────────┬─────────┘                  ▲
//		            ▼                           ▼                           ▼                            │
//		       new streamID               new streamID                 new streamID                      │
//		    ┌────────────────┐         ┌────────────────┐           ┌────────────────┐                   │
//		    │RangefeedRequest│         │RangefeedRequest│           │RangefeedRequest│                   │
//		    └────────────────┘         └────────────────┘           └────────────────┘                   │
//		      rangefeedMuxer             rangefeedMuxer               rangefeedMuxer                     │
//		  establishMuxConnection     establishMuxConnection       establishMuxConnection                 │
//		            │                           │                            │                           │
//		            ▼                           ▼                            ▼                           │
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
//			        └────────────┬──────────────┘           │             │                                │
//			                     │                          │             │                                │
//			            ┌────────▼─────────┐                │             │                                │
//			            │ Stores.Rangefeed │                │             │                                │
//			            └────────┬─────────┘                │             │                                │
//			                     │                          │             │                                │
//			             ┌───────▼─────────┐         UnbufferedSender  UnbufferedSender                    │
//			             │ Store.Rangefeed │         SendUnbuffered    SendBufferedError ─────► UnbufferedSender.run
//					         └───────┬─────────┘               ▲               ▲                               ▲
//			 		                 │                         │               │                               │
//			 		        ┌────────▼──────────┐              │               │                               │
//			 		        │ Replica.Rangefeed │              │               │                               │
//			 		        └────────┬──────────┘              │               │                               │
//			 		                 │                         │               │                               │
//			              ┌──────▼───────┐                 │               │                               │
//			              │ Registration │                 │               │                               │
//			              └──────┬───────┘                 │               │                               │
//		                       │      								   │							 │						           				 │
//			  	                 │                         │               │                               │
//			         	 	         └─────────────────────────┘───────────────┘───────────────────────────────┘
//			          			                  PerRangeEventSink.Send   PerRangeEventSink.Disconnect
//
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
	// taskCancel is a function to cancel UnbufferedSender.run spawned in the
	// background. It is called by UnbufferedSender.Stop. It is expected to be
	// called after UnbufferedSender.Start.
	taskCancel context.CancelFunc

	// wg is used to coordinate async tasks spawned by UnbufferedSender.
	// Currently, there is only one task spawned by UnbufferedSender.Start
	// (UnbufferedSender.run).
	wg sync.WaitGroup

	// errCh is used to signal errors from UnbufferedSender.run back to the
	// caller. If non-empty, the UnbufferedSender.run is finished and error should
	// be handled. Note that it is possible for UnbufferedSender.run to be
	// finished without sending an error to errCh. Other goroutines are expected
	// to receive the same shutdown signal in this case and handle error
	// appropriately.
	errCh chan error

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// notifyMuxError is a buffered channel of size 1 used to signal the presence
	// of muxErrors. Additional signals are dropped if the channel is already full
	// so that it's non-blocking.
	notifyMuxError chan struct{}

	// streamID -> context cancellation
	streams syncutil.Map[int64, context.CancelFunc]

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	mu struct {
		syncutil.Mutex
		// muxErrors is a slice of mux rangefeed completion errors to be sent back
		// to the client. Upon receiving the error, the client restart rangefeed
		// when possible.
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func NewUnbufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *UnbufferedSender {
	return &UnbufferedSender{
		notifyMuxError: make(chan struct{}, 1),
		sender:         sender,
		metrics:        metrics,
	}
}

// SendBufferedError 1. Sends a mux rangefeed completion error to the
// client without blocking. It does so by delegating the responsibility of
// sending mux error to UnbufferedSender.run 2. Disconnects the stream with
// ev.StreamID. Safe to call repeatedly for the same stream, but subsequent
// errors are ignored.
//
// The error event is not sent immediately. It is deferred to
// UnbufferedSender.run (async). If a node level shutdown occurs (such as
// underlying grpc stream is broken), UnbufferedSender.run would return early,
// and the error may not be sent successfully. In that case, Node.MuxRangefeed
// would return, allowing clients to know rangefeed completions.
//
// Note that this function can be called by the processor worker while holding
// raftMu, so it is important that this function doesn't block on IO. Caller
// needs to make sure this is called only with non-nil error events. Important
// to be thread-safe.
func (ubs *UnbufferedSender) SendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	if ev.Error == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
	}

	if cancel, ok := ubs.streams.LoadAndDelete(ev.StreamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		(*cancel)()
		ubs.metrics.UpdateMetricsOnRangefeedDisconnect()
		ubs.appendMuxError(ev)
	}
}

// SendUnbuffered blocks until the event is sent to the underlying grpc stream.
// It should be only called for non-error events. If this function returns an
// error, caller must ensure that no further events are sent from
// rangefeed.Stream to avoid potential event loss. (NB: While subsequent Send
// should also return an error if one is encountered, let's play safe.)
// Important to be thread-safe.
func (ubs *UnbufferedSender) SendUnbuffered(event *kvpb.MuxRangeFeedEvent) error {
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
func (ubs *UnbufferedSender) run(ctx context.Context, stopper *stop.Stopper) error {
	for {
		select {
		case <-ubs.notifyMuxError:
			toSend := ubs.detachMuxErrors()
			for _, clientErr := range toSend {
				if err := ubs.sender.Send(clientErr); err != nil {
					log.Errorf(ctx,
						"failed to send rangefeed completion error back to client due to broken stream: %v", err)
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

// Start launches UnbufferedSender.run in the background if no error is
// returned. UnbufferedSender.run continues running until it errors or
// UnbufferedSender.Stop is called. The caller is responsible for calling
// UnbufferedSender.Stop and handle any cleanups for any active streams. Note
// that it is not valid to call Start multiple times or restart after Stop.
// Example usage:
//
//	if err := UnbufferedSender.Start(ctx, stopper); err != nil {
//	 return err
//	}
//
// defer UnbufferedSender.Stop()
func (ubs *UnbufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	if ubs.errCh != nil {
		log.Fatalf(ctx, "UnbufferedSender.Start called multiple times")
	}
	ubs.errCh = make(chan error, 1)
	ctx, ubs.taskCancel = context.WithCancel(ctx)
	ubs.wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "unbuffered sender", func(ctx context.Context) {
		defer ubs.wg.Done()
		if err := ubs.run(ctx, stopper); err != nil {
			ubs.errCh <- err
		}
	}); err != nil {
		ubs.taskCancel()
		ubs.wg.Done()
		return err
	}
	return nil
}

// Error returns a channel that can be used to receive errors from
// UnbufferedSender.run. Only non-nil errors are sent on this channel. If
// non-empty, UnbufferedSender.run is finished, and the caller is responsible
// for handling the error.
func (ubs *UnbufferedSender) Error() chan error {
	if ubs.errCh == nil {
		log.Fatalf(context.Background(), "UnbufferedSender.Error called before UnbufferedSender.Start")
	}
	return ubs.errCh
}

// Stop cancels the UnbufferedSender.run task and waits for it to complete. It
// does nothing if UnbufferedSender.run is already finished. It is expected to
// be called after UnbufferedSender.Start. Note that the caller is responsible
// for handling any cleanups for any active streams or mux errors that are not
// sent back successfully.
func (ubs *UnbufferedSender) Stop() {
	ubs.taskCancel()
	ubs.wg.Wait()

	// It is okay to not clean up mux errors here since node level shutdown is
	// happening. It is also okay to not disconnect all active streams (context
	// cancellation, decrement metrics here since SendBufferedError will be called
	// again by rangefeed.Stream after that. No errors will be sent but metrics
	// cleanup will still happen during SendBufferedError.
}

// AddStream registers a rangefeed.Stream with UnbufferedSender. It remains
// active until SendBufferedError is called with the same streamID.
// Caller must ensure no duplicate stream IDs are added without disconnecting
// the old one first.
func (ubs *UnbufferedSender) AddStream(streamID int64, cancel context.CancelFunc) {
	if _, loaded := ubs.streams.LoadOrStore(streamID, &cancel); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	ubs.metrics.UpdateMetricsOnRangefeedConnect()
}
