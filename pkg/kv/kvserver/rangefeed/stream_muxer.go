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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RangefeedMetricsRecorder is an interface for recording rangefeed metrics.
type RangefeedMetricsRecorder interface {
	UpdateMetricsOnRangefeedConnect()
	UpdateMetricsOnRangefeedDisconnect()
}

// ServerStreamSender forwards MuxRangefeedEvents from StreamMuxer to underlying
// stream.
type ServerStreamSender interface {
	// Send must be thread safe to be called concurrently.
	Send(*kvpb.MuxRangeFeedEvent) error
	// SendIsThreadSafe is a no-op declaration method. It is a contract that the
	// interface has a thread-safe Send method.
	SendIsThreadSafe()
}

// StreamMuxer is responsible for managing a set of active rangefeed streams and
// forwarding rangefeed completion errors to the client.
//
//			                            ┌───────────────────────────┐
//			                            │ DistSender.RangefeedSpans │  rangefeedMuxer
//			                            └───────────────────────────┘
//			                                         │ divideAllSpansOnRangeBoundaries
//			             ┌───────────────────────────┬───────────────────────────┐
//			             ▼                           ▼                           ▼
//			   ┌────────────────────┐     ┌────────────────────┐      ┌────────────────────┐
//			   │   rangefeedMuxer   │     │   rangefeedMuxer   │      │   rangefeedMuxer   │
//			   │startSingleRangefeed│     │startSingleRangefeed│      │startSingleRangefeed│
//			   └─────────┬──────────┘     └──────────┬─────────┘      └──────────┬─────────┘
//			             ▼                           ▼                           ▼
//			        new streamID               new streamID                 new streamID
//			     ┌────────────────┐         ┌────────────────┐           ┌────────────────┐
//			     │RangefeedRequest│         │RangefeedRequest│           │RangefeedRequest│
//			     └────────────────┘         └────────────────┘           └────────────────┘
//			       rangefeedMuxer             rangefeedMuxer               rangefeedMuxer
//			   establishMuxConnection     establishMuxConnection       establishMuxConnection
//			             │                           │                            │
//			             ▼                           ▼                            ▼
//			           rangefeedMuxer.startNodeMuxRangefeed           rangefeedMuxer.startNodeMuxRangefeed
//	               rangefeedMuxer.receiveEventsFromNode           rangefeedMuxer.receiveEventsFromNode
//			        ┌─────────────────────────────────────────┐    ┌─────────────────────────────────────────┐
//			        │rpc.RestrictedInternalClient.MuxRangeFeed│    │rpc.RestrictedInternalClient.MuxRangeFeed│
//			        └─────────────┬────────────▲──────────────┘    └─────────────────────────────────────────┘
//			kvpb.RangefeedRequest │            │ kvpb.MuxRangefeedEvent
//		          ┌─────────────▼────────────┴──────────────┐
//		          │            Node.MuxRangeFeed            │◄───────────────── MuxRangefeedEvent with kvpb.RangeFeedError
//		          └─────────────────┬───▲───────────────────┘                   (client: rangefeedMuxer.restartActiveRangeFeed)
//		      StreamMuxer.AddStream │   │LockedMuxStream.Send(*kvpb.MuxRangefeedEvent)                          │
//		                       ┌────▼───┴────┐                                                                  │
//		                       │ StreamMuxer ├────────────────────────────────────┬─────────────────────────────┐
//		                       └──────┬──────┘                                    │                             │
//		                              │                                           │                             │
//		                     ┌────────▼─────────┐                                 │                             │
//		                     │ Stores.Rangefeed │                                 │                             │
//		                     └────────┬─────────┘                                 │                             │
//		                              │                                           │                             │
//		                      ┌───────▼─────────┐                           StreamMuxer                   StreamMuxer
//		                      │ Store.Rangefeed │              RegisterRangefeedCleanUp  DisconnectRangefeedWithError
//		                      └───────┬─────────┘                                 ▲                             ▲
//		                              │                                           │                             │
//		                     ┌────────▼──────────┐                                │                             │
//		                     │ Replica.Rangefeed │                                │                             │
//		                     └────────┬──────────┘                                │                             │
//		                              │                                           │                             │
//		                       ┌──────▼───────┐                                   │                             │
//		                       │ Registration ├───────────────────────────────────┘                             │
//		                       └──────┬───────┘       ScheduledProcessor.Register                               │
//		                              │                                                                         │
//		                              └─────────────────────────────────────────────────────────────────────────┘
//		                                              registration.disconnect
type StreamMuxer struct {
	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender
	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	// streamID -> context.CancelFunc for active rangefeeds
	activeStreams sync.Map

	// notifyMuxError is a buffered channel of size 1 used to signal the presence
	// of muxErrors. Additional signals are dropped if the channel is already full
	// so that it's unblocking.
	notifyMuxError chan struct{}

	mu struct {
		syncutil.Mutex
		// muxErrors is a slice of mux rangefeed completion errors to be sent back
		// to the client. Upon receiving the error, the client restart rangefeed
		// when possible.
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

// NewStreamMuxer creates a new StreamMuxer. There should only be one per node.
func NewStreamMuxer(sender ServerStreamSender, metrics RangefeedMetricsRecorder) *StreamMuxer {
	return &StreamMuxer{
		sender:         sender,
		metrics:        metrics,
		notifyMuxError: make(chan struct{}, 1),
	}
}

// Start launches StreamMuxer.Run in the background if no error is returned and
// provides a callback to stop the StreamMuxer. It continues running until
// StreamMuxer.Run completes or the provided stop function is called. The caller
// must invoke StreamMuxer.DisconnectAllWithError to disconnect all streams
// after StreamMuxer is stopped. Note that the StreamMuxer will not send
// completion errors after it is stopped. Example usage:
//
//	streamMuxerStop, err := streamMuxer.Start(ctx, n.stopper, errCh)
//	defer streamMuxerStop()
//	if err != nil {
//		return err
//	}
func (sm *StreamMuxer) Start(
	ctx context.Context, stopper *stop.Stopper, errCh chan error,
) (streamMuxerStop func(), err error) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "test-stream-muxer", func(ctx context.Context) {
		defer wg.Done()
		if err := sm.Run(ctx, stopper); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}); err != nil {
		cancel()
		wg.Done()
		return func() {}, err // noop if error
	}
	return func() {
		cancel()
		wg.Wait()
	}, nil
}

// AddStream registers a server rangefeed stream with the StreamMuxer. It
// remains active until DisconnectRangefeedWithError is called with the same
// streamID. Caller must ensure no duplicate stream IDs are added without
// disconnecting the old one first.
func (sm *StreamMuxer) AddStream(streamID int64, cancel context.CancelFunc) {
	if _, loaded := sm.activeStreams.LoadOrStore(streamID, cancel); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	sm.metrics.UpdateMetricsOnRangefeedConnect()
}

// transformRangefeedErrToClientError converts a rangefeed error to a client
// error to be sent back to client. This also handles nil values, preventing nil
// pointer dereference.
func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	if err == nil {
		// When processor is stopped when it no longer has any registrations, it
		// would attempt to close all feeds again with a nil error. Theoretically,
		// this should never happen as processor would always stop with a reason if
		// feeds are active.
		return kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}
	return err
}

// appendMuxError appends a mux rangefeed completion error to be sent back to
// the client. Note that this method cannot block on IO. If the underlying
// stream is broken, the error will be dropped.
func (sm *StreamMuxer) appendMuxError(e *kvpb.MuxRangeFeedEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.muxErrors = append(sm.mu.muxErrors, e)
	// Note that notifyMuxError is non-blocking.
	select {
	case sm.notifyMuxError <- struct{}{}:
	default:
	}
}

// DisconnectRangefeedWithError disconnects a stream with an error. Safe to call
// repeatedly for the same stream, but subsequent errors are ignored. It ensures
// 1. the stream context is cancelled 2. exactly one error is sent back to the
// client on behalf of the stream.
//
// Note that this function can be called by the processor worker while holding
// raftMu, so it is important that this function doesn't block IO. It does so by
// delegating the responsibility of sending mux error to StreamMuxer.Run.
func (sm *StreamMuxer) DisconnectRangefeedWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	if cancelFunc, ok := sm.activeStreams.LoadAndDelete(streamID); ok {
		f, ok := cancelFunc.(context.CancelFunc)
		if !ok {
			log.Fatalf(context.Background(), "unexpected stream type %T", cancelFunc)
		}
		f()
		clientErrorEvent := transformRangefeedErrToClientError(err)
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		ev.MustSetValue(&kvpb.RangeFeedError{
			Error: *clientErrorEvent,
		})
		sm.appendMuxError(ev)
		sm.metrics.UpdateMetricsOnRangefeedDisconnect()
	}
}

// detachMuxErrors returns muxErrors and clears the slice. Caller must ensure
// the returned errors are sent back to the client.
func (sm *StreamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toSend := sm.mu.muxErrors
	sm.mu.muxErrors = nil
	return toSend
}

// Run forwards rangefeed completion errors back to the client.
//
// Run is expected to be called in a goroutine and will block until the context
// is done or the stopper is quiesced. Example usage:
//
// muxer := NewStreamMuxer(muxStream, metrics)
// var wg sync.WaitGroup
// defer wg.Wait()
// defer cancel() - important to stop the muxer before wg.Wait()
// wg.Add(1)
//
//	if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
//	 defer wg.Done()
//	 muxer.Run(ctx, stopper)
//	}); err != nil {
//	 wg.Done()
//	}
func (sm *StreamMuxer) Run(ctx context.Context, stopper *stop.Stopper) error {
	for {
		select {
		case <-sm.notifyMuxError:
			toSend := sm.detachMuxErrors()
			for _, clientErr := range toSend {
				if err := sm.sender.Send(clientErr); err != nil {
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
