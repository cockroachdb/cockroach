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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ServerStreamSender forwards MuxRangefeedEvents from StreamMuxer to the
// underlying stream.
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
	// taskCancel is a function to cancel StreamMuxer.run spawned in the
	// background. It is called by StreamMuxer.Stop. It is expected to be called
	// after StreamMuxer.Start.
	taskCancel context.CancelFunc

	// wg is used to coordinate async tasks spawned by StreamMuxer. Currently,
	// there is only one task spawned by StreamMuxer.Start (StreamMuxer.run).
	wg sync.WaitGroup

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

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

// NewStreamMuxer creates a new StreamMuxer. There should only one for each
// incoming node.MuxRangefeed RPC stream.
func NewStreamMuxer(sender ServerStreamSender) *StreamMuxer {
	return &StreamMuxer{
		sender:         sender,
		notifyMuxError: make(chan struct{}, 1),
	}
}

// AppendMuxError appends a mux rangefeed completion error to be sent back to
// the client. Note that this method cannot block on IO. If the underlying
// stream is broken, the error will be dropped.
func (sm *StreamMuxer) AppendMuxError(e *kvpb.MuxRangeFeedEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.muxErrors = append(sm.mu.muxErrors, e)
	// Note that notify is unblocking.
	select {
	case sm.notifyMuxError <- struct{}{}:
	default:
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

// run forwards rangefeed completion errors back to the client. run is expected
// to be called in a goroutine and will block until the context is done or the
// stopper is quiesced. StreamMuxer will stop forward rangefeed completion
// errors after run completes, and caller is responsible for handling shutdown.
func (sm *StreamMuxer) run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case <-sm.notifyMuxError:
			for _, clientErr := range sm.detachMuxErrors() {
				if err := sm.sender.Send(clientErr); err != nil {
					log.Errorf(ctx,
						"failed to send rangefeed completion error back to client due to broken stream: %v", err)
					return
				}
			}
		case <-ctx.Done():
			return
		case <-stopper.ShouldQuiesce():
			return
		}
	}
}

// Stop cancels the StreamMuxer.run task and waits for it to complete. It does
// nothing if StreamMuxer.run is already finished. It is expected to be called
// after StreamMuxer.Start. Note that the caller is responsible for handling any
// cleanups for any active streams.
func (sm *StreamMuxer) Stop() {
	sm.taskCancel()
	sm.wg.Wait()
}

// Start launches StreamMuxer.run in the background if no error is returned.
// StreamMuxer.run continues running until it errors or StreamMuxer.Stop is
// called. The caller is responsible for calling StreamMuxer.Stop and handle any
// cleanups for any active streams. Example usage:
//
//	if err := streamMuxer.Start(ctx, stopper); err != nil {
//	 return err
//	}
//
// defer streamMuxer.Stop()
func (sm *StreamMuxer) Start(ctx context.Context, stopper *stop.Stopper) error {
	ctx, sm.taskCancel = context.WithCancel(ctx)
	sm.wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "test-stream-muxer", func(ctx context.Context) {
		defer sm.wg.Done()
		sm.run(ctx, stopper)
	}); err != nil {
		sm.taskCancel()
		sm.wg.Done()
		return err
	}
	return nil
}
