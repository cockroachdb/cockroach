// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
	// taskCancel is a function to cancel BufferedSender.run spawned in the
	// background. It is called by BufferedSender.Stop. It is expected to be
	// called after BufferedSender.Start.
	taskCancel context.CancelFunc

	// wg is used to coordinate async tasks spawned by BufferedSender. Currently,
	// there is only one task spawned by BufferedSender.Start
	// (BufferedSender.run).
	wg sync.WaitGroup

	// errCh is used to signal errors from BufferedSender.run back to the caller.
	// If non-empty, the BufferedSender.run is finished and error should be
	// handled. Note that it is possible for BufferedSender.run to be finished
	// without sending an error to errCh. Other goroutines are expected to receive
	// the same shutdown signal in this case and handle error appropriately.
	errCh chan error

	// streamID -> cleanup callback
	activeStreams syncutil.Map[int64, disconnector]

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	queueMu struct {
		syncutil.Mutex
		stopped bool
		buffer  *eventQueue
	}

	// Unblocking channel to notify the BufferedSender.run goroutine that there
	// are events to send.
	notifyDataC chan struct{}
}

func (bs *BufferedSender) Disconnect(ev *kvpb.MuxRangeFeedEvent) bool {
	if ev.Error == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
	}

	if r, ok := bs.activeStreams.Load(ev.StreamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		log.Infof(context.Background(), "disconnect(%v)", &ev.Error.Error)
		(*r).disconnect(&ev.Error.Error)
		return true
	}
	return false
}

func NewBufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *BufferedSender {
	bs := &BufferedSender{
		sender:  sender,
		metrics: metrics,
	}
	bs.queueMu.buffer = newEventQueue()
	bs.notifyDataC = make(chan struct{}, 1)
	return bs
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender.
//
// alloc.Release is nil-safe. SendBuffered will take the ownership of the alloc
// and release it if the return error is non-nil. Note that it is safe to send
// error events without being blocked for too long.
func (bs *BufferedSender) SendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) (err error) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		log.Errorf(context.Background(), "stream sender is stopped")
		return errors.New("stream sender is stopped")
	}
	alloc.Use(context.Background())
	bs.queueMu.buffer.pushBack(sharedMuxEvent{ev, alloc})
	select {
	case bs.notifyDataC <- struct{}{}:
	default:
	}
	return nil
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

func (bs *BufferedSender) waitForEmptyBuffer(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		bs.queueMu.Lock()
		caughtUp := bs.queueMu.buffer.Len() == 0 // nolint:deferunlockcheck
		bs.queueMu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("buffered sender failed to send in time")
}

func (bs *BufferedSender) SendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	if ev.Error == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
	}
	if err := bs.SendBuffered(ev, nil); err != nil {
		// Fine to skip nil checking here since that would be a programming error.
		// Ignore error since the stream is already disconnecting. There is nothing
		// else that could be done. When SendBuffered is returning an error, a node
		// level shutdown from node.MuxRangefeed is happening soon to let clients
		// know that the rangefeed is shutting down.
		log.Infof(context.Background(),
			"failed to buffer rangefeed complete event for stream %d due to %s, "+
				"but a node level shutdown should be happening", ev.StreamID, ev.Error)
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
//func (bs *BufferedSender) RegisterRangefeedCleanUp(streamID int64, cleanUp func()) {
//	bs.rangefeedCleanup.Store(streamID, &cleanUp)
//}

// disconnectAll disconnects all active streams and invokes all rangefeed clean
// up callbacks. It is expected to be called during StreamMuxer.Stop.
func (bs *BufferedSender) disconnectAll() {
	bs.activeStreams.Range(func(streamID int64, r *disconnector) bool {
		(*r).disconnect(nil)
		// Remove the stream from the activeStreams map.
		bs.activeStreams.Delete(streamID)
		bs.metrics.UpdateMetricsOnRangefeedDisconnect()
		return true
	})
}

func (bs *BufferedSender) AddStream(streamID int64, r Disconnector) {
	if _, loaded := bs.activeStreams.LoadOrStore(streamID, &r); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	bs.metrics.UpdateMetricsOnRangefeedConnect()
}

// run forwards buffered events back to the client. run is expected to be called
// in a goroutine and will block until the context is done or the stopper is
// quiesced. BufferedSender will stop forwarding events after run completes. It
// may still buffer more events in the buffer, but they will be cleaned up soon
// during bs.Stop(), and there should be no new events buffered after that.
func (bs *BufferedSender) run(ctx context.Context, stopper *stop.Stopper) error {
	for {
		select {
		case <-ctx.Done():
			// Top level goroutine will receive the context cancellation and handle
			// ctx.Err().
			return nil
		case <-stopper.ShouldQuiesce():
			// Top level goroutine will receive the stopper quiesce signal and handle
			// error.
			return nil
		case <-bs.notifyDataC:
			for {
				e, success := bs.popFront()
				if success {
					err := bs.sender.Send(e.ev)
					e.alloc.Release(ctx)
					if e.ev.Error != nil {
						// Add metrics here
						if r, ok := bs.activeStreams.LoadAndDelete(e.ev.StreamID); ok {
							// TODO(wenyihu6): add more observability metrics into how long the
							// clean up call is taking
							// TODO(wenyihu): add to buffered sender memory queue now
							bs.metrics.UpdateMetricsOnRangefeedDisconnect()
							(*r).disconnect(&e.ev.Error.Error)
						}
					}
					if err != nil {
						return err
					}
				} else {
					break
				}
			}
		}
	}
}

func (bs *BufferedSender) popFront() (e sharedMuxEvent, success bool) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.popFront()
	return event, ok
}

func (bs *BufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	bs.errCh = make(chan error, 1)
	bs.wg.Add(1)
	ctx, bs.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(ctx, "buffered stream output", func(ctx context.Context) {
		defer bs.wg.Done()
		if err := bs.run(ctx, stopper); err != nil {
			bs.errCh <- err
		}
	}); err != nil {
		bs.taskCancel()
		bs.wg.Done()
		return err
	}
	return nil
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
	bs.taskCancel()
	bs.wg.Wait()
	bs.disconnectAll()

	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	bs.queueMu.buffer.removeAll()
}

func (bs *BufferedSender) Error() chan error {
	if bs.errCh == nil {
		log.Fatalf(context.Background(), "BufferedSender.Error called before BufferedSender.Start")
	}
	return bs.errCh
}
