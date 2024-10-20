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
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var bufferedSenderCapacity = envutil.EnvOrDefaultInt64(
	"COCKROACH_BUFFERED_STREAM_CAPACITY_PER_STREAM", math.MaxInt64)

type sharedMuxEvent struct {
	event *kvpb.MuxRangeFeedEvent
	alloc *SharedBudgetAllocation
}

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

	// streamID -> context cancellation
	// TODO: figure out how to remove this
	//streams syncutil.Map[int64, context.CancelFunc]

	// streamID -> managedRegistration
	registrations syncutil.Map[int64, managedRegistration]

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	queueMu struct {
		syncutil.Mutex
		stopped  bool
		capacity int64
		buffer   *eventQueue
		overflow bool
	}

	// Unblocking channel to notify the BufferedSender.run goroutine that there
	// are events to send.
	notifyDataC chan struct{}
}

type managedRegistration struct {
	reg     registration
	cleanup func(registration) bool
}

func NewBufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *BufferedSender {
	bs := &BufferedSender{
		sender:  sender,
		metrics: metrics,
	}
	bs.queueMu.buffer = newEventQueue()
	bs.queueMu.capacity = bufferedSenderCapacity
	bs.notifyDataC = make(chan struct{}, 1)
	return bs
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender.
//
// alloc.Release is nil-safe. SendBuffered will take the ownership of the alloc.
// It is responsible for using if the return error is nil and releasing the
// token in the end. Note that it is safe to send error events here without
// being worried about getting blocked for too long.
func (bs *BufferedSender) SendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) (err error) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		log.Errorf(context.Background(), "stream sender is stopped")
		return errors.New("stream sender is stopped")
	}
	if bs.queueMu.overflow {
		log.Error(context.Background(), "buffer capacity exceeded")
		return newRetryErrBufferCapacityExceeded()
	}
	if bs.queueMu.buffer.Len() >= bs.queueMu.capacity {
		bs.queueMu.overflow = true
		return newRetryErrBufferCapacityExceeded()
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
	bs.metrics.IncNodeLevelEvents()
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

func (bs *BufferedSender) Disconnect(streamID int64, err *kvpb.Error) {
	if err == nil {
		log.Fatalf(context.Background(), "unexpected: Disconnect called with non-error event")
	}
	if r, ok := bs.registrations.Load(streamID); ok {
		r.reg.disconnect(err)
		//bs.sendBufferedError(ev)
	}
}

func (bs *BufferedSender) sendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	if ev.Error == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
	}
	if _, ok := bs.registrations.Load(ev.StreamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		bs.metrics.UpdateMetricsOnRangefeedDisconnect()
		if err := bs.SendBuffered(ev, nil); err != nil {
			// Ignore error since the stream is already disconnecting. There is nothing
			// else that could be done. When SendBuffered is returning an error, a node
			// level shutdown from node.MuxRangefeed is happening soon to let clients
			// know that the rangefeed is shutting down.
			log.Infof(context.Background(),
				"failed to buffer rangefeed complete event for stream %d due to %s, "+
					"but a node level shutdown should be happening", ev.StreamID, ev.Error)
		}
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
func (bs *BufferedSender) AddRegistration(streamID int64, r registration, cleanUp func(registration) bool) {
	bs.registrations.Store(streamID, &managedRegistration{
		reg:     r,
		cleanup: cleanUp,
	})
}

// disconnectAll disconnects all active streams and invokes all rangefeed clean
// up callbacks. It is expected to be called during BufferedSender.Stop.
func (bs *BufferedSender) disconnectAll() {
	//bs.streams.Range(func(streamID int64, cancel *context.CancelFunc) bool {
	//	//(*cancel)()
	//	// Remove the stream from the activeStreams map.
	//	//bs.streams.Delete(streamID)
	//	//bs.metrics.UpdateMetricsOnRangefeedDisconnect()
	//	return true
	//})

	bs.registrations.Range(func(streamID int64, mr *managedRegistration) bool {
		// disconnect is going to send error again but that's fine
		// Important to do it here since reg.disconnect would trigger send buffered
		// error again. We deduplicate by deleting here.
		bs.registrations.Delete(streamID)
		mr.reg.disconnect(nil)
		mr.cleanup(mr.reg)
		bs.metrics.DecRangefeedCleanUp()
		return true
	})
}

//func (bs *BufferedSender) AddStream(streamID int64, cancel context.CancelFunc) {
//	if _, loaded := bs.streams.LoadOrStore(streamID, &cancel); loaded {
//		log.Fatalf(context.Background(), "stream %d already exists", streamID)
//	}
//	bs.metrics.UpdateMetricsOnRangefeedConnect()
//}

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
				e, success, overflowed, remains := bs.popFront()
				if success {
					bs.metrics.UpdateQueueSize(remains)
					bs.metrics.IncEventsSentCount()
					bs.metrics.IncNodeLevelEvents()
					err := bs.sender.Send(e.event)
					e.alloc.Release(ctx)
					if e.event.Error != nil {
						// TODO(wenyihu6): check if we need to cancel context here
						bs.metrics.IncErrorEvents()
						// Add metrics here
						if mr, ok := bs.registrations.LoadAndDelete(e.event.StreamID); ok {
							bs.metrics.DecRangefeedCleanUp()
							// TODO(wenyihu6): add more observability metrics into how long the
							// clean up call is taking
							mr.cleanup(mr.reg)
						}
					}
					if err != nil {
						return err
					}
					if overflowed && remains == int64(0) {
						return newRetryErrBufferCapacityExceeded()
					}
				} else {
					break
				}
			}
		}
	}
}

func (bs *BufferedSender) popFront() (
	e sharedMuxEvent,
	success bool,
	overflowed bool,
	remains int64,
) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.popFront()
	return event, ok, bs.queueMu.overflow, bs.queueMu.buffer.Len()
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
// BufferedSender.Start. After this function returns, BufferedSend will return
// an error to avoid more being buffered afterwards. The caller is also not
// expected to call RegisterRangefeedCleanUp after this function ends and assume
// that the registered cleanup callback will be executed after calling
// SendBufferedError, or assume that SendBufferedError would send an error back
// to the client.
// TODO(wenyihu6): add observability into when this goes wrong
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
