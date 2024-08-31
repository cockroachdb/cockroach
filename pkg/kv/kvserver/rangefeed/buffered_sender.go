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
	"github.com/cockroachdb/cockroach/pkg/util/queue"
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
	streams syncutil.Map[int64, context.CancelFunc]

	// streamID -> cleanup callback
	rangefeedCleanup syncutil.Map[int64, func()]

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	queueMu struct {
		syncutil.Mutex
		stopped  bool
		capacity int64
		buffer   *queue.QueueWithFixedChunkSize[*sharedMuxEvent]
		overflow bool
	}
}

func NewBufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *BufferedSender {
	bs := &BufferedSender{
		sender:  sender,
		metrics: metrics,
	}
	bs.queueMu.buffer = queue.NewQueueWithFixedChunkSize[*sharedMuxEvent]()
	bs.queueMu.capacity = bufferedSenderCapacity
	return bs
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender. Currently, this function is only used for testing
// purposes, so it just mimics the behavior we’d expect from BufferedSender when
// an event is ready to be sent to the underlying gRPC stream. We plan to
// implement this fully by buffering the event in a queue in the future.
//
// alloc.Release is nil-safe. SendBuffered will take the ownership of the alloc
// and release it if the return error is non-nil.
func (bs *BufferedSender) SendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		return errors.New("stream sender is stopped")
	}
	if bs.queueMu.overflow {
		return newRetryErrBufferCapacityExceeded()
	}

	if bs.queueMu.buffer.Len() >= bs.queueMu.capacity {
		bs.queueMu.overflow = true
		return newRetryErrBufferCapacityExceeded()
	}

	bs.metrics.IncQueueSize()
	bs.queueMu.buffer.Enqueue(&sharedMuxEvent{ev, alloc})
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
		caughtUp := bs.queueMu.buffer.Empty() // nolint:deferunlockcheck
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
// up callbacks. It is expected to be called during BufferedSender.Stop.
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

// run forwards rangefeed completion errors back to the client. run is expected
// to be called in a goroutine and will block until the context is done or the
// stopper is quiesced. BufferedSender will stop forward rangefeed completion
// errors after run completes, but a node level shutdown from Node.MuxRangefeed
// should happen soon.
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
		default:
			e, success, overflowed, remains := bs.popFront()
			bs.metrics.DecQueueSize()
			if success {
				err := bs.sender.Send(e.event)
				e.alloc.Release(ctx)
				if e.event.Error != nil {
					// Add metrics here
					if cleanUp, ok := bs.rangefeedCleanup.LoadAndDelete(e.event.StreamID); ok {
						// TODO(wenyihu6): add more observability metrics into how long the
						// clean up call is taking
						(*cleanUp)()
					}
				}
				if err != nil {
					return err
				}
			}
			if overflowed && remains == int64(0) {
				return newRetryErrBufferCapacityExceeded()
			}
		}
	}
}

func (bs *BufferedSender) popFront() (
	e *sharedMuxEvent,
	success bool,
	overflowed bool,
	remains int64,
) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.Dequeue()
	if ok {
		bs.metrics.DecQueueSize()
	}
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
	bs.queueMu.buffer.RemoveAll(func(e *sharedMuxEvent) {
		e.alloc.Release(context.Background())
	})
}

func (bs *BufferedSender) Error() chan error {
	if bs.errCh == nil {
		log.Fatalf(context.Background(), "BufferedSender.Error called before BufferedSender.Start")
	}
	return bs.errCh
}
