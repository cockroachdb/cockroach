// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// StreamManager manages one or more streams. It is responsible for starting and
// stopping the underlying sender, as well as managing the streams themselves.
type StreamManager struct {
	// taskCancel is a function to cancel sender.run spawned in the background. It
	// is called by StreamManager.Stop. It is expected to be called after
	// StreamManager.Start.
	taskCancel context.CancelFunc

	// wg is used to coordinate async tasks spawned by sender. Currently, there is
	// only one task spawned by StreamManager.Start (sender.run).
	wg sync.WaitGroup

	// errCh is used to signal errors from sender.run back to the caller. If
	// non-empty, the sender.run is finished and error should be handled. Note
	// that it is possible for sender.run to be finished without sending an error
	// to errCh. Other goroutines are expected to receive the same shutdown signal
	// in this case and handle error appropriately.
	errCh chan error

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety. Implemented by BufferedSender and UnbufferedSender.
	sender sender

	// streamID -> Disconnector
	streams struct {
		syncutil.Mutex
		m map[int64]Disconnector
	}

	// metrics is used to record rangefeed metrics for the node. It tracks number
	// of active rangefeeds.
	metrics RangefeedMetricsRecorder
}

// sender is an interface that is implemented by BufferedSender and
// UnbufferedSender. It is wrapped under StreamManager, Stream, and
// BufferedStream.
type sender interface {
	sendUnbuffered(ev *kvpb.MuxRangeFeedEvent) error
	// send sends a RangeFeedEvent to the underlying sender. BufferedStream,
	// Stream share this method. The contract is: send should not block if
	// ev.Error != nil. If alloc is not nil, BufferedSender should buffer events
	// in the queue. Otherwise, BufferedSender/UnbufferedSender should send the
	// event to the underlying gRPC stream directly.
	sendBuffered(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error
	// run is the main loop for the sender. It is expected to run in the
	// background until a node level error is encountered which would shut down
	// all streams in StreamManager.
	run(ctx context.Context, stopper *stop.Stopper, onError func(int64)) error
	// cleanup is called when the sender is stopped. It is expected to clean up
	// any resources used by the sender.
	cleanup()
}

func NewStreamManager(sender sender, metrics RangefeedMetricsRecorder) *StreamManager {
	sm := &StreamManager{
		sender:  sender,
		metrics: metrics,
	}
	sm.streams.m = make(map[int64]Disconnector)
	return sm
}

// NewStream creates a new Stream for the given streamID and rangeID.
func (sm *StreamManager) NewStream(streamID int64, rangeID roachpb.RangeID) (sink Stream) {
	return NewPerRangeEventSink(rangeID, streamID, sm.sender)
}

// OnError is a callback that is called when a sender sends a rangefeed
// completion error back to the client. Note that we check for the existence of
// streamID to avoid metrics inaccuracy when the error is sent before the stream
// is added to the StreamManager.
func (sm *StreamManager) OnError(streamID int64) {
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if _, ok := sm.streams.m[streamID]; ok {
		delete(sm.streams.m, streamID)
		sm.metrics.UpdateMetricsOnRangefeedDisconnect()
	}
}

// DisconnectStream disconnects the stream with the given streamID.
func (sm *StreamManager) DisconnectStream(streamID int64, err *kvpb.Error) {
	if err == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
		return
	}
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if disconnector, ok := sm.streams.m[streamID]; ok {
		// Fine to skip nil checking here since that would be a programming error.
		disconnector.Disconnect(err)
	}
}

// AddStream adds a stream to the StreamManager. At this point, the stream had
// been registered with the processor and started receiving events.
//  1. If the stream is already disconnected before we acquire locks, it is not
//     added to the StreamManager.
//     a. OnError callback has already been called for this stream: error has
//     been sent back to the client.
//     b. OnError callback has not been called for this stream: error will be
//     called later on without updating the metrics.
//  2. Otherwise, it is added.
//     a. Disconnect is being called before we add the stream to the
//     StreamManager. OnError needs to acquire the lock to remove the stream and
//     do cleanup.
func (sm *StreamManager) AddStream(streamID int64, d Disconnector) {
	// We need the lock here because we need to make sure OnError sees the stream
	// during run.
	sm.streams.Lock()
	defer sm.streams.Unlock()
	// Check disconnected under lock to avoid race conditions where d becomes
	// disconnected right before we add it.
	if d.IsDisconnected() {
		// todo clean up if we dont do unregister async - careful of deadlock
		// Don't add to sm because it's already disconnected.
		return
	}
	if _, ok := sm.streams.m[streamID]; ok {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	sm.streams.m[streamID] = d
	sm.metrics.UpdateMetricsOnRangefeedConnect()
}

// Start launches sender.run in the background if no error is returned.
// sender.run continues running until it errors or StreamManager.Stop is called.
// The caller is responsible for calling StreamManager.Stop and handle any
// cleanups for any active streams. Note that it is not valid to call Start
// multiple times or restart after Stop. Example usage:
//
//	if err := StreamManager.Start(ctx, stopper); err != nil {
//	 return err
//	}
//
// defer StreamManager.Stop()
func (sm *StreamManager) Start(ctx context.Context, stopper *stop.Stopper) error {
	sm.errCh = make(chan error, 1)
	sm.wg.Add(1)
	ctx, sm.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(ctx, "buffered stream output", func(ctx context.Context) {
		defer sm.wg.Done()
		if err := sm.sender.run(ctx, stopper, sm.OnError); err != nil {
			sm.errCh <- err
		}
	}); err != nil {
		sm.taskCancel()
		sm.wg.Done()
		return err
	}
	return nil
}

// Stop cancels the sender.run task and waits for it to complete. It does
// nothing if sender.run is already finished. It is expected to be called after
// StreamManager.Start. Note that the caller is responsible for handling any
// cleanups for any active streams or mux errors that are not sent back
// successfully.
func (sm *StreamManager) Stop() {
	sm.taskCancel()
	sm.wg.Wait()
	sm.sender.cleanup()
	sm.streams.Lock()
	defer sm.streams.Unlock()
	for _, disconnector := range sm.streams.m {
		disconnector.Disconnect(
			kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)))
		sm.metrics.UpdateMetricsOnRangefeedDisconnect()
	}
	sm.streams.m = make(map[int64]Disconnector)
}

// Error returns a channel that can be used to receive errors from sender.run.
// Only non-nil errors are sent on this channel. If non-empty, sender.run is
// finished, and the caller is responsible for handling the error.
func (sm *StreamManager) Error() chan error {
	if sm.errCh == nil {
		log.Fatalf(context.Background(), "BufferedSender.Error called before BufferedSender.Start")
	}
	return sm.errCh
}
