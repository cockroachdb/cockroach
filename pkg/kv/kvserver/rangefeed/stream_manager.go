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

// +-----------------+               +-----------------+                  +-----------------+
// |                 | Disconnect    |                 |  r.disconnect    |                 |
// |  MuxRangeFeed   +-------------->|  StreamManager  +----------------->|   Registration  |
// |                 |               |                 |  Send/SendError  |                 |
// |                 |               |                 |<-----------------+                 |
// +-----------------+               +-----------------+                  +-------------+---+
//                                                                            ^         |
//                                                                r.disconnect|         | p.asyncUnregisterReq
//                                                                            |         v
//                                  +-----------------+                 +---+-------------+
//                                  |                 | UnregFromReplica|                 |
//                                  |    Replica      |<----------------+   Processor     |
//                                  |                 |                 |                 |
//                                  +-----------------+                 +-----------------+

// StreamManager manages one or more streams. It is responsible for starting and
// stopping the underlying sender, as well as managing the streams themselves.
type StreamManager struct {
	// taskCancel cancels the context used by the sender.runspawn goroutine,
	// causing it to exit. It is called in Stop().
	taskCancel context.CancelFunc

	// wg is used to coordinate goroutines spawned by StreamManager.
	wg sync.WaitGroup

	// errCh delivers errors from sender.run back to the caller. If non-empty, the
	// sender.run is finished and error should be handled. Note that it is
	// possible for sender.run to be finished without sending an error to errCh.
	errCh chan error

	// Implemented by UnbufferedSender and BufferedSender. Implementations should
	// ensure that sendUnbuffered and sendBuffered are thread-safe.
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
	// sendUnbuffered sends a RangeFeedEvent to the underlying grpc stream
	// directly. This call may block and must be thread-safe.
	sendUnbuffered(ev *kvpb.MuxRangeFeedEvent) error
	// sendBuffered buffers a RangeFeedEvent before sending to the underlying grpc
	// stream. This call must be non-blocking and thread-safe.
	sendBuffered(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error
	// run is the main loop for the sender. It is expected to run in the
	// background until a node level error is encountered which would shut down
	// all streams in StreamManager.
	run(ctx context.Context, stopper *stop.Stopper, onError func(int64)) error
	// cleanup is called when the sender is stopped. It is expected to clean up
	// any resources used by the sender.
	cleanup(ctx context.Context)
}

func NewStreamManager(sender sender, metrics RangefeedMetricsRecorder) *StreamManager {
	sm := &StreamManager{
		sender:  sender,
		metrics: metrics,
	}
	sm.streams.m = make(map[int64]Disconnector)
	return sm
}

func (sm *StreamManager) NewStream(streamID int64, rangeID roachpb.RangeID) (sink Stream) {
	switch sender := sm.sender.(type) {
	case *BufferedSender:
		return &BufferedPerRangeEventSink{
			PerRangeEventSink: NewPerRangeEventSink(rangeID, streamID, sender),
		}
	case *UnbufferedSender:
		return NewPerRangeEventSink(rangeID, streamID, sender)
	default:
		log.Fatalf(context.Background(), "unexpected sender type %T", sm)
		return nil
	}
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
		log.Fatalf(context.Background(),
			"unexpected: DisconnectStream called with nil error")
		return
	}
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if disconnector, ok := sm.streams.m[streamID]; ok {
		// Fine to skip nil checking here since that would be a programming error.
		disconnector.Disconnect(err)
	}
}

// AddStream adds a streamID with its disconnector to the StreamManager.
// StreamManager can use the disconnector to shut down the rangefeed stream
// later on.
func (sm *StreamManager) AddStream(streamID int64, d Disconnector) {
	// At this point, the stream had been registered with the processor and
	// started receiving events. We need to lock here to avoid race conditions
	// with a disconnect error passing through before the stream is added.
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if d.IsDisconnected() {
		// If the stream is already disconnected, we don't add it to streams. The
		// registration will have already sent an error to the client.
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
// Note that it is not valid to call Start multiple times or restart after Stop.
// Example usage:
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
	if err := stopper.RunAsyncTask(ctx, "stream-manager-sender", func(ctx context.Context) {
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
// StreamManager.Start.
func (sm *StreamManager) Stop(ctx context.Context) {
	sm.taskCancel()
	sm.wg.Wait()
	sm.sender.cleanup(ctx)
	sm.streams.Lock()
	defer sm.streams.Unlock()
	rangefeedClosedErr := kvpb.NewError(
		kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	sm.metrics.UpdateMetricsOnRangefeedDisconnectBy(int64(len(sm.streams.m)))
	for _, disconnector := range sm.streams.m {
		// Disconnect all streams with a retry error. No rangefeed errors will be
		// sent to the client after shutdown, but the gRPC stream will still
		// terminate.
		disconnector.Disconnect(rangefeedClosedErr)
	}
	sm.streams.m = nil
}

// Error returns a channel for receiving errors from sender.run. Only non-nil
// errors are sent, and at most one error is delivered. If the channel is
// non-empty, sender.run has finished, and the error should be handled.
// sender.run may also finish without sending anything to the channel.
func (sm *StreamManager) Error() <-chan error {
	if sm.errCh == nil {
		log.Fatalf(context.Background(), "StreamManager.Error called before StreamManager.Start")
	}
	return sm.errCh
}

// For testing only.
func (sm *StreamManager) activeStreamCount() int {
	sm.streams.Lock()
	defer sm.streams.Unlock()
	return len(sm.streams.m)
}
