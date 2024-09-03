// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// StreamManager manages one or more streams.
//
// Implemented by BufferedSender and StreamManager.
type StreamManager struct {
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
	sender sender

	// streamID -> context cancellation
	// Note that we do not delete from the map if an error is coming from
	// r.disconnect. Is that okay
	streams syncutil.Map[int64, Disconnector]

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder
}

type sender interface {
	send(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error
	run(ctx context.Context, stopper *stop.Stopper) error
	cleanup()
}

func NewStreamManager(sender sender, metrics RangefeedMetricsRecorder) *StreamManager {
	return &StreamManager{
		sender:  sender,
		metrics: metrics,
	}
}

func (sm *StreamManager) NewStream(streamID int64, rangeID roachpb.RangeID) (sink Stream) {
	switch sm.sender.(type) {
	case *BufferedSender:
		return &BufferedPerRangeEventSink{
			PerRangeEventSink: NewPerRangeEventSink(rangeID, streamID, sm),
		}
	case *UnbufferedSender:
		return NewPerRangeEventSink(rangeID, streamID, sm)
	default:
		log.Fatalf(context.Background(), "unexpected sender type %T", sm)
		return nil
	}
}

func (sm *StreamManager) send(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error {
	if ev.Error != nil {
		sm.metrics.UpdateMetricsOnRangefeedDisconnect()
	}
	//// Fine to skip nil checking here since that would be a programming error.
	//// Ignore error since the stream is already disconnecting. There is nothing
	//// else that could be done. When SendBuffered is returning an error, a node
	//// level shutdown from node.MuxRangefeed is happening soon to let clients
	//// know that the rangefeed is shutting down.
	//log.Infof(context.Background(),
	//	"failed to buffer rangefeed complete event for stream %d due to %s, "+
	//		"but a node level shutdown should be happening", ev.StreamID, ev.Error)
	return sm.sender.send(ev, alloc)
}

func (sm *StreamManager) DisconnectStream(streamID int64, rangeID roachpb.RangeID, err *kvpb.Error) {
	if err == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
		return
	}
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *err,
	})
	if disconnector, ok := sm.streams.LoadAndDelete(ev.StreamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		(*disconnector).Disconnect(err)
	}
}

func (sm *StreamManager) AddStream(streamID int64, d Disconnector) {
	// TODO(wenyihu6): make sure pointers to interface are doing well here
	if _, loaded := sm.streams.LoadOrStore(streamID, &d); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	sm.metrics.UpdateMetricsOnRangefeedConnect()
}

func (sm *StreamManager) Start(ctx context.Context, stopper *stop.Stopper) error {
	sm.errCh = make(chan error, 1)
	sm.wg.Add(1)
	ctx, sm.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(ctx, "buffered stream output", func(ctx context.Context) {
		defer sm.wg.Done()
		if err := sm.sender.run(ctx, stopper); err != nil {
			sm.errCh <- err
		}
	}); err != nil {
		sm.taskCancel()
		sm.wg.Done()
		return err
	}
	return nil
}

func (sm *StreamManager) Stop() {
	sm.taskCancel()
	sm.wg.Wait()
	sm.sender.cleanup()
}

func (sm *StreamManager) Error() chan error {
	if sm.errCh == nil {
		log.Fatalf(context.Background(), "BufferedSender.Error called before BufferedSender.Start")
	}
	return sm.errCh
}
