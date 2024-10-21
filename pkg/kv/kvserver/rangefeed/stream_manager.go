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

	// streamID -> disconnects
	streams struct {
		syncutil.Mutex
		m map[int64]Disconnector
	}

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder
}

type sender interface {
	send(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error
	run(ctx context.Context, stopper *stop.Stopper, onError func(int64)) error
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

func (sm *StreamManager) NewStream(streamID int64, rangeID roachpb.RangeID) (sink Stream) {
	switch sender := sm.sender.(type) {
	case *BufferedSender:
		return &BufferedPerRangeEventSink{
			PerRangeEventSink: NewPerRangeEventSink(rangeID, streamID, sender),
			sender:            sender,
		}
	case *UnbufferedSender:
		return NewPerRangeEventSink(rangeID, streamID, sender)
	default:
		log.Fatalf(context.Background(), "unexpected sender type %T", sm)
		return nil
	}
}

func (sm *StreamManager) OnError(streamID int64) {
	sm.streams.Lock()
	defer sm.streams.Unlock()
	delete(sm.streams.m, streamID)
	sm.metrics.UpdateMetricsOnRangefeedDisconnect()
}

func (sm *StreamManager) DisconnectStream(streamID int64, rangeID roachpb.RangeID, err *kvpb.Error) {
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

// lock in streamManager.m
// lock in registration.IsDisconnected

// We should never add a stream if the stream has disconnected and OnError had been called.
// Nothing would trigger a cleanup.

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

func (sm *StreamManager) Stop() {
	sm.taskCancel()
	sm.wg.Wait()
	sm.sender.cleanup()
	for _, d := range sm.streams.m {
		d.Disconnect(nil)
	}
}

func (sm *StreamManager) Error() chan error {
	if sm.errCh == nil {
		log.Fatalf(context.Background(), "BufferedSender.Error called before BufferedSender.Start")
	}
	return sm.errCh
}
