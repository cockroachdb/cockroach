// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StreamManager manages the lifecycle of multiplexed txnfeed streams. It owns
// the mapping from stream IDs to Disconnectors and coordinates the background
// BufferedSender goroutine that delivers events to the client.
//
// The architecture mirrors rangefeed.StreamManager:
//
//	+-----------------+               +-----------------+                  +-----------------+
//	|                 | Disconnect    |                 |  r.disconnect    |                 |
//	|  MuxTxnFeed     +-------------->|  StreamManager  +----------------->|   Registration  |
//	|                 |               |                 |  Send/SendError  |                 |
//	|                 |               |                 |<-----------------+                 |
//	+-----------------+               +-----------------+                  +-------------+---+
//	                                                                           ^         |
//	                                                               r.disconnect|         | p.unregister
//	                                                                           |         v
//	                                  +-----------------+                +---+-------------+
//	                                  |                 | Unregister     |                 |
//	                                  |    Replica      |<---------------+   Processor     |
//	                                  |                 |                |                 |
//	                                  +-----------------+                +-----------------+
type StreamManager struct {
	// taskCancel cancels the context used by sender.run, causing it to exit.
	// Called in Stop().
	taskCancel context.CancelFunc

	// wg coordinates goroutines spawned by StreamManager.
	wg sync.WaitGroup

	// errCh delivers errors from sender.run back to the caller. If non-empty,
	// sender.run has finished and the error should be handled.
	errCh chan error

	// sender is the BufferedSender that handles event delivery.
	sender *BufferedSender

	// streams maps streamID to Disconnector for all active streams.
	streams struct {
		syncutil.Mutex
		m map[int64]Disconnector
	}
}

// NewStreamManager creates a new StreamManager with the given sender.
func NewStreamManager(sender *BufferedSender) *StreamManager {
	sm := &StreamManager{
		sender: sender,
	}
	sm.streams.m = make(map[int64]Disconnector)
	return sm
}

// NewStream creates a PerRangeTxnFeedSink for the given stream and range.
func (sm *StreamManager) NewStream(streamID int64, rangeID roachpb.RangeID) *PerRangeTxnFeedSink {
	return newPerRangeTxnFeedSink(rangeID, streamID, sm.sender)
}

// RegisteringStream is called once a stream will be registered. After this
// point, the stream may start to receive events.
func (sm *StreamManager) RegisteringStream(streamID int64) {
	sm.sender.addStream(streamID)
}

// OnError is a callback invoked by the sender after it delivers an error
// event to the client. It asserts the stream is disconnected, calls Unregister
// to remove it from the processor, and removes it from the StreamManager's
// map.
func (sm *StreamManager) OnError(streamID int64) {
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if d, ok := sm.streams.m[streamID]; ok {
		if !d.IsDisconnected() {
			log.KvExec.Fatalf(
				context.Background(), "OnError called on connected stream %d", streamID)
		}
		d.Unregister()
		delete(sm.streams.m, streamID)
	}
}

// DisconnectStream disconnects the stream with the given streamID.
func (sm *StreamManager) DisconnectStream(streamID int64, err *kvpb.Error) {
	if err == nil {
		log.KvExec.Fatalf(
			context.Background(), "DisconnectStream called with nil error")
		return
	}
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if disconnector, ok := sm.streams.m[streamID]; ok {
		disconnector.Disconnect(err)
	}
}

// AddStream adds a streamID with its disconnector to the StreamManager. The
// StreamManager uses the disconnector to shut down the txnfeed stream later.
func (sm *StreamManager) AddStream(streamID int64, d Disconnector) {
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if d.IsDisconnected() {
		// If the stream is already disconnected (race between registration error
		// and AddStream), don't add it. The error event has already been sent.
		d.Unregister()
		return
	}
	if _, ok := sm.streams.m[streamID]; ok {
		log.KvExec.Fatalf(
			context.Background(), "stream %d already exists", streamID)
	}
	sm.streams.m[streamID] = d
}

// Start launches sender.run in the background. It continues running until it
// errors or StreamManager.Stop is called. Not valid to call Start multiple
// times or restart after Stop.
func (sm *StreamManager) Start(ctx context.Context, stopper *stop.Stopper) error {
	sm.errCh = make(chan error, 1)
	sm.wg.Add(1)
	ctx, sm.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(
		ctx, "txnfeed-stream-manager-sender", func(ctx context.Context) {
			defer sm.wg.Done()
			if err := sm.sender.run(ctx, stopper, sm.OnError); err != nil {
				sm.errCh <- err
			}
		},
	); err != nil {
		sm.taskCancel()
		sm.wg.Done()
		return err
	}
	return nil
}

// Stop cancels the sender.run task and waits for it to complete. Then it
// cleans up the sender and disconnects all remaining streams. Expected to be
// called after StreamManager.Start.
func (sm *StreamManager) Stop(ctx context.Context) {
	sm.taskCancel()
	sm.wg.Wait()
	sm.sender.cleanup(ctx)
	sm.streams.Lock()
	defer sm.streams.Unlock()
	log.KvExec.VInfof(
		ctx, 2, "stopping txnfeed stream manager: disconnecting %d streams",
		len(sm.streams.m))
	txnFeedClosedErr := kvpb.NewError(
		kvpb.NewTxnFeedRetryError(kvpb.TxnFeedRetryError_REASON_TXNFEED_CLOSED))
	for _, disconnector := range sm.streams.m {
		disconnector.Disconnect(txnFeedClosedErr)
		disconnector.Unregister()
	}
	sm.streams.m = nil
}

// Error returns a channel for receiving errors from sender.run. Only non-nil
// errors are sent, and at most one error is delivered.
func (sm *StreamManager) Error() <-chan error {
	if sm.errCh == nil {
		log.KvExec.Fatalf(
			context.Background(), "StreamManager.Error called before StreamManager.Start")
	}
	return sm.errCh
}
