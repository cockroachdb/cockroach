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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type rangefeedMetricsRecorder interface {
	IncrementRangefeedCounter()
	DecrementRangefeedCounter()
}

// sender wraps the underlying grpc server stream. Note that Send must be
// thread safe to be called concurrently.
type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
}

type StreamMuxer struct {
	sender severStreamSender
	metrics        rangefeedMetricsRecorder

	// streamID -> streamInfo for active rangefeeds
	activeStreams sync.Map

	// notifyMuxError is a buffered channel of size 1 used to signal the presence
	// of muxErrors. Additional signals are dropped if the channel is already full
	// so that it's unblocking.
	notifyMuxError chan struct{}

	mu struct {
		syncutil.Mutex
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func NewStreamMuxer(sender severStreamSender, metrics rangefeedMetricsRecorder) *StreamMuxer {
	return &StreamMuxer{
		sender:         sender,
		metrics:        metrics,
		notifyMuxError: make(chan struct{}, 1),
	}
}

// AddStream registers a server rangefeed stream with the muxer. It remains
// active until DisconnectRangefeedWithError is called with the same streamID.
// Caller must ensure no duplicate stream IDs are added without disconnecting
// the old one first.
func (sm *StreamMuxer) AddStream(streamID int64, cancel context.CancelFunc) {
	sm.activeStreams.Store(streamID, cancel)
	sm.metrics.IncrementRangefeedCounter()
}

// Send forwards events to client. Caller must ensure the stream remains active
// as no checks will be performed. It returns an error when the underlying grpc
// server stream is broken. Safe for concurrent calls to Send.
func (sm *StreamMuxer) Send(e *kvpb.MuxRangeFeedEvent) error {
	return sm.sender.Send(e)
}

// transformRangefeedErrToClientError converts a rangefeed error to a client
// error to be sent back to client. This also handles nil values, preventing nil
// pointer dereference.
func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	if err == nil {
		// When processor is stopped when it no longer has any registrations, it
		// would attempt to close all feeds again with a nil error. This should
		// never happen as processor would always stop with a reason if feeds are
		// active. Return a retry error instead of nil so that client side feed can
		// retry if necessary.
		return kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}
	return err
}

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
// 1. cancel stream context 2. send error back to client
//
// Note that this function can be called by the processor worker, so this cannot
// block IO. It does so by delegating the responsibility of sending mux error to
// muxer.Run.
func (sm *StreamMuxer) DisconnectRangefeedWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	if cancelFunc, ok := sm.activeStreams.LoadAndDelete(streamID); ok {
		if f, ok := cancelFunc.(context.CancelFunc); ok {
			f()
		}
		clientErrorEvent := transformRangefeedErrToClientError(err)
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		ev.SetValue(&kvpb.RangeFeedError{
			Error: *clientErrorEvent,
		})
		sm.appendMuxError(ev)
		sm.metrics.DecrementRangefeedCounter()
	}
}

func (sm *StreamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toSend := sm.mu.muxErrors
	sm.mu.muxErrors = nil
	return toSend
}

// Run forwards rangefeed errors and invokes rangefeed stream clean up. It
// ensures proper shutdown of streams using DisconnectAllWithErr before
// returning.
//
// Run is expected to be called in a goroutine and will block until the context
// is done or the stopper is quiesced. Example usage:
//
// muxer := NewStreamMuxer(muxStream, metrics)
// var wg sync.WaitGroup
// defer wg.Wait()
// defer stopper.Stop(ctx) // or defer cancel() - important to stop the muxer before wg.Wait()
//
//	wg.Add(1)
//	if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
//		defer wg.Done()
//		muxer.Run(ctx, stopper)
//	}); err != nil {
//		wg.Done()
//	}
func (sm *StreamMuxer) Run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case <-sm.notifyMuxError:
			toSend := sm.detachMuxErrors()
			for _, clientErr := range toSend {
				if err := sm.sender.Send(clientErr); err != nil {
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
