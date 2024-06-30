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

type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
}

type StreamMuxer struct {
	sender         severStreamSender
	metrics        rangefeedMetricsRecorder
	activeStreams  sync.Map
	notifyMuxError chan struct{}

	rangefeedCleanUps sync.Map
	notifyCleanUp     chan struct{}

	mu struct {
		syncutil.Mutex
		muxErrors  []*kvpb.MuxRangeFeedEvent
		cleanUpIDs []int64
	}
}

func NewStreamMuxer(sender severStreamSender, metrics rangefeedMetricsRecorder) *StreamMuxer {
	return &StreamMuxer{
		sender:         sender,
		metrics:        metrics,
		notifyMuxError: make(chan struct{}, 1),
	}
}

type streamInfo struct {
	rangeID roachpb.RangeID
	cancel  context.CancelFunc
}

func (sm *StreamMuxer) AddStream(
	streamID int64, rangeID roachpb.RangeID, cancel context.CancelFunc,
) {
	sm.activeStreams.Store(streamID, &streamInfo{
		rangeID: rangeID,
		cancel:  cancel,
	})
	sm.metrics.IncrementRangefeedCounter()
}

func (sm *StreamMuxer) Send(e *kvpb.MuxRangeFeedEvent) error {
	return sm.sender.Send(e)
}

func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	if err == nil {
		return kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}
	return err
}

func (sm *StreamMuxer) appendMuxError(e *kvpb.MuxRangeFeedEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.muxErrors = append(sm.mu.muxErrors, e)
	// Note that notify is unblocking.
	select {
	case sm.notifyMuxError <- struct{}{}:
	default:
	}
}

func (sm *StreamMuxer) appendCleanUp(streamID int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.cleanUpIDs = append(sm.mu.cleanUpIDs, streamID)

	select {
	case sm.notifyCleanUp <- struct{}{}:
	default:
	}
}

func (sm *StreamMuxer) RegisterRangefeedCleanUp(streamID int64, cleanUp func()) {
	sm.rangefeedCleanUps.Store(streamID, cleanUp)
}

// Safe to call repeatedly for the same stream. Subsequent errors are ignored.
func (sm *StreamMuxer) DisconnectRangefeedWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	if stream, ok := sm.activeStreams.LoadAndDelete(streamID); ok {
		if f, ok := stream.(*streamInfo); ok {
			f.cancel()
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

	// Note that we may repeatedly append clean up for the same id. We will rely
	// on rangefeedCleanUps to dedup during Run.
	if _, ok := sm.rangefeedCleanUps.Load(streamID); ok {
		sm.appendCleanUp(streamID)
	}
}

func (sm *StreamMuxer) DisconnectAllWithErr(err *kvpb.Error) {
	sm.activeStreams.Range(func(k, v interface{}) bool {
		if streamID, ok := k.(int64); ok {
			if info, ok := v.(*streamInfo); ok {
				info.cancel()
				// If err is nil, we do not send an error to the stream since the stream
				// is likely broken.
				if err != nil {
					ev := &kvpb.MuxRangeFeedEvent{
						StreamID: streamID,
						RangeID:  info.rangeID,
					}
					ev.SetValue(&kvpb.RangeFeedError{
						Error: *err,
					})
					_ = sm.sender.Send(ev) // Shutting down anyway, ignore error.
				}
			}
		}
		sm.metrics.DecrementRangefeedCounter()
		sm.activeStreams.Delete(k)
		return true
	})
	sm.rangefeedCleanUps.Range(func(k, v interface{}) bool {
		if cleanUp, ok := v.(func()); ok {
			cleanUp()
		}
		sm.rangefeedCleanUps.Delete(k)
		return true
	})
}

func (sm *StreamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toSend := sm.mu.muxErrors
	sm.mu.muxErrors = nil
	return toSend
}

func (sm *StreamMuxer) detachCleanUpIDs() []int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toCleanUp := sm.mu.cleanUpIDs
	sm.mu.cleanUpIDs = nil
	return toCleanUp
}

func (sm *StreamMuxer) Run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case <-sm.notifyMuxError:
			toSend := sm.detachMuxErrors()
			for _, clientErr := range toSend {
				if err := sm.sender.Send(clientErr); err != nil {
					sm.DisconnectAllWithErr(nil)
					return
				}
			}
		case <-sm.notifyCleanUp:
			toCleanUp := sm.detachCleanUpIDs()
			for _, streamID := range toCleanUp {
				if cleanUp, ok := sm.rangefeedCleanUps.LoadAndDelete(streamID); ok {
					if f, ok := cleanUp.(func()); ok {
						f()
					}
				}
			}
		case <-ctx.Done():
			// ctx should be canceled if the underlying stream is broken.
			sm.DisconnectAllWithErr(kvpb.NewError(ctx.Err()))
			return
		case <-stopper.ShouldQuiesce():
			// TODO(wenyihu6): should we cancel context here?
			sm.DisconnectAllWithErr(nil)
			return
		}
	}
}
