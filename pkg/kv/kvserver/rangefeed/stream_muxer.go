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

type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
}

type StreamMuxer struct {
	sender         severStreamSender
	activeStreams  sync.Map
	notifyMuxError chan struct{}

	mu struct {
		syncutil.Mutex
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func NewStreamMuxer(sender severStreamSender) *StreamMuxer {
	return &StreamMuxer{
		sender:         sender,
		notifyMuxError: make(chan struct{}, 1),
	}
}

func (sm *StreamMuxer) AddStream(streamID int64, cancel context.CancelFunc) {
	sm.activeStreams.Store(streamID, cancel)
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

// Safe to call repeatedly for the same stream. Subsequent errors are ignored.
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
	}
}

func (sm *StreamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toSend := sm.mu.muxErrors
	sm.mu.muxErrors = nil
	return toSend
}

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
