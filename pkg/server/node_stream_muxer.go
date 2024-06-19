// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type rangefeedMetricsRecorder interface {
	incrementRangefeedCounter()
	decrementRangefeedCounter()
}

type streamMuxer struct {
	notify       chan struct{}
	sendToStream func(*kvpb.MuxRangeFeedEvent) error
	// need active streams here so that stream  muxer know if the stream already
	// terminates -> we only want to send one error back
	// streamID -> context.CancelFunc
	activeStreams    sync.Map
	rangefeedMetrics rangefeedMetricsRecorder

	mu struct {
		syncutil.Mutex
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func (s *streamMuxer) newStream(streamID int64, cancel context.CancelFunc) {
	s.rangefeedMetrics.incrementRangefeedCounter()
	s.activeStreams.Store(streamID, cancel)
}

func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	// TODO(wenyihu6): try to make server return an error here instead
	if err == nil {
		return kvpb.NewError(
			kvpb.NewRangeFeedRetryError(
				kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}

	return err
}

func newStreamMuxer(
	sendToStream func(*kvpb.MuxRangeFeedEvent) error, metrics rangefeedMetricsRecorder,
) *streamMuxer {
	return &streamMuxer{
		sendToStream:     sendToStream,
		notify:           make(chan struct{}, 1),
		rangefeedMetrics: metrics,
	}
}

func (s *streamMuxer) notifyMuxErrors(ev *kvpb.MuxRangeFeedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.muxErrors = append(s.mu.muxErrors, ev)
	// Note that notify is unblocking.
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *streamMuxer) disconnectRangefeedWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	if cancelFunc, ok := s.activeStreams.LoadAndDelete(streamID); ok {
		f := cancelFunc.(context.CancelFunc)
		f()

		clientErrorEvent := transformRangefeedErrToClientError(err)
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		ev.SetValue(&kvpb.RangeFeedError{
			Error: *clientErrorEvent,
		})

		s.notifyMuxErrors(ev)
		s.rangefeedMetrics.decrementRangefeedCounter()
	}
}

func (s *streamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	toSend := s.mu.muxErrors
	s.mu.muxErrors = nil
	return toSend
}

func (s *streamMuxer) run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case <-s.notify:
			for _, clientErr := range s.detachMuxErrors() {
				if err := s.sendToStream(clientErr); err != nil {
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
