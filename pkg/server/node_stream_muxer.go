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

var _ rangefeedMetricsRecorder = (*nodeMetrics)(nil)

// severStreamSender is a wrapper around a grpc stream. Note that it should be
// safe for concurrent Sends.
type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
}

var _ severStreamSender = (*lockedMuxStream)(nil)

type streamMuxer struct {
	// stream is the server stream to which the muxer sends events. Note that the
	// stream is a locked mux stream, so it is safe for concurrent Sends.
	sender severStreamSender

	// metrics is nodeMetrics used to update rangefeed metrics.
	metrics rangefeedMetricsRecorder

	// streamID -> context.CancelFunc; ActiveStreams is a map of active
	// rangefeeds. Canceling the context using the associated CancelFunc will
	// disconnect the registration. It is not expected to repeatedly shut down a
	// stream.
	activeStreams sync.Map

	rangefeedCleanUps sync.Map

	// notifyCompletion is a buffered channel of size 1 used to signal the
	// presence of muxErrors that need to be sent. Additional signals are dropped
	// if the channel is already full so that it's unblocking.
	notifyCompletion chan struct{}

	mu struct {
		syncutil.Mutex

		// muxErrors is a list of errors that need to be sent to the client to
		// signal rangefeed completion.
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func (s *streamMuxer) newStream(streamID int64, cancel context.CancelFunc) {
	s.metrics.incrementRangefeedCounter()
	s.activeStreams.Store(streamID, cancel)
}

// transformToClientErr transforms a rangefeed completion error to a client side
// error which will be sent to the client.
func transformToClientErr(err *kvpb.Error) *kvpb.Error {
	// When the processor is torn down because it no longer has active
	// registrations, it would attempt to close all feeds again with a nil error.
	// However, this should never occur, as the processor should stop with a
	// reason if registrations are still active.
	if err == nil {
		return kvpb.NewError(
			kvpb.NewRangeFeedRetryError(
				kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}

	return err
}

func newStreamMuxer(sender severStreamSender, metrics rangefeedMetricsRecorder) *streamMuxer {
	return &streamMuxer{
		sender:           sender,
		metrics:          metrics,
		notifyCompletion: make(chan struct{}, 1),
	}
}

func (s *streamMuxer) registerRangefeedCleanUp(streamID int64, cleanUp func()) {
	s.rangefeedCleanUps.Store(streamID, cleanUp)
}

// send annotates the rangefeed event with streamID and rangeID and sends it to
// the grpc stream.
func (s *streamMuxer) send(
	streamID int64, rangeID roachpb.RangeID, event *kvpb.RangeFeedEvent,
) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		StreamID:       streamID,
		RangeID:        rangeID,
	}
	return s.sender.Send(response)
}

// appendMuxError appends the mux error to the muxer's error slice. This slice
// is processed by streamMuxer.run and sent to the client. We want to avoid
// blocking here.
func (s *streamMuxer) appendMuxError(ev *kvpb.MuxRangeFeedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.muxErrors = append(s.mu.muxErrors, ev)

	// Note that notifyCompletion is non-blocking. We want to avoid blocking on IO
	// (stream.Send) on processor goroutine.
	select {
	case s.notifyCompletion <- struct{}{}:
	default:
	}
}

// disconnectRangefeedWithError disconnects the rangefeed stream with the given
// streamID and sends the error to the client.
func (s *streamMuxer) disconnectRangefeedWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	if cancelFunc, ok := s.activeStreams.LoadAndDelete(streamID); ok {
		f := cancelFunc.(context.CancelFunc)
		// Canceling the context will cause the registration to disconnect unless
		// registration is not set up yet in which case it will be a no-op.
		f()

		clientErrorEvent := transformToClientErr(err)
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		ev.SetValue(&kvpb.RangeFeedError{
			Error: *clientErrorEvent,
		})

		s.appendMuxError(ev)
		s.metrics.decrementRangefeedCounter()
	}
}

// detachMuxErrors returns mux errors that need to be sent to the client. The
// caller should make sure to send these errors to the client.
func (s *streamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	toSend := s.mu.muxErrors
	s.mu.muxErrors = nil
	return toSend
}

// If run returns (due to context cancellation, broken stream, or quiescing),
// there is nothing we could do. We expect registrations to receive the same
// error and shut streams down.
func (s *streamMuxer) run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case <-s.notifyCompletion:
			toSend := s.detachMuxErrors()
			for _, clientErr := range toSend {
				// have another slice to process disconnect signals can deadlock here in
				// callback and also disconnected signal
				if cleanUp, ok := s.rangefeedCleanUps.LoadAndDelete(clientErr.StreamID); ok {
					f := cleanUp.(func())
					f()
				}
				if err := s.sender.Send(clientErr); err != nil {
					return
				}
			}
		case <-ctx.Done():
			return
		case <-stopper.ShouldQuiesce():
			// TODO(wenyihu6): should we cancel context here?
			return
		}
	}
}
