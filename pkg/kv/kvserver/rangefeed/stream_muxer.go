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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Implemented by nodeMetrics.
type rangefeedMetricsRecorder interface {
	IncrementRangefeedCounter()
	DecrementRangefeedCounter()
}

// severStreamSender is a wrapper around a grpc stream. Note that it should be
// safe for concurrent Sends. Implemented by lockedMuxStream.
type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
}

type StreamMuxer struct {
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
	notifyCleanUp     chan struct{}

	// notifyCompletion is a buffered channel of size 1 used to signal the
	// presence of muxErrors that need to be sent. Additional signals are dropped
	// if the channel is already full so that it's unblocking.
	notifyCompletion chan struct{}

	mu struct {
		syncutil.Mutex

		// muxErrors is a list of errors that need to be sent to the client to
		// signal rangefeed completion.
		muxErrors  []*kvpb.MuxRangeFeedEvent
		cleanUpIDs []int64
	}
}
type streamInfo struct {
	rangeID roachpb.RangeID
	cancel  context.CancelFunc
}

func (s *StreamMuxer) AddStream(
	streamID int64, rangeID roachpb.RangeID, cancel context.CancelFunc,
) {
	s.activeStreams.Store(streamID, &streamInfo{
		rangeID: rangeID,
		cancel:  cancel,
	})
	s.metrics.IncrementRangefeedCounter()
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

func NewStreamMuxer(sender severStreamSender, metrics rangefeedMetricsRecorder) *StreamMuxer {
	return &StreamMuxer{
		sender:           sender,
		metrics:          metrics,
		notifyCompletion: make(chan struct{}, 1),
		notifyCleanUp:    make(chan struct{}, 1),
	}
}

// Note that the cleanup function has to be thread safe.
func (s *StreamMuxer) RegisterRangefeedCleanUp(streamID int64, cleanUp func()) {
	s.rangefeedCleanUps.Store(streamID, cleanUp)
}

// send annotates the rangefeed event with streamID and rangeID and sends it to
// the grpc stream.
func (s *StreamMuxer) Send(streamID int64, event *kvpb.RangeFeedEvent) error {
	stream, ok := s.activeStreams.Load(streamID)
	if !ok {
		// Check if we should return err here.
		// This is how we reject events on shutting down stream while reg clean up takes place.
		return errors.Errorf("stream %d not found", streamID)
	}

	streamInfo, ok := stream.(*streamInfo)
	if !ok {
		log.Errorf(context.Background(), "unexpected stream type %T", stream)
		return errors.Errorf("unexpected ")
	}
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		StreamID:       streamID,
		RangeID:        streamInfo.rangeID,
	}
	return s.sender.Send(response)
}

// appendMuxError appends the mux error to the muxer's error slice. This slice
// is processed by streamMuxer.run and sent to the client. We want to avoid
// blocking here.
func (s *StreamMuxer) appendMuxError(ev *kvpb.MuxRangeFeedEvent) {
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

func (s *StreamMuxer) appendCleanUp(streamID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.cleanUpIDs = append(s.mu.cleanUpIDs, streamID)

	select {
	case s.notifyCleanUp <- struct{}{}:
	default:
	}
}

func (s *StreamMuxer) disconnectActiveStreams(streamID int64, err *kvpb.Error) {
	stream, ok := s.activeStreams.LoadAndDelete(streamID)
	if !ok {
		return
	}
	// Canceling the context will cause the registration to disconnect unless
	// registration is not set up yet in which case it will be a no-op.
	f, ok := stream.(*streamInfo)
	if !ok {
		return
	}
	f.cancel()

	clientErrorEvent := transformToClientErr(err)
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  f.rangeID,
	}
	ev.SetValue(&kvpb.RangeFeedError{
		Error: *clientErrorEvent,
	})

	s.appendMuxError(ev)
	s.metrics.DecrementRangefeedCounter()
}

// disconnectRangefeedWithError disconnects the rangefeed stream with the given
// streamID and sends the error to the client.
func (s *StreamMuxer) DisconnectRangefeedWithError(streamID int64, err *kvpb.Error) {
	s.disconnectActiveStreams(streamID, err)
	if _, ok := s.rangefeedCleanUps.Load(streamID); ok {
		s.appendCleanUp(streamID)
	}
}

// detachMuxErrors returns mux errors that need to be sent to the client. The
// caller should make sure to send these errors to the client.
func (s *StreamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	toSend := s.mu.muxErrors
	s.mu.muxErrors = nil
	return toSend
}

func (s *StreamMuxer) detachCleanUpIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	toCleanUp := s.mu.cleanUpIDs
	s.mu.cleanUpIDs = nil
	return toCleanUp
}

// Note that since we are already in the muxer goroutine, we are okay with
// blocking and calling rangefeed clean up.
func (s *StreamMuxer) DisconnectAllWithErr(err error) {
	s.activeStreams.Range(func(key, value interface{}) bool {
		defer func() {
			s.activeStreams.Delete(key)
		}()
		streamID, ok := key.(int64)
		if !ok {
			log.Errorf(context.Background(), "unexpected streamID type %T", key)
			return true
		}
		info, ok := value.(*streamInfo)
		if !ok {
			log.Errorf(context.Background(), "unexpected streamID type %T", key)
			return true
		}
		info.cancel()
		if err == nil {
			return true
		}
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  info.rangeID,
		}
		ev.SetValue(&kvpb.RangeFeedError{
			Error: *kvpb.NewError(err),
		})
		_ = s.sender.Send(ev) // check if we should handle this err
		return true
	})

	s.rangefeedCleanUps.Range(func(key, value interface{}) bool {
		// TODO(wenyihu6): think about whether this is okay to call before r.disconnect
		cleanUp := value.(func())
		cleanUp()
		s.rangefeedCleanUps.Delete(key)
		return true
	})
}

// If run returns (due to context cancellation, broken stream, or quiescing),
// there is nothing we could do. We expect registrations to receive the same
// error and shut streams down.
func (s *StreamMuxer) Run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case <-s.notifyCompletion:
			toSend := s.detachMuxErrors()
			for _, clientErr := range toSend {
				// have another slice to process disconnect signals can deadlock here in
				// callback and also disconnected signal
				if err := s.sender.Send(clientErr); err != nil {
					s.DisconnectAllWithErr(err)
					return
				}
			}
		case <-s.notifyCleanUp:
			toCleanUp := s.detachCleanUpIDs()
			for _, streamID := range toCleanUp {
				if cleanUp, ok := s.rangefeedCleanUps.LoadAndDelete(streamID); ok {
					if f, ok := cleanUp.(func()); ok {
						f()
					}
				}
			}
		case <-ctx.Done():
			s.DisconnectAllWithErr(ctx.Err())
			return
		case <-stopper.ShouldQuiesce():
			s.DisconnectAllWithErr(nil)
			// TODO(wenyihu6): should we cancel context here?
			return
		}
	}
}
