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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// sender wraps the underlying grpc server stream. Note that Send must be
// thread safe to be called concurrently.
type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
	SendIsThreadSafe()
}

// StreamMuxer is responsible for managing a set of active rangefeed streams and
// forwarding rangefeed completion errors to the client.
type StreamMuxer struct {
	sender         severStreamSender

	// notifyMuxError is a buffered channel of size 1 used to signal the presence
	// of muxErrors. Additional signals are dropped if the channel is already full
	// so that it's unblocking.
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

func (sm *StreamMuxer) AppendMuxError(e *kvpb.MuxRangeFeedEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.muxErrors = append(sm.mu.muxErrors, e)
	// Note that notify is unblocking.
	select {
	case sm.notifyMuxError <- struct{}{}:
	default:
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
			for _, clientErr := range sm.detachMuxErrors() {
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
