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

type severStreamSender interface {
	Send(*kvpb.MuxRangeFeedEvent) error
}

type StreamMuxer struct {
	sender         severStreamSender
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
