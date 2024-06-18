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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type streamMuxer struct {
	syncutil.Mutex
	muxErrors    []*kvpb.MuxRangeFeedEvent
	notify       chan struct{}
	sendToStream func(*kvpb.MuxRangeFeedEvent) error
}

func newStreamMuxer(sendToStream func(*kvpb.MuxRangeFeedEvent) error) *streamMuxer {
	return &streamMuxer{
		notify:       make(chan struct{}, 1),
		sendToStream: sendToStream,
	}
}

func (s *streamMuxer) notifyMuxErrors(ev *kvpb.MuxRangeFeedEvent) {
	s.Lock()
	defer s.Unlock()
	s.muxErrors = append(s.muxErrors, ev)
	// Note that notify is unblocking.
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *streamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	s.Lock()
	defer s.Unlock()
	toSend := s.muxErrors
	s.muxErrors = nil
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
