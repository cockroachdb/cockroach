// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// stopTrigger is used by modules to signal the desire to stop the server. When
// signaled, the stopTrigger notifies listeners on a channel.
type stopTrigger struct {
	mu struct {
		syncutil.Mutex
		shutdownRequest serverctl.ShutdownRequest
	}
	c chan serverctl.ShutdownRequest
}

func newStopTrigger() *stopTrigger {
	return &stopTrigger{
		// The channel is buffered so that there's no requirement that anyone ever
		// calls C() and reads from this channel.
		c: make(chan serverctl.ShutdownRequest, 1),
	}
}

// signalStop is used to signal that the server should shut down. The shutdown
// is asynchronous.
func (s *stopTrigger) signalStop(ctx context.Context, r serverctl.ShutdownRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mu.shutdownRequest.Empty() {
		// Someone else already triggered the shutdown.
		log.Infof(ctx, "received a second shutdown request: %s", r.ShutdownCause())
		return
	}
	s.mu.shutdownRequest = r
	// Writing to s.c is done under the lock, so there can ever only be one value
	// written and the writer does not block.
	s.c <- r
}

// C returns the channel that is signaled by signaledStop().
//
// Generally, there should be only one caller to C(); shutdown requests are
// delivered once, not broadcast.
func (s *stopTrigger) C() <-chan serverctl.ShutdownRequest {
	return s.c
}
