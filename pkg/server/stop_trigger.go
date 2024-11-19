// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// stopTrigger is used by modules to signal the desire to stop the
// server. When signaled, the stopTrigger notifies listeners on a
// channel.
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
		c: make(chan serverctl.ShutdownRequest, 2),
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
		// We want to ensure that non-graceful shutdowns are always queued.
		// We have three possible situations:
		// - the first shutdown request was graceful, and the second one
		//   is not, in which case we want to ensure that the second one
		//   is queued.
		// - the first shutdown request was non-graceful, in which case we can
		//   ignore the second one.
		// - the first shutdown request was graceful, and the second one is
		//   too, in which case we can ignore the second one too.
		if !s.mu.shutdownRequest.TerminateUsingGracefulDrain() || r.TerminateUsingGracefulDrain() {
			return
		}
		// Other cases: we replace the previous one (which was graceful)
		// with the new one (which isn't graceful).
	}
	s.mu.shutdownRequest = r
	// Writing to s.c is done under the lock, so there can ever only be
	// one or two values written and the writer does not block.
	s.c <- r
}

// C returns the channel that is signaled by signaledStop().
//
// Generally, there should be only one caller to C(); shutdown requests are
// delivered once, not broadcast.
func (s *stopTrigger) C() <-chan serverctl.ShutdownRequest {
	return s.c
}
