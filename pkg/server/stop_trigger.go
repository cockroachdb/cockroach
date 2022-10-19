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

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// stopTrigger is used by modules to signal the desire to stop the server. When
// signaled, the stopTrigger notifies listeners on a channel.
type stopTrigger struct {
	mu struct {
		syncutil.Mutex
		shutdownRequest ShutdownRequest
	}
	c chan ShutdownRequest
}

func newStopTrigger() *stopTrigger {
	return &stopTrigger{
		// The channel is buffered so that there's no requirement that anyone ever
		// calls C() and reads from this channel.
		c: make(chan ShutdownRequest, 1),
	}
}

// signalStop is used to signal that the server should shut down. The shutdown
// is asynchronous.
func (s *stopTrigger) signalStop(r ShutdownRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.shutdownRequest != (ShutdownRequest{}) {
		// Someone else already triggered the shutdown.
		return
	}
	s.mu.shutdownRequest = r
	// Writing to s.c is done under the lock, so there can ever only be one value
	// written and the writer does not block.
	s.c <- r
}

type ShutdownRequest struct {
	DrainRPC         bool
	ServerStartupErr error
	Err              error
}

func (r ShutdownRequest) String() string {
	switch {
	case r.DrainRPC:
		return "shutdown requested by drain RPC"
	case r.ServerStartupErr != nil:
		return r.ServerStartupErr.Error()
	case r.Err != nil:
		return r.Err.Error()
	default:
		return ""
	}
}

// C returns the channel that is signaled by signaledStop().
//
// Generally, there should be only one caller to C(); shutdown requests are
// delivered once, not broadcast.
func (s *stopTrigger) C() <-chan ShutdownRequest {
	return s.c
}
