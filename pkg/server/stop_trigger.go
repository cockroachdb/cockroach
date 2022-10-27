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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

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
func (s *stopTrigger) signalStop(ctx context.Context, r ShutdownRequest) {
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

// ShutdownRequest is used to signal a request to shutdown the server through
// stopTrigger. It carries the reason for the shutdown.
type ShutdownRequest struct {
	// Reason identifies the cause of the shutdown.
	Reason ShutdownReason
	// Err is populated for reason ServerStartupError and FatalError.
	Err error
}

// MakeShutdownRequest constructs a ShutdownRequest.
func MakeShutdownRequest(reason ShutdownReason, err error) ShutdownRequest {
	if reason == ShutdownReasonDrainRPC && err != nil {
		panic("unexpected err for ShutdownReasonDrainRPC")
	}
	return ShutdownRequest{
		Reason: reason,
		Err:    err,
	}
}

// ShutdownReason identifies the reason for a ShutdownRequest.
type ShutdownReason int

const (
	// ShutdownReasonDrainRPC represents a drain RPC with the shutdown flag set.
	ShutdownReasonDrainRPC ShutdownReason = iota
	// ShutdownReasonServerStartupError means that the server startup process
	// failed.
	ShutdownReasonServerStartupError
	// ShutdownReasonFatalError identifies an error that requires the server be
	// terminated. Compared to a panic or log.Fatal(), the termination can be more
	// graceful, though.
	ShutdownReasonFatalError
)

// ShutdownCause returns the reason for the shutdown request, as an error.
func (r ShutdownRequest) ShutdownCause() error {
	switch r.Reason {
	case ShutdownReasonDrainRPC:
		return errors.Newf("shutdown requested by drain RPC")
	default:
		return r.Err
	}
}

// C returns the channel that is signaled by signaledStop().
//
// Generally, there should be only one caller to C(); shutdown requests are
// delivered once, not broadcast.
func (s *stopTrigger) C() <-chan ShutdownRequest {
	return s.c
}

// Empty returns true if the receiver is the zero value.
func (r ShutdownRequest) Empty() bool {
	return r == (ShutdownRequest{})
}
