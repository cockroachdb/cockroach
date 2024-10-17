// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// StreamManager manages one or more streams.
//
// Implemented by BufferedSender and UnbufferedSender.
type StreamManager interface {
	// SendBufferedError sends an error back to client. This call is
	// non-blocking.
	SendBufferedError(ev *kvpb.MuxRangeFeedEvent)

	// Disconnect disconnects the stream with the ev.StreamID. This call is
	// non-blocking, and additional clean-up takes place async. Caller cannot
	// expect immediate disconnection. The returned bool indicates whether the
	// given stream was previously registered with this manager.
	Disconnect(ev *kvpb.MuxRangeFeedEvent) bool

	// AddStream adds a new per-range stream for the streamManager to manage.
	AddStream(streamID int64, r disconnector)

	// Start starts the streamManager background job to manage all active streams.
	// It continues until it errors or Stop is called. It is not valid to call
	// Start multiple times or restart after Stop.
	Start(ctx context.Context, stopper *stop.Stopper) error

	// Stop streamManager background job if it is still running.
	Stop()

	// Error returns a channel that will be non-empty if the streamManager
	// encounters an error and a node level shutdown is required.
	Error() chan error
}

type disconnector interface {
	// disconnect disconnects the registration with the provided error. Safe to
	// run multiple times, but subsequent errors would be discarded.
	disconnect(pErr *kvpb.Error)
	// maybeinvoke here to avoid forgetting about ()()
	getUnreg() func()
}
