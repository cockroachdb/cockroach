// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
)

// StreamTokenCounterProvider is the interface for retrieving token counters
// for a given stream.
type StreamTokenCounterProvider interface {
	// EvalTokenCounterForStream returns the evaluation token counter for the
	// given stream.
	EvalTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
	// SendTokenCounterForStream returns the send token counter for the given
	// stream.
	SendTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
}

// StreamWatcherHandleID is a unique identifier for a handle that is
// watching for available tokens on a stream.
type StreamWatcherHandleID int64

// StreamTokenWatcher is the interface for watching and waiting on available
// elastic tokens. The watcher registers a notification, which will be called
// when elastic tokens are available for a given stream.
type StreamTokenWatcher interface {
	// NotifyWhenAvailable queues up for elastic tokens for the given token
	// counter. When elastic tokens are available, the provided
	// TokenGrantNotification is called. It is the caller's responsibility to
	// call CancelHandle when tokens are no longer needed, or when the caller is
	// done.
	NotifyWhenAvailable(
		TokenCounter,
		TokenGrantNotification,
	) StreamWatcherHandleID
	// CancelHandle cancels the given handle, stopping it from being notified
	// when tokens are available.
	CancelHandle(StreamWatcherHandleID)
}

// TokenGrantNotification is an interface for notifying when tokens are
// available.
type TokenGrantNotification interface {
	// Notify is called when tokens are available to be granted.
	Notify(context.Context)
}
