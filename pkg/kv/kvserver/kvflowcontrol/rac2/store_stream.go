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
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type StreamTokenCounterProvider interface {
	// Eval returns the evaluation token counter for the given stream.
	Eval(kvflowcontrol.Stream) TokenCounter
	// Send returns the send token counter for the given stream.
	Send(kvflowcontrol.Stream) TokenCounter
}

// SendTokenWatcherHandleID is a unique identifier for a handle that is
// watching for available elastic send tokens on a stream.
type SendTokenWatcherHandleID int64

// SendTokenWatcherProvider is the interface for retrieving SendTokenWatchers.
// It is intended to be used to retrieve a SendTokenWatcher when initializing a
// replica send stream.
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type SendTokenWatcherProvider interface {
	// SendTokenWatcher returns a SendTokenWatcher for the given stream.
	SendTokenWatcher(kvflowcontrol.Stream) SendTokenWatcher
}

// SendTokenWatcher is the interface for watching and waiting on available
// elastic send tokens, for a stream. The watcher registers a notification,
// which will be called when elastic tokens are available for the stream this
// watcher is monitoring. Note only elastic tokens are watched as this is
// intended to be used when a send queue exists.
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type SendTokenWatcher interface {
	// NotifyWhenAvailable queues up for elastic tokens for the given stream's
	// send token counter. When elastic tokens are available, the provided
	// TokenGrantNotification is called. It is the caller's responsibility to
	// call CancelHandle when tokens are no longer needed, or when the caller is
	// done.
	NotifyWhenAvailable(TokenGrantNotification) SendTokenWatcherHandleID
	// CancelHandle cancels the given handle, stopping it from being notified
	// when tokens are available. CancelHandle should be called at most once.
	CancelHandle(SendTokenWatcherHandleID)
}

// TokenGrantNotification is called when tokens are available.
type TokenGrantNotification func(context.Context)
