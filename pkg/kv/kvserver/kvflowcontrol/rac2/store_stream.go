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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StreamTokenCounterProvider is the interface for retrieving token counters
// for a given stream.
//
// TODO(kvoli): Add stream deletion upon decommissioning a store.
type StreamTokenCounterProvider struct {
	settings                   *cluster.Settings
	sendCounters, evalCounters syncutil.Map[kvflowcontrol.Stream, TokenCounter]
}

// NewStreamTokenCounterProvider creates a new StreamTokenCounterProvider.
func NewStreamTokenCounterProvider(settings *cluster.Settings) *StreamTokenCounterProvider {
	return &StreamTokenCounterProvider{settings: settings}
}

// Eval returns the evaluation token counter for the given stream.
func (p *StreamTokenCounterProvider) Eval(stream kvflowcontrol.Stream) *TokenCounter {
	t, _ := p.evalCounters.LoadOrStore(stream, NewTokenCounter(p.settings))
	return t
}

// Send returns the send token counter for the given stream.
func (p *StreamTokenCounterProvider) Send(stream kvflowcontrol.Stream) *TokenCounter {
	t, _ := p.sendCounters.LoadOrStore(stream, NewTokenCounter(p.settings))
	return t
}

// SendTokenWatcherHandleID is a unique identifier for a handle that is
// watching for available elastic send tokens on a stream.
type SendTokenWatcherHandleID int64

// SendTokenWatcher is the interface for watching and waiting on available
// elastic send tokens. The watcher registers a notification, which will be
// called when elastic tokens are available for the stream this watcher is
// monitoring. Note only elastic tokens are watched as this is intended to be
// used when a send queue exists.
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type SendTokenWatcher interface {
	// NotifyWhenAvailable queues up for elastic tokens for the given send token
	// counter. When elastic tokens are available, the provided
	// TokenGrantNotification is called. It is the caller's responsibility to
	// call CancelHandle when tokens are no longer needed, or when the caller is
	// done.
	NotifyWhenAvailable(
		*TokenCounter,
		TokenGrantNotification,
	) SendTokenWatcherHandleID
	// CancelHandle cancels the given handle, stopping it from being notified
	// when tokens are available. CancelHandle should be called at most once.
	CancelHandle(SendTokenWatcherHandleID)
}

// TokenGrantNotification is an interface that is called when tokens are
// available.
type TokenGrantNotification interface {
	// Notify is called when tokens are available to be granted.
	Notify(context.Context)
}
