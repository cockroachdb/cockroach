// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logconfig

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// LoggingShutdown is a utility used by the logging system to
// trigger the teardown of logging facilities gracefully within
// CockroachDB.
//
// The API allows us to register buffered log sinks with RegisterBufferSink
// so we can wait for them to drain/timeout before exiting.
//
// The API also allows us to trigger the logging shutdown sequence &
// wait for all registered buffered log sinks to finish processing
// before exiting, to help ensure a graceful shutdown of buffered
// log sinks.
type LoggingShutdown struct {
	shutdownFn func()
	mu         struct {
		syncutil.Mutex
		bufferSinkWg sync.WaitGroup
	}
}

// NewLoggingShutdown returns a new instance of a LoggingShutdown.
//
// NB: It is up to the caller to define an appropriate logging
// cleanup function via SetShutdownFn, as the default is an empty
// function.
func NewLoggingShutdown() *LoggingShutdown {
	return &LoggingShutdown{
		shutdownFn: func() {},
	}
}

// SetShutdownFn sets the provided function on the LoggingShutdown
// to be called as a part of SignalAndWaitForShutdown.
//
// It's expected that the provided cleanupFn cancels any context.Context's
// used within the logging system - especially those used by buffered log
// sinks.
func (l *LoggingShutdown) SetShutdownFn(shutdownFn func()) {
	l.shutdownFn = shutdownFn
}

// RegisterBufferSink notifies the LoggingShutdown of the existence
// of an active buffered log sink within the logging system.
//
// This increments a sync.WaitGroup counter, so be sure that the
// caller also has a subsequent call to BufferSinkDone.
func (l *LoggingShutdown) RegisterBufferSink() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.bufferSinkWg.Add(1)
}

// BufferSinkDone notifies the LoggingShutdown that one of the buffered
// log sinks registered via RegisterBufferSink has finished processing
// & has terminated.
func (l *LoggingShutdown) BufferSinkDone() {
	l.mu.bufferSinkWg.Done()
}

// SignalAndWaitForShutdown triggers the logging shutdown process, calling
// the shutdown function provided via SetShutdownFn and waiting for all
// buffered log sinks registered with the LoggingShutdown to signal that
// they've finished processing before returning.
func (l *LoggingShutdown) SignalAndWaitForShutdown() {
	l.shutdownFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.bufferSinkWg.Wait()
}
