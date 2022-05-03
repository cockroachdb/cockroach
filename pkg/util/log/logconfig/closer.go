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

// LoggingCloser is a utility used by the logging system to
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
type LoggingCloser struct {
	shutdownFn func()
	mu         struct {
		syncutil.Mutex
		bufferSinkWg sync.WaitGroup
	}
}

// NewLoggingCloser returns a new instance of a LoggingCloser.
//
// NB: It is up to the caller to define an appropriate logging
// cleanup function via SetShutdownFn, as the default is an empty
// function.
func NewLoggingCloser() *LoggingCloser {
	return &LoggingCloser{
		shutdownFn: func() {},
	}
}

// SetShutdownFn sets the provided function on the LoggingCloser
// to be called as a part of Close.
//
// It's expected that the provided cleanupFn cancels any context.Context's
// used within the logging system - especially those used by buffered log
// sinks.
func (l *LoggingCloser) SetShutdownFn(shutdownFn func()) {
	l.shutdownFn = shutdownFn
}

// RegisterBufferSink notifies the LoggingCloser of the existence
// of an active buffered log sink within the logging system.
//
// This increments a sync.WaitGroup counter, so be sure that the
// caller also has a subsequent call to BufferSinkDone.
func (l *LoggingCloser) RegisterBufferSink() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.bufferSinkWg.Add(1)
}

// BufferSinkDone notifies the LoggingCloser that one of the buffered
// log sinks registered via RegisterBufferSink has finished processing
// & has terminated.
func (l *LoggingCloser) BufferSinkDone() {
	l.mu.bufferSinkWg.Done()
}

// Close triggers the logging shutdown process, calling
// the shutdown function provided via SetShutdownFn and waiting for all
// buffered log sinks registered with the LoggingCloser to signal that
// they've finished processing before returning.
func (l *LoggingCloser) Close() {
	l.shutdownFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.bufferSinkWg.Wait()
}
