// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Closer is a utility used by the logging system to
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
type Closer struct {
	shutdownFn func()
	// The timeout used in Close as a solution to potential deadlocks.
	// Is only intended to be modified for testing purposes.
	timeout time.Duration
	wgMu    struct {
		syncutil.Mutex
		bufferSinkWg sync.WaitGroup
	}
	// We maintain a separate registryMu to give ourselves the ability
	// to remove items from the registry while the Mutex for bufferSinkWg
	// is held during Close.
	registryMu struct {
		syncutil.Mutex
		// sinkRegistry acts as a set and stores references to all bufferSink's
		// registered with this Closer instance. Only useful for debugging
		// purposes.
		sinkRegistry map[*bufferSink]struct{}
	}
}

// Default timeout used in Close
var defaultCloserTimeout = 90 * time.Second

// NewCloser returns a new instance of a Closer.
//
// NB: It is up to the caller to define an appropriate logging
// cleanup function via SetShutdownFn, as the default is an empty
// function.
func NewCloser() *Closer {
	closer := &Closer{
		shutdownFn: func() {},
		timeout:    defaultCloserTimeout,
	}
	closer.registryMu.sinkRegistry = make(map[*bufferSink]struct{})
	return closer
}

// SetShutdownFn sets the provided function on the Closer
// to be called as a part of Close.
//
// It's expected that the provided shutdownFn cancels any context.Context's
// used within the logging system - especially those used by buffered log
// sinks.
func (l *Closer) SetShutdownFn(shutdownFn func()) {
	l.shutdownFn = shutdownFn
}

// RegisterBufferSink notifies the Closer of the existence
// of an active buffered log sink within the logging system.
// This increments a sync.WaitGroup counter, so be sure that the
// caller also has a subsequent call to BufferSinkDone.
//
// A reference to the bufferSink is also maintained in an
// internal registry to aid in debug capabilities.
func (l *Closer) RegisterBufferSink(bs *bufferSink) {
	l.wgMu.Lock()
	defer l.wgMu.Unlock()

	l.registryMu.Lock()
	defer l.registryMu.Unlock()

	if _, ok := l.registryMu.sinkRegistry[bs]; ok {
		panic(errors.New("single buffered log sink registered more than once within log.Closer"))
	}

	l.registryMu.sinkRegistry[bs] = struct{}{}
	l.wgMu.bufferSinkWg.Add(1)
}

// BufferSinkDone notifies the Closer that one of the buffered
// log sinks registered via RegisterBufferSink has finished processing
// & has terminated.
func (l *Closer) BufferSinkDone(bs *bufferSink) {
	l.registryMu.Lock()
	defer l.registryMu.Unlock()
	// If we don't have the sink in the registry, then the sink is not accounted for
	// in the WaitGroup. Warn and return early - to signal the WaitGroup could prematurely
	// end the shutdown sequence of a different bufferSink that is registered.
	if _, ok := l.registryMu.sinkRegistry[bs]; !ok {
		fmt.Printf(
			"# WARNING: log shutdown sequence has detected an unregistered log sink: %p\n", bs)
		return
	}
	delete(l.registryMu.sinkRegistry, bs)
	l.wgMu.bufferSinkWg.Done()
}

// Close triggers the logging shutdown process, calling
// the shutdown function provided via SetShutdownFn and waiting for all
// buffered log sinks registered with the Closer to signal that
// they've finished processing before returning.
//
// Will timeout after 90 seconds to protect against registered sink(s)
// failing to call BufferSinkDone from causing the process to indefinitely
// hang.
func (l *Closer) Close() {
	l.shutdownFn()
	l.wgMu.Lock()
	defer l.wgMu.Unlock()

	doneCh := make(chan struct{})
	timer := time.After(l.timeout)

	go func() {
		l.wgMu.bufferSinkWg.Wait()
		doneCh <- struct{}{}
	}()

	select {
	case <-doneCh:
	case <-timer:
		l.registryMu.Lock()
		defer l.registryMu.Unlock()
		leakedAddrs := make([]string, 0, len(l.registryMu.sinkRegistry))
		for bs := range l.registryMu.sinkRegistry {
			leakedAddrs = append(leakedAddrs, fmt.Sprintf("%p", bs))
		}
		fmt.Printf(
			"# WARNING: log shutdown sequence has detected a deadlock & has timed out. Hanging log sink(s): %v\n",
			leakedAddrs)
	}
}
