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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// BufferedSinkCloser is a utility used by the logging system to
// trigger the teardown of logging facilities gracefully within
// CockroachDB.
//
// The API allows us to register buffered log sinks with RegisterBufferedSink
// so we can wait for them to drain/timeout before exiting.
//
// The API also allows us to trigger the shutdown sequence via Close and
// wait for all registered buffered log sinks to finish processing
// before exiting, to help ensure a graceful shutdown of buffered
// log sinks.
type BufferedSinkCloser struct {
	// ctx is the BufferedSinkCloser's own context created at initialization.
	// All registered bufferedSink's are provided a child context of this
	// context to detect cancellation on `ctx.Done()`.
	ctx context.Context
	// cancel is the context cancellation function associated with ctx.
	cancel context.CancelFunc
	// timeout is used in Close as a solution to potential deadlocks.
	// Is only intended to be modified for testing purposes.
	timeout time.Duration
	// wg is the WaitGroup used during the Close procedure to ensure that
	// all registered bufferedSink's have completed before returning.
	wg sync.WaitGroup
	mu struct {
		syncutil.Mutex
		// sinkRegistry acts as a set and stores references to all bufferSink's
		// registered with this BufferedSinkCloser instance. Only useful for debugging
		// purposes.
		sinkRegistry map[*bufferedSink]struct{}
	}
}

// Default timeout used in Close
var defaultCloserTimeout = 90 * time.Second

// NewCloser returns a new instance of a BufferedSinkCloser.
func NewBufferedSinkCloser() *BufferedSinkCloser {
	ctx, cancel := context.WithCancel(context.Background())
	closer := &BufferedSinkCloser{
		ctx:     ctx,
		cancel:  cancel,
		timeout: defaultCloserTimeout,
	}
	closer.mu.sinkRegistry = make(map[*bufferedSink]struct{})
	return closer
}

// RegisterBufferedSink notifies the BufferedSinkCloser of the existence
// of an active buffered log sink within the logging system.
// This increments a sync.WaitGroup counter, so be sure that the
// caller also has a subsequent call to BufferedSinkDone.
//
// A reference to the bufferSink is also maintained in an
// internal registry to aid in debug capabilities.
//
// Returns a context, which will be cancelled on Close.
func (l *BufferedSinkCloser) RegisterBufferedSink(bs *bufferedSink) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.mu.sinkRegistry[bs]; ok {
		panic(errors.AssertionFailedf("buffered log sink registered more than once within log.BufferedSinkCloser: %T", bs.child))
	}

	l.mu.sinkRegistry[bs] = struct{}{}
	l.wg.Add(1)
	return l.ctx
}

// BufferedSinkDone notifies the BufferedSinkCloser that one of the buffered
// log sinks registered via RegisterBufferedSink has finished processing
// & has terminated.
func (l *BufferedSinkCloser) BufferedSinkDone(bs *bufferedSink) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// If we don't have the sink in the registry, then the sink is not accounted for
	// in the WaitGroup. Warn and return early - to signal the WaitGroup could prematurely
	// end the shutdown sequence of a different bufferSink that is registered.
	if _, ok := l.mu.sinkRegistry[bs]; !ok {
		panic(errors.AssertionFailedf(
			"# WARNING: log shutdown sequence has detected an unregistered log sink: %T\n", bs.child))
		return
	}
	delete(l.mu.sinkRegistry, bs)
	l.wg.Done()
}

// Close triggers the logging shutdown process, closing the held context and waiting
// for all buffered log sinks registered with the BufferedSinkCloser to signal that
// they've finished processing before returning.
//
// Will give up on waiting after l.timeout to protect against registered sink(s)
// failing to call BufferedSinkDone causing the process to indefinitely hang.
func (l *BufferedSinkCloser) Close() error {
	l.cancel()
	doneCh := make(chan struct{})
	go func() {
		l.wg.Wait()
		doneCh <- struct{}{}
	}()

	select {
	case <-doneCh:
		return nil
	case <-time.After(l.timeout):
		l.mu.Lock()
		defer l.mu.Unlock()
		leakedSinks := make([]string, 0, len(l.mu.sinkRegistry))
		for bs := range l.mu.sinkRegistry {
			leakedSinks = append(leakedSinks, fmt.Sprintf("%T", bs.child))
		}
		return errors.Newf(
			"# WARNING: log shutdown sequence has detected a deadlock & has timed out. Hanging log sink(s): %v\n",
			leakedSinks)
	}
}
