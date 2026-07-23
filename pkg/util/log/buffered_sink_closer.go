// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// bufferedSinkCloser is a utility used by the logging system to
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
type bufferedSinkCloser struct {
	// stopC is closed by Close() to signal all the registered sinks to shut down.
	stopC chan struct{}
	// wg is the WaitGroup used during the Close procedure to ensure that
	// all registered bufferedSink's have completed before returning.
	wg sync.WaitGroup
	mu struct {
		syncutil.Mutex
		// sinkRegistry acts as a set and stores references to all bufferSink's
		// registered with this bufferedSinkCloser instance. Only useful for debugging
		// purposes.
		sinkRegistry map[*bufferedSink]struct{}
	}
}

// newBufferedSinkCloser returns a new bufferedSinkCloser.
func newBufferedSinkCloser() *bufferedSinkCloser {
	closer := &bufferedSinkCloser{
		stopC: make(chan struct{}),
	}
	closer.mu.sinkRegistry = make(map[*bufferedSink]struct{})
	return closer
}

// RegisterBufferedSink registers a bufferedSink with closer. closer.Close will
// block for this sink's shutdown.
//
// A reference to the bufferSink is maintained in an internal registry to aid in
// debug capabilities.
//
// Returns a channel that will be closed when closer.Close() is called. The
// bufferedSink should listen to this channel and shutdown. The cleanup function
// needs to be called once the bufferedSink has shutdown.
func (closer *bufferedSinkCloser) RegisterBufferedSink(
	bs *bufferedSink,
) (shutdown <-chan (struct{}), cleanup func()) {
	closer.mu.Lock()
	defer closer.mu.Unlock()

	if _, ok := closer.mu.sinkRegistry[bs]; ok {
		panic(errors.AssertionFailedf("buffered log sink registered more than once within log.bufferedSinkCloser: %T", bs.child))
	}

	closer.mu.sinkRegistry[bs] = struct{}{}
	closer.wg.Add(1)
	return closer.stopC, func() { closer.bufferedSinkDone(bs) }
}

// bufferedSinkDone notifies the bufferedSinkCloser that one of the buffered
// log sinks registered via RegisterBufferedSink has finished processing
// & has terminated.
func (closer *bufferedSinkCloser) bufferedSinkDone(bs *bufferedSink) {
	closer.mu.Lock()
	defer closer.mu.Unlock()
	// If we don't have the sink in the registry, then the sink is not accounted for
	// in the WaitGroup. Warn and return early - to signal the WaitGroup could prematurely
	// end the shutdown sequence of a different bufferSink that is registered.
	if _, ok := closer.mu.sinkRegistry[bs]; !ok {
		panic(errors.AssertionFailedf(
			"log shutdown sequence has detected an unregistered log sink: %T\n", bs.child))
	}
	delete(closer.mu.sinkRegistry, bs)
	closer.wg.Done()
}

// defaultCloserTimeout is the default duration that
// bufferedSinkCloser.Close(timeout) will wait for sinks to shut down.
const defaultCloserTimeout = 90 * time.Second

// Close triggers the logging shutdown process, signaling all registered sinks
// to shut down and waiting for them to do so up to timeout.
func (closer *bufferedSinkCloser) Close(timeout time.Duration) error {
	close(closer.stopC)
	doneCh := make(chan struct{})
	go func() {
		closer.wg.Wait()
		doneCh <- struct{}{}
	}()

	select {
	case <-doneCh:
		return nil
	case <-time.After(timeout):
		closer.mu.Lock()
		defer closer.mu.Unlock()
		leakedSinks := make([]string, 0, len(closer.mu.sinkRegistry))
		for bs := range closer.mu.sinkRegistry {
			leakedSinks = append(leakedSinks, fmt.Sprintf("%T", bs.child))
		}
		return errors.Newf(
			"log shutdown sequence has detected a deadlock & has timed out. Hanging log sink(s): %v",
			leakedSinks)
	}
}
