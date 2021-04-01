// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// ieResultChannel is used to coordinate passing results from an
// internalExecutor to its corresponding iterator.
type ieResultChannel interface {
	ieResultReader
	ieResultWriter
}

// ieResultReader is used to read internalExecutor results.
// It is managed by the rowsIterator.
type ieResultReader interface {

	// firstResult returns the first result. The return values carry the same
	// semantics as of nextResult. This method assumes that the writer is not
	// currently blocked and waits for the initial result to be written.
	firstResult(ctx context.Context) (_ ieIteratorResult, done bool, err error)

	// nextResult returns the nextResult. Done will always be true if err
	// is non-nil. Err will be non-nil if either close has been called or
	// the passed context is finished.
	nextResult(ctx context.Context) (_ ieIteratorResult, done bool, err error)

	// close ensures that the either writer has finished writing. In the case
	// of an asynchronous channel, close will drain the writer's channel. In the
	// case of the synchronous channel, it will ensure that the writer receives
	// an error when it wakes.
	close() error
}

// ieResultWriter is used by the internalExecutor to write results to an
// iterator.
type ieResultWriter interface {

	// addResult adds a result. It may block until the next result is requested
	// by the reader, depending on the synchronization strategy.
	addResult(ctx context.Context, result ieIteratorResult) error

	// finish is used to indicate that the writer is done writing rows.
	finish()
}

var asyncIEResultChannelBufferSize = util.ConstantWithMetamorphicTestRange(
	"async-IE-result-channel-buffer-size",
	32, /* defaultValue */
	1,  /* min */
	32, /* max */
)

// newAsyncIEResultChannel returns an ieResultChannel which does not attempt to
// synchronize the writer with the reader.
func newAsyncIEResultChannel() ieResultChannel {
	return &asyncIEResultChannel{
		dataCh: make(chan ieIteratorResult, asyncIEResultChannelBufferSize),
	}
}

type asyncIEResultChannel struct {
	dataCh chan ieIteratorResult
}

var _ ieResultChannel = &asyncIEResultChannel{}

func (c *asyncIEResultChannel) firstResult(
	ctx context.Context,
) (_ ieIteratorResult, done bool, err error) {
	select {
	case <-ctx.Done():
		return ieIteratorResult{}, true, ctx.Err()
	case res, ok := <-c.dataCh:
		if !ok {
			return ieIteratorResult{}, true, nil
		}
		return res, false, nil
	}
}

func (c *asyncIEResultChannel) nextResult(
	ctx context.Context,
) (_ ieIteratorResult, done bool, err error) {
	return c.firstResult(ctx)
}

func (c *asyncIEResultChannel) close() error {
	var firstErr error
	for {
		res, done, err := c.nextResult(context.TODO())
		if firstErr == nil {
			if res.err != nil {
				firstErr = res.err
			} else if err != nil {
				firstErr = err
			}
		}
		if done {
			return firstErr
		}
	}
}

func (c *asyncIEResultChannel) addResult(ctx context.Context, result ieIteratorResult) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.dataCh <- result:
		return nil
	}
}

func (c *asyncIEResultChannel) finish() {
	close(c.dataCh)
}

// syncIEResultChannel is used to ensure that in execution scenarios which
// do not permit concurrency that there is none. It works by blocking the
// writing goroutine immediately upon sending on the data channel and only
// unblocking it after the reader signals.
type syncIEResultChannel struct {

	// dataCh is the channel on which the connExecutor goroutine sends the rows
	// (in addResult) and will block on waitCh after each send. The iterator
	// goroutine blocks on dataCh until there is something to receive (rows or
	// other metadata) and will return the data to the caller. On the next call
	// to Next(), the iterator goroutine unblocks the producer and will block
	// itself again. dataCh will be closed (in finish()) when the connExecutor
	// goroutine exits its run() loop whereas waitCh is closed when closing the
	// iterator.
	dataCh chan ieIteratorResult

	// waitCh is never closed. In all places where the caller may interact with it
	// the doneCh is also used. This policy is in place to make it safe to unblock
	// both the reader and the writer without any hazards of a blocked reader
	// attempting to send on a closed channel.
	waitCh chan struct{}

	// doneCh is used to indicate that the ReadWriter has been closed.
	// doneCh is closed under the doneOnce. The doneCh is only used for the
	// syncIEResultChannel. This is crucial to ensure that a synchronous writer
	// does not attempt to continue to operate after the reader has called close.
	doneCh   chan struct{}
	doneOnce sync.Once
}

var _ ieResultChannel = &syncIEResultChannel{}

// newSyncIEResultChannel returns an ieResultChannel which synchronizes the
// writer with the reader.
func newSyncIEResultChannel() ieResultChannel {
	return &syncIEResultChannel{
		dataCh: make(chan ieIteratorResult),
		waitCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (i *syncIEResultChannel) firstResult(
	ctx context.Context,
) (_ ieIteratorResult, done bool, err error) {
	select {
	case <-ctx.Done():
		return ieIteratorResult{}, true, ctx.Err()
	case <-i.doneCh:
		return ieIteratorResult{}, true, nil
	case res, ok := <-i.dataCh:
		if !ok {
			return ieIteratorResult{}, true, nil
		}
		return res, false, nil
	}
}

func (i *syncIEResultChannel) unblockWriter(ctx context.Context) (done bool, err error) {
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-i.doneCh:
		return true, nil
	case i.waitCh <- struct{}{}:
		return false, nil
	}
}

func (i *syncIEResultChannel) finish() {
	close(i.dataCh)
}

func (i *syncIEResultChannel) nextResult(
	ctx context.Context,
) (_ ieIteratorResult, done bool, err error) {
	if done, err = i.unblockWriter(ctx); done {
		return ieIteratorResult{}, done, err
	}
	return i.firstResult(ctx)
}

func (i *syncIEResultChannel) close() error {
	i.doneOnce.Do(func() { close(i.doneCh) })
	return nil
}

// errSyncIEResultReaderCanceled is returned by the writer when the reader has
// closed syncIEResultChannel. The error indicates to the writer to shut down
// the query execution, but the reader won't propagate it further.
var errSyncIEResultReaderCanceled = errors.New("synchronous ieResultReader closed")

func (i *syncIEResultChannel) addResult(ctx context.Context, result ieIteratorResult) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.doneCh:
		return errSyncIEResultReaderCanceled
	case i.dataCh <- result:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.doneCh:
		return errSyncIEResultReaderCanceled
	case <-i.waitCh:
		return nil
	}
}
