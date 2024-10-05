// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
)

// ieResultReader is used to read internalExecutor results.
// It is managed by the rowsIterator.
type ieResultReader interface {

	// firstResult returns the first result. The return values carry the same
	// semantics as of nextResult. This method assumes that the writer is not
	// currently blocked and waits for the initial result to be written.
	firstResult(ctx context.Context) (_ ieIteratorResult, done bool, err error)

	// nextResult returns the next result. Done will always be true if err
	// is non-nil. Err will be non-nil if either close has been called or
	// the passed context is finished.
	nextResult(ctx context.Context) (_ ieIteratorResult, done bool, err error)

	// close ensures that either writer has finished writing. The writer will
	// receive a signal to drain, and close will drain the writer's channel.
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

var asyncIEResultChannelBufferSize = metamorphic.ConstantWithTestRange(
	"async-IE-result-channel-buffer-size",
	32, /* defaultValue */
	1,  /* min */
	32, /* max */
)

// newAsyncIEResultChannel returns an ieResultChannel which does not attempt to
// synchronize the writer with the reader.
func newAsyncIEResultChannel() *ieResultChannel {
	return &ieResultChannel{
		dataCh: make(chan ieIteratorResult, asyncIEResultChannelBufferSize),
		doneCh: make(chan struct{}),
	}
}

// ieResultChannel is used to coordinate passing results from an
// internalExecutor to its corresponding iterator. It can be constructed to
// ensure that there is no concurrency between the reader and writer.
type ieResultChannel struct {

	// dataCh is the channel on which the connExecutor goroutine sends the rows
	// (in addResult) and, in the synchronous case, will block on waitCh after
	// each send. The iterator goroutine blocks on dataCh until there is
	// something to receive (rows or other metadata) and will return the data to
	// the caller. On the next call to Next(), the iterator goroutine unblocks
	// the producer and will block itself again. dataCh will be closed (in
	// finish()) when the connExecutor goroutine exits its run() loop whereas
	// waitCh is closed when closing the iterator.
	dataCh chan ieIteratorResult

	// waitCh is nil for async ieResultChannels. It is never closed. In all places
	// where the caller may interact with it the doneCh is also used. This policy
	// is in place to make it safe to unblock both the reader and the writer
	// without any hazards of a blocked reader attempting to send on a closed
	// channel.
	waitCh chan struct{}

	// doneCh is used to indicate that the ieResultReader has been closed and is
	// closed under the doneOnce, the writer will transition to draining. This
	// is crucial to ensure that a synchronous writer does not attempt to
	// continue to operate after the reader has called close.
	doneCh   chan struct{}
	doneErr  error
	doneOnce sync.Once
}

// newSyncIEResultChannel is used to ensure that in execution scenarios which
// do not permit concurrency that there is none. It works by blocking the
// writing goroutine immediately upon sending on the data channel and only
// unblocking it after the reader signals.
func newSyncIEResultChannel() *ieResultChannel {
	return &ieResultChannel{
		dataCh: make(chan ieIteratorResult),
		waitCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (i *ieResultChannel) firstResult(
	ctx context.Context,
) (_ ieIteratorResult, done bool, err error) {
	// errors.Wrap returns nil if err is nil.
	const wrapMsg = "failed to read query result"
	getCtxErr := func(ctx context.Context) error {
		if ctx.Err() == nil {
			return nil
		}
		return cancelchecker.QueryCanceledError
	}
	select {
	case <-ctx.Done():
		return ieIteratorResult{}, true, errors.Wrap(getCtxErr(ctx), wrapMsg)
	case <-i.doneCh:
		return ieIteratorResult{}, true, errors.Wrap(getCtxErr(ctx), wrapMsg)
	case res, ok := <-i.dataCh:
		if !ok {
			return ieIteratorResult{}, true, errors.Wrap(getCtxErr(ctx), wrapMsg)
		}
		return res, false, nil
	}
}

func (i *ieResultChannel) maybeUnblockWriter(ctx context.Context) (done bool, err error) {
	if i.async() {
		return false, nil
	}
	// errors.Wrap returns nil if ctx.Err() is nil.
	const wrapMsg = "maybe unblock writer"
	select {
	case <-ctx.Done():
		return true, errors.Wrap(ctx.Err(), wrapMsg)
	case <-i.doneCh:
		return true, errors.Wrap(ctx.Err(), wrapMsg)
	case i.waitCh <- struct{}{}:
		return false, nil
	}
}

func (i *ieResultChannel) async() bool {
	return i.waitCh == nil
}

func (i *ieResultChannel) nextResult(
	ctx context.Context,
) (_ ieIteratorResult, done bool, err error) {
	if done, err = i.maybeUnblockWriter(ctx); done {
		return ieIteratorResult{}, done, err
	}
	return i.firstResult(ctx)
}

func (i *ieResultChannel) close() error {
	i.doneOnce.Do(func() {
		close(i.doneCh)
		for {
			// In the async case, res might contain some actual rows, but we're
			// not interested in them; in the sync case, only errors are
			// expected to be retrieved from now one because the writer
			// transitions to draining.
			res, done, err := i.nextResult(context.TODO())
			if i.doneErr == nil {
				if res.err != nil {
					i.doneErr = res.err
				} else if err != nil {
					i.doneErr = err
				}
			}
			if done {
				return
			}
		}
	})
	return i.doneErr
}

// errIEResultChannelClosed is returned by the writer when the reader has closed
// ieResultChannel. The error indicates to the writer to drain the query
// execution, but the reader won't propagate it further.
var errIEResultChannelClosed = errors.New("ieResultReader closed")

func (i *ieResultChannel) addResult(ctx context.Context, result ieIteratorResult) error {
	// errors.Wrap returns nil if ctx.Err() is nil.
	const wrapMsg = "add result"
	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), wrapMsg)
	case <-i.doneCh:
		// Prefer the context error if there is one.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Wrap(ctx.Err(), wrapMsg)
		}
		return errIEResultChannelClosed
	case i.dataCh <- result:
	}
	return i.maybeBlock(ctx)
}

func (i *ieResultChannel) maybeBlock(ctx context.Context) error {
	// errors.Wrap returns nil if ctx.Err() is nil.
	const wrapMsg = "maybe block"
	if i.async() {
		return nil
	}
	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), wrapMsg)
	case <-i.doneCh:
		// Prefer the context error if there is one.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Wrap(ctxErr, wrapMsg)
		}
		return errIEResultChannelClosed
	case <-i.waitCh:
		return nil
	}
}

func (i *ieResultChannel) finish() {
	close(i.dataCh)
}
