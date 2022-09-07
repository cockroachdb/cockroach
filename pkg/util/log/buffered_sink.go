// Copyright 2021 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// bufferedSink wraps a child sink to add buffering. Messages are accumulated
// and passed to the child sink in bulk when the buffer is flushed. The buffer
// is flushed periodically, and also when it reaches a configured size. Flushes
// can also be requested manually through the extraFlush and forceSync output()
// options.
//
// bufferedSink's output() method never blocks on the child (except when the
// forceSync option is used). Instead, old messages are dropped if the buffer is
// overflowing a configured limit.
//
// Should an error occur in the child sink, it's forwarded to the provided
// onAsyncFlushErr (unless forceSync is requested, in which case the error is
// returned synchronously, as it would for any other sink).
type bufferedSink struct {
	// child is the wrapped logSink.
	child logSink
	// maxStaleness is the duration after which a flush is triggered.
	// 0 disables this trigger.
	maxStaleness time.Duration
	// triggerSize is the size in bytes of accumulated messages which trigger a flush.
	// 0 disables this trigger.
	triggerSize uint64
	// crashOnAsyncFlushFailure, if set, causes the sink to terminate the process
	// on an async flush failure.
	//
	// There's also sync flushes, which have the opportunity to deliver their
	// errors to the caller, so those are not subject to this crash.
	crashOnAsyncFlushFailure bool

	// flushC is a channel on which requests to flush the buffer are sent to the
	// runFlusher goroutine. Each request to flush comes with a channel (can be nil)
	// on which the result of the flush is to be communicated.
	flushC chan struct{}

	mu struct {
		syncutil.Mutex
		// buf buffers the messages that have yet to be flushed.
		buf msgBuf
		// timer is set when a flushAsync() call is scheduled to happen in the
		// future.
		timer *time.Timer
	}
}

// newBufferedSink creates a bufferedSink that wraps child.
//
// Start() must be called on it before use.
//
// maxStaleness and triggerSize control the circumstances under which the sink
// automatically flushes its contents to the child sink. Zero values disable
// these flush triggers. If all triggers are disabled, the buffer is only ever
// flushed when a flush is explicitly requested through the extraFlush or
// forceSync options passed to output().
//
// maxBufferSize, if not zero, limits the size of the buffer. When a new message
// is causing the buffer to overflow, old messages are dropped. The caller must
// ensure that maxBufferSize makes sense in relation to triggerSize: triggerSize
// should be lower (otherwise the buffer will never flush based on the size
// threshold), and there should be enough of a gap between the two to generally
// fit at least one message (otherwise the buffer might again never flush, since
// incoming messages would cause old messages to be dropped and the buffer's
// size might never fall in between triggerSize and maxSize). See the diagram
// below.
//
// |msg|msg|msg|msg|msg|msg|msg|msg|msg|
// └----------------------^--------------┘
//
//	triggerSize    maxBufferSize
//	  └--------------┘
//	     sized-based flush is triggered when size falls in this range
//
// maxBufferSize should also be set such that it makes sense in relationship
// with the flush latency: only one flush is ever in flight at a time, so the
// buffer should be sized to generally hold at least the amount of data that is
// expected to be produced during the time it takes one flush to complete.
func newBufferedSink(
	child logSink,
	maxStaleness time.Duration,
	triggerSize uint64,
	maxBufferSize uint64,
	crashOnAsyncFlushErr bool,
) *bufferedSink {
	if triggerSize != 0 && maxBufferSize != 0 {
		// Validate triggerSize in relation to maxBufferSize. As explained above, we
		// actually want some gap between these two, but the minimum acceptable gap
		// is left to the caller (which does its own validation).
		if triggerSize >= maxBufferSize {
			panic(errors.AssertionFailedf("expected triggerSize (%d) < maxBufferSize (%d)",
				triggerSize, maxBufferSize))
		}
	}
	sink := &bufferedSink{
		child: child,
		// flushC is a buffered channel, so that an async flush triggered while
		// another flush is in progress doesn't block.
		flushC:                   make(chan struct{}, 1),
		triggerSize:              triggerSize,
		maxStaleness:             maxStaleness,
		crashOnAsyncFlushFailure: crashOnAsyncFlushErr,
	}
	sink.mu.buf.maxSizeBytes = maxBufferSize
	return sink
}

// Start starts an internal goroutine that will run until the provided closer is
// closed.
func (bs *bufferedSink) Start(closer *bufferedSinkCloser) {
	stopC, unregister := closer.RegisterBufferedSink(bs)
	// Start the runFlusher goroutine & mark as done on the
	// closer once it exits.
	go func() {
		defer unregister()
		bs.runFlusher(stopC)
	}()
}

// active returns true if this sink is currently active.
func (bs *bufferedSink) active() bool {
	return bs.child.active()
}

// attachHints attaches some hints about the location of the message
// to the stack message.
func (bs *bufferedSink) attachHints(b []byte) []byte {
	return bs.child.attachHints(b)
}

// output emits some formatted bytes to this sink.
// the sink is invited to perform an extra flush if indicated
// by the argument. This is set to true for e.g. Fatal
// entries.
//
// The parent logger's outputMu is held during this operation: log
// sinks must not recursively call into logging when implementing
// this method.
//
// If forceSync is set, the output() call blocks on the child sink flush and
// returns the child sink's error (which is otherwise handled via the
// bufferedSink's onAsyncFlushErr). If the bufferedSink drops this message instead of
// passing it to the child sink, errSyncMsgDropped is returned.
func (bs *bufferedSink) output(b []byte, opts sinkOutputOptions) error {
	// Make a copy to live in the async buffer.
	// We can't take ownership of the slice we're passed --
	// it belongs to a buffer that's synchronously being returned
	// to the pool for reuse.
	msg := getBuffer()
	_, _ = msg.Write(b)

	var errC chan error
	if opts.forceSync {
		// We'll ask to be notified on errC when the flush is complete.
		errC = make(chan error)
	}

	bs.mu.Lock()
	// Append the message to the buffer.
	if err := bs.mu.buf.appendMsg(msg, errC); err != nil {
		bs.mu.Unlock()
		return err
	}

	flush := opts.extraFlush || opts.forceSync || (bs.triggerSize > 0 && bs.mu.buf.size() >= bs.triggerSize)
	if flush {
		// Trigger a flush. The flush will take effect asynchronously (and can be
		// arbitrarily delayed if there's another flush in progress). In the
		// meantime, the current buffer continues accumulating messages until it
		// hits its limit.
		bs.flushAsyncLocked()
	} else {
		// Schedule a flush unless one is scheduled already.
		if bs.mu.timer == nil && bs.maxStaleness > 0 {
			bs.mu.timer = time.AfterFunc(bs.maxStaleness, func() {
				bs.mu.Lock()
				bs.flushAsyncLocked()
				bs.mu.Unlock()
			})
		}
	}
	bs.mu.Unlock()

	// If this is a synchronous flush, wait for its completion.
	if errC != nil {
		return <-errC
	}
	return nil
}

// flushAsyncLocked signals the flusher goroutine to flush.
func (bs *bufferedSink) flushAsyncLocked() {
	// Make a best-effort attempt to stop a scheduled future flush, if any.
	// flushAsyncLocked might have been called by the timer, in which case the
	// timer.Stop() call below will be a no-op. It's possible that
	// flushAsyncLocked() is not called by the timer, and timer.Stop() still
	// returns false, indicating that another flushAsyncLocked() call is imminent.
	// That's also fine - we'll flush again, which will be a no-op if the buffer
	// remains empty until then.
	if bs.mu.timer != nil {
		bs.mu.timer.Stop()
		bs.mu.timer = nil
	}
	// Signal the runFlusher to flush, unless it's already been signaled.
	select {
	case bs.flushC <- struct{}{}:
	default:
	}
}

// exitCode returns the exit code to use if the logger decides
// to terminate because of an error in output().
func (bs *bufferedSink) exitCode() exit.Code {
	return bs.child.exitCode()
}

// runFlusher waits for flush signals in a loop and, when it gets one, flushes
// bs.msgBuf to the wrapped sink. The function returns when ctx is canceled.
//
// TODO(knz): How does this interact with the runFlusher logic in log_flush.go?
// See: https://github.com/cockroachdb/cockroach/issues/72458
func (bs *bufferedSink) runFlusher(stopC <-chan struct{}) {
	buf := &bs.mu.buf
	for {
		done := false
		select {
		case <-bs.flushC:
		case <-stopC:
			// We'll return after flushing everything.
			done = true
		}
		bs.mu.Lock()
		msg, errC := buf.flush()
		bs.mu.Unlock()
		if msg == nil {
			// Nothing to flush.
			// NOTE: This can happen in the done case, or if we get two flushC signals
			// in close succession: one from a manual flush and another from a
			// scheduled flush that wasn't canceled in time.
			if done {
				return
			}
			continue
		}

		err := bs.child.output(msg.Bytes(), sinkOutputOptions{extraFlush: true, forceSync: errC != nil})
		if errC != nil {
			errC <- err
		} else if err != nil {
			Ops.Errorf(context.Background(), "logging error from %T: %v", bs.child, err)
			if bs.crashOnAsyncFlushFailure {
				logging.mu.Lock()
				f := logging.mu.exitOverride.f
				logging.mu.Unlock()
				code := bs.exitCode()
				if f != nil {
					f(code, err)
				} else {
					exit.WithCode(code)
				}
			}
		}
		if done {
			return
		}
	}
}

// msgBuf accumulates messages (represented as buffers) and tracks their size.
//
// msgBuf is not thread-safe. It is protected by the bufferedSink's lock.
type msgBuf struct {
	// maxSizeBytes is the size limit. Trying to appendMsg() a message that would
	// cause the buffer to exceed this limit returns an error. 0 means no limit.
	maxSizeBytes uint64

	// The messages that have been appended to the buffer.
	messages []*buffer
	// The sum of the sizes of messages.
	sizeBytes uint64
	// errC, if set, specifies that, when the buffer is flushed, the result of the
	// flush (success or error) should be signaled on this channel.
	errC chan<- error
}

// size returns the size of b's contents, in bytes.
func (b *msgBuf) size() uint64 {
	// We account for the newline after each message.
	return b.sizeBytes + uint64(len(b.messages))
}

var errMsgTooLarge = errors.New("message dropped because it is too large")

// appendMsg appends msg to the buffer. If errC is not nil, then this channel
// will be signaled when the buffer is flushed.
func (b *msgBuf) appendMsg(msg *buffer, errC chan<- error) error {
	msgLen := uint64(msg.Len())

	// Make room for the new message, potentially by dropping the oldest messages
	// in the buffer.
	if b.maxSizeBytes > 0 {
		if msgLen > b.maxSizeBytes {
			// This message will never fit.
			return errMsgTooLarge
		}

		// The +1 accounts for a trailing newline.
		for b.size()+msgLen+1 > b.maxSizeBytes {
			b.dropFirstMsg()
		}
	}

	b.messages = append(b.messages, msg)
	b.sizeBytes += msgLen

	// Assert that b.errC is not already set. It shouldn't be set
	// because, if there was a previous message with errC set, that
	// message must have had the forceSync flag set and thus acts as a barrier:
	// no more messages are sent until the flush of that message completes.
	//
	// If b.errorCh were to be set, we wouldn't know what to do about it
	// since we can't overwrite it in case m.errorCh is also set.
	if b.errC != nil {
		panic(errors.AssertionFailedf("unexpected errC already set"))
	}
	b.errC = errC
	return nil
}

// flush resets b, returning its contents in concatenated form. If b is empty, a
// nil buffer is returned.
func (b *msgBuf) flush() (*buffer, chan<- error) {
	msg := b.concatMessages()
	b.messages = nil
	b.sizeBytes = 0
	errC := b.errC
	b.errC = nil
	return msg, errC
}

// concatMessages copies over the contents of all the buffers to the first one,
// which is returned.
//
// All buffers but the first one are released to the pool.
func (b *msgBuf) concatMessages() *buffer {
	if len(b.messages) == 0 {
		return nil
	}
	var totalSize int
	for _, msg := range b.messages {
		totalSize += msg.Len() + 1 // leave space for newLine
	}
	// Append all the messages in the first buffer.
	buf := b.messages[0]
	buf.Grow(totalSize - buf.Len())
	for i, b := range b.messages {
		if i == 0 {
			// First buffer skips putBuffer --
			// we're still using it and it's a weird size
			// for reuse.
			continue
		}
		buf.WriteByte('\n')
		buf.Write(b.Bytes())
		// Make b available for reuse.
		putBuffer(b)
	}
	return buf
}

func (b *msgBuf) dropFirstMsg() {
	// TODO(knz): This needs to get reported somehow, see
	// https://github.com/cockroachdb/cockroach/issues/72453
	firstMsg := b.messages[0]
	b.messages = b.messages[1:]
	b.sizeBytes -= uint64(firstMsg.Len())
	putBuffer(firstMsg)
}
