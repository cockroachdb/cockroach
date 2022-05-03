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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
)

// bufferSink wraps a child logSink to add buffering and asynchronous behavior.
// The buffer is flushed at a configurable size threshold and/or max staleness.
// bufferSink's output method will not block on downstream I/O (unless required
// by the forceSync option).
//
// Incoming messages are accumulated in a "bundle" until the configured staleness
// or size threshold is met, at which point they are put in a queue to be flushed
// by the child sink.  If the queue is full, current bundle is compacted rather
// sent, which currently drops the messages but retains their count for later
// reporting.
// TODO(knz): Actually report the count of dropped messages.
// See: https://github.com/cockroachdb/cockroach/issues/72453
//
// Should an error occur in the child sink, it's forwarded to the provided
// errCallback (unless forceSync is requested, in which case the error is returned
// synchronously, as it would for any other sink).
type bufferSink struct {
	// child is the wrapped logSink.
	child logSink

	// messageCh sends messages from output(), which is called when a
	// log entry is initially created, to accumulator(), which is running
	// asynchronously to populate the buffer.
	messageCh chan bufferSinkMessage
	// flushCh sends bundles of messages from accumulator(), which is
	// running asynchronously and collects incoming log entries, to
	// flusher(), which is running asynchronously and pushes entries to
	// the child sink.
	flushCh chan bufferSinkBundle
	// nInFlight internally tracks the population of flushCh to detect when it's full.
	nInFlight int32

	// maxStaleness is the duration after which a flush is triggered.
	// 0 disables this trigger.
	maxStaleness time.Duration
	// triggerSize is the size in bytes of accumulated messages which trigger a flush.
	// 0 disables this trigger.
	triggerSize int
	// maxInFlight is the maximum number of flushes to buffer before dropping messages.
	maxInFlight int32
	// errCallback is called when the child sync has an error.
	errCallback func(error)

	// inErrorState is used internally to temporarily disable the sink during error handling.
	inErrorState bool

	// shutdown is used to both register the bufferSink with the logging system's shutdown sequence,
	// as well as to signal the shutdown sequence that the bufferSink has successfully terminated.
	// This allows us to wait for all bufferSink's to gracefully finish processing before the process exits.
	shutdown *logconfig.LoggingShutdown
}

const bufferSinkDefaultMaxInFlight = 4

func newBufferSink(
	ctx context.Context,
	child logSink,
	maxStaleness time.Duration,
	triggerSize int,
	maxInFlight int32,
	errCallback func(error),
	shutdown *logconfig.LoggingShutdown,
) *bufferSink {
	if maxInFlight <= 0 {
		maxInFlight = bufferSinkDefaultMaxInFlight
	}

	sink := &bufferSink{
		child:        child,
		messageCh:    make(chan bufferSinkMessage),
		flushCh:      make(chan bufferSinkBundle, maxInFlight),
		maxStaleness: maxStaleness,
		triggerSize:  triggerSize,
		maxInFlight:  maxInFlight,
		errCallback:  errCallback,
		shutdown:     shutdown,
	}
	go sink.accumulator(ctx)
	go sink.flusher(ctx)
	shutdown.RegisterBufferSink()
	return sink
}

// accumulator accumulates messages and sends bundles of them to the flusher.
func (bs *bufferSink) accumulator(ctx context.Context) {
	var (
		b bufferSinkBundle
		// timer tracks the staleness of the oldest unflushed message.
		// It can be thought of as having 3 states:
		//   - nil when there are no accumulated messages.
		//   - non-nil but unreadable when the oldest accumulated message is
		//     younger than maxStaleness.
		//   - non-nil and readable when the older accumulated message is older
		//     than maxStaleness and a flush should therefore be triggered.
		timer <-chan time.Time // staleness timer
	)
	reset := func() {
		b = bufferSinkBundle{}
		timer = nil
	}

	for {
		flush := false

		appendMessage := func(m bufferSinkMessage) {
			b.messages = append(b.messages, m)
			b.byteLen += len(m.b.Bytes()) + 1 // account for the final newline.
			if m.flush || m.errorCh != nil || (bs.triggerSize > 0 && b.byteLen > bs.triggerSize) {
				flush = true
				// TODO(knz): This seems incorrect. If there is a non-empty
				// bufferSinkBundle already; with errorCh already set
				// (ie. synchronous previous log entry) and then an entry
				// is emitted with *another* errorCh, the first one gets lost.
				// See: https://github.com/cockroachdb/cockroach/issues/72454
				b.errorCh = m.errorCh
			} else if timer == nil && bs.maxStaleness != 0 {
				timer = time.After(bs.maxStaleness)
			}
		}
		select {
		case <-timer:
			flush = true
		case <-ctx.Done():
			// Finally, drain all remaining messages on messageCh, so messages don't
			// get dropped.
			for {
				select {
				case m := <-bs.messageCh:
					appendMessage(m)
				default:
					b.done = true
				}
				if b.done {
					break
				}
			}
			flush = true
		case m := <-bs.messageCh:
			appendMessage(m)
		}

		done := b.done
		if flush {
			// TODO(knz): This logic seems to contain a race condition (with
			// the flusher). Also it's not clear why this is using a custom
			// atomic counter? Why not using a buffered channel and check
			// via `select` that the write is possible?
			// See: https://github.com/cockroachdb/cockroach/issues/72460
			if atomic.LoadInt32(&bs.nInFlight) < bs.maxInFlight {
				bs.flushCh <- b
				atomic.AddInt32(&bs.nInFlight, 1)
				reset()
			} else {
				b.compact()
			}
		}
		if done {
			return
		}
	}
}

// flusher concatenates bundled messages and sends them to the child sink.
//
// TODO(knz): How does this interact with the flusher logic in log_flush.go?
// See: https://github.com/cockroachdb/cockroach/issues/72458
func (bs *bufferSink) flusher(ctx context.Context) {
	processLogBundle := func(b bufferSinkBundle) (done bool) {
		if len(b.messages) > 0 {
			// Append all the messages in the first buffer.
			buf := b.messages[0].b
			buf.Grow(b.byteLen - len(buf.Bytes()))
			for i, m := range b.messages {
				if i == 0 {
					// First buffer skips putBuffer --
					// we're still using it and it's a weird size
					// for reuse.
					continue
				}
				buf.WriteByte('\n')
				buf.Write(m.b.Bytes())
				putBuffer(m.b)
			}
			forceSync := b.errorCh != nil
			// Send the accumulated messages to the child sink.
			err := bs.child.output(buf.Bytes(),
				sinkOutputOptions{extraFlush: true, forceSync: forceSync})
			if forceSync {
				b.errorCh <- err
			} else if err != nil && bs.errCallback != nil {
				// Forward error to the callback, if provided.
				// Temporarily disable this sink so it's skipped by
				// any logging in the callback.
				bs.inErrorState = true
				bs.errCallback(err)
				bs.inErrorState = false
			}
		}
		// Decrease visible queue size at the end,
		// so a long-running flush triggers a compaction
		// instead of a blocked channel.
		atomic.AddInt32(&bs.nInFlight, -1)
		return b.done
	}

	// Notify the LoggingShutdown upon return that we've finished processing.
	defer bs.shutdown.BufferSinkDone()
	drain := false
	for {
		select {
		case <-ctx.Done():
			drain = true
		case b := <-bs.flushCh:
			if done := processLogBundle(b); done {
				return
			}
		}
		if drain {
			break
		}
	}
	// If we reach this stage, the ctx has been cancelled. Attempt to drain
	// any remaining bundles in the flushCh with a timeout.
	timer := time.After(10 * time.Second)
	for {
		select {
		case <-timer:
			return
		case b := <-bs.flushCh:
			if done := processLogBundle(b); done {
				return
			}
		}
	}
}

// bufferSinkMessage holds an individual log message sent from output to accumulate.
type bufferSinkMessage struct {
	b *buffer
	// flush is set if the call explicitly requests to trigger a flush.
	flush bool
	// errorCh is set iff the message was emitted with the forceSync flag.
	// This indicates that the caller is interested in knowing the error status
	// of child sink writes.
	// The caller will block expecting a (possibly nil) error to return synchronously.
	errorCh chan<- error
}

// bufferSinkBundle is the accumulated state; the unit sent from the accumulator to the flusher.
type bufferSinkBundle struct {
	messages []bufferSinkMessage
	// byteLen is the total length in bytes of the accumulated messages,
	// plus enough for separators.
	byteLen int
	// droppedCount is the number of dropped messages due to buffer fullness.
	// TODO(knz): This needs to get reported somehow, see
	// https://github.com/cockroachdb/cockroach/issues/72453
	droppedCount int
	// errorCh, if non-nil, expects to receive the (possibly nil) error
	// after the flush completes.
	errorCh chan<- error
	// done indicates that this is the last bundle and the flusher
	// should shutdown after sending.
	done bool
}

// compact compacts a bundle, and is called if the buffer is full.
// Currently, drops all messages and keeps track of the
// count. In the future, if there's visibility into message
// severities, some prioritization could happen to keep more
// important messages if there's room. Maybe the timestamp
// range too.
func (b *bufferSinkBundle) compact() {
	b.droppedCount += len(b.messages)
	for _, m := range b.messages {
		putBuffer(m.b)
	}
	b.messages = nil
}

// active returns true if this sink is currently active.
func (bs *bufferSink) active() bool {
	return !bs.inErrorState && bs.child.active()
}

// attachHints attaches some hints about the location of the message
// to the stack message.
func (bs *bufferSink) attachHints(b []byte) []byte {
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
// If forceSync is set, returns the child sink's error (which is otherwise
// handled via the bufferSink's errCallback.)
func (bs *bufferSink) output(b []byte, opts sinkOutputOptions) error {
	// Make a copy to live in the async buffer.
	// We can't take ownership of the slice we're passed --
	// it belongs to a buffer that's synchronously being returned
	// to the pool for reuse.
	buf := getBuffer()
	if _, err := buf.Write(b); err != nil {
		return err
	}
	if opts.forceSync {
		errorCh := make(chan error)
		bs.messageCh <- bufferSinkMessage{buf, opts.extraFlush, errorCh}
		return <-errorCh
	}
	bs.messageCh <- bufferSinkMessage{buf, opts.extraFlush, nil}
	return nil
}

// exitCode returns the exit code to use if the logger decides
// to terminate because of an error in output().
func (bs *bufferSink) exitCode() exit.Code {
	return bs.child.exitCode()
}
