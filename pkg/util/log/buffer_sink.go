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
)

type bufferSinkMessage struct {
	b      *buffer
	flush  bool
	syncCh chan<- error
}

type bufferSink struct {
	child        logSink
	messageCh    chan<- bufferSinkMessage
	inErrorState bool
}

const maxFlushQueueLen = 5

func newBufferSink(
	ctx context.Context,
	child logSink,
	maxStaleness time.Duration,
	triggerSize int,
	errCallback func(error),
) *bufferSink {
	// bundle is the accumulated state; the unit to be flushed
	type bundle struct {
		messages     []bufferSinkMessage
		droppedCount int
		// If non-nil, expects to receive error (possibly nil) after the flush completes.
		errorCh chan<- error
	}
	// compact a bundle, to be called if the buffer is full
	// Currently, drops all messages and keeps track of the
	// count. In the future, if there's visibility into message
	// severities, some prioritization could happen to keep more
	// important messages if there's room. Maybe the timestamp
	// range too.
	compact := func(b *bundle) {
		b.droppedCount += len(b.messages)
		for _, m := range b.messages {
			putBuffer(m.b)
		}
		b.messages = nil
	}

	messageCh := make(chan bufferSinkMessage)      // sends messages to the accumulator
	flushCh := make(chan bundle, maxFlushQueueLen) // sends bundles to the flusher
	// manually tracks the population of flushCh to detect when its full
	var nQueuedFlushes int32

	sink := &bufferSink{
		child:     child,
		messageCh: messageCh,
	}

	// Accumulator
	go func() {
		var (
			b       bundle
			byteLen int
			timer   <-chan time.Time // staleness timer
		)
		reset := func() {
			b = bundle{}
			byteLen = -1 // first message doesn't have newline seperator
			timer = nil
		}
		reset()

		for {
			flush := false
			done := false

			select {
			case <-timer:
				flush = true
			case <-ctx.Done():
				done = true
				flush = true
			case m := <-messageCh:
				b.messages = append(b.messages, m)
				byteLen += len(m.b.Bytes()) + 1
				if m.flush || m.syncCh != nil || (triggerSize > 0 && byteLen > triggerSize) {
					flush = true
					b.errorCh = m.syncCh
				} else if timer == nil && maxStaleness != 0 {
					timer = time.After(maxStaleness)
				}
			}

			if flush {
				if atomic.LoadInt32(&nQueuedFlushes) < maxFlushQueueLen {
					flushCh <- b
					atomic.AddInt32(&nQueuedFlushes, 1)
					reset()
				} else {
					compact(&b)
				}
			}
			if done {
				return
			}
		}
	}()

	// Flusher
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case b := <-flushCh:
				// Calculate size of all-but-first buffer.
				tailSize := 0
				for i, m := range b.messages {
					if i == 0 {
						continue
					}
					// Include an extra byte for the newline.
					tailSize += m.b.Len() + 1
				}
				// Append all the messages in the first buffer.
				buf := b.messages[0].b
				buf.Grow(tailSize)
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
				// Send the accumulated messages to the child sink.
				err := child.output(buf.Bytes(), sinkOutputOptions{extraFlush: true})
				if b.errorCh != nil {
					b.errorCh <- err
				} else if err != nil && errCallback != nil {
					// Forward error to the callback, if provided.
					// Temporarily disable this sink so it's skipped by
					// any logging in the callback.
					sink.inErrorState = true
					errCallback(err)
					sink.inErrorState = false
				}
				// Decrease visible queue size at the end,
				// so a long-running flush triggers a compact
				// instead of a blocked channel.
				atomic.AddInt32(&nQueuedFlushes, -1)
			}
		}
	}()

	return sink
}

// active returns true if this sink is currently active.
func (s *bufferSink) active() bool {
	return !s.inErrorState && s.child.active()
}

// attachHints attaches some hints about the location of the message
// to the stack message.
func (s *bufferSink) attachHints(b []byte) []byte {
	return s.child.attachHints(b)
}

// output emits some formatted bytes to this sink.
// the sink is invited to perform an extra flush if indicated
// by the argument. This is set to true for e.g. Fatal
// entries.
//
// The parent logger's outputMu is held during this operation: log
// sinks must not recursively call into logging when implementing
// this method.
func (s *bufferSink) output(b []byte, opts sinkOutputOptions) error {
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
		s.messageCh <- bufferSinkMessage{buf, opts.extraFlush, errorCh}
		return <-errorCh
	}
	s.messageCh <- bufferSinkMessage{buf, opts.extraFlush, nil}
	return nil
}

// exitCode returns the exit code to use if the logger decides
// to terminate because of an error in output().
func (s *bufferSink) exitCode() exit.Code {
	return s.child.exitCode()
}
