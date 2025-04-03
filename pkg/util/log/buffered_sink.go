// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// bufferedSink wraps a child sink to add buffering. Messages are accumulated
// and passed to the child sink in bulk when the buffer is flushed. The buffer
// is flushed periodically, and also when it reaches a configured size. Flushes
// can also be requested manually through the extraFlush and tryForceSync output()
// options.
//
// bufferedSink's output() method never blocks on the child (except when the
// tryForceSync option is used). Instead, old messages are dropped if the buffer is
// overflowing a configured limit.
//
// Should an error occur in the child sink, it's forwarded to the provided
// onAsyncFlushErr (unless tryForceSync is requested, in which case the error is
// returned synchronously, as it would for any other sink).
//
// Note that tryForceSync is best-effort, but there are scenarios where it can't
// be honored. See output() documentation for additional details.
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

	format *bufferFmtConfig

	mu struct {
		syncutil.Mutex
		// buf buffers the messages that have yet to be flushed.
		buf msgBuf
		// timer is set when a flushAsync() call is scheduled to happen in the
		// future.
		timer *time.Timer
	}
}

type bufferFmtConfig struct {
	fmtType logconfig.BufferFormat

	// delimiter is the string used to separate log entries
	delimiter string

	// prefix is the string that is prepended to the entire buffer output
	prefix string

	// suffix is the string that is appended to the entire buffer output
	suffix string
}

func newBufferFmtConfig(bufferFmt *logconfig.BufferFormat) *bufferFmtConfig {
	// Use newline by default.
	cfg := bufferFmtConfig{delimiter: "\n", fmtType: logconfig.BufferFmtNewline}
	if bufferFmt == nil {
		return &cfg
	}

	switch *bufferFmt {
	case logconfig.BufferFmtNewline:
		// Do nothing, we'll return the default cfg after.
	case logconfig.BufferFmtJsonArray:
		cfg.fmtType = logconfig.BufferFmtJsonArray
		cfg.delimiter = ","
		cfg.prefix = "["
		cfg.suffix = "]"
	case logconfig.BufferFmtNone:
		cfg.fmtType = logconfig.BufferFmtNone
		cfg.delimiter = ""
	default:
		panic(errors.AssertionFailedf("unknown BufferFormat: %v", *bufferFmt))
	}

	return &cfg
}

// newBufferedSink creates a bufferedSink that wraps child.
//
// Start() must be called on it before use.
//
// maxStaleness and triggerSize control the circumstances under which the sink
// automatically flushes its contents to the child sink. Zero values disable
// these flush triggers. If all triggers are disabled, the buffer is only ever
// flushed when a flush is explicitly requested through the extraFlush or
// tryForceSync options passed to output().
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
	bufferFmt *logconfig.BufferFormat,
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

	cfg := newBufferFmtConfig(bufferFmt)

	sink := &bufferedSink{
		child: child,
		// flushC is a buffered channel, so that an async flush triggered while
		// another flush is in progress doesn't block.
		flushC:                   make(chan struct{}, 1),
		triggerSize:              triggerSize,
		maxStaleness:             maxStaleness,
		crashOnAsyncFlushFailure: crashOnAsyncFlushErr,
		format:                   cfg,
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
// If tryForceSync is set, the output() call attempts to block on the
// child sink flush and returns the child sink's error (which is otherwise
// handled via the bufferedSink's onAsyncFlushErr). However, if a previous
// tryForceSync is already scheduled on flushC, we do not block. In this case,
// the msg provided to this output() call will be included in the already
// scheduled tryForceSync's flush, which is imminent. This is an edge case
// that should rarely happen, but it's possible.
//
// If the bufferedSink drops this message instead of passing it to the child
// sink, errSyncMsgDropped is returned. This is generally due to buffer size
// limitations.
func (bs *bufferedSink) output(b []byte, opts sinkOutputOptions) error {
	// Make a copy to live in the async buffer.
	// We can't take ownership of the slice we're passed --
	// it belongs to a buffer that's synchronously being returned
	// to the pool for reuse.
	msg := getBuffer()
	_, _ = msg.Write(b)

	var errC chan error

	err := func() error {
		bs.mu.Lock()
		defer bs.mu.Unlock()
		// Append the message to the buffer.
		err := bs.mu.buf.appendMsg(msg)
		if err != nil {
			// Release the msg buffer, since our append failed.
			putBuffer(msg)
			return err
		}

		// If the errC on the buffer is already set, then a synchronous
		// flush must already be scheduled & waiting on flushC to be executed.
		// We only support scheduling one single synchronous flush at a time
		// in the bufferedSink, triggered via the tryForceSync option.
		//
		// Since b.errC is already set by the currently scheduled synchronous flush,
		// we can't honor the tryForceSync option for this output() call. However,
		// this output() call's msg will be picked up in the imminent flush anyway,
		// since it's already been buffered.
		//
		// WARNING: Attempting to use the buffer's errC when a tryForceSync flush
		// is already scheduled, such as overwriting the reference here with a new
		// errC, will cause the goroutine already waiting on errC to deadlock!
		// Don't do this!
		syncFlushAlreadyScheduled := bs.mu.buf.errC != nil
		if !syncFlushAlreadyScheduled && opts.tryForceSync {
			// We'll ask to be notified on errC when the flush is complete.
			errC = make(chan error)
			bs.mu.buf.errC = errC
		}
		if syncFlushAlreadyScheduled && opts.tryForceSync {
			fmt.Printf(
				"tryForceSync called on %T while one already scheduled. Msg will be included in imminent flush instead.\n",
				bs.child)
		}

		// If a synchronous flush is already scheduled, then a flush is imminent, so don't bother
		// scheduling another. Our msg will be included in the upcoming flush.
		flush := !syncFlushAlreadyScheduled &&
			(opts.extraFlush || opts.tryForceSync || (bs.triggerSize > 0 && bs.mu.buf.size() >= bs.triggerSize))
		if flush {
			// Trigger a flush. The flush will take effect asynchronously (and can be
			// arbitrarily delayed if there's another flush in progress). In the
			// meantime, the current buffer continues accumulating messages until it
			// hits its limit.
			bs.flushAsyncLocked()
		} else {
			// Schedule a flush for the future based on maxStaleness, unless
			// one is scheduled already.
			if bs.mu.timer == nil && bs.maxStaleness > 0 {
				bs.mu.timer = time.AfterFunc(bs.maxStaleness, func() {
					bs.mu.Lock()
					defer bs.mu.Unlock()
					bs.flushAsyncLocked()
				})
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

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
	loggingErr := Every(time.Minute)
	for {
		done := false
		select {
		case <-bs.flushC:
		case <-stopC:
			// We'll return after flushing everything.
			done = true
		}
		msg, errC := func() (*buffer, chan<- error) {
			bs.mu.Lock()
			defer bs.mu.Unlock()
			return buf.flush(bs.format.prefix, bs.format.suffix, bs.format.delimiter)
		}()
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

		err := bs.child.output(msg.Bytes(), sinkOutputOptions{extraFlush: true, tryForceSync: errC != nil})
		if errC != nil {
			errC <- err
		} else if err != nil {
			if loggingErr.ShouldLog() {
				Ops.Errorf(context.Background(), "logging error from %T: %v", bs.child, err)
			}
			if bs.crashOnAsyncFlushFailure {
				f := func() func(exit.Code, error) {
					logging.mu.Lock()
					defer logging.mu.Unlock()
					return logging.mu.exitOverride.f
				}()
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

// appendMsg appends msg to the buffer. If msg can't fit in the buffer,
// errMsgTooLarge is returned.
//
// If the buffer is full, then we drop older messages in the buffer
// until we have space for the new message.
func (b *msgBuf) appendMsg(msg *buffer) error {
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
	return nil
}

// flush resets b, returning its contents in concatenated form. If b is empty, a
// nil buffer is returned.
func (b *msgBuf) flush(prefix string, suffix string, delimiter string) (*buffer, chan<- error) {
	msg := b.concatMessages(prefix, suffix, delimiter)
	b.messages = nil
	b.sizeBytes = 0
	errC := b.errC
	b.errC = nil
	return msg, errC
}

// concatMessages copies over the contents of all the buffers to the first one,
// which is returned.
//
// Note that the first buffer is used for writing if there is no prefix provided.
// All buffers (except potentially the first) are released to the pool.
func (b *msgBuf) concatMessages(prefix string, suffix string, delimiter string) *buffer {
	if len(b.messages) == 0 {
		return nil
	}
	totalSize := len(prefix) + len(suffix) + len(delimiter)*(len(b.messages)-1)
	for _, msg := range b.messages {
		totalSize += msg.Len()
	}

	// Append all the messages in the first buffer, and prepend the prefix string if it exists.
	buf := b.messages[0]

	if prefix != "" {
		buf = getBuffer()
		buf.Grow(totalSize)
		buf.WriteString(prefix)
		buf.Write(b.messages[0].Bytes())
		putBuffer(b.messages[0])
	} else {
		buf.Grow(totalSize - buf.Len())
	}

	// The last buffer is occasionally empty to trigger a flush.
	// To prevent unexpected formatting output, we'll exclude the last
	// buffer if it is empty.
	lastBuf := b.messages[len(b.messages)-1]
	if lastBuf.Len() == 0 {
		b.messages = b.messages[:len(b.messages)-1]
		putBuffer(lastBuf)
	}

	for i, b := range b.messages {
		if i == 0 {
			// First buffer skips putBuffer --  we're still using it
			// or have already put it back.
			continue
		}
		buf.WriteString(delimiter)
		buf.Write(b.Bytes())
		// Make b available for reuse.
		putBuffer(b)
	}
	buf.WriteString(suffix)
	return buf
}

func (b *msgBuf) dropFirstMsg() {
	firstMsg := b.messages[0]
	b.messages = b.messages[1:]
	b.sizeBytes -= uint64(firstMsg.Len())
	logging.metrics.IncrementCounter(BufferedSinkMessagesDropped, 1)
	putBuffer(firstMsg)
}
