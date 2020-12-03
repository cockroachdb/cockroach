// Copyright 2019 The Cockroach Authors.
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
	"bufio"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
// file's Sync method and providing a wrapper for the Write method that provides log
// file rotation. There are conflicting methods, so the file cannot be embedded.
// l.mu is held for all its methods.
type syncBuffer struct {
	*bufio.Writer

	fileSink     *fileSink
	file         *os.File
	lastRotation int64
	nbytes       int64 // The number of bytes written to this file so far.
}

// Sync implements the flushSyncWriter interface.
//
// Note: the other methods from flushSyncWriter (Flush, io.Writer) is
// implemented by the embedded *bufio.Writer directly.
func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	maxFileSize := atomic.LoadInt64(&sb.fileSink.logFileMaxSize)
	if maxFileSize > 0 && sb.nbytes+int64(len(p)) >= maxFileSize {
		if err := sb.rotateFileLocked(timeutil.Now()); err != nil {
			return 0, err
		}
	}
	n, err = sb.Writer.Write(p)
	sb.nbytes += int64(n)
	return n, err
}

// writeToFileLocked writes to the file and applies the synchronization policy.
// Assumes that l.mu is held by the caller.
func (l *fileSink) writeToFileLocked(data []byte) error {
	_, err := l.mu.file.Write(data)
	return err
}

// ensureFileLocked ensures that l.file is set and valid.
// Assumes that l.mu is held by the caller.
func (l *fileSink) ensureFileLocked() error {
	if l.mu.file == nil {
		return l.createFileLocked()
	}
	return nil
}

// createFileLocked initializes the syncBuffer for a logger, and
// triggers creation of the log file. This is called the first time an
// event is logged after the output directory is (re)configured, or
// the first log write after closeFileLocked() in e.g. tests.
//
// In the common case, files are rotated instead during calls to
// (*syncBuffer).Write().
//
// Assumes that l.mu is held by the caller.
func (l *fileSink) createFileLocked() error {
	now := timeutil.Now()
	if l.mu.file == nil {
		sb := &syncBuffer{
			fileSink: l,
		}
		if err := sb.rotateFileLocked(now); err != nil {
			return err
		}
		l.mu.file = sb
	}
	return nil
}

// closeFileLocked() closes the output file without opening a new
// file. This is used by the TestLogScope only when the scope is
// closed. In non-test cases, files are only closed in the context of
// rotation to new files, in which case a new file is immediately
// opened. See rotateFileLocked() below.
func (l *fileSink) closeFileLocked() error {
	if l.mu.file == nil {
		return nil
	}

	// First disconnect stderr, if it was connected. We do this before
	// closing the file to ensure no direct stderr writes are lost.
	if err := l.maybeRelinquishInternalStderrLocked(); err != nil {
		return err
	}

	if sb, ok := l.mu.file.(*syncBuffer); ok {
		if err := sb.file.Close(); err != nil {
			return err
		}
	}
	l.mu.file = nil

	return nil
}

// maybeRelinquishInternalStderrLocked restores the internal fd 2 and
// os.Stderr if this logger had previously taken ownership of it.
//
// This is used by the TestLogScope only when the scope is closed.
func (l *fileSink) maybeRelinquishInternalStderrLocked() error {
	if !l.mu.currentlyOwnsInternalStderr {
		return nil
	}
	if err := hijackStderr(OrigStderr); err != nil {
		return err
	}
	l.mu.currentlyOwnsInternalStderr = false
	return nil
}

// rotateFileLocked closes the syncBuffer's file and starts a new one.
//
// This function has a challenging dance to accomplish: it must
// move from one log file to another but not lose any log events
// while doing so. So it really wants to keep the previous file active
// until it is somewhat sure that the new file works.
//
// This requirement mandates that the function creates and opens the
// new file before it can close the previous one.
//
// However, if there is any error while switching over to the new
// file, we also don't want to litter the filesystem with a new
// file that's become unused. In that error case, the function also
// (attempts to) remove the newly-created-but-not-actually-used file.
func (sb *syncBuffer) rotateFileLocked(now time.Time) (err error) {
	// First things first: try to get all pending data to disk.  If we
	// can't get past that, no good will come from trying anything else.
	if sb.file != nil {
		if err := sb.Flush(); err != nil {
			return err
		}
	}

	// Then to create the new file. If that fails, we prefer to
	// continue using the previous file instead of breaking logging
	// altogether.
	newFile, newLastRotation, newFileName, symLinkName, err := create(
		sb.fileSink.mu.logDir, sb.fileSink.prefix, now, sb.lastRotation)
	if err != nil {
		return err
	}

	// At this point we have a new file. We may fail below:
	// - if we fail before the switchover, we want to delete the new file.
	// - if we fail after the switchover, we want to keep the new file.
	switchOverDone := false
	defer func() {
		if err != nil && !switchOverDone {
			// We're exiting with an error, there's a new file and the
			// switchover was not done yet. Give up on the new file and
			// remove it.
			err = errors.CombineErrors(err, newFile.Close())
			err = errors.CombineErrors(err, os.Remove(newFileName))
		}
	}()

	// Initialize the new file: write headers and stuff. We do this
	// before switching stderr over below, so that stderr output if any
	// makes it to the file after the headers.
	newWriter, nbytes, err := sb.fileSink.initializeNewOutputFile(newFile, now)
	if err != nil {
		return err
	}

	// Switch over internal stderr writes, if currently captured, to the
	// new file.
	if sb.fileSink.mu.redirectInternalStderrWrites {
		if err := hijackStderr(newFile); err != nil {
			return err
		}
	}

	oldFile := sb.file

	// At this point we're committed to the new file.
	switchOverDone = true
	sb.file, sb.Writer, sb.nbytes, sb.lastRotation = newFile, newWriter, nbytes, newLastRotation

	// Now close the old file if any.
	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			// Ooof. We are likely leaking a file descriptor.
			return errors.Wrap(err, "log: unable to close previous file")
		}
	}

	// And create a symlink to the new file (best effort).
	createSymlink(newFileName, symLinkName)

	// Finally, inform the garbage collector that they can do a round of
	// checks.
	select {
	case sb.fileSink.gcNotify <- struct{}{}:
	default:
	}

	return nil
}

// initializeNewOutputFile writes the log format headers at the top of
// a new output file.
func (l *fileSink) initializeNewOutputFile(
	file *os.File, now time.Time,
) (newWriter *bufio.Writer, nbytes int64, err error) {
	// bufferSize sizes the buffer associated with each log file. It's large
	// so that log records can accumulate without the logging thread blocking
	// on disk I/O. The flushDaemon will block instead.
	const bufferSize = 256 * 1024

	newWriter = bufio.NewWriterSize(file, bufferSize)

	if l.getStartLines != nil {
		bufs := l.getStartLines(now)
		for _, buf := range bufs {
			var n int
			var thisErr error
			n, thisErr = file.Write(buf.Bytes())
			nbytes += int64(n)
			// Note: we combine the errors, instead of stopping at the first
			// error encountered, to ensure that all the buffers get
			// released back to the pool.
			err = errors.CombineErrors(err, thisErr)
			putBuffer(buf)
		}
	}

	if err != nil {
		return nil, nbytes, err
	}

	return newWriter, nbytes, nil
}
