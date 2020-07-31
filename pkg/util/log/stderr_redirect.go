// Copyright 2017 The Cockroach Authors.
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
	"os"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// OrigStderr points to the original stderr stream when the process
// started.
// Note that it is generally not sound to restore os.Stderr
// and fd 2 from here as long as a stack of loggers (especially test
// log scopes) are active, as the loggers keep track of what they are
// redirecting themselves in a stack structure.
var OrigStderr = func() *os.File {
	fd, err := dupFD(os.Stderr.Fd())
	if err != nil {
		panic(err)
	}

	return os.NewFile(fd, os.Stderr.Name())
}()

// LoggingToStderr returns true if log messages of the given severity
// sent to the main logger are also visible on stderr. This is used
// e.g. by the startup code to announce server details both on the
// external stderr and to the log file.
//
// This is also the logic used by Shout calls.
func LoggingToStderr(s Severity) bool {
	return s >= mainLog.stderrThreshold.get()
}

// hijackStderr replaces stderr with the given file descriptor.
//
// A client that wishes to use the original stderr (the process'
// external stderr stream) must use OrigStderr defined above.
func hijackStderr(f *os.File) error {
	return redirectStderr(f)
}

// osStderrMu ensures that concurrent redirects of stderr don't
// overwrite each other.
var osStderrMu syncutil.Mutex

// takeOverInternalStderr tells the given logger that it is to take over
// direct writes to fd 2 by e.g. the Go runtime, or direct writes to
// os.Stderr.
//
// This also enforces that at most one logger can redirect stderr in
// this way. It also returns an error if stderr has already been
// taken over in this way. It also errors if the target logger has no
// valid output directory and no output file has been created (or
// could be created).
func (l *loggerT) takeOverInternalStderr() error {
	takeOverStderrMu.Lock()
	defer takeOverStderrMu.Unlock()

	if anyLoggerHasInternalStderrOwnership() {
		return errors.AssertionFailedf(
			"can't take over stderr; first takeover:\n%s",
			takeOverStderrMu.previousStderrTakeover)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Ensure there's a file to work with.
	if err := l.ensureFileLocked(); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "can't take over stderr without a file")
	}
	// Ensure there's a _real_ file to work with.
	sb, ok := l.mu.file.(*syncBuffer)
	if !ok {
		return errors.AssertionFailedf("can't take over stderr with a non-file writer")
	}
	// Take over stderr with this writer.
	if err := hijackStderr(sb.file); err != nil {
		return errors.Wrap(err, "unable to take over stderr")
	}
	// Mark the stderr as taken over.
	l.mu.currentlyOwnsInternalStderr = true
	// Request internal stderr redirection for future file rotations.
	l.mu.redirectInternalStderrWrites = true

	// Success: remember who called us, in case the next
	// caller comes along with the wrong call sequence.
	takeOverStderrMu.previousStderrTakeover = string(debug.Stack())
	return nil
}

// relinquishInternalStderr relinquishes a takeover by
// takeOverInternalStderr(). It returns an error if the
// logger did not take over internal stderr writes already.
func (l *loggerT) relinquishInternalStderr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.mu.redirectInternalStderrWrites {
		const basemsg = "can't relinquish stderr writes - this logger is not owner%s"
		// Try to help the caller a bit.
		takeOverStderrMu.Lock()
		defer takeOverStderrMu.Unlock()
		var extra string
		if anyLoggerHasInternalStderrOwnership() {
			extra = fmt.Sprintf("; previous take over:\n%s", takeOverStderrMu.previousStderrTakeover)
		}
		return errors.AssertionFailedf(basemsg, extra)
	}

	// If stderr actually redirected, restore it.
	if l.mu.currentlyOwnsInternalStderr {
		if err := hijackStderr(OrigStderr); err != nil {
			return errors.Wrap(err, "unable to restore internal stderr")
		}
	}

	// Remove the ownership.
	l.mu.currentlyOwnsInternalStderr = false
	l.mu.redirectInternalStderrWrites = false
	return nil
}

// anyLoggerHasInternalStderrOwnership returns true iff any of the
// loggers currently has redirectInternalStderrWrites set.
//
// Used by takeOverInternalStderr() to enforce its invariant.
func anyLoggerHasInternalStderrOwnership() bool {
	mainLog.mu.Lock()
	mainLogHasOwnership := mainLog.mu.redirectInternalStderrWrites
	mainLog.mu.Unlock()
	if mainLogHasOwnership {
		return true
	}
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, secL := range secondaryLogRegistry.mu.loggers {
		secL.logger.mu.Lock()
		hasOwnership := secL.logger.mu.redirectInternalStderrWrites
		secL.logger.mu.Unlock()
		if hasOwnership {
			return true
		}
	}
	return false
}

var takeOverStderrMu struct {
	syncutil.Mutex

	// previousStderrTakeover is the stack trace of the previous call to
	// takeOverStderrInternal(). This can be used to troubleshoot
	// invalid call sequences.
	previousStderrTakeover string
}
