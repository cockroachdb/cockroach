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
	"context"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
)

// SetExitFunc allows setting a function that will be called to exit
// the process when a Fatal message is generated. The supplied bool,
// if true, suppresses the stack trace, which is useful for test
// callers wishing to keep the logs reasonably clean.
//
// Use ResetExitFunc() to reset.
func SetExitFunc(hideStack bool, f func(int)) {
	if f == nil {
		panic("nil exit func invalid")
	}
	setExitErrFunc(hideStack, func(x int, err error) { f(x) })
}

// setExitErrFunc is like SetExitFunc but the function can also
// observe the error that is triggering the exit.
func setExitErrFunc(hideStack bool, f func(int, error)) {
	logging.mu.Lock()
	defer logging.mu.Unlock()

	logging.mu.exitOverride.f = f
	logging.mu.exitOverride.hideStack = hideStack
}

// ResetExitFunc undoes any prior call to SetExitFunc.
func ResetExitFunc() {
	logging.mu.Lock()
	defer logging.mu.Unlock()

	logging.mu.exitOverride.f = nil
	logging.mu.exitOverride.hideStack = false
}

// exitLocked is called if there is trouble creating or writing log files, or
// writing to stderr. It flushes the logs and exits the program; there's no
// point in hanging around.
//
// l.outputMu is held; l.fileSink.mu is not held; logging.mu is not held.
func (l *loggerT) exitLocked(err error) {
	l.outputMu.AssertHeld()

	l.reportErrorEverywhereLocked(context.Background(), err)

	logging.mu.Lock()
	f := logging.mu.exitOverride.f
	logging.mu.Unlock()
	if f != nil {
		f(2, err)
	} else {
		os.Exit(2)
	}
}

// reportErrorEverywhereLocked writes the error details to both the
// process' original stderr and the log file if configured.
//
// This assumes l.outputMu is held, but l.fileSink.mu is not held.
func (l *loggerT) reportErrorEverywhereLocked(ctx context.Context, err error) {
	// Make a valid log entry for this error.
	entry := MakeEntry(
		ctx, severity.ERROR, channel.OPS,
		&l.logCounter, 2 /* depth */, l.redactableLogs.Get(),
		"logging error: %v", err)

	// Either stderr or our log file is broken. Try writing the error to both
	// streams in the hope that one still works or else the user will have no idea
	// why we crashed.
	//
	// Note that we're already in error. If an additional error is encountered
	// here, we can't do anything but raise our hands in the air.

	buf := logging.stderrFormatter.formatEntry(entry, nil /*stack*/)
	_, _ = OrigStderr.Write(buf.Bytes())
	putBuffer(buf)

	if fileSink := l.getFileSink(); fileSink != nil {
		buf := fileSink.formatter.formatEntry(entry, nil /*stack*/)
		fileSink.emergencyOutput(buf.Bytes())
		putBuffer(buf)
	}
}
