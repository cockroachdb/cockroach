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

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
)

// SetExitFunc allows setting a function that will be called to exit
// the process when a Fatal message is generated. The supplied bool,
// if true, suppresses the stack trace, which is useful for test
// callers wishing to keep the logs reasonably clean.
//
// Use ResetExitFunc() to reset.
func SetExitFunc(hideStack bool, f func(exit.Code)) {
	if f == nil {
		panic("nil exit func invalid")
	}
	setExitErrFunc(hideStack, func(x exit.Code, err error) { f(x) })
}

// setExitErrFunc is like SetExitFunc but the function can also
// observe the error that is triggering the exit.
func setExitErrFunc(hideStack bool, f func(exit.Code, error)) {
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
func (l *loggerT) exitLocked(err error, code exit.Code) {
	l.outputMu.AssertHeld()

	l.reportErrorEverywhereLocked(context.Background(), err)

	logging.mu.Lock()
	f := logging.mu.exitOverride.f
	logging.mu.Unlock()
	if f != nil {
		f(code, err)
	} else {
		exit.WithCode(code)
	}
}

// reportErrorEverywhereLocked writes the error details to both the
// process' original stderr and the log file if configured.
//
// This assumes l.outputMu is held, but l.fileSink.mu is not held.
func (l *loggerT) reportErrorEverywhereLocked(ctx context.Context, err error) {
	// Make a valid log entry for this error.
	entry := makeUnstructuredEntry(
		ctx, severity.ERROR, channel.OPS,
		2,    /* depth */
		true, /* redactable */
		"logging error: %v", err)

	// Either stderr or our log file is broken. Try writing the error to both
	// streams in the hope that one still works or else the user will have no idea
	// why we crashed.
	//
	// Note that we're already in error. If an additional error is encountered
	// here, we can't do anything but raise our hands in the air.

	// TODO(knz): we may want to push this information to more channels,
	// e.g. to the OPS channel, if we are reporting an error while writing
	// to a non-OPS channel.

	for _, s := range l.sinkInfos {
		sink := s.sink
		if logpb.Severity_ERROR >= s.threshold && sink.active() {
			buf := s.formatter.formatEntry(entry)
			sink.emergencyOutput(buf.Bytes())
			putBuffer(buf)
		}
	}
}
