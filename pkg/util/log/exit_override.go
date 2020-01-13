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
	"fmt"
	"io"
	"os"
)

// SetExitFunc allows setting a function that will be called to exit
// the process when a Fatal message is generated. The supplied bool,
// if true, suppresses the stack trace, which is useful for test
// callers wishing to keep the logs reasonably clean.
//
// Call with a nil function to undo.
func SetExitFunc(hideStack bool, f func(int)) {
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
// l.mu is held; logging.mu is not held.
func (l *loggerT) exitLocked(err error) {
	l.mu.AssertHeld()

	// Either stderr or our log file is broken. Try writing the error to both
	// streams in the hope that one still works or else the user will have no idea
	// why we crashed.
	outputs := make([]io.Writer, 2)
	outputs[0] = OrigStderr
	if f, ok := l.mu.file.(*syncBuffer); ok {
		// Don't call syncBuffer's Write method, because it can call back into
		// exitLocked. Go directly to syncBuffer's underlying writer.
		outputs[1] = f.Writer
	} else {
		outputs[1] = l.mu.file
	}
	for _, w := range outputs {
		if w == nil {
			continue
		}
		fmt.Fprintf(w, "log: exiting because of error: %s\n", err)
	}
	l.flushAndSync(true /*doSync*/)
	logging.mu.Lock()
	f := logging.mu.exitOverride.f
	logging.mu.Unlock()
	if f != nil {
		f(2, err)
	} else {
		os.Exit(2)
	}
}
