// Copyright 2016 The Cockroach Authors.
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
	"io/ioutil"
	"os"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/errors"
)

// TestLogScope represents the lifetime of a logging output.  It
// ensures that the log files are stored in a directory specific to a
// test, and asserts that logging output is not written to this
// directory beyond the lifetime of the scope.
type TestLogScope struct {
	logDir  string
	cleanup func()
}

// tShim is the part of testing.T used by TestLogScope.
// We can't use testing.T directly because we have
// a linter which forbids its use in public interfaces.
type tShim interface {
	Fatal(...interface{})
	Failed() bool
	Error(...interface{})
	Errorf(fmt string, args ...interface{})
	Name() string
	Log(...interface{})
	Logf(fmt string, args ...interface{})
}

// showLogs is used for testing
var showLogs bool

// Scope creates a TestLogScope which corresponds to the lifetime of a logging
// directory. The logging directory is named after the calling test. It also
// disables logging to stderr.
func Scope(t tShim) *TestLogScope {
	if showLogs {
		return (*TestLogScope)(nil)
	}

	scope := ScopeWithoutShowLogs(t)
	t.Log("use -show-logs to present logs inline")
	return scope
}

// ScopeWithoutShowLogs ignores the -show-logs flag and should be used for tests
// that require the logs go to files.
func ScopeWithoutShowLogs(t tShim) *TestLogScope {
	tempDir, err := ioutil.TempDir("", "log"+fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	if err := dirTestOverride("", tempDir); err != nil {
		t.Fatal(err)
	}
	undo, err := enableLogFileOutput(tempDir, Severity_NONE)
	if err != nil {
		undo()
		t.Fatal(err)
	}
	t.Logf("test logs captured to: %s", tempDir)
	return &TestLogScope{logDir: tempDir, cleanup: undo}
}

// enableLogFileOutput turns on logging using the specified directory.
// For unittesting only.
func enableLogFileOutput(dir string, stderrSeverity Severity) (func(), error) {
	oldStderrThreshold, err := func() (Severity, error) {
		mainLog.mu.Lock()
		defer mainLog.mu.Unlock()

		return mainLog.stderrThreshold.get(), mainLog.logDir.Set(dir)
	}()
	if err != nil {
		return nil, err
	}

	var cancelStderr func()
	undo := func() {
		if cancelStderr != nil {
			cancelStderr()
			_ = hijackStderr(OrigStderr)
		}

		mainLog.mu.Lock()
		defer mainLog.mu.Unlock()
		mainLog.stderrThreshold.set(oldStderrThreshold)
	}

	mainLog.stderrThreshold.set(stderrSeverity)
	cancelStderr, err = SetupRedactionAndStderrRedirects()
	return undo, err
}

// Rotate closes the current log files so that the next log call will
// reopen them with current settings. This is useful when e.g. a test
// changes the logging configuration after opening a test log scope.
func (l *TestLogScope) Rotate(t tShim) {
	// Ensure remaining logs are written.
	Flush()

	func() {
		mainLog.mu.Lock()
		defer mainLog.mu.Unlock()
		if err := mainLog.closeFileLocked(); err != nil {
			t.Fatal(err)
		}
	}()

	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, l := range secondaryLogRegistry.mu.loggers {
		func() {
			l.logger.mu.Lock()
			defer l.logger.mu.Unlock()
			if err := l.logger.closeFileLocked(); err != nil {
				t.Fatal(err)
			}
		}()
	}
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted, unless the test has failed and the directory is non-empty.
func (l *TestLogScope) Close(t tShim) {
	// Ensure any remaining logs are written.
	Flush()

	if l == nil {
		// Never initialized.
		return
	}
	defer func() {
		// Check whether there is something to remove.
		emptyDir, err := isDirEmpty(l.logDir)
		if err != nil {
			t.Fatal(err)
		}
		inPanic := calledDuringPanic()
		if (t.Failed() && !emptyDir) || inPanic {
			// If the test failed or there was a panic, we keep the log
			// files for further investigation.
			if inPanic {
				fmt.Fprintln(OrigStderr, "\nERROR: a panic has occurred!\n"+
					"Details cannot be printed yet because we are still unwinding.\n"+
					"Hopefully the test harness prints the panic below, otherwise check the test logs.\n")
			}
			fmt.Fprintln(OrigStderr, "test logs left over in:", l.logDir)
		} else {
			// Clean up.
			if err := os.RemoveAll(l.logDir); err != nil {
				t.Error(err)
			}
		}
	}()
	defer l.cleanup()

	// Flush/Close the log files.
	if err := dirTestOverride(l.logDir, ""); err != nil {
		t.Fatal(err)
	}
}

// calledDuringPanic returns true if panic() is one of its callers.
func calledDuringPanic() bool {
	var pcs [40]uintptr
	runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:])

	for {
		f, more := frames.Next()
		if f.Function == "runtime.gopanic" {
			return true
		}
		if !more {
			break
		}
	}
	return false
}

// dirTestOverride sets the default value for the logging output directory
// for use in tests.
func dirTestOverride(expected, newDir string) error {
	if err := mainLog.dirTestOverride(expected, newDir); err != nil {
		return err
	}
	// Same with secondary loggers.
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, l := range secondaryLogRegistry.mu.loggers {
		if err := l.logger.dirTestOverride(expected, newDir); err != nil {
			return err
		}
	}
	return nil
}

func (l *loggerT) dirTestOverride(expected, newDir string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logDir.Lock()
	// The following check is intended to catch concurrent uses of
	// Scope() or TestLogScope.Close(), which would be invalid.
	if l.logDir.name != expected {
		l.logDir.Unlock()
		return errors.Errorf("unexpected logDir setting: set to %q, expected %q",
			l.logDir.name, expected)
	}
	l.logDir.name = newDir
	l.logDir.Unlock()

	// When we change the directory we close the current logging
	// output, so that a rotation to the new directory is forced on
	// the next logging event.
	return l.closeFileLocked()
}

func (l *loggerT) closeFileLocked() error {
	if l.mu.file != nil {
		if sb, ok := l.mu.file.(*syncBuffer); ok {
			if err := sb.file.Close(); err != nil {
				return err
			}
		}
		l.mu.file = nil
	}
	return restoreStderr()
}

func isDirEmpty(dirname string) (bool, error) {
	f, err := os.Open(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	list, err := f.Readdir(1)
	errClose := f.Close()
	if err != nil && err != io.EOF {
		return false, err
	}
	if errClose != nil {
		return false, errClose
	}
	return len(list) == 0, nil
}
