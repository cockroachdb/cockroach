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
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

// TestLogScope represents the lifetime of a logging output.  It
// ensures that the log files are stored in a directory specific to a
// test, and asserts that logging output is not written to this
// directory beyond the lifetime of the scope.
type TestLogScope struct {
	logDir          string
	stderrThreshold Severity
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

// Scope creates a TestLogScope which corresponds to the lifetime of a
// temporary logging directory. If -show-logs was passed on the
// command line, this is a no-op. Otherwise, it behaves
// like ScopeWithoutShowLogs().
//
// See the documentation of ScopeWithoutShowLogs() for API usage and
// restrictions.
func Scope(t tShim) *TestLogScope {
	if logging.showLogs {
		return (*TestLogScope)(nil)
	}

	scope := ScopeWithoutShowLogs(t)
	t.Log("use -show-logs to present logs inline")
	return scope
}

// ScopeWithoutShowLogs creates a TestLogScope which corresponds to
// the lifetime of a temporary logging directory. The logging
// directory is named after the calling test.
// ScopeWithoutShowLogs ignores the -show-logs flag and should be used
// for tests that require the logs go to files in any case.
//
// The TestLogScope redirects any log calls "under it" to the temporary
// directory.
// When the scope ends, the previous configuration is restored.
//
// ScopeWithoutShowLogs() is only valid if file output was not yet
// active. If it was, the test fails with an assertion error.
// The motivation for this restriction is to simplify reasoning by
// users of this facility: if a user has set up their code so that
// logging goes to files already, they are signaling that they want
// logging to go there and not elsewhere. In that case, it is
// undesirable to come along with a new random directory and take
// logging over there.
//
// ScopeWithoutShowLogs() is only valid if there were no secondary
// loggers already active. If there were, the test fails with an
// assertion error.
// The reason for this restriction is ease of implementation: to
// support TestLogScope "under" multiple loggers, we'd need to
// extend the implementation to save/restore the state of all the loggers,
// not just debugLog. This would be necessary because loggers don't
// necessarily have the same config. Until that is implemented,
// we prevent the use case altogether.
//
// ScopeWithoutShowLogs() does not enable redirection of internal
// stderr writes to files. Tests that wish to use that facility should
// call the other APIs in these package after setting up a
// TestLogScope.
func ScopeWithoutShowLogs(t tShim) (sc *TestLogScope) {
	// Refuse to work "under" secondary loggers (saving+restoring
	// state for secondary loggers is not implemented yet).
	func() {
		if allLoggers.len() > 1 {
			t.Fatal("can't use TestLogScope with secondary loggers active")
		}
	}()

	// The challenge of a log scope is that it needs to "scoop up" all
	// the logging output but then also restore the original
	// configuration at the end. What does "scooping up" mean here? and
	// what does "restore the original configuration" mean?
	//
	// - logging was not yet going to files and it must now go to files.
	//   This means configuring a log directory and ensuring files for all the loggers.
	//   => At the end, the log directory config must be reset and the log files closed.
	// - if logging was configured to also go to stderr via stderrThreshold, that must stop.
	//   => At the end, the stderrThreshold setting(s) must be restored.

	sc = &TestLogScope{
		// Remember the stderr threshold. Close() will restore it.
		stderrThreshold: logging.stderrSinkInfo.threshold.Get(),
	}
	defer func() {
		// If any of the following initialization fails, we close the scope.
		// We use the scope's Close() method as general-purpose finalizer,
		// to avoid writing a complex defer function here.
		if t.Failed() {
			sc.Close(t)
		}
	}()

	tempDir, err := ioutil.TempDir("", "log"+fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	// Remember the directory name for the Close() function.
	sc.logDir = tempDir

	// Make the main logger switch over to files, into the new temp
	// directory. The first argument "" asserts that file output was not
	// active yet. This enforces the invariant of the API.
	if err := dirTestOverride("", tempDir); err != nil {
		t.Fatal(err)
	}

	// Override the stderr threshold for the main logger.
	// From this point log entries do not show up on stderr any more;
	// they only go to files.
	logging.stderrSinkInfo.threshold.SetValue(severity.NONE)

	t.Logf("test logs captured to: %s", tempDir)
	return sc
}

// Rotate closes the current log files so that the next log call will
// reopen them with current settings. This is useful when e.g. a test
// changes the logging configuration after opening a test log scope.
func (l *TestLogScope) Rotate(t tShim) {
	// Ensure remaining logs are written.
	Flush()

	if err := allFileSinks.iter(func(l *fileSink) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.closeFileLocked()
	}); err != nil {
		t.Fatal(err)
	}
}

// restoreStderrThreshold restores the stderr output threshold at the end
// of a scope.
func (l *TestLogScope) restoreStderrThreshold() {
	logging.stderrSinkInfo.threshold.SetValue(l.stderrThreshold)
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted, unless the test has failed and the directory is non-empty.
func (l *TestLogScope) Close(t tShim) {
	if l == nil || l.logDir == "" {
		// Never initialized.
		return
	}

	// Ensure any remaining logs are written to files.
	Flush()

	// Restore the stderr threshold (which log events are copied
	// to external stderr).
	l.restoreStderrThreshold()

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
	if err := allLoggers.iter(func(l *loggerT) error {
		fileSink := l.getFileSink()
		if fileSink == nil {
			return nil
		}
		l.outputMu.Lock()
		defer l.outputMu.Unlock()
		return fileSink.dirTestOverride(expected, newDir)
	}); err != nil {
		return err
	}
	// Set the default also for any subsequently defined secondary
	// logger.
	_ = logging.logDir.Set(newDir)
	return nil
}

func (l *fileSink) dirTestOverride(expected, newDir string) error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// The following check is intended to catch concurrent uses of
	// Scope() or TestLogScope.Close(), which would be invalid.
	if l.mu.logDir != expected {
		return errors.Errorf("unexpected logDir setting: set to %q, expected %q",
			l.mu.logDir, expected)
	}
	l.mu.logDir = newDir
	l.enabled.Set(l.mu.logDir != "")

	// When we change the directory we close the current logging
	// output, so that a rotation to the new directory is forced on
	// the next logging event.
	return l.closeFileLocked()
}

func isDirEmpty(dirname string) (bool, error) {
	f, err := os.Open(dirname)
	if err != nil {
		if oserror.IsNotExist(err) {
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
