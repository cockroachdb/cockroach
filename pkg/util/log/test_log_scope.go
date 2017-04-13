// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
)

// TestLogScope represents the lifetime of a logging output.  It
// ensures that the log files are stored in a directory specific to a
// test, and asserts that logging output is not written to this
// directory beyond the lifetime of the scope.
type TestLogScope struct {
	logDir   string
	keepLogs bool
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

var showLogs bool

// Scope creates a TestLogScope which corresponds to the lifetime of a logging
// directory. The logging directory is named after the calling test. It also
// disables logging to stderr for severity levels below ERROR.
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
	// Subtests are slash-delimited, which can confuse things when slash is also
	// the path separator.
	tempDir, err := ioutil.TempDir("", "log"+strings.Replace(t.Name(), "/", "_", -1))
	if err != nil {
		t.Fatal(err)
	}
	if err := dirTestOverride(tempDir); err != nil {
		t.Fatal(err)
	}
	if err := enableLogFileOutput(tempDir, Severity_ERROR); err != nil {
		t.Fatal(err)
	}
	t.Logf("test logs captured to: %s", tempDir)
	return &TestLogScope{logDir: tempDir}
}

// enableLogFileOutput turns on logging using the specified directory.
// For unittesting only.
func enableLogFileOutput(dir string, stderrSeverity Severity) error {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.stderrThreshold = stderrSeverity
	return logDir.Set(dir)
}

// KeepLogs can be used to control whether the log files get removed by Close().
// Log files are always kept if the test Failed() or panicked, but this doesn't
// cover everything (e.g. race detector). Recommended usage:
//
//   defer l.Close(t)
//   l.KeepLogs(true)
//
//   .. test code ..
//
//   // If we got this far, we don't need to keep logs (unless the test fails).
//   l.KeepLogs(false)
func (l *TestLogScope) KeepLogs(keep bool) {
	if l != nil {
		l.keepLogs = true
	}
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted, unless the test has failed and the directory is non-empty.
func (l *TestLogScope) Close(t tShim) {
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
		} else if !l.keepLogs {
			// Clean up.
			if err := os.RemoveAll(l.logDir); err != nil {
				t.Error(err)
			}
		}
	}()
	// Flush/Close the log files.
	if err := dirTestOverride(""); err != nil {
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
func dirTestOverride(dir string) error {
	// Ensure any remaining logs are written.
	Flush()

	logDir.Lock()
	logDir.name = dir
	logDir.Unlock()

	// When we change the directory we close the current logging
	// output, so that a rotation to the new directory is forced on
	// the next logging event.
	logging.mu.Lock()
	err := logging.closeFileLocked()
	logging.mu.Unlock()

	return err
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
