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

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors/oserror"
)

// TestLogScope represents the lifetime of a logging output.  It
// ensures that the log files are stored in a directory specific to a
// test, and enforces that logging output is not written to this
// directory beyond the lifetime of the scope.
type TestLogScope struct {
	logDir    string
	cleanupFn func()
	previous  struct {
		appliedConfig           string
		stderrSinkInfoTemplate  sinkInfo
		stderrSinkInfo          *sinkInfo
		channels                map[Channel]*loggerT
		debugLog                *loggerT
		testingFd2CaptureLogger *loggerT
		exitOverrideFn          func(exit.Code, error)
		exitOverrideHideStack   bool
	}
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
	Helper()
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
		return newLogScope(t, false /* use files */)
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
// ScopeWithoutShowLogs() does not enable redirection of internal
// stderr writes to files. Tests that wish to use that facility should
// call the other APIs in these package after setting up a
// TestLogScope.
func ScopeWithoutShowLogs(t tShim) (sc *TestLogScope) {
	t.Helper()
	return newLogScope(t, true /* use files */)
}

func newLogScope(t tShim, useFiles bool) (sc *TestLogScope) {
	t.Helper()
	sc = &TestLogScope{}

	// Remember a textual representation of the configuration.
	// We'll use this as a double-check that our save and restore
	// logic work properly.
	sc.previous.appliedConfig = DescribeAppliedConfig()

	sc.previous.stderrSinkInfoTemplate = logging.stderrSinkInfoTemplate
	logging.rmu.RLock()
	sc.previous.stderrSinkInfo = logging.rmu.currentStderrSinkInfo
	sc.previous.channels = logging.rmu.channels
	logging.rmu.RUnlock()
	sc.previous.debugLog = debugLog
	sc.previous.testingFd2CaptureLogger = logging.testingFd2CaptureLogger
	if cl := logging.testingFd2CaptureLogger; cl != nil {
		// Temporarily give up the previous internal fd2 capture. We'll set
		// up a new one with the new configuration below.
		// The original will be restored in the Close() function.
		if err := cl.getFileSink().relinquishInternalStderr(); err != nil {
			// This should not fail. If it does, some caller messed up by
			// switching over stderr redirection to a different file sink
			// without our involvement. That's invalid API usage.
			panic(err)
		}
	}
	logging.testingFd2CaptureLogger = nil
	logging.mu.Lock()
	sc.previous.exitOverrideFn = logging.mu.exitOverride.f
	sc.previous.exitOverrideHideStack = logging.mu.exitOverride.hideStack
	logging.mu.Unlock()

	defer func() {
		// If any of the following initialization fails, we close the scope.
		// We use the scope's Close() method as general-purpose finalizer,
		// to avoid writing a complex defer function here.
		if t.Failed() {
			sc.Close(t)
		}
	}()

	var fileDir *string
	if useFiles {
		tempDir, err := ioutil.TempDir("", "log"+fileutil.EscapeFilename(t.Name()))
		if err != nil {
			t.Fatal(err)
		}
		// Remember the directory name for the Close() function.
		sc.logDir = tempDir
		fileDir = &sc.logDir
	}

	// Obtain the standard test configuration, with the configured
	// destination directory.
	cfg, err := getTestConfig(fileDir)
	if err != nil {
		t.Fatal(err)
	}

	// Reset the server identifiers, so that new servers
	// can report their IDs through logging.
	TestingClearServerIdentifiers()

	// Switch to the new configuration.
	TestingResetActive()
	sc.cleanupFn, err = ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if useFiles {
		t.Logf("test logs captured to: %s", *fileDir)
	}
	return sc
}

// getTestConfig initialize the logging configuration to parameters
// suitable for use in tests.
func getTestConfig(fileDir *string) (testConfig logconfig.Config, err error) {
	testConfig = logconfig.DefaultConfig()

	if err := testConfig.Validate(fileDir); err != nil {
		return testConfig, err
	}

	if fileDir == nil {
		// File output is disabled; we use stderr for everything.

		// All messages go to stderr.
		testConfig.Sinks.Stderr.Filter = severity.INFO
		// Ensure all channels go to stderr.
		testConfig.Sinks.Stderr.Channels.Channels = logconfig.SelectAllChannels()
		// Remove all sinks other than stderr.
		testConfig.Sinks.FluentServers = nil
		testConfig.Sinks.FileGroups = nil
	} else {
		// Output to files enabled.

		// Even though we use file output, make all logged errors/fatal
		// calls go to the external stderr, in addition to the log file.
		testConfig.Sinks.Stderr.Filter = severity.ERROR
	}

	if skip.UnderBench() {
		// Avoid logging anything to stderr, to avoid polluting the output
		// of benchmarks. This is necessary because 'go test' unhelpfully
		// merges stdout and stderr writes together.
		//
		// This overrides any default set above.
		testConfig.Sinks.Stderr.Filter = severity.NONE
	}

	// Disable the internal fd2 capture to file, to ensure that panic
	// objects get reported to stderr.
	testConfig.CaptureFd2.Enable = false
	// Since we are letting writes go to the external stderr,
	// we cannot keep redaction markers there.
	*testConfig.Sinks.Stderr.Redactable = false

	return testConfig, nil
}

// GetDirectory retrieves the log directory for this scope.
func (l *TestLogScope) GetDirectory() string {
	return l.logDir
}

// Rotate closes the current log files so that the next log call will
// reopen them with current settings. This is useful when e.g. a test
// changes the logging configuration after opening a test log scope.
func (l *TestLogScope) Rotate(t tShim) {
	t.Helper()
	t.Logf("-- test log scope file rotation --")
	// Ensure remaining logs are written.
	Flush()

	if err := allSinkInfos.iterFileSinks(func(l *fileSink) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.closeFileLocked()
	}); err != nil {
		t.Fatal(err)
	}
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted, unless the test has failed and the directory is non-empty.
func (l *TestLogScope) Close(t tShim) {
	t.Helper()
	if l == nil {
		// Never initialized.
		return
	}
	t.Logf("-- test log scope end --")

	// Ensure any remaining logs are written to files.
	Flush()

	if l.logDir != "" {
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
	}

	if l.cleanupFn != nil {
		l.cleanupFn()
	}
	logging.stderrSinkInfoTemplate = l.previous.stderrSinkInfoTemplate
	logging.setChannelLoggers(l.previous.channels, l.previous.stderrSinkInfo)
	debugLog = l.previous.debugLog
	logging.testingFd2CaptureLogger = l.previous.testingFd2CaptureLogger
	if cl := logging.testingFd2CaptureLogger; cl != nil {
		if err := cl.getFileSink().takeOverInternalStderr(cl); err != nil {
			t.Error(err)
		}
	}
	logging.mu.Lock()
	logging.mu.exitOverride.f = l.previous.exitOverrideFn
	logging.mu.exitOverride.hideStack = l.previous.exitOverrideHideStack
	logging.mu.Unlock()

	// Sanity check: if the restore logic is complete, the applied
	// configuration should be the same as when the scope started.
	restoredConfig := DescribeAppliedConfig()
	if restoredConfig != l.previous.appliedConfig {
		t.Errorf(
			"bug in TestLogScope - previous config:\n%s\nafter restore:\n%s",
			l.previous.appliedConfig, restoredConfig)
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
