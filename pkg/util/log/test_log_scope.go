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
	"os"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
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

		allSinkInfos []*sinkInfo
		allLoggers   []*loggerT
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
		return newLogScope(t, true /* mostlyInline */)
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
	return newLogScope(t, false /* mostlyInline */)
}

func newLogScope(t tShim, mostlyInline bool) (sc *TestLogScope) {
	t.Helper()
	sc = &TestLogScope{}

	// Remember a textual representation of the configuration.
	// We'll use this as a double-check that our save and restore
	// logic work properly.
	sc.previous.appliedConfig = DescribeAppliedConfig()

	logging.allSinkInfos.mu.Lock()
	sc.previous.allSinkInfos = logging.allSinkInfos.mu.sinkInfos
	logging.allSinkInfos.mu.Unlock()
	logging.allLoggers.mu.Lock()
	sc.previous.allLoggers = logging.allLoggers.mu.loggers
	logging.allLoggers.mu.Unlock()

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

	err := func() error {
		tempDir, err := os.MkdirTemp("", "log"+fileutil.EscapeFilename(t.Name()))
		if err != nil {
			return err
		}
		// Remember the directory name for the Close() function.
		sc.logDir = tempDir

		// Obtain the standard test configuration, with the configured
		// destination directory.
		cfg := getTestConfig(&sc.logDir, mostlyInline)

		// Switch to the new configuration.
		TestingResetActive()
		sc.cleanupFn, err = ApplyConfig(cfg)
		if err != nil {
			return err
		}

		t.Logf("test logs captured to: %s", sc.logDir)
		return nil
	}()
	if err != nil {
		// If any of the initialization failed, we close the scope. We use the
		// scope's Close() method as general-purpose finalizer, to avoid writing
		// a more complex function here.
		sc.Close(t)
		t.Fatal(err)
	}

	return sc
}

// getTestConfig initialize the logging configuration to parameters
// suitable for use in tests.
//
// fileDir is nil when the function is called without an output
// directory, to construct the default log config upon init().
// mostlyInline is true when -show-logs is specified.
func getTestConfig(fileDir *string, mostlyInline bool) (testConfig logconfig.Config) {
	testConfig = logconfig.DefaultConfig()

	forcePanicsToStderr := func(c *logconfig.Config) {
		// Disable the internal fd2 capture to file, to ensure that panic
		// objects get reported to stderr.
		c.CaptureFd2.Enable = false
		// Since we are letting fd2 writes go to the external stderr,
		// we cannot keep redaction markers there.
		*c.Sinks.Stderr.Redactable = false
	}

	if logging.testLogConfig != "" {
		// The person running the test has requested a custom logging configuration.
		// Use their choice.
		h := logconfig.Holder{Config: testConfig}
		if err := h.Set(logging.testLogConfig); err != nil {
			panic(errors.Wrap(err, "error in test log config spec"))
		}

		forcePanicsToStderr(&h.Config)

		// Validate the configuration. This also ensures that if the
		// user's choice does not set a target directory, we use the
		// test-specific target directory instead.
		if err := h.Config.Validate(fileDir); err != nil {
			panic(errors.Wrap(err, "error in test log config spec"))
		}

		return h.Config
	}

	if fileDir == nil {
		// File output is disabled; we use stderr for everything.
		// This happens e.g. when a test is not using log.Scope().

		// All messages go to stderr.
		testConfig.Sinks.Stderr.Filter = severity.INFO
		// Ensure all channels go to stderr.
		testConfig.Sinks.Stderr.Channels = logconfig.SelectChannels(logconfig.AllChannels()...)
		// Remove all sinks other than stderr.
		testConfig.Sinks.FluentServers = nil
		testConfig.Sinks.FileGroups = nil
	} else {
		// Output to files enabled.

		// We have two desired cases: -show-logs (mostlyInline = true)
		// and without -show-logs (default, mostlyInline = false).

		// In both cases, we emit output to files, with the STORAGE, HEALTH, and
		// KV_DISTRIBUTION channels split into separate files.  We do keep a copy
		// of HEALTH, STORAGE, and KV_DISTRIBUTION warning and errors on the main
		// file though.
		testConfig.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
			"storage": {
				Channels: logconfig.SelectChannels(channel.STORAGE),
			},
			"health": {
				Channels: logconfig.SelectChannels(channel.HEALTH),
			},
			"kv-distribution": {
				Channels: logconfig.SelectChannels(channel.KV_DISTRIBUTION),
			},
			// Also add the WARNING+ events from the separated channels into the
			// default file group.
			"default": {
				Channels: logconfig.ChannelFilters{
					Filters: map[logpb.Severity]logconfig.ChannelList{
						severity.WARNING: {Channels: []logpb.Channel{channel.STORAGE, channel.HEALTH,
							channel.KV_DISTRIBUTION}},
					},
				},
			},
		}

		// Then we need to choose what to do with the stderr sink.
		if mostlyInline {
			// User requested -show-logs.
			//
			// Include everything on stderr except STORAGE, HEALTH, and
			// KV_DISTRIBUTION at severity below WARNING.
			testConfig.Sinks.Stderr.Filter = severity.INFO
			testConfig.Sinks.Stderr.Channels.Filters = map[logpb.Severity]logconfig.ChannelList{
				logpb.Severity_INFO: {Channels: selectAllChannelsExceptSeparated()},
				logpb.Severity_WARNING: {Channels: []logpb.Channel{channel.HEALTH, channel.STORAGE,
					channel.KV_DISTRIBUTION}},
			}
		} else {
			// Preferring file output only.
			//
			// Even though we use file output, make all logged fatal
			// calls go to the external stderr, in addition to the log file.
			//
			// We want FATAL messages because those don't pertain to a
			// specific test, but rather to the whole package / go test
			// invocation because they stop the execution. They elucidate
			// why the 'go test' invocation fails prematurely.
			testConfig.Sinks.Stderr.Filter = severity.FATAL
		}
	}

	if skip.UnderBench() {
		// Avoid logging anything to stderr, to avoid polluting the output
		// of benchmarks. This is necessary because 'go test' unhelpfully
		// merges stdout and stderr writes together.
		//
		// This overrides any default set above.
		testConfig.Sinks.Stderr.Filter = severity.NONE
	}

	forcePanicsToStderr(&testConfig)

	if err := testConfig.Validate(fileDir); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "error in predefined test log config"))
	}
	return testConfig
}

func selectAllChannelsExceptSeparated() []logpb.Channel {
	res := logconfig.AllChannels()
	k := 0
	for _, ch := range res {
		if ch == channel.HEALTH || ch == channel.STORAGE || ch == channel.KV_DISTRIBUTION {
			continue
		}
		res[k] = ch
		k++
	}
	res = res[:k]
	return res
}

// SetupSingleFileLogging modifies the configuration of the Scope
// to ensure all the logging output gets redirected to one file.
// By default, the logging output in tests gets split into
// multiple files to avoid noise on the main logging output.
//
// This facility exists for tests like TestStatusLogsAPI which
// inspect/assert the number of generated log files.
//
// Note: this is a no-op when the scope is currently redirected to
// stderr, e.g. when instantiated via log.Scope() and run with
// -show-logs.
// To ensure that output always goes to one file, use
// log.ScopeWithoutShowLogs().
func (l *TestLogScope) SetupSingleFileLogging() (cleanup func()) {
	if l.logDir == "" {
		// No log directory: no-op.
		return func() {}
	}

	// Set up a logging configuration with just one file sink.
	cfg := logconfig.DefaultConfig()
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"default": {
			Channels: logconfig.SelectChannels(logconfig.AllChannels()...)},
	}
	// Disable the -stderr.log output so there's really just 1 file.
	cfg.CaptureFd2.Enable = false

	// Derive a full config using the same directory as the
	// TestLogScope.
	if err := cfg.Validate(&l.logDir); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error in predefined log config"))
	}

	// Apply the configuration.
	TestingResetActive()
	cleanup, err := ApplyConfig(cfg)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error in predefined log config"))
	}
	return cleanup
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

	if err := logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
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
					fmt.Fprint(OrigStderr, "\nERROR: a panic has occurred!\n"+
						"Details cannot be printed yet because we are still unwinding.\n"+
						"Hopefully the test harness prints the panic below, otherwise check the test logs.\n\n")
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

	logging.allSinkInfos.mu.Lock()
	logging.allSinkInfos.mu.sinkInfos = l.previous.allSinkInfos
	logging.allSinkInfos.mu.Unlock()
	logging.allLoggers.mu.Lock()
	logging.allLoggers.mu.loggers = l.previous.allLoggers
	logging.allLoggers.mu.Unlock()

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
