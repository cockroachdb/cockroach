// Copyright 2015 The Cockroach Authors.
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
	"flag"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/errors"
)

func init() {
	logflags.InitFlags(
		&mainLog.logDir,
		&showLogs,
		&logging.noColor,
		&redactableLogsRequested, // NB: see doc on the variable definition.
		&logging.vmoduleConfig.mu.vmodule,
		&LogFileMaxSize, &LogFilesCombinedMaxSize,
	)
	// We define these flags here because they have the type Severity
	// which we can't pass to logflags without creating an import cycle.
	flag.Var(&mainLog.stderrThreshold,
		logflags.LogToStderrName, "logs at or above this threshold go to stderr")
	flag.Var(&mainLog.fileThreshold,
		logflags.LogFileVerbosityThresholdName, "minimum verbosity of messages written to the log file")
}

// IsActive returns true iff the main logger already has some events
// logged, or some secondary logger was created with configuration
// taken from the main logger.
//
// This is used to assert that configuration is performed
// before logging has been used for the first time.
func IsActive() (active bool, firstUse string) {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	return logging.mu.active, logging.mu.firstUseStack
}

// SetupRedactionAndStderrRedirects should be called once after
// command-line flags have been parsed, and before the first log entry
// is emitted.
//
// The returned cleanup fn can be invoked by the caller to terminate
// the secondary logger. This is only useful in tests: for a
// long-running server process the cleanup function should likely not
// be called, to ensure that the file used to capture direct stderr
// writes remains open up until the process entirely terminates. This
// ensures that any Go runtime assertion failures on the way to
// termination can be properly captured.
func SetupRedactionAndStderrRedirects() (cleanupForTestingOnly func(), err error) {
	// The general goal of this function is to set up a secondary logger
	// to capture internal Go writes to os.Stderr / fd 2 and redirect
	// them to a separate (non-redactable) log file, This is, of course,
	// only possible if there is a log directory to work with -- until
	// we extend the log package to use e.g. network sinks.
	//
	// In case there is no log directory, we must be careful to not
	// enable log redaction whatsoever.
	//
	// This is because otherwise, it is possible for some direct writer
	// to fd 2, for example the Go runtime when processing an internal
	// assertion error, to interleave its writes going to stderr
	// together with a logger that wants to insert log redaction markers
	// also on stderr. Because the log code cannot control this
	// interleaving, it cannot guarantee that the direct fd 2 writes
	// won't be positioned outside of log redaction markers and
	// mistakenly considered as "safe for reporting".

	// Sanity check.
	if active, firstUse := IsActive(); active {
		panic(errors.Newf("logging already active; first use:\n%s", firstUse))
	}

	if mainLog.logDir.IsSet() {
		// We have a log directory. We can enable stderr redirection.

		// Our own cancellable context to stop the secondary logger.
		//
		// Note: we don't want to take a cancellable context from the
		// caller, because in the usual case we don't want to stop the
		// logger when the remainder of the process stops. See the
		// discussion on cancel at the top of the function.
		ctx, cancel := context.WithCancel(context.Background())
		secLogger := NewSecondaryLogger(ctx, &mainLog.logDir, "stderr",
			true /* enableGC */, true /* forceSyncWrites */, false /* enableMsgCount */)

		// Stderr capture produces unsafe strings. This logger
		// thus generally produces non-redactable entries.
		secLogger.logger.redactableLogs.Set(false)

		// Force a log entry. This does two things: it forces
		// the creation of a file and introduces a timestamp marker
		// for any writes to the file performed after this point.
		secLogger.Logf(ctx, "stderr capture started")

		// Now tell this logger to capture internal stderr writes.
		if err := secLogger.logger.takeOverInternalStderr(); err != nil {
			// Oof, it turns out we can't use this logger after all. Give up
			// on it.
			cancel()
			secLogger.Close()
			return nil, err
		}

		// Now inform the other functions using stderrLog that we
		// have a new logger for it.
		prevStderrLogger := stderrLog
		stderrLog = &secLogger.logger

		// The cleanup fn is for use in tests.
		cleanup := func() {
			// Relinquish the stderr redirect.
			if err := secLogger.logger.relinquishInternalStderr(); err != nil {
				// This should not fail. If it does, some caller messed up by
				// switching over stderr redirection to a different logger
				// without our involvement. That's invalid API usage.
				panic(err)
			}

			// Restore the apparent stderr logger used by Shout() and tests.
			stderrLog = prevStderrLogger

			// Cancel the gc process for the secondary logger.
			cancel()

			// Close the logger.
			secLogger.Close()
		}

		// Now that stderr is properly redirected, we can enable log file
		// redaction as requested. It is safe because no interleaving
		// is possible any more.
		mainLog.redactableLogs.Set(redactableLogsRequested)

		return cleanup, nil
	}

	// There is no log directory.

	// If redaction is requested and we have a chance to produce some
	// log entries on stderr, that's a configuration we cannot support
	// safely. Reject it.
	if redactableLogsRequested && mainLog.stderrThreshold.get() != Severity_NONE {
		return nil, errors.WithHintf(
			errors.New("cannot enable redactable logging without a logging directory"),
			"You can pass --%s to set up a logging directory explicitly.", cliflags.LogDir.Name)
	}

	// Configuration valid. Assign it.
	// (Note: This is a no-op, because either redactableLogsRequested is false,
	// or it's true but stderrThreshold filters everything.)
	mainLog.redactableLogs.Set(redactableLogsRequested)
	return nil, nil
}

// We use redactableLogsRequested instead of mainLog.redactableLogs
// directly when parsing command-line flags, to prevent the redactable
// flag from being set until SetupRedactionAndStderrRedirects() has
// been called.
//
// This ensures that we don't mistakenly start producing redaction
// markers until we have some confidence they won't be interleaved
// with arbitrary writes to the stderr file descriptor.
var redactableLogsRequested bool

// TestingResetActive clears the active bit. This is for use in tests
// that use stderr redirection alongside other tests that use
// logging.
func TestingResetActive() {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.mu.active = false
}

// RedactableLogsEnabled reports whether redaction markers were
// actually enabled for the main logger. This is used for flag
// telemetry; a better API would be needed for more fine grained
// information, as each different logger may have a different
// redaction setting.
func RedactableLogsEnabled() bool {
	return mainLog.redactableLogs.Get()
}
