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
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type config struct {
	// showLogs reflects the use of -show-logs on the command line and is
	// used for testing.
	showLogs bool

	// syncWrites can be set asynchronously to force all file output to
	// synchronize to disk. This is set via SetSync() and used e.g. in
	// start.go upon encountering errors.
	syncWrites syncutil.AtomicBool

	// the --log-dir flag. This is the directory used for file loggers when
	// not overridden for a given logger.
	logDir DirName

	// Default value of logFileMaxSize for loggers.
	logFileMaxSize int64

	// Default value of logFilesCombinedMaxSize for loggers.
	logFilesCombinedMaxSize int64

	// Default value of fileThreshold for loggers.
	fileThreshold Severity

	// Default value of redactableLogs for loggers.
	redactableLogs bool

	// Whether redactable logs are requested.
	//
	// We use redactableLogsRequested instead of setting redactableLogs
	// above directly when parsing command-line flags, to prevent the
	// redactable flag from being set until
	// SetupRedactionAndStderrRedirects() has been called.
	//
	// This ensures that we don't mistakenly start producing redaction
	// markers until we have some confidence they won't be interleaved
	// with arbitrary writes to the stderr file descriptor.
	redactableLogsRequested bool
}

var debugLog *loggerT

// stderrLog is the logger where writes performed directly
// to the stderr file descriptor (such as that performed
// by the go runtime) *may* be redirected.
// NB: whether they are actually redirected is determined
// by stderrLog.redirectInternalStderrWrites().
var stderrLog *loggerT

func init() {
	// Default stderrThreshold and fileThreshold to log everything
	// both to the output file and to the process' external stderr
	// (OrigStderr).
	logging.fileThreshold = severity.INFO
	logging.stderrSink.threshold = severity.INFO
	logging.stderrSink.formatter = formatCrdbV1TTYWithCounter{}

	// Default maximum size of individual log files.
	logging.logFileMaxSize = 10 << 20 // 10MiB
	// Default combined size of a log file group.
	logging.logFilesCombinedMaxSize = logging.logFileMaxSize * 10 // 100MiB

	debugLog = &loggerT{}
	debugFileSink := newFileSink(
		"",    /* dir */
		"",    /* fileNamePrefix */
		false, /* forceSyncWrites */
		logging.fileThreshold,
		logging.logFileMaxSize,
		logging.logFilesCombinedMaxSize,
		debugLog.getStartLines,
	)
	// TODO(knz): Make all this configurable.
	// (As done in https://github.com/cockroachdb/cockroach/pull/51987.)
	debugLog.sinkInfos = []sinkInfo{
		{
			// Nb: some tests in this package assume that the stderr sink is
			// the first one. If this changes, the tests need to be updated
			// as well.
			sink: &logging.stderrSink,
			// stderr editor.
			// We don't redact upfront, and we keep the redaction markers.
			editor: getEditor(SelectEditMode(false /* redact */, true /* keepRedactable */)),
			// failure to write to stderr is critical for now. We may want
			// to make this non-critical in the future, since it's common
			// for folk to close the terminal where they launched 'cockroach
			// start --background'.
			// We keep this true for now for backward-compatibility.
			criticality: true,
		},
		{
			sink: debugFileSink,
			// file editor.
			// We don't redact upfront, and the "--redactable-logs" flag decides
			// whether to keep the redaction markers in the output.
			editor: getEditor(SelectEditMode(false /* redact */, logging.redactableLogs /* keepRedactable */)),
			// failure to write to file is definitely critical.
			criticality: true,
		},
	}
	allLoggers.put(debugLog)
	allFileSinks.put(debugFileSink)

	stderrLog = debugLog

	logflags.InitFlags(
		&logging.logDir,
		&logging.showLogs,
		&logging.stderrSink.noColor,
		&logging.redactableLogsRequested,
		&logging.vmoduleConfig.mu.vmodule,
		&logging.logFileMaxSize,
		&logging.logFilesCombinedMaxSize,
	)
	// We define these flags here because they have the type Severity
	// which we can't pass to logflags without creating an import cycle.
	flag.Var(&logging.stderrSink.threshold, logflags.LogToStderrName,
		"logs at or above this threshold go to stderr")
	flag.Var(&logging.fileThreshold, logflags.LogFileVerbosityThresholdName,
		"minimum verbosity of messages written to the log file")
}

// initDebugLogFromDefaultConfig initializes debugLog from the defaults
// in logging.config. This is called upon package initialization
// so that tests have a default config to work with; and also
// during SetupRedactionAndStderrRedirects() after the custom
// logging configuration has been selected.
func initDebugLogFromDefaultConfig() {
	for i := range debugLog.sinkInfos {
		fileSink, ok := debugLog.sinkInfos[i].sink.(*fileSink)
		if !ok {
			continue
		}
		// Re-configure the redaction editor. This may have changed
		// after SetupRedactionAndStderrRedirects() reconfigures
		// logging.redactableLogs.
		//
		// TODO(knz): Remove this initialization when
		// https://github.com/cockroachdb/cockroach/pull/51987 introduces
		// proper configurability.
		debugLog.sinkInfos[i].editor = getEditor(SelectEditMode(false /* redact */, logging.redactableLogs /* keepRedactable */))
		func() {
			fileSink.mu.Lock()
			defer fileSink.mu.Unlock()
			fileSink.prefix = program
			fileSink.mu.logDir = logging.logDir.String()
			fileSink.enabled.Set(fileSink.mu.logDir != "")
			fileSink.logFileMaxSize = logging.logFileMaxSize
			fileSink.logFilesCombinedMaxSize = logging.logFilesCombinedMaxSize
			fileSink.threshold = logging.fileThreshold
		}()
	}
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

	// Regardless of what happens below, we are going to set the
	// debugLog parameters from the outcome.
	defer initDebugLogFromDefaultConfig()

	if logging.logDir.IsSet() {
		// We have a log directory. We can enable stderr redirection.

		// Our own cancellable context to stop the secondary logger.
		//
		// Note: we don't want to take a cancellable context from the
		// caller, because in the usual case we don't want to stop the
		// logger when the remainder of the process stops. See the
		// discussion on cancel at the top of the function.
		ctx, cancel := context.WithCancel(context.Background())
		secLogger := NewSecondaryLogger(ctx, &logging.logDir, "stderr",
			true /* enableGC */, true /* forceSyncWrites */, false /* enableMsgCount */)
		fileSink := secLogger.logger.getFileSink()

		// Stderr capture produces unsafe strings. This logger
		// thus generally produces non-redactable entries.
		for i := range secLogger.logger.sinkInfos {
			secLogger.logger.sinkInfos[i].editor = getEditor(SelectEditMode(false /* redact */, false /* keepRedactable */))
		}

		// Force a log entry. This does two things: it forces
		// the creation of a file and introduces a timestamp marker
		// for any writes to the file performed after this point.
		secLogger.Logf(ctx, "stderr capture started")

		// Now tell this logger to capture internal stderr writes.
		if err := fileSink.takeOverInternalStderr(&secLogger.logger); err != nil {
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
			if err := secLogger.logger.getFileSink().relinquishInternalStderr(); err != nil {
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
		logging.redactableLogs = logging.redactableLogsRequested

		return cleanup, nil
	}

	// There is no log directory.

	// If redaction is requested and we have a chance to produce some
	// log entries on stderr, that's a configuration we cannot support
	// safely. Reject it.
	if logging.redactableLogsRequested && logging.stderrSink.threshold != severity.NONE {
		return nil, errors.WithHintf(
			errors.New("cannot enable redactable logging without a logging directory"),
			"You can pass --%s to set up a logging directory explicitly.", cliflags.LogDir.Name)
	}

	// Configuration valid. Assign it.
	// (Note: This is a no-op, because either redactableLogsRequested is false,
	// or it's true but stderrThreshold filters everything.)
	logging.redactableLogs = logging.redactableLogsRequested
	return nil, nil
}

// TestingResetActive clears the active bit. This is for use in tests
// that use stderr redirection alongside other tests that use
// logging.
func TestingResetActive() {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.mu.active = false
}

// RedactableLogsEnabled reports whether redaction markers were
// actually enabled. This is used for flag telemetry; a better API
// would be needed for more fine grained information, as each
// different logger may have a different redaction setting.
func RedactableLogsEnabled() bool {
	return logging.redactableLogs
}
