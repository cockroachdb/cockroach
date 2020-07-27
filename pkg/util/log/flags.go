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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type config struct {
	// the --no-color flag. When set it disables escapes code on the
	// stderr copy.
	noColor bool

	// showLogs reflects the use of -show-logs on the command line and is
	// used for testing.
	showLogs bool

	// Level at or beyond which entries submitted to any logger are
	// written to the process' external standard error stream
	// (OrigStderr).  This acts as a filter between the log entry
	// producers and the stderr sink.
	stderrThreshold Severity

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
	logging.stderrThreshold = severity.INFO
	logging.fileThreshold = severity.INFO
	logging.stderrFormatter = formatCrdbV1TTYWithCounter{}

	// Default maximum size of individual log files.
	logging.logFileMaxSize = 10 << 20 // 10MiB
	// Default combined size of a log file group.
	logging.logFilesCombinedMaxSize = logging.logFileMaxSize * 10 // 100MiB

	debugLog = &loggerT{}
	debugLog.fileSink = newFileSink(
		"", /* dir */
		"", /* fileNamePrefix */
		logging.fileThreshold,
		logging.logFileMaxSize,
		logging.logFilesCombinedMaxSize,
		debugLog.getStartLines)
	registry.put(debugLog)

	stderrLog = debugLog

	logflags.InitFlags(
		&logging.logDir,
		&logging.showLogs,
		&logging.noColor,
		&logging.redactableLogsRequested,
		&logging.vmoduleConfig.mu.vmodule,
		&logging.logFileMaxSize,
		&logging.logFilesCombinedMaxSize,
	)
	// We define these flags here because they have the type Severity
	// which we can't pass to logflags without creating an import cycle.
	flag.Var(&logging.stderrThreshold, logflags.LogToStderrName,
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
	debugLog.fileSink.mu.Lock()
	defer debugLog.fileSink.mu.Unlock()
	debugLog.fileSink.prefix = program
	debugLog.fileSink.mu.logDir = logging.logDir.String()
	debugLog.fileSink.enabled.Set(debugLog.fileSink.mu.logDir != "")
	debugLog.fileSink.logFileMaxSize = logging.logFileMaxSize
	debugLog.fileSink.logFilesCombinedMaxSize = logging.logFilesCombinedMaxSize
	debugLog.fileSink.fileThreshold = logging.fileThreshold
	debugLog.redactableLogs.Set(logging.redactableLogs)
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

// auditChannels indicate which channels are to be configured
// as "audit" logs: with event numbering and with synchronous writes.
// Audit logs are configured this way to ensure non-repudiability.
//
// TODO(knz): make this flag configurable per channel.
var auditChannels = map[Channel]bool{
	channel.SESSIONS:         true,
	channel.USER_ADMIN:       true,
	channel.PRIVILEGES:       true,
	channel.SENSITIVE_ACCESS: true,
}

// enableBackwardCompatibleLogFileNames can be set to true to name
// secondary loggers in a way compatible with v20.1 and prior.
//
// When set to false, the log files are named after the channel
// names. In a later revision, we will make the file names
// configurable.
//
// This configuration flag exists so that the channel functionality
// becomes back-portable to v20.1 and prior.
const enableBackwardCompatibleLogFileNames = false

var backwardCompatibleLogFileNames = map[Channel]string{
	channel.SESSIONS:          "sql-auth",
	channel.SENSITIVE_ACCESS:  "sql-audit",
	channel.SQL_EXEC:          "sql-exec",
	channel.SQL_PERF:          "sql-slow",
	channel.SQL_INTERNAL_PERF: "sql-slow-internal-only",
}

// getChannelFilenamePrefix returns the secondary logger filename
// prefix to use for a given channel. For example, the file name
// prefix for the OPS channel is `ops` so that the generated file name
// will be `cockroach-ops.log`.
//
// If getChannelFilenamePrefix is true above, then special cases are
// implemented for certain channels, to ensure that the filenames are
// generated in the same way as previous versions of CockroachDB.
func getChannelFilenamePrefix(ch Channel) string {
	if enableBackwardCompatibleLogFileNames {
		if prefix, ok := backwardCompatibleLogFileNames[ch]; ok {
			return prefix
		}
	}
	chName, ok := logpb.Channel_name[int32(ch)]
	if !ok {
		panic(errors.Newf("invalid channel: %v", ch))
	}
	return strings.ReplaceAll(strings.ToLower(chName), "_", "-")
}

// DefaultConfig is the default logging configuration.
var DefaultConfig = func() *logconfig.Config {
	c := logconfig.DefaultConfig()
	return &c
}()

// SetupRedactionAndLoggingChannels should be called once after
// command-line flags have been parsed, and before the first log entry
// is emitted.
//
// The returned cleanup fn can be invoked by the caller to close
// the non-DEV channels and resume the default of collapsing all
// the logging output to the DEV channel.
// This is only useful in tests: for a long-running server process the
// cleanup function should likely not be called, to ensure that the
// file used to capture direct stderr writes remains open up until the
// process entirely terminates. This ensures that any Go runtime
// assertion failures on the way to termination can be properly
// captured.
func SetupRedactionAndLoggingChannels(config *logconfig.Config) (cleanupFn func(), err error) {
	// The general goal of this function is to set up secondary loggers
	// for the various logical channel, and also a special stderr logger
	// to capture internal Go writes to os.Stderr / fd 2 and redirect
	// them to a separate (non-redactable) log file.
	//
	// The stderr logger is, of course, only possible if there is a log
	// directory to work with -- until we extend the log package to use
	// e.g. network sinks.
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

	// TODO(knz): Change the configuration logic.
	// for each directive:
	//    1. instantiate a logger with that directive's config
	//    2. for each channel in the directive
	//           (temporarily) if there is already a logger for this channel, error out
	//           attach the logger to the channel channels[ch] = logger
	//    3. is there a channel without a logger yet? if yes:
	//           build a default shared logger
	//           for each channel:
	//               if there is a logger override, apply it
	//               otherwise, connect to the shared logger

	hasLogDirectory := logging.logDir.IsSet()

	if !hasLogDirectory {
		// If redaction is requested and we have a chance to produce some
		// log entries on stderr, that's a configuration we cannot support
		// safely. Reject it.
		if logging.redactableLogsRequested && logging.stderrThreshold != severity.NONE {
			return nil, errors.WithHintf(
				errors.New("cannot enable redactable logging without a logging directory"),
				"You can pass --%s to set up a logging directory explicitly.", cliflags.LogDir.Name)
		}
	}

	// Our own cancellable context to stop the secondary loggers below.
	//
	// Note: we don't want to take a cancellable context from the
	// caller, because in the usual case we don't want to stop the
	// logger when the remainder of the process stops. See the
	// discussion on cancel at the top of the function.
	secLoggersCtx, secLoggersCancel := context.WithCancel(context.Background())

	var secLoggers []*secondaryLogger
	var stderrCleanupFn func()
	cleanupFn = func() {
		// Reset the logging channels to default.
		channels = make(map[Channel]channelSink)

		if stderrCleanupFn != nil {
			// Restore the stderr redirect, if it was defined (see below).
			stderrCleanupFn()
		}
		// Stop the GC processes.
		secLoggersCancel()
		// Clean up the loggers.
		for _, l := range secLoggers {
			l.Close()
		}
	}

	// Regardless of what happens below, we are going to set the
	// debugLog parameters from the outcome.
	defer initDebugLogFromDefaultConfig()

	if hasLogDirectory {
		// We have a log directory. We can enable stderr redirection.
		secLogger := newSecondaryLogger(secLoggersCtx, &logging.logDir, "stderr",
			true /* enableGC */, true /* forceSyncWrites */, false /* enableMsgCount */)
		secLoggers = append(secLoggers, secLogger)

		// Stderr capture produces unsafe strings. This logger
		// thus generally produces non-redactable entries.
		secLogger.logger.redactableLogs.Set(false)

		// Force a log entry. This does two things: it forces
		// the creation of a file and the redirection of fd 2 / os.Stderr.
		// It also introduces a timestamp marker.
		secLogger.output(secLoggersCtx, 0, severity.INFO, channel.DEV, "stderr capture started")

		// Now tell this logger to capture internal stderr writes.
		if err := secLogger.logger.fileSink.takeOverInternalStderr(&secLogger.logger); err != nil {
			// Oof, it turns out we can't use this logger after all. Give up
			// on everything we did.
			cleanupFn()
			return nil, err
		}

		// Now inform the other functions using stderrLog that we
		// have a new logger for it.
		prevStderrLogger := stderrLog
		stderrLog = &secLogger.logger

		// The cleanup fn is for use in tests.
		stderrCleanupFn = func() {
			// Relinquish the stderr redirect.
			if err := secLogger.logger.fileSink.relinquishInternalStderr(); err != nil {
				// This should not fail. If it does, some caller messed up by
				// switching over stderr redirection to a different logger
				// without our involvement. That's invalid API usage.
				panic(err)
			}

			// Restore the apparent stderr logger used by Shout() and tests.
			stderrLog = prevStderrLogger

			// Note: the remainder of the code in cleanupFn()
			// will remove the logger and close it. No need to also do it here.
		}

		// Now that stderr is properly redirected, we can enable log file
		// redaction as requested. It is safe because no interleaving
		// is possible any more.
		logging.redactableLogs = logging.redactableLogsRequested
	}

	// Set up the default channel loggers.
	// TODO(knz): make this more configurable.
	for chi := range logpb.Channel_name {
		ch := Channel(chi)
		if ch == channel.DEV {
			// DEV uses mainLog for now.
			continue
		}
		isAuditLog := auditChannels[ch]
		fileNamePrefix := getChannelFilenamePrefix(Channel(ch))
		secLogger := newSecondaryLogger(secLoggersCtx, &logging.logDir, fileNamePrefix,
			true /* enableGC */, isAuditLog /* forceSyncWrites */, isAuditLog /* enableMsgCount */)

		secLoggers = append(secLoggers, secLogger)
		channels[ch] = channelSink{logger: secLogger}
	}

	if err := applyConfiguration(config); err != nil {
		cleanupFn()
		return nil, err
	}
	return cleanupFn, nil
}

func applyConfiguration(config *logconfig.Config) error {
	return nil
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
