// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SecondaryLogger represents a secondary / auxiliary logging channel
// whose logging events go to a different file than the main logging
// facility.
type SecondaryLogger struct {
	logger          loggerT
	forceSyncWrites bool
}

var secondaryLogRegistry struct {
	mu struct {
		syncutil.Mutex
		loggers []*SecondaryLogger
	}
}

// NewSecondaryLogger creates a secondary logger.
//
// The given directory name can be either nil or empty, in which case
// the global logger's own dirName is used; or non-nil and non-empty,
// in which case it specifies the directory for that new logger.
//
// The logger's GC daemon stops when the provided context is canceled.
//
// The caller is responsible for ensuring the Close() method is
// eventually called.
func NewSecondaryLogger(
	ctx context.Context,
	dirName *DirName,
	fileNamePrefix string,
	enableGc bool,
	forceSyncWrites bool,
	enableMsgCount bool,
) *SecondaryLogger {
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	var dir string
	if dirName != nil {
		dir = dirName.String()
	}
	if dir == "" {
		dir = mainLog.logDir.String()
	}
	l := &SecondaryLogger{
		logger: loggerT{
			logDir:          DirName{name: dir},
			prefix:          program + "-" + fileNamePrefix,
			fileThreshold:   Severity_INFO,
			stderrThreshold: mainLog.stderrThreshold.get(),
			logCounter:      EntryCounter{EnableMsgCount: enableMsgCount},
			gcNotify:        make(chan struct{}, 1),
			// Only one logger can have redirectInternalStderrWrites set to
			// true; this is going to be either mainLog or stderrLog
			// depending on configuration.
			redirectInternalStderrWrites: false,
		},
		forceSyncWrites: forceSyncWrites,
	}
	l.logger.redactableLogs.Set(mainLog.redactableLogs.Get())
	l.logger.mu.syncWrites = forceSyncWrites || mainLog.mu.syncWrites

	// Ensure the registry knows about this logger.
	secondaryLogRegistry.mu.Lock()
	secondaryLogRegistry.mu.loggers = append(secondaryLogRegistry.mu.loggers, l)
	secondaryLogRegistry.mu.Unlock()

	if enableGc {
		// Start the log file GC for the secondary logger.
		go l.logger.gcDaemon(ctx)
	}

	return l
}

// Close implements the stopper.Closer interface.
func (l *SecondaryLogger) Close() {
	// Make the registry forget about this logger. This avoids
	// stacking many secondary loggers together when there are
	// subsequent tests starting servers in the same package.
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for i, thatLogger := range secondaryLogRegistry.mu.loggers {
		if thatLogger != l {
			continue
		}
		secondaryLogRegistry.mu.loggers = append(secondaryLogRegistry.mu.loggers[:i], secondaryLogRegistry.mu.loggers[i+1:]...)
		return
	}
}

func (l *SecondaryLogger) output(
	ctx context.Context, depth int, sev Severity, format string, args ...interface{},
) {
	entry := MakeEntry(
		ctx, sev, &l.logger.logCounter, depth+1, l.logger.redactableLogs.Get(), format, args...)
	l.logger.outputLogEntry(entry)
}

// Logf logs an event on a secondary logger.
func (l *SecondaryLogger) Logf(ctx context.Context, format string, args ...interface{}) {
	l.output(ctx, 1, Severity_INFO, format, args...)
}

// LogfDepth logs an event on a secondary logger, offsetting the caller's stack
// frame by 'depth'
func (l *SecondaryLogger) LogfDepth(
	ctx context.Context, depth int, format string, args ...interface{},
) {
	l.output(ctx, depth+1, Severity_INFO, format, args...)
}

// LogSev logs an event at the specified severity on a secondary logger.
func (l *SecondaryLogger) LogSev(ctx context.Context, sev Severity, args ...interface{}) {
	l.output(ctx, 1, Severity_INFO, "", args...)
}
