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

	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
)

// SecondaryLogger represents a secondary / auxiliary logging channel
// whose logging events go to a different file than the main logging
// facility.
type SecondaryLogger struct {
	logger loggerT
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
	// Any consumption of configuration off the main logger
	// makes the logging module "active" and prevents further
	// configuration changes.
	setActive()

	var dir string
	if dirName != nil {
		dir = dirName.String()
	}
	if dir == "" {
		dir = logging.logDir.String()
	}
	l := &SecondaryLogger{
		logger: loggerT{},
	}
	// TODO(knz): Make all this configurable.
	// (As done in https://github.com/cockroachdb/cockroach/pull/51987.)
	fileSink := newFileSink(
		dir,
		fileNamePrefix,
		forceSyncWrites,
		logging.logFileMaxSize,
		logging.logFilesCombinedMaxSize,
		l.logger.getStartLines,
	)
	fileSinkInfo := &sinkInfo{
		sink:      fileSink,
		threshold: severity.INFO,
		formatter: formatCrdbV1{},
		// file editor.
		// We don't redact upfront, and the "--redactable-logs" flag decides
		// whether to keep the redaction markers in the output.
		editor: getEditor(SelectEditMode(false /* redact */, logging.redactableLogs /* keepRedactable */)),
		// failure to write to file is definitely critical.
		criticality: true,
	}
	if enableMsgCount {
		fileSinkInfo.formatter = formatCrdbV1WithCounter{}
	}
	l.logger.sinkInfos = []*sinkInfo{
		&logging.stderrSinkInfo,
		fileSinkInfo,
	}

	// Ensure the registry knows about this logger.
	allFileSinks.put(fileSink)
	allLoggers.put(&l.logger)

	if enableGc {
		// Start the log file GC for the secondary logger.
		go fileSink.gcDaemon(ctx)
	}

	return l
}

// Close implements the stopper.Closer interface.
func (l *SecondaryLogger) Close() {
	if fileSink := l.logger.getFileSink(); fileSink != nil {
		allFileSinks.del(fileSink)
	}
	allLoggers.del(&l.logger)
}

func (l *SecondaryLogger) output(
	ctx context.Context, depth int, sev Severity, format string, args ...interface{},
) {
	entry := MakeEntry(
		ctx, sev, depth+1, true /* redactable */, format, args...)
	l.logger.outputLogEntry(entry)
}

// Logf logs an event on a secondary logger.
func (l *SecondaryLogger) Logf(ctx context.Context, format string, args ...interface{}) {
	l.output(ctx, 1, severity.INFO, format, args...)
}

// LogfDepth logs an event on a secondary logger, offsetting the caller's stack
// frame by 'depth'
func (l *SecondaryLogger) LogfDepth(
	ctx context.Context, depth int, format string, args ...interface{},
) {
	l.output(ctx, depth+1, severity.INFO, format, args...)
}
