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
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SecondaryLogger represents a secondary / auxiliary logging channel
// whose logging events go to a different file than the main logging
// facility.
type SecondaryLogger struct {
	logger          loggingT
	msgCount        uint64
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
func NewSecondaryLogger(
	ctx context.Context, dirName *DirName, fileNamePrefix string, enableGc, forceSyncWrites bool,
) *SecondaryLogger {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	var dir string
	if dirName != nil {
		dir = dirName.String()
	}
	if dir == "" {
		dir = logging.logDir.String()
	}
	l := &SecondaryLogger{
		logger: loggingT{
			logDir:           DirName{name: dir},
			noStderrRedirect: true,
			prefix:           program + "-" + fileNamePrefix,
			stderrThreshold:  logging.stderrThreshold,
			fileThreshold:    Severity_INFO,
			syncWrites:       forceSyncWrites || logging.syncWrites,
			gcNotify:         make(chan struct{}, 1),
			disableDaemons:   logging.disableDaemons,
		},
		forceSyncWrites: forceSyncWrites,
	}

	// Ensure the registry knows about this logger.
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	secondaryLogRegistry.mu.loggers = append(secondaryLogRegistry.mu.loggers, l)

	if enableGc {
		// Start the log file GC for the secondary logger.
		go l.logger.gcDaemon(ctx)
	}

	return l
}

// Logf logs an event on a secondary logger.
func (l *SecondaryLogger) Logf(ctx context.Context, format string, args ...interface{}) {
	file, line, _ := caller.Lookup(1)
	var buf strings.Builder
	formatTags(ctx, &buf)

	// Add a counter. This is important for auditing.
	counter := atomic.AddUint64(&l.msgCount, 1)
	fmt.Fprintf(&buf, "%d ", counter)

	fmt.Fprintf(&buf, format, args...)
	l.logger.outputLogEntry(Severity_INFO, file, line, buf.String())
}
