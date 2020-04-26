// Copyright 2019 The Cockroach Authors.
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
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

// flushSyncWriter is the interface satisfied by logging destinations.
type flushSyncWriter interface {
	Flush() error
	Sync() error
	io.Writer
}

// Flush explicitly flushes all pending log I/O.
// See also flushDaemon() that manages background (asynchronous)
// flushes, and signalFlusher() that manages flushes in reaction to a
// user signal.
func Flush() {
	mainLog.lockAndFlushAndSync(true /*doSync*/)
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, l := range secondaryLogRegistry.mu.loggers {
		// Some loggers (e.g. the audit log) want to keep all the files.
		l.logger.lockAndFlushAndSync(true /*doSync*/)
	}
}

func init() {
	go flushDaemon()
	go signalFlusher()
}

// flushInterval is the delay between periodic flushes of the buffered log data.
const flushInterval = time.Second

// syncInterval is the multiple of flushInterval where the log is also synced to disk.
const syncInterval = 30

// maxSyncDuration is set to a conservative value since this is a new mechanism.
// In practice, even a fraction of that would indicate a problem.
var maxSyncDuration = envutil.EnvOrDefaultDuration("COCKROACH_LOG_MAX_SYNC_DURATION", 30*time.Second)

// flushDaemon periodically flushes and syncs the log file buffers.
// This manages both the primary and secondary loggers.
//
// Flush propagates the in-memory buffer inside CockroachDB to the
// in-memory buffer(s) of the OS. The flush is relatively frequent so
// that a human operator can see "up to date" logging data in the log
// file.
//
// Syncs ensure that the OS commits the data to disk. Syncs are less
// frequent because they can incur more significant I/O costs.
func flushDaemon() {
	syncCounter := 1

	// This doesn't need to be Stop()'d as the loop never escapes.
	for range time.Tick(flushInterval) {
		doSync := syncCounter == syncInterval
		syncCounter = (syncCounter + 1) % syncInterval

		// Is flushing disabled?
		logging.mu.Lock()
		disableDaemons := logging.mu.disableDaemons
		logging.mu.Unlock()

		// Flush the main log.
		if !disableDaemons {
			mainLog.lockAndFlushAndSync(doSync)

			// Flush the secondary logs.
			secondaryLogRegistry.mu.Lock()
			for _, l := range secondaryLogRegistry.mu.loggers {
				l.logger.lockAndFlushAndSync(doSync)
			}
			secondaryLogRegistry.mu.Unlock()
		}
	}
}

// signalFlusher flushes the log(s) every time SIGHUP is received.
// This handles both the primary and secondary loggers.
func signalFlusher() {
	ch := sysutil.RefreshSignaledChan()
	for sig := range ch {
		Infof(context.Background(), "%s received, flushing logs", sig)
		Flush()
	}
}

// lockAndFlushAndSync is like flushAndSync but locks l.mu first.
func (l *loggerT) lockAndFlushAndSync(doSync bool) {
	l.mu.Lock()
	l.flushAndSync(doSync)
	l.mu.Unlock()
}

// SetSync configures whether logging synchronizes all writes.
// This overrides the synchronization setting for both primary
// and secondary loggers.
// This is used e.g. in `cockroach start` when an error occurs,
// to ensure that all log writes from the point the error
// occurs are flushed to logs (in case the error degenerates
// into a panic / segfault on the way out).
func SetSync(sync bool) {
	mainLog.lockAndSetSync(sync)
	func() {
		secondaryLogRegistry.mu.Lock()
		defer secondaryLogRegistry.mu.Unlock()
		for _, l := range secondaryLogRegistry.mu.loggers {
			if !sync && l.forceSyncWrites {
				// We're not changing this.
				continue
			}
			l.logger.lockAndSetSync(sync)
		}
	}()
	if sync {
		// There may be something in the buffers already; flush it.
		Flush()
	}
}

// lockAndSetSync configures syncWrites.
func (l *loggerT) lockAndSetSync(sync bool) {
	l.mu.Lock()
	l.mu.syncWrites = sync
	l.mu.Unlock()
}

// flushAndSync flushes the current log and, if doSync is set,
// attempts to sync its data to disk.
//
// l.mu is held.
func (l *loggerT) flushAndSync(doSync bool) {
	if l.mu.file == nil {
		return
	}

	// If we can't sync within this duration, exit the process.
	t := time.AfterFunc(maxSyncDuration, func() {
		// NB: the disk-stall-detected roachtest matches on this message.
		Shoutf(context.Background(), Severity_FATAL,
			"disk stall detected: unable to sync log files within %s", maxSyncDuration,
		)
	})
	defer t.Stop()

	_ = l.mu.file.Flush() // ignore error
	if doSync {
		_ = l.mu.file.Sync() // ignore error
	}
}
