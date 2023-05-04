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
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

// flushSyncWriter is the interface satisfied by logging destinations.
type flushSyncWriter interface {
	Flush() error
	Sync() error
	io.Writer
}

// flushActive indicates if a current Flush() is executing. If true,
// additional calls to Flush() will be a noop and return early, until
// the current Flush() call has completed.
var flushActive syncutil.AtomicBool

// Flush explicitly flushes all asynchronous buffered logging sinks,
// including pending log file I/O and buffered network sinks.
//
// NB: This is a synchronous operation, and will block until all flushes
// have completed. Generally only recommended for use in crash reporting
// scenarios.
//
// When flushing buffered network logging sinks, each sink is given a
// 5-second timeout before we move on to attempt flushing the next.
func Flush() {
	if flushActive.Swap(true) {
		return
	}
	defer flushActive.Swap(false)

	// Flush all file sinks.
	_ = logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
		l.lockAndFlushAndMaybeSync(true /*doSync*/)
		return nil
	})

	// Flush all buffered network sinks.
	_ = logging.allSinkInfos.iterBufferedSinks(func(bs *bufferedSink) error {
		doneCh := make(chan struct{})
		// Set a timer, so we don't prevent the process from exiting if the
		// child sink is unavailable & the request hangs.
		timer := time.NewTimer(5 * time.Second)
		go func() {
			// Trigger a synchronous flush by calling output on the bufferedSink
			// with a `forceSync` option.
			err := bs.output([]byte{}, sinkOutputOptions{forceSync: true})
			if err != nil {
				// We don't want to let errors to stop us from iterating and flushing
				// the remaining buffered log sinks. Nor do we want to log the error
				// using the logging system, as it's unlikely to make it to the
				// destination sink anyway (there's a good chance we're flushing
				// as part of handling a panic). Display the error.
				fmt.Fprintf(OrigStderr, "error draining buffered log sink: %v\n", err)
			}
			doneCh <- struct{}{}
		}()

		select {
		case <-doneCh:
		case <-timer.C:
			fmt.Fprintf(OrigStderr, "timed out draining buffered log sink: %T\n", bs.child)
		}
		// In the event of errors or timeouts, we still want to attempt to flush
		// any remaining buffered sinks. Return nil so the iterator can continue.
		return nil
	})
}

func init() {
	flushActive.Set(false)
	go flushDaemon()
	go signalFlusher()
}

// flushInterval is the delay between periodic flushes of the buffered log data.
const flushInterval = time.Second

// syncInterval is the multiple of flushInterval where the log is also synced to disk.
const syncInterval = 30

// maxSyncDuration is set to a conservative value since this is a new mechanism.
// In practice, even a fraction of that would indicate a problem. This metric's
// default should ideally match its sister metric in the storage engine, set by
// COCKROACH_ENGINE_MAX_SYNC_DURATION.
var maxSyncDuration = envutil.EnvOrDefaultDuration("COCKROACH_LOG_MAX_SYNC_DURATION", 20*time.Second)

// syncWarnDuration is the threshold after which a slow disk warning is written
// to the log and to stderr.
const syncWarnDuration = 10 * time.Second

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
	syncCounter := 0

	// This doesn't need to be Stop()'d as the loop never escapes.
	for range time.Tick(flushInterval) {
		syncCounter = (syncCounter + 1) % syncInterval
		doSync := syncCounter == 0

		// Is flushing disabled?
		logging.mu.Lock()
		disableDaemons := logging.mu.disableDaemons
		logging.mu.Unlock()

		if !disableDaemons {
			// Flush the loggers.
			_ = logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
				l.lockAndFlushAndMaybeSync(doSync)
				return nil
			})
		}
	}
}

// signalFlusher flushes the log(s) every time SIGHUP is received.
// This handles both the primary and secondary loggers.
func signalFlusher() {
	ch := sysutil.RefreshSignaledChan()
	for sig := range ch {
		Ops.Infof(context.Background(), "%s received, flushing logs", sig)
		Flush()
	}
}

// StartAlwaysFlush configures all loggers to start flushing writes.
// This is used e.g. in `cockroach start` when an error occurs,
// to ensure that all log writes from the point the error
// occurs are flushed to logs (in case the error degenerates
// into a panic / segfault on the way out).
func StartAlwaysFlush() {
	logging.flushWrites.Set(true)
	// There may be something in the buffers already; flush it.
	Flush()
}
