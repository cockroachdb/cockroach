// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"
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

// FlushFiles explicitly flushes all pending log file I/O.
// See also flushDaemon() that manages background (asynchronous)
// flushes, and signalFlusher() that manages flushes in reaction to a
// user signal.
func FlushFiles() {
	_ = logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
		l.lockAndFlushAndMaybeSync(true /*doSync*/)
		return nil
	})
}

// FlushAllSync explicitly flushes all asynchronous buffered logging sinks,
// including pending log file I/O and buffered network sinks.
//
// NB: This is a synchronous operation, and will block until all flushes
// have completed. Generally only recommended for use in crash reporting
// and shutdown scenarios. Note that `tryForceSync` is best effort, so the
// possibility exists that a buffered log sink is unable to block until
// the flush completes. In such a case though, the expectation that a flush
// is already imminent for that sink.
//
// Each sink we attempt to flush is attempted with a timeout.
func FlushAllSync() {
	FlushFiles()
	_ = logging.allSinkInfos.iterBufferedSinks(func(bs *bufferedSink) error {
		// Trigger a synchronous flush by calling output on the bufferedSink
		// with a `tryForceSync` option.
		doneCh := make(chan struct{})
		go func() {
			err := bs.output([]byte{}, sinkOutputOptions{tryForceSync: true})
			if err != nil {
				fmt.Printf("Error draining buffered log sink %T: %v\n", bs.child, err)
			}
			doneCh <- struct{}{}
		}()
		// Don't wait forever if the underlying sink happens to be unavailable.
		// Set a timeout to avoid holding up the panic handle process for too long.
		select {
		case <-time.After(3 * time.Second):
			fmt.Printf("Timed out waiting on buffered log sink %T to drain.\n", bs.child)
		case <-doneCh:
		}
		// We don't want to let errors stop us from iterating and flushing
		// the remaining buffered log sinks. Nor do we want to log the error
		// using the logging system, as it's unlikely to make it to the
		// destination sink anyway (there's a good chance we're flushing
		// as part of handling a panic). If an error occurs, it will be displayed.
		// Regardless, we return nil so the iteration continues.
		return nil
	})
}

func init() {
	go flushDaemon()
	go signalFlusher()
}

// flushInterval is the delay between periodic flushes of the buffered log data.
const flushInterval = time.Second

// syncInterval is the multiple of flushInterval where the log is also synced to disk.
const syncInterval = 30

// maxSyncDuration is the maximum duration the file sink is allowed to take to
// write a log entry before we fatal the process for a disk stall. Note that
// this fataling behaviour can be disabled by the cluster setting
// storage.max_sync_duration.fatal.enabled for most uses of the logger.
//
// This setting may sound similar to `ExitTimeoutForFatalLog`, however that
// parameter configures how long we wait to write a *fatal* log entry to _any_
// log sink before we exit the process. This one configures how long we wait
// to write any log entry to a _file_ sink before we write a fatal log entry
// instead (that could then take up to `ExitTimeoutForFatalLog` before crashing
// the process).
//
// This metric's default should ideally match its sister metrics: one in the
// storage engine, set by COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT and the
// storage.max_sync_duration cluster setting, and another in `ExitTimeoutForFatalLog`.
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

// signalFlusher updates any header values from files in the http sinks
// and also flushes the log(s) every time SIGHUP is received.
// This handles both the primary and secondary loggers.
func signalFlusher() {
	ch := sysutil.RefreshSignaledChan()
	for sig := range ch {
		Ops.Infof(context.Background(), "%s received, flushing logs", sig)
		err := RefreshHttpSinkHeaders()
		if err != nil {
			Ops.Infof(context.Background(), "error while refreshing http sink headers: %s", err)
		}
		FlushFiles()
	}
}

// StartAlwaysFlush configures all loggers to start flushing writes.
// This is used e.g. in `cockroach start` when an error occurs,
// to ensure that all log writes from the point the error
// occurs are flushed to logs (in case the error degenerates
// into a panic / segfault on the way out).
func StartAlwaysFlush() {
	logging.flushWrites.Store(true)
	// There may be something in the buffers already; flush it.
	FlushFiles()
}

// RefreshHttpSinkHeaders will iterate over all http sinks and replace the sink's
// dynamicHeaders with newly generated dynamicHeaders.
func RefreshHttpSinkHeaders() error {
	return logging.allSinkInfos.iterHTTPSinks(func(hs *httpSink) error {
		return hs.RefreshDynamicHeaders()
	})
}
