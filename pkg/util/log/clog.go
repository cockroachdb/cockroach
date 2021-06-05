// Copyright 2013 Google Inc. All Rights Reserved.
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code originated in the github.com/golang/glog package.

package log

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// logging is the global state of the logging setup.
var logging loggingT

// loggingT collects all the global state of the logging setup.
//
// TODO(knz): better separate global state and per-logger state.
type loggingT struct {
	config

	// allocation pool for entry formatting buffers.
	bufPool sync.Pool
	// allocation pool for slices of buffer pointers.
	bufSlicePool sync.Pool

	// interceptor contains the configured InterceptorFn callbacks, if any.
	interceptor interceptorSink

	// vmoduleConfig maintains the configuration for the log.V and vmodule
	// facilities.
	vmoduleConfig vmoduleConfig

	// The common stderr sink.
	stderrSink stderrSink
	// The template for the stderr sink info. This is where the configuration
	// parameters are applied in ApplyConfig().
	// This template is copied to the heap for use as sinkInfo in the
	// actual loggers, so that configuration updates do not race
	// with logging operations.
	stderrSinkInfoTemplate sinkInfo

	// the mapping of channels to loggers.
	//
	// This is under a R/W lock because some tests leak goroutines
	// asynchronously with TestLogScope.Close() calls. If/when that
	// misdesign is corrected, this lock can be dropped.
	rmu struct {
		syncutil.RWMutex
		channels map[Channel]*loggerT
		// currentStderrSinkInfo is the currently active copy of
		// stderrSinkInfoTemplate. This is used in tests and
		// DescribeAppliedConfiguration().
		currentStderrSinkInfo *sinkInfo
	}

	// testingFd2CaptureLogger remembers the logger that was last set up
	// to capture fd2 writes. Used by unit tests in this package.
	testingFd2CaptureLogger *loggerT

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.
	// mu should be held only for short periods of time and
	// its critical sections cannot contain logging calls themselves.
	mu struct {
		syncutil.Mutex

		// disableDaemons can be used to turn off both the GC and flush deamons.
		disableDaemons bool

		// exitOverride is used when shutting down logging.
		exitOverride struct {
			f         func(exit.Code, error) // overrides exit.WithCode when non-nil; testing only
			hideStack bool                   // hides stack trace; only in effect when f is not nil
		}

		// fatalCh is closed on fatal errors.
		fatalCh chan struct{}

		// active indicates that at least one event has been logged
		// to this logger already.
		active        bool
		firstUseStack string
	}

	idMu struct {
		syncutil.RWMutex
		idPayload
	}
}

type idPayload struct {
	// the Cluster ID is reported on every new log file so as to ease
	// the correlation of panic reports with self-reported log files.
	clusterID string
	// the node ID is reported like the cluster ID, for the same reasons.
	// We avoid using roahcpb.NodeID to avoid a circular reference.
	nodeID int32
	// ditto for the tenant ID.
	tenantID string
	// ditto for the SQL instance ID.
	sqlInstanceID int32
}

func init() {
	logging.bufPool.New = newBuffer
	logging.bufSlicePool.New = newBufferSlice
	logging.mu.fatalCh = make(chan struct{})
	logging.stderrSinkInfoTemplate.sink = &logging.stderrSink
	si := logging.stderrSinkInfoTemplate
	logging.setChannelLoggers(make(map[Channel]*loggerT), &si)
}

type sinkInfo struct {
	// sink is where the log entries should be written.
	sink logSink

	// Level at or beyond which entries are output to this sink.
	threshold Severity

	// editor is the optional step that occurs prior to emitting the log
	// entry.
	editor redactEditor

	// formatter for entries written via this sink.
	formatter logFormatter

	// msgCount supports the generation of a per-entry log entry
	// counter. This is needed in audit logs to hinder malicious
	// repudiation of log events by manually erasing log files or log
	// entries.
	msgCount uint64

	// criticality indicates whether a failure to output some log
	// entries should incur the process to terminate.
	criticality bool

	// redact and redactable memorize the input configuration
	// that was used to create the editor above.
	redact, redactable bool
}

// loggerT represents the logging source for a given log channel.
type loggerT struct {
	// sinkInfos stores the destinations for log entries.
	sinkInfos []*sinkInfo

	// outputMu is used to coordinate output to the sinks, to guarantee
	// that the ordering of events the same on all sinks.
	outputMu syncutil.Mutex
}

// getFileSinkIndex retrieves the index of the fileSink, if defined,
// in the sinkInfos. Returns -1 if there is no file sink.
func (l *loggerT) getFileSinkIndex() int {
	for i, s := range l.sinkInfos {
		if _, ok := s.sink.(*fileSink); ok {
			return i
		}
	}
	return -1
}

// getFileSink retrieves the file sink if defined.
func (l *loggerT) getFileSink() *fileSink {
	if i := l.getFileSinkIndex(); i != -1 {
		return l.sinkInfos[i].sink.(*fileSink)
	}
	return nil
}

// FatalChan is closed when Fatal is called. This can be used to make
// the process stop handling requests while the final log messages and
// crash report are being written.
func FatalChan() <-chan struct{} {
	return logging.mu.fatalCh
}

// s ignalFatalCh signals the listeners of l.mu.fatalCh by closing the
// channel.
// l.mu is not held.
func (l *loggingT) signalFatalCh() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Close l.fatalCh if it is not already closed (note that we're
	// holding l.mu to guard against concurrent closes).
	select {
	case <-l.mu.fatalCh:
	default:
		close(l.mu.fatalCh)
	}
}

// SetNodeIDs stores the Node and Cluster ID for further reference.
func SetNodeIDs(clusterID string, nodeID int32) {
	// Ensure that the IDs are logged with the same format as for
	// new log files, even on the first log file. This ensures that grep
	// will always find it.
	ctx := logtags.AddTag(context.Background(), "config", nil)
	logfDepth(ctx, 1, severity.INFO, channel.OPS, "clusterID: %s", clusterID)
	if nodeID != 0 {
		logfDepth(ctx, 1, severity.INFO, channel.OPS, "nodeID: n%d", nodeID)
	}

	// Perform the change proper.
	logging.idMu.Lock()
	defer logging.idMu.Unlock()

	if logging.idMu.clusterID != "" {
		panic("clusterID already set")
	}

	logging.idMu.clusterID = clusterID
	logging.idMu.nodeID = nodeID
}

// SetTenantIDs stores the tenant ID and instance ID for further reference.
func SetTenantIDs(tenantID string, sqlInstanceID int32) {
	// Ensure that the IDs are logged with the same format as for
	// new log files, even on the first log file. This ensures that grep
	// will always find it.
	ctx := logtags.AddTag(context.Background(), "config", nil)
	logfDepth(ctx, 1, severity.INFO, channel.OPS, "tenantID: %s", tenantID)
	logfDepth(ctx, 1, severity.INFO, channel.OPS, "instanceID: %d", sqlInstanceID)

	// Perform the change proper.
	logging.idMu.Lock()
	defer logging.idMu.Unlock()

	if logging.idMu.tenantID != "" {
		panic("tenantID already set")
	}

	logging.idMu.tenantID = tenantID
	logging.idMu.sqlInstanceID = sqlInstanceID
}

// outputLogEntry marshals a log entry proto into bytes, and writes
// the data to the log files. If a trace location is set, stack traces
// are added to the entry before marshaling.
func (l *loggerT) outputLogEntry(entry logEntry) {
	// Mark the logger as active, so that further configuration changes
	// are disabled. See IsActive() and its callers for details.
	setActive()
	var fatalTrigger chan struct{}
	extraFlush := false

	if entry.sev == severity.FATAL {
		extraFlush = true
		logging.signalFatalCh()

		switch traceback {
		case tracebackSingle:
			entry.stacks = getStacks(false)
		case tracebackAll:
			entry.stacks = getStacks(true)
		}

		for _, s := range l.sinkInfos {
			entry.stacks = s.sink.attachHints(entry.stacks)
		}

		// Explain to the (human) user that we would like to hear from them.
		entry.stacks = append(entry.stacks, []byte(fatalErrorPostamble)...)

		// We don't want to hang forever writing our final log message. If
		// things are broken (for example, if the disk fills up and there
		// are cascading errors and our process manager has stopped
		// reading from its side of a stderr pipe), it's more important to
		// let the process exit than limp along.
		//
		// Note that we do not use os.File.SetWriteDeadline because not
		// all files support this (for example, plain files on a network
		// file system do not support deadlines but can block
		// indefinitely).
		//
		// https://github.com/cockroachdb/cockroach/issues/23119
		fatalTrigger = make(chan struct{})
		exitFunc := func(x exit.Code, _ error) { exit.WithCode(x) }
		logging.mu.Lock()
		if logging.mu.exitOverride.f != nil {
			if logging.mu.exitOverride.hideStack {
				entry.stacks = []byte("stack trace omitted via SetExitFunc()\n")
			}
			exitFunc = logging.mu.exitOverride.f
		}
		logging.mu.Unlock()
		exitCalled := make(chan struct{})

		// This defer prevents outputLogEntry() from returning until the
		// exit function has been called.
		defer func() {
			<-exitCalled
		}()
		go func() {
			select {
			case <-time.After(10 * time.Second):
			case <-fatalTrigger:
			}
			exitFunc(exit.FatalError(), nil)
			close(exitCalled)
		}()
	}

	// The following buffers contain the formatted entry before it enters the sink.
	// We need different buffers because the different sinks use different formats.
	// For example, the fluent sink needs JSON, and the file sink does not use
	// the terminal escape codes that the stderr sink uses.
	bufs := getBufferSlice(len(l.sinkInfos))
	defer putBufferSlice(bufs)

	// The following code constructs / populates the formatted entries
	// for each sink.
	// We only do the work if the sink is active and the filtering does
	// not eliminate the event.
	someSinkActive := false
	for i, s := range l.sinkInfos {
		// Note: we need to use the .Get() method instead of reading the
		// severity threshold directly, because some tests are unruly and
		// let goroutines live and perform log calls beyond their
		// Stopper's Stop() call (e.g. the pgwire async processing
		// goroutine). These asynchronous log calls are concurrent with
		// the stderrSinkInfo update in (*TestLogScope).Close().
		if entry.sev < s.threshold.Get() || !s.sink.active() {
			continue
		}
		editedEntry := entry

		// Add a counter. This is important for e.g. the SQL audit logs.
		// Note: whether the counter is displayed or not depends on
		// the formatter.
		editedEntry.counter = atomic.AddUint64(&s.msgCount, 1)

		// Process the redation spec.
		editedEntry.payload = maybeRedactEntry(editedEntry.payload, s.editor)

		// Format the entry for this sink.
		bufs.b[i] = s.formatter.formatEntry(editedEntry)
		someSinkActive = true
	}

	// If any of the sinks is active, it is now time to send it out.

	if someSinkActive {
		// The critical section here exists so that the output
		// side effects from the same event (above) are emitted
		// atomically. This ensures that the order of logging
		// events is preserved across all sinks.
		l.outputMu.Lock()
		defer l.outputMu.Unlock()

		var outputErr error
		var outputErrExitCode exit.Code
		for i, s := range l.sinkInfos {
			if bufs.b[i] == nil {
				// The sink was not accepting entries at this level. Nothing to do.
				continue
			}
			if err := s.sink.output(extraFlush, bufs.b[i].Bytes()); err != nil {
				if !s.criticality {
					// An error on this sink is not critical. Just report
					// the error and move on.
					l.reportErrorEverywhereLocked(context.Background(), err)
				} else {
					// This error is critical. We'll have to terminate the
					// process below.
					if outputErr == nil {
						outputErrExitCode = s.sink.exitCode()
					}
					outputErr = errors.CombineErrors(outputErr, err)
				}
			}
		}
		if outputErr != nil {
			// Some sink was unavailable. However, the sink was active as
			// per the threshold, so abandoning the write would be a
			// contract violation.
			//
			// We definitely do not like to lose log entries, so we stop
			// here. Note that exitLocked() shouts the error to all sinks,
			// so even though this sink is not available any more, we'll
			// keep a trace of the error in another sink.
			l.exitLocked(outputErr, outputErrExitCode)
			return // unreachable except in tests
		}
	}

	// Flush and exit on fatal logging.
	if entry.sev == severity.FATAL {
		close(fatalTrigger)
		// Note: although it seems like the function is allowed to return
		// below when s == severity.FATAL, this is not so, because the
		// anonymous function func() { <-exitCalled } is deferred
		// above. That function ensures that outputLogEntry() will wait
		// until the exit function has been called. If the exit function
		// is exit.WithCode, it will never return, outputLogEntry()'s defer will
		// never complete and all is well. If the exit function was
		// overridden, then the client that has overridden the exit
		// function is expecting log.Fatal to return and all is well too.
	}
}

// DumpStacks produces a dump of the stack traces in the logging output.
func DumpStacks(ctx context.Context) {
	allStacks := getStacks(true)
	// TODO(knz): This should really be a "debug" level, not "info".
	Infof(ctx, "stack traces:\n%s", allStacks)
}

func setActive() {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	if !logging.mu.active {
		logging.mu.active = true
		logging.mu.firstUseStack = string(debug.Stack())
	}
}

const fatalErrorPostamble = `

****************************************************************************

This node experienced a fatal error (printed above), and as a result the
process is terminating.

Fatal errors can occur due to faulty hardware (disks, memory, clocks) or a
problem in CockroachDB. With your help, the support team at Cockroach Labs
will try to determine the root cause, recommend next steps, and we can
improve CockroachDB based on your report.

Please submit a crash report by following the instructions here:

    https://github.com/cockroachdb/cockroach/issues/new/choose

If you would rather not post publicly, please contact us directly at:

    support@cockroachlabs.com

The Cockroach Labs team appreciates your feedback.
`
