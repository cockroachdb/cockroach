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
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// mainLog is the primary logger instance.
var mainLog loggingT

// loggingT collects all the global state of the logging setup.
//
// TODO(knz): better separate global state and per-logger state.
type loggingT struct {
	noStderrRedirect bool

	// Directory prefix where to store this logger's files.
	logDir DirName

	// Name prefix for log files.
	prefix string

	// Level flag for output to stderr. Handled atomically.
	stderrThreshold Severity
	// Level flag for output to files.
	fileThreshold Severity

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.
	mu syncutil.Mutex
	// file holds the log file writer.
	file flushSyncWriter
	// syncWrites if true calls file.Flush and file.Sync on every log write.
	syncWrites bool
	// pcsPool maintains a set of [1]uintptr buffers to be used in V to avoid
	// allocating every time we compute the caller's PC.
	pcsPool sync.Pool
	// vmap is a cache of the V Level for each V() call site, identified by PC.
	// It is wiped whenever the vmodule flag changes state.
	vmap map[uintptr]level
	// filterLength stores the length of the vmodule filter chain. If greater
	// than zero, it means vmodule is enabled. It may be read safely
	// using sync.LoadInt32, but is only modified under mu.
	filterLength int32
	// traceLocation is the state of the -log_backtrace_at flag.
	traceLocation traceLocation
	// disableDaemons can be used to turn off both the GC and flush deamons.
	disableDaemons bool
	// These flags are modified only under lock, although verbosity may be fetched
	// safely using atomic.LoadInt32.
	vmodule      moduleSpec // The state of the --vmodule flag.
	verbosity    level      // V logging level, the value of the --verbosity flag/
	exitOverride struct {
		f         func(int) // overrides os.Exit when non-nil; testing only
		hideStack bool      // hides stack trace; only in effect when f is not nil
	}
	gcNotify chan struct{} // notify GC daemon that a new log file was created
	fatalCh  chan struct{} // closed on fatal error

	interceptor atomic.Value // InterceptorFn

	// The Cluster ID is reported on every new log file so as to ease the correlation
	// of panic reports with self-reported log files.
	clusterID string
}

func init() {
	// Default stderrThreshold and fileThreshold to log everything.
	// This will be the default in tests unless overridden; the CLI
	// commands set their default separately in cli/flags.go
	mainLog.stderrThreshold = Severity_INFO
	mainLog.fileThreshold = Severity_INFO

	mainLog.prefix = program
	mainLog.fatalCh = make(chan struct{})
}

// FatalChan is closed when Fatal is called. This can be used to make
// the process stop handling requests while the final log messages and
// crash report are being written.
func FatalChan() <-chan struct{} {
	return mainLog.fatalCh
}

// SetClusterID stores the Cluster ID for further reference.
//
// TODO(knz): This should not be configured per-logger.
// See: https://github.com/cockroachdb/cockroach/issues/40983
func SetClusterID(clusterID string) {
	mainLog.setClusterID(clusterID)
}

func (l *loggingT) setClusterID(clusterID string) {
	// Ensure that the clusterID is logged with the same format as for
	// new log files, even on the first log file. This ensures that grep
	// will always find it.
	file, line, _ := caller.Lookup(1)
	l.outputLogEntry(Severity_INFO, file, line,
		fmt.Sprintf("[config] clusterID: %s", clusterID))

	// Perform the change proper.
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.clusterID != "" {
		panic("clusterID already set")
	}

	l.clusterID = clusterID
}

// ensureFile ensures that l.file is set and valid.
// Assumes that l.mu is held by the caller.
func (l *loggingT) ensureFile() error {
	if l.file == nil {
		return l.createFile()
	}
	return nil
}

// writeToFile writes to the file and applies the synchronization policy.
func (l *loggingT) writeToFile(data []byte) error {
	if _, err := l.file.Write(data); err != nil {
		return err
	}
	if l.syncWrites {
		_ = l.file.Flush()
		_ = l.file.Sync()
	}
	return nil
}

// outputLogEntry marshals a log entry proto into bytes, and writes
// the data to the log files. If a trace location is set, stack traces
// are added to the entry before marshaling.
func (l *loggingT) outputLogEntry(s Severity, file string, line int, msg string) {
	// Set additional details in log entry.
	now := timeutil.Now()
	entry := MakeEntry(s, now.UnixNano(), file, line, msg)

	if f, ok := l.interceptor.Load().(InterceptorFn); ok && f != nil {
		f(entry)
		return
	}

	// TODO(tschottdorf): this is a pretty horrible critical section.
	l.mu.Lock()

	var stacks []byte
	var fatalTrigger chan struct{}
	if s == Severity_FATAL {
		// Close l.fatalCh if it is not already closed (note that we're
		// holding l.mu to guard against concurrent closes).
		select {
		case <-l.fatalCh:
		default:
			close(l.fatalCh)
		}

		switch traceback {
		case tracebackSingle:
			stacks = getStacks(false)
		case tracebackAll:
			stacks = getStacks(true)
		}
		stacks = append(stacks, []byte(fatalErrorPostamble)...)

		logExitFunc = func(error) {} // If we get a write error, we'll still exit.

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
		exitFunc := os.Exit
		if l.exitOverride.f != nil {
			if l.exitOverride.hideStack {
				stacks = []byte("stack trace omitted via SetExitFunc)\n")
			}
			exitFunc = l.exitOverride.f
		}
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
			exitFunc(255) // C++ uses -1, which is silly because it's anded with 255 anyway.
			close(exitCalled)
		}()
	} else if l.traceLocation.isSet() {
		if l.traceLocation.match(file, line) {
			stacks = getStacks(false)
		}
	}

	if s >= l.stderrThreshold.get() || (s == Severity_FATAL && l.stderrRedirected()) {
		// We force-copy FATAL messages to stderr, because the process is bound
		// to terminate and the user will want to know why.
		l.outputToStderr(entry, stacks)
	}
	if l.logDir.IsSet() && s >= l.fileThreshold.get() {
		if err := l.ensureFile(); err != nil {
			// Make sure the message appears somewhere.
			l.outputToStderr(entry, stacks)
			l.exitLocked(err)
			l.mu.Unlock()
			return
		}

		buf := l.processForFile(entry, stacks)
		data := buf.Bytes()

		if err := l.writeToFile(data); err != nil {
			l.exitLocked(err)
			l.mu.Unlock()
			putBuffer(buf)
			return
		}

		putBuffer(buf)
	}
	// Flush and exit on fatal logging.
	if s == Severity_FATAL {
		l.flushAndSync(true /*doSync*/)
		close(fatalTrigger)
		// Note: although it seems like the function is allowed to return
		// below when s == Severity_FATAL, this is not so, because the
		// anonymous function func() { <-exitCalled } is deferred
		// above. That function ensures that outputLogEntry() will wait
		// until the exit function has been called. If the exit function
		// is os.Exit, it will never return, outputLogEntry()'s defer will
		// never complete and all is well. If the exit function was
		// overridden, then the client that has overridden the exit
		// function is expecting log.Fatal to return and all is well too.
	}
	l.mu.Unlock()
}

// printPanicToFile copies the panic details to the log file. This is
// useful when the standard error is not redirected to the log file
// (!stderrRedirected), as the go runtime will only print panics to
// stderr.
func (l *loggingT) printPanicToFile(r interface{}) {
	if !l.logDir.IsSet() {
		// There's no log file. Nothing to do.
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureFile(); err != nil {
		fmt.Fprintf(OrigStderr, "log: %v", err)
		return
	}

	panicBytes := []byte(fmt.Sprintf("%v\n\n%s\n", r, debug.Stack()))
	if err := l.writeToFile(panicBytes); err != nil {
		fmt.Fprintf(OrigStderr, "log: %v", err)
		return
	}
}

func (l *loggingT) outputToStderr(entry Entry, stacks []byte) {
	buf := l.processForStderr(entry, stacks)
	_, err := OrigStderr.Write(buf.Bytes())
	putBuffer(buf)
	if err != nil {
		l.exitLocked(err)
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
