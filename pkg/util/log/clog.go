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
var mainLog loggerT

// stderrLog is the logger where writes performed directly
// to the stderr file descriptor (such as that performed
// by the go runtime) *may* be redirected.
// NB: whether they are actually redirected is determined
// by stderrLog.redirectInternalStderrWrites().
var stderrLog = &mainLog

// logging is the global state of the logging setup.
var logging loggingT

// loggingT collects all the global state of the logging setup.
//
// TODO(knz): better separate global state and per-logger state.
type loggingT struct {
	// pool for entry formatting buffers.
	bufPool sync.Pool

	// interceptor is the configured InterceptorFn callback, if any.
	interceptor atomic.Value

	// vmoduleConfig maintains the configuration for the log.V and vmodule
	// facilities.
	vmoduleConfig vmoduleConfig

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
			f         func(int, error) // overrides os.Exit when non-nil; testing only
			hideStack bool             // hides stack trace; only in effect when f is not nil
		}

		// the Cluster ID is reported on every new log file so as to ease the correlation
		// of panic reports with self-reported log files.
		clusterID string

		// fatalCh is closed on fatal errors.
		fatalCh chan struct{}
	}
}

type loggerT struct {
	// Directory prefix where to store this logger's files.
	logDir DirName

	// Name prefix for log files.
	prefix string

	// Level beyond which entries submitted to this logger are written
	// to the output file. This acts as a filter between the log entry
	// producers and the file sink.
	fileThreshold Severity

	// Level beyond which entries submitted to this logger are written
	// to the process' external standard error stream (OrigStderr).
	// This acts as a filter between the log entry producers and the
	// stderr sink.
	stderrThreshold Severity

	// noRedirectInternalStderrWrites, when UNset (set to false), causes
	// this logger to capture writes to system-wide file descriptor 2
	// (the standard error stream) and os.Stderr and redirect them to this
	// logger's output file.
	// Users of the logging package should ensure that at most one
	// logger has this flag set to redirect the system-wide stderr.
	//
	// Note that this mechanism redirects file descriptor 2, and does
	// not only assign a different *os.File reference to
	// os.Stderr. This is because the Go runtime hardcodes stderr writes
	// as writes to file descriptor 2 and disregards the value of
	// os.Stderr entirely.
	//
	// Callers are encouraged to use the redirectInternalStderrWrites()
	// accessor method for clarity.
	//
	// TODO(knz): The double negative is somewhat inconvenient. We could
	// consider flipping the meaning for enhanced clarity.
	noRedirectInternalStderrWrites bool

	// notify GC daemon that a new log file was created
	gcNotify chan struct{}

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.
	mu struct {
		syncutil.Mutex

		// file holds the log file writer.
		file flushSyncWriter

		// syncWrites if true calls file.Flush and file.Sync on every log write.
		syncWrites bool
	}
}

func init() {
	logging.bufPool.New = newBuffer
	logging.mu.fatalCh = make(chan struct{})
	mainLog.prefix = program
	// Default stderrThreshold and fileThreshold to log everything
	// both to the output file and to the process' external stderr
	// (OrigStderr).
	// This will be the default in tests unless overridden; the CLI
	// commands set their default separately in cli/flags.go.
	mainLog.stderrThreshold = Severity_INFO
	mainLog.fileThreshold = Severity_INFO
	// In addition, we want all writes performed directly to the
	// internal stderr file descriptor (fd 2) to go to the stderr log
	// file.
	stderrLog.noRedirectInternalStderrWrites = false
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

// SetClusterID stores the Cluster ID for further reference.
//
// TODO(knz): This should not be configured per-logger.
// See: https://github.com/cockroachdb/cockroach/issues/40983
func SetClusterID(clusterID string) {
	// Ensure that the clusterID is logged with the same format as for
	// new log files, even on the first log file. This ensures that grep
	// will always find it.
	file, line, _ := caller.Lookup(1)
	mainLog.outputLogEntry(Severity_INFO, file, line,
		fmt.Sprintf("[config] clusterID: %s", clusterID))

	// Perform the change proper.
	logging.mu.Lock()
	defer logging.mu.Unlock()

	if logging.mu.clusterID != "" {
		panic("clusterID already set")
	}

	logging.mu.clusterID = clusterID
}

// ensureFile ensures that l.file is set and valid.
// Assumes that l.mu is held by the caller.
func (l *loggerT) ensureFile() error {
	if l.mu.file == nil {
		return l.createFile()
	}
	return nil
}

// redirectInternalStderrWrites is the positive accessor
// for the noRedirectInternalStderrWrites flag.
func (l *loggerT) redirectInternalStderrWrites() bool {
	return !l.noRedirectInternalStderrWrites
}

// writeToFile writes to the file and applies the synchronization policy.
// Assumes that l.mu is held by the caller.
func (l *loggerT) writeToFile(data []byte) error {
	if _, err := l.mu.file.Write(data); err != nil {
		return err
	}
	if l.mu.syncWrites {
		_ = l.mu.file.Flush()
		_ = l.mu.file.Sync()
	}
	return nil
}

// outputLogEntry marshals a log entry proto into bytes, and writes
// the data to the log files. If a trace location is set, stack traces
// are added to the entry before marshaling.
func (l *loggerT) outputLogEntry(s Severity, file string, line int, msg string) {
	// Set additional details in log entry.
	now := timeutil.Now()
	entry := MakeEntry(s, now.UnixNano(), file, line, msg)

	if f, ok := logging.interceptor.Load().(InterceptorFn); ok && f != nil {
		f(entry)
		return
	}

	// TODO(tschottdorf): this is a pretty horrible critical section.
	l.mu.Lock()

	var stacks []byte
	var fatalTrigger chan struct{}
	if s == Severity_FATAL {
		logging.signalFatalCh()

		switch traceback {
		case tracebackSingle:
			stacks = getStacks(false)
		case tracebackAll:
			stacks = getStacks(true)
		}

		// Since the Fatal output will be copied to stderr below, it may
		// show up to a (human) observer through a different channel than
		// a file in the log directory. So remind them where to look for
		// more.
		if logDir := l.logDir.String(); logDir != "" {
			stacks = append(stacks, []byte(fmt.Sprintf("\nFor more context, check log files in: %s\n", l.logDir.String()))...)
		}

		// Explain to the (human) user that we would like to hear from them.
		stacks = append(stacks, []byte(fatalErrorPostamble)...)

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
		exitFunc := func(x int, _ error) { os.Exit(x) }
		logging.mu.Lock()
		if logging.mu.exitOverride.f != nil {
			if logging.mu.exitOverride.hideStack {
				stacks = []byte("stack trace omitted via SetExitFunc()\n")
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
			exitFunc(255, nil) // C++ uses -1, which is silly because it's anded with 255 anyway.
			close(exitCalled)
		}()
	}

	if s >= l.stderrThreshold.get() {
		if err := l.outputToStderr(entry, stacks); err != nil {
			// The external stderr log is unavailable.  However, stderr was
			// chosen by the stderrThreshold configuration, so abandoning
			// the stderr write would be a contract violation.
			//
			// We definitely do not like to lose log entries, so we stop
			// here. Note that exitLocked() shouts the error to both stderr
			// and the log file, so even though stderr is not available any
			// more, we'll keep a trace of the error in the file.
			l.exitLocked(err)
			l.mu.Unlock() // unreachable except in tests
			return        // unreachable except in tests
		}
	}
	if l.logDir.IsSet() && s >= l.fileThreshold.get() {
		if err := l.ensureFile(); err != nil {
			// We definitely do not like to lose log entries, so we stop
			// here. Note that exitLocked() shouts the error to both stderr
			// and the log file, so even though the file is not available
			// any more, we'll likely keep a trace of the error in stderr.
			l.exitLocked(err)
			l.mu.Unlock() // unreachable except in tests
			return        // unreachable except in tests
		}

		buf := logging.processForFile(entry, stacks)
		data := buf.Bytes()

		if err := l.writeToFile(data); err != nil {
			l.exitLocked(err)
			l.mu.Unlock()  // unreachable except in tests
			putBuffer(buf) // unreachable except in tests
			return         // unreachable except in tests
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

// DumpStacks produces a dump of the stack traces in the logging output.
func DumpStacks(ctx context.Context) {
	allStacks := getStacks(true)
	// TODO(knz): This should really be a "debug" level, not "info".
	Infof(ctx, "stack traces:\n%s", allStacks)
}

// printPanicToFile copies the panic details to the log file. This is
// useful when the internal stderr writes are not redirected to the
// log file (!stderrRedirected), as the go runtime hardcodes
// its writes to file descriptor 2.
//
// TODO(knz): Create a log entry header for the panic details, to
// get a timestamp and later a redactable marker.
func (l *loggerT) printPanicToFile(r interface{}) {
	if !l.logDir.IsSet() {
		// There's no log file. Can't do anything.
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureFile(); err != nil {
		// We're already exiting; no need to pile an error upon an
		// error. Simply report the logging error and continue.
		l.reportErrorEverywhereLocked(err)
		return
	}

	panicBytes := []byte(fmt.Sprintf("%v\n\n%s\n", r, debug.Stack()))
	if err := l.writeToFile(panicBytes); err != nil {
		// Ditto; report the error but continue. We're terminating anyway.
		l.reportErrorEverywhereLocked(err)
		return
	}
}

// outputToStderr writes the provided entry and potential stack
// trace(s) to the process' external stderr stream.
func (l *loggerT) outputToStderr(entry Entry, stacks []byte) error {
	buf := logging.processForStderr(entry, stacks)
	_, err := OrigStderr.Write(buf.Bytes())
	putBuffer(buf)
	return err
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
