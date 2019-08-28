// Copyright 2014 The Cockroach Authors.
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
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/petermattis/goid"
)

func init() {
	copyStandardLogTo("INFO")
	errors.SetWarningFn(Warningf)
}

// FatalOnPanic recovers from a panic and exits the process with a
// Fatal log. This is useful for avoiding a panic being caught through
// a CGo exported function or preventing HTTP handlers from recovering
// panics and ignoring them.
func FatalOnPanic() {
	if r := recover(); r != nil {
		Fatalf(context.Background(), "unexpected panic: %s", r)
	}
}

// SetExitFunc allows setting a function that will be called to exit the
// process when a Fatal message is generated. The supplied bool, if true,
// suppresses the stack trace, which is useful for test callers wishing
// to keep the logs reasonably clean.
//
// Call with a nil function to undo.
func SetExitFunc(hideStack bool, f func(int)) {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.exitOverride.f = f
	logging.exitOverride.hideStack = hideStack
}

// ResetExitFunc undoes any prior call to SetExitFunc.
func ResetExitFunc() {
	logging.mu.Lock()
	defer logging.mu.Unlock()

	logging.exitOverride.f = nil
	logging.exitOverride.hideStack = false
}

// logDepth uses the PrintWith to format the output string and
// formulate the context information into the machine-readable
// dictionary for separate binary-log output.
func logDepth(ctx context.Context, depth int, sev Severity, format string, args []interface{}) {
	// TODO(tschottdorf): logging hooks should have their entry point here.
	addStructured(ctx, sev, depth+1, format, args)
}

// Shout logs to the specified severity's log, and also to the real
// stderr if logging is currently redirected to a file.
func Shout(ctx context.Context, sev Severity, args ...interface{}) {
	if sev == Severity_FATAL {
		// Fatal error handling later already tries to exit even if I/O should
		// block, but crash reporting might also be in the way.
		t := time.AfterFunc(10*time.Second, func() {
			os.Exit(1)
		})
		defer t.Stop()
	}
	if stderrRedirected() {
		fmt.Fprintf(OrigStderr, "*\n* %s: %s\n*\n", sev.String(),
			strings.Replace(MakeMessage(ctx, "", args), "\n", "\n* ", -1))
	}
	logDepth(ctx, 1, sev, "", args)
}

// Infof logs to the INFO log.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func Infof(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, Severity_INFO, format, args)
}

// Info logs to the INFO log.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Print; a newline is
// appended.
func Info(ctx context.Context, args ...interface{}) {
	logDepth(ctx, 1, Severity_INFO, "", args)
}

// InfoDepth logs to the INFO log, offsetting the caller's stack frame by
// 'depth'.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Print; a newline is
// appended.
func InfoDepth(ctx context.Context, depth int, args ...interface{}) {
	logDepth(ctx, depth+1, Severity_INFO, "", args)
}

// InfofDepth logs to the INFO log, offsetting the caller's stack frame by
// 'depth'.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func InfofDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logDepth(ctx, depth+1, Severity_INFO, format, args)
}

// Warningf logs to the WARNING and INFO logs.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func Warningf(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, Severity_WARNING, format, args)
}

// Warning logs to the WARNING and INFO logs.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Print; a newline is
// appended.
func Warning(ctx context.Context, args ...interface{}) {
	logDepth(ctx, 1, Severity_WARNING, "", args)
}

// WarningfDepth logs to the WARNING and INFO logs, offsetting the caller's
// stack frame by 'depth'.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func WarningfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logDepth(ctx, depth+1, Severity_WARNING, format, args)
}

// Errorf logs to the ERROR, WARNING, and INFO logs.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func Errorf(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, Severity_ERROR, format, args)
}

// Error logs to the ERROR, WARNING, and INFO logs.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Print; a newline is
// appended.
func Error(ctx context.Context, args ...interface{}) {
	logDepth(ctx, 1, Severity_ERROR, "", args)
}

// ErrorfDepth logs to the ERROR, WARNING, and INFO logs, offsetting the
// caller's stack frame by 'depth'.
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logDepth(ctx, depth+1, Severity_ERROR, format, args)
}

// Fatalf logs to the INFO, WARNING, ERROR, and FATAL logs, including a stack
// trace of all running goroutines, then calls os.Exit(255).
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func Fatalf(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, Severity_FATAL, format, args)
}

// Fatal logs to the INFO, WARNING, ERROR, and FATAL logs, including a stack
// trace of all running goroutines, then calls os.Exit(255).
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Print; a newline is
// appended.
func Fatal(ctx context.Context, args ...interface{}) {
	logDepth(ctx, 1, Severity_FATAL, "", args)
}

// FatalfDepth logs to the INFO, WARNING, ERROR, and FATAL logs (offsetting the
// caller's stack frame by 'depth'), including a stack trace of all running
// goroutines, then calls os.Exit(255).
// It extracts log tags from the context and logs them along with the given
// message. Arguments are handled in the manner of fmt.Printf; a newline is
// appended.
func FatalfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logDepth(ctx, depth+1, Severity_FATAL, format, args)
}

// V returns true if the logging verbosity is set to the specified level or
// higher.
//
// See also ExpensiveLogEnabled().
//
// TODO(andrei): Audit uses of V() and see which ones should actually use the
// newer ExpensiveLogEnabled().
func V(level int32) bool {
	return VDepth(level, 1)
}

// ExpensiveLogEnabled is used to test whether effort should be used to produce
// log messages whose construction has a measurable cost. It returns true if
// either the current context is recording the trace, or if the caller's
// verbosity is above level.
//
// NOTE: This doesn't take into consideration whether tracing is generally
// enabled or whether a trace.EventLog or a trace.Trace (i.e. sp.netTr) is
// attached to ctx. In particular, if some OpenTracing collection is enabled
// (e.g. LightStep), that, by itself, does NOT cause the expensive messages to
// be enabled. "SET tracing" and friends, on the other hand, does cause
// these messages to be enabled, as it shows that a user has expressed
// particular interest in a trace.
//
// Usage:
//
// if ExpensiveLogEnabled(ctx, 2) {
//   msg := constructExpensiveMessage()
//   log.VEventf(ctx, 2, msg)
// }
//
func ExpensiveLogEnabled(ctx context.Context, level int32) bool {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if tracing.IsRecording(sp) {
			return true
		}
	}
	if VDepth(level, 1 /* depth */) {
		return true
	}
	return false
}

// MakeEntry creates an Entry.
func MakeEntry(s Severity, t int64, file string, line int, msg string) Entry {
	return Entry{
		Severity:  s,
		Time:      t,
		Goroutine: goid.Get(),
		File:      file,
		Line:      int64(line),
		Message:   msg,
	}
}

// Format writes the log entry to the specified writer.
func (e Entry) Format(w io.Writer) error {
	buf := formatLogEntry(e, nil, nil)
	defer logging.putBuffer(buf)
	_, err := w.Write(buf.Bytes())
	return err
}

// SetVModule alters the vmodule logging level to the passed in value.
func SetVModule(value string) error {
	return logging.vmodule.Set(value)
}
