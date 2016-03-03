// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf

package log

import "golang.org/x/net/context"

// Entry represents a cockroach structured log entry.
type Entry struct {
	Severity int    `json:"severity"` // Log message severity.
	Time     int64  `json:"time"`     // Time, measured in nanoseconds since the epoch.
	File     string `json:"file"`     // File which generated log statement.
	Line     int    `json:"line"`     // Line in file which generated log statement.
	// TODO(pmattis): The json output should be called `message` as well. Need to
	// fix the UI.
	Message string `json:"format"` // Log message.
}

func init() {
	copyStandardLogTo("INFO")
}

// FatalOnPanic recovers from a panic and exits the process with a
// Fatal log. This is useful for avoiding a panic being caught through
// a CGo exported function or preventing HTTP handlers from recovering
// panics and ignoring them.
func FatalOnPanic() {
	if r := recover(); r != nil {
		Fatalf("unexpected panic: %s", r)
	}
}

// EnableLogFileOutput turns on logging using the specified directory.
// For unittesting only.
func EnableLogFileOutput(dir string) {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logDir = dir
	logging.toStderr = false
	logging.stderrThreshold = InfoLog
}

// DisableLogFileOutput turns off logging. For unittesting only.
func DisableLogFileOutput() {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	if err := logging.removeFilesLocked(); err != nil {
		logging.exit(err)
	}
	logDir = ""
	logging.toStderr = true
	logging.stderrThreshold = NumSeverity
}

// logDepth uses the PrintWith to format the output string and
// formulate the context information into the machine-readable
// dictionary for separate binary-log output.
func logDepth(ctx context.Context, depth int, sev Severity, format string, args []interface{}) {
	// TODO(tschottdorf): logging hooks should have their entry point here.
	addStructured(ctx, sev, depth+1, format, args)
}

// Infoc logs to the WARNING and INFO logs. It extracts values from the context
// using the Field keys specified in this package and logs them along with the
// given message and any additional pairs specified as consecutive elements in
// kvs.
func Infoc(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, InfoLog, format, args)
}

// Info logs to the INFO log.
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Info(args ...interface{}) {
	logDepth(nil, 1, InfoLog, "", args)
}

// Infof logs to the INFO log. Don't use it; use Info or Infoc instead.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Infof(format string, args ...interface{}) {
	logDepth(nil, 1, InfoLog, format, args)
}

// InfofDepth logs to the INFO log, offsetting the caller's stack frame by
// 'depth'. Passing an empty string for `format` causes this method to
// naturally format `args`.
func InfofDepth(depth int, format string, args ...interface{}) {
	logDepth(nil, depth+1, InfoLog, format, args)
}

// Warningc logs to the WARNING and INFO logs. It extracts values from the
// context using the Field keys specified in this package and logs them along
// with the given message and any additional pairs specified as consecutive
// elements in kvs.
func Warningc(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, WarningLog, format, args)
}

// Warning logs to the WARNING and INFO logs.
// Warningf logs to the WARNING and INFO logs. Don't use it; use Warning or
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Warning(args ...interface{}) {
	logDepth(nil, 1, WarningLog, "", args)
}

// Warningf logs to the WARNING and INFO logs. Don't use it; use Warning or
// Warningc instead. Arguments are handled in the manner of fmt.Printf; a
// newline is appended if missing.
func Warningf(format string, args ...interface{}) {
	logDepth(nil, 1, WarningLog, format, args)
}

// WarningfDepth logs to the WARNING and INFO logs, offsetting the caller's
// stack frame by 'depth'. Passing an empty string for `format` causes this
// method to naturally format `args`.
func WarningfDepth(depth int, format string, args ...interface{}) {
	logDepth(nil, depth+1, WarningLog, format, args)
}

// Errorc logs to the ERROR, WARNING, and INFO logs. It extracts values from
// Field keys specified in this package and logs them along with the given
// message and any additional pairs specified as consecutive elements in kvs.
func Errorc(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, ErrorLog, format, args)
}

// Error logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Error(args ...interface{}) {
	logDepth(nil, 1, ErrorLog, "", args)
}

// Errorf logs to the ERROR, WARNING, and INFO logs. Don't use it; use Error
// Info or Errorc instead. Arguments are handled in the manner of fmt.Printf;
// a newline is appended if missing.
func Errorf(format string, args ...interface{}) {
	logDepth(nil, 1, ErrorLog, format, args)
}

// ErrorfDepth logs to the ERROR, WARNING, and INFO logs, offsetting the
// caller's stack frame by 'depth'. Passing an empty string for `format` causes
// this method to naturally format `args`.
func ErrorfDepth(depth int, format string, args ...interface{}) {
	logDepth(nil, depth+1, ErrorLog, format, args)
}

// Fatalc logs to the INFO, WARNING, ERROR, and FATAL logs, including a stack
// trace of all running goroutines, then calls os.Exit(255). It extracts values
// from the context using the Field keys specified in this package and logs
// them along with the given message and any additional pairs specified as
// consecutive elements in kvs.
func Fatalc(ctx context.Context, format string, args ...interface{}) {
	logDepth(ctx, 1, FatalLog, format, args)
}

// Fatal logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Fatal(args ...interface{}) {
	logDepth(nil, 1, FatalLog, "", args)
}

// Fatalf logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Printf; a newline is appended.
func Fatalf(format string, args ...interface{}) {
	logDepth(nil, 1, FatalLog, format, args)
}

// FatalfDepth logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255),
// offsetting the caller's stack frame by 'depth'. Passing an empty string for
// `format` causes this method to naturally format `args`.
func FatalfDepth(depth int, format string, args ...interface{}) {
	logDepth(nil, depth+1, FatalLog, format, args)
}

// V returns true if the logging verbosity is set to the specified level or
// higher.
func V(level level) bool {
	return VDepth(level, 1)
}
