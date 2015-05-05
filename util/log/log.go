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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Brad Seiler (cockroach@bradseiler.com)

package log

import "github.com/golang/glog"

func init() {
	glog.CopyStandardLogTo("INFO")
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

// Info logs to the INFO log.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
var Info = glog.Info

// Infof logs to the INFO log.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
var Infof = glog.Infof

// Infoln logs to the INFO log.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
var Infoln = glog.Infoln

// InfoDepth logs to the INFO log, ofsetting the caller's stack frame by 'depth'
var InfoDepth = glog.InfoDepth

// Warning logs to the INFO and WARNING logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
var Warning = glog.Warning

// Warningf logs to the INFO and WARNING logs.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
var Warningf = glog.Warningf

// Warningln logs to the INFO and WARNING logs.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
var Warningln = glog.Warningln

// WarningDepth logs to the INFO and WARNING logs, ofsetting the caller's stack frame by 'depth'
var WarningDepth = glog.WarningDepth

// Error logs to the INFO, WARNING, and ERROR logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
var Error = glog.Error

// Errorf logs to the INFO, WARNING, and ERROR logs.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
var Errorf = glog.Errorf

// Errorln logs to the INFO, WARNING, and ERROR logs.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
var Errorln = glog.Errorln

// ErrorDepth logs to the INFO, WARNING, and ERROR logs, ofsetting the caller's stack
// frame by 'depth'
var ErrorDepth = glog.ErrorDepth

// Fatal logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
var Fatal = glog.Fatal

// Fatalf logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
var Fatalf = glog.Fatalf

// Fatalln logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
var Fatalln = glog.Fatalln

// FatalDepth logs to the INFO, WARNING, and ERROR, and FATAL logs, ofsetting the caller's stack
// frame by 'depth', then calls os.Exit(255).
var FatalDepth = glog.FatalDepth

// V wraps glog.V. See that documentation for details.
// glog has a custom type alias for bool and implements logging functions on
// it; we don't want to have that here since the calls would go straight to
// the glog package and not to whatever functions we define here.
func V(level glog.Level) bool {
	return bool(glog.V(level))
}
