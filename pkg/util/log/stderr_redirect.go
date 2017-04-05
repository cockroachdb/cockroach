// Copyright 2017 The Cockroach Authors.
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

package log

import (
	"fmt"
	"os"
)

// OrigStderr points to the original stderr stream.
var OrigStderr = func() *os.File {
	fd, err := dupFD(os.Stderr.Fd())
	if err != nil {
		panic(err)
	}

	return os.NewFile(fd, os.Stderr.Name())
}()

// stderrRedirected attempts to track whether stderr was redirected.
// This is used to de-duplicate the panic log.
var stderrRedirected bool

// hijackStderr replaces stderr with the given file descriptor.
//
// A client that wishes to use the original stderr must use
// OrigStderr defined above.
func hijackStderr(f *os.File) error {
	stderrRedirected = true
	return redirectStderr(f)
}

// restoreStderr cancels the effect of hijackStderr().
func restoreStderr() error {
	stderrRedirected = false
	return redirectStderr(OrigStderr)
}

// RecoverAndReportPanic can be invoked on goroutines that run with
// stderr redirected to logs to ensure the user gets informed on the
// real stderr a panic has occurred.
func RecoverAndReportPanic() {
	if r := recover(); r != nil {
		ReportPanic(r)
		panic(r)
	}
}

// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(r interface{}) {
	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	Flush()

	if stderrRedirected {
		// The panic message will go to "stderr" which is actually the log
		// file. Copy it to the real stderr to give the user a chance to
		// see it.
		fmt.Fprintln(OrigStderr, r)
	} else {
		// We're not redirecting stderr at this point, so the panic
		// message should be printed below. However we're not very strict
		// in this package about whether "stderrRedirected" is accurate,
		// so hint the user that they may still need to look at the log
		// file.
		fmt.Fprintln(OrigStderr, "\nERROR: a panic has occurred!\n"+
			"If no details are printed below, check the log file for details.")
	}
}
