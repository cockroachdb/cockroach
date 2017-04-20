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
	"runtime/debug"

	"golang.org/x/net/context"
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
func RecoverAndReportPanic(ctx context.Context) {
	if r := recover(); r != nil {
		ReportPanic(ctx, r)
		panic(r)
	}
}

// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(ctx context.Context, r interface{}) {
	Shout(ctx, Severity_ERROR, "a panic has occurred!")

	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	Flush()

	// We do not use Shout() to print the panic details here, because
	// if stderr is not redirected (e.g. when logging to file is
	// disabled) Shout() would copy its argument to stderr
	// unconditionally, and we don't want that: Go's runtime system
	// already unconditonally copies the panic details to stderr.
	// Instead, we copy manually the details to stderr, only when stderr
	// is redirected to a file otherwise.
	if stderrRedirected {
		fmt.Fprintf(OrigStderr, "%v\n\n%s\n", r, debug.Stack())
	}
}
