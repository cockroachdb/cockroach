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
//

package log

import (
	"fmt"
	"os"
	"syscall"
)

// OrigStderrFd is a file descriptor that is initialized to a copy of
// the original stderr file descriptor 2, before file descriptor 2 is
// replaced to point to a log file by hijackStderr().
var OrigStderrFd int

// OrigStderr points to the original stderr stream.
var OrigStderr *os.File

func init() {
	var err error
	OrigStderrFd, err = syscall.Dup(syscall.Stderr)
	if err != nil {
		panic(err)
	}
	OrigStderr = os.NewFile(uintptr(OrigStderrFd), "/dev/stderr")
	if OrigStderr == nil {
		panic(err)
	}
}

// hijackStderr replaces syscall.Stderr (and thus the target of
// os.Stderr and pretty much anything that targets stderr using
// standard ways) by the given file descriptor.
// A client that wishes to use the original stderr must use
// OrigStderrFd / OrigStderr defined above.
func hijackStderr(fd int) error {
	return syscall.Dup2(fd, syscall.Stderr)
}

// restoreStderr cancels the effect of hijackStderr()
func restoreStderr() error {
	return syscall.Dup2(OrigStderrFd, syscall.Stderr)
}

// RecoverAndReportPanic can be invoked on goroutines that run with
// stderr redirected to logs to ensure the user gets informed on the
// real stderr a panic has occurred.
func RecoverAndReportPanic() {
	if r := recover(); r != nil {
		// Ensure that the logs are flushed before letting a panic
		// terminate the server.
		Flush()

		fmt.Fprintln(OrigStderr, "\nERROR: a panic has occurred!\n"+
			"If no details are printed below, check the log file for details.")
		panic(r)
	}
}
