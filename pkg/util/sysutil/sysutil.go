// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sysutil is a cross-platform compatibility layer on top of package
// syscall. It exposes APIs for common operations that require package syscall
// and re-exports several symbols from package syscall that are known to be
// safe. Using package syscall directly from other packages is forbidden.
package sysutil

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// Signal is syscall.Signal.
type Signal = syscall.Signal

// Errno is syscall.Errno.
type Errno = syscall.Errno

// ExitStatus returns the exit status contained within an exec.ExitError.
func ExitStatus(err *exec.ExitError) int {
	// err.Sys() is of type syscall.WaitStatus on all supported platforms.
	// syscall.WaitStatus has a different type on Windows, but that type has an
	// ExitStatus method with an identical signature, so no need for conditional
	// compilation.
	return err.Sys().(syscall.WaitStatus).ExitStatus()
}

const refreshSignal = syscall.SIGHUP

// RefreshSignaledChan returns a channel that will receive an os.Signal whenever
// the process receives a "refresh" signal (currently SIGHUP). A refresh signal
// indicates that the user wants to apply nondisruptive updates, like reloading
// certificates and flushing log files.
//
// On Windows, the returned channel will never receive any values, as Windows
// does not support signals. Consider exposing a refresh trigger through other
// means if Windows support is important.
func RefreshSignaledChan() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, refreshSignal)
	return ch
}
