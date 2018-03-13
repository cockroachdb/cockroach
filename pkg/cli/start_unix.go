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

// +build !windows

package cli

import (
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

// drainSignals are the signals that will cause the server to drain and exit.
//
// If two drain signals are seen, the second drain signal will be reraised
// without a signal handler. The default action of any signal listed here thus
// must terminate the process.
var drainSignals = []os.Signal{unix.SIGINT, unix.SIGTERM, unix.SIGQUIT}

func reraiseSignal(sig os.Signal) error {
	// Reset drain signals to their original disposition.
	signal.Reset(drainSignals)

	// Reraise the signal. On Unix, os.Signal is always syscall.Signal.
	if err := unix.Kill(unix.Getpid(), sig.(sysutil.Signal)); err != nil {
		return err
	}

	// Block to wait for the signal to be delivered.
	select {}
}

// exitCodeForSignal returns the exit code that should be used to indicate that
// the process exited in response to the signal.
func exitCodeForSignal(sig os.Signal) int {
	// On Unix, os.Signal is sysutil.Signal and it's convertible to int.
	return 128 + int(sig.(sysutil.Signal))
}

var startBackground bool

func init() {
	BoolFlag(StartCmd.Flags(), &startBackground, cliflags.Background, false)
}

func maybeRerunBackground() (bool, error) {
	if startBackground {
		args := make([]string, 0, len(os.Args))
		foundBackground := false
		for _, arg := range os.Args {
			if arg == "--background" || strings.HasPrefix(arg, "--background=") {
				foundBackground = true
				continue
			}
			args = append(args, arg)
		}
		if !foundBackground {
			args = append(args, "--background=false")
		}
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = stderr
		return true, sdnotify.Exec(cmd)
	}
	return false, nil
}
