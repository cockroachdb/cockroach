// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package cli

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"golang.org/x/sys/unix"
)

// drainSignals are the signals that will cause the server to drain and exit.
//
// If two drain signals are seen, the second drain signal will be reraised
// without a signal handler. The default action of any signal listed here thus
// must terminate the process.
var drainSignals = []os.Signal{unix.SIGINT, unix.SIGTERM}

// termSignal is the signal that causes an idempotent graceful
// shutdown (i.e. second occurrence does not incur hard shutdown).
var termSignal os.Signal = unix.SIGTERM

// quitSignal is the signal to recognize to dump Go stacks.
var quitSignal os.Signal = unix.SIGQUIT

func handleSignalDuringShutdown(sig os.Signal) {
	// On Unix, a signal that was not handled gracefully by the application
	// should be reraised so it is visible in the exit code.

	// Reset signal to its original disposition.
	signal.Reset(sig)

	// Reraise the signal. os.Signal is always sysutil.Signal.
	if err := unix.Kill(unix.Getpid(), sig.(sysutil.Signal)); err != nil {
		// Sending a valid signal to ourselves should never fail.
		//
		// Unfortunately it appears (#34354) that some users
		// run CockroachDB in containers that only support
		// a subset of all syscalls. If this ever happens, we
		// still need to quit immediately.
		log.Fatalf(context.Background(), "unable to forward signal %v: %v", sig, err)
	}

	// Block while we wait for the signal to be delivered.
	select {}
}

const backgroundFlagDefined = true

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

		// Notify to ourselves that we're restarting.
		_ = os.Setenv(backgroundEnvVar, "1")

		return true, sdnotify.Exec(cmd)
	}
	return false, nil
}

func disableOtherPermissionBits() {
	mask := unix.Umask(0000)
	mask |= 00007
	_ = unix.Umask(mask)
}

func useUnixSocketsInDemo() bool {
	return true
}
