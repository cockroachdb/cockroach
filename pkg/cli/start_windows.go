// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
)

// DrainSignals are the signals that will cause the server to drain and exit.
var DrainSignals = []os.Signal{os.Interrupt}

// termSignal is the signal that causes an idempotent graceful
// shutdown (i.e. second occurrence does not incur hard shutdown).
var termSignal os.Signal = nil

// quitSignal is the signal to recognize to dump Go stacks.
var quitSignal os.Signal = nil

// debugSignal is the signal to open a pprof debugging server.
var debugSignal os.Signal = nil

// exitAbruptlySignal is the signal to make the process exit immediately. It is
// preferable to SIGKILL when running with coverage instrumentation because the
// coverage profile gets dumped on exit.
var exitAbruptlySignal os.Signal = nil

const backgroundFlagDefined = false

func handleSignalDuringShutdown(os.Signal) {
	// Windows doesn't indicate whether a process exited due to a signal in the
	// exit code, so we don't need to do anything but exit with a failing code.
	// The error message has already been printed.
	exit.WithCode(exit.UnspecifiedError())
}

func maybeRerunBackground() (bool, error) {
	return false, nil
}

func disableOtherPermissionBits() {
	// No-op on windows, which does not support umask.
}

func closeAllSockets() {
	// No-op on windows.
	// TODO(jackson): Is there something else we can do on Windows?
}
