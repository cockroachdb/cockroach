// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
)

// drainSignals are the signals that will cause the server to drain and exit.
var drainSignals = []os.Signal{os.Interrupt}

// termSignal is the signal that causes an idempotent graceful
// shutdown (i.e. second occurrence does not incur hard shutdown).
var termSignal os.Signal = nil

// quitSignal is the signal to recognize to dump Go stacks.
var quitSignal os.Signal = nil

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

func useUnixSocketsInDemo() bool {
	// No unix sockets on windows.
	return false
}
