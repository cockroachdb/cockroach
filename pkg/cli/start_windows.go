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

import "os"

// drainSignals are the signals that will cause the server to drain and exit.
var drainSignals = []os.Signal{os.Interrupt}

// quitSignal is the signal to recognize to dump Go stacks.
var quitSignal os.Signal = nil

func handleSignalDuringShutdown(os.Signal) {
	// Windows doesn't indicate whether a process exited due to a signal in the
	// exit code, so we don't need to do anything but exit with a failing code.
	// The error message has already been printed.
	os.Exit(1)
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
