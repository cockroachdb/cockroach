// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cli

import "os"

// drainSignals are the signals that will cause the server to drain and exit.
var drainSignals = []os.Signal{os.Interrupt}

func handleSignalDuringShutdown(os.Signal) {
	// Windows doesn't indicate whether a process exited due to a signal in the
	// exit code, so we don't need to do anything but exit with a failing code.
	// The error message has already been printed.
	os.Exit(1)
}

func maybeRerunBackground() (bool, error) {
	return false, nil
}
