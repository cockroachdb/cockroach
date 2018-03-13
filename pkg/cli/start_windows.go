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

package cli

import (
	"os"
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"golang.org/x/sys/unix"
)

// drainSignals are the signals that will cause the server to drain and exit.
var drainSignals = []os.Signal{os.Interrupt}

func reraiseSignal(sig os.Signal) error {
	// Reset signals to their original disposition.
	signal.Reset(drainSignals)

	// Reraise the signal. On Unix, os.Signal is always syscall.Signal.
	if err := unix.Kill(unix.Getpid(), sig.(sysutil.Signal)); err != nil {
		return err
	}

	// Block to wait for the signal to be delivered.
	select {}
}

func maybeRerunBackground() (bool, error) {
	return false, nil
}
