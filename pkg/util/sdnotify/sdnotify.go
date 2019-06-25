// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sdnotify implements both sides of the systemd readiness
// protocol. Servers can use sdnotify.Ready() to signal that they are
// ready to receive traffic, and process managers can use
// sdnotify.Exec() to run processes that implement this protocol.
package sdnotify

import "os/exec"

// Ready sends a readiness signal using the systemd notification
// protocol. It should be called (once) by a server after it has
// completed its initialization (including but not necessarily limited
// to binding ports) and is ready to receive traffic.
func Ready() error {
	return ready()
}

// Exec the given command in the background using the systemd
// notification protocol. This function returns once the command has
// either exited or signaled that it is ready. If the command exits
// with a non-zero status before signaling readiness, returns an
// exec.ExitError.
func Exec(cmd *exec.Cmd) error {
	return bgExec(cmd)
}
