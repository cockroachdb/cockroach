// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !windows
// +build !windows

package cli

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
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

// debugSignal is the signal to open a pprof debugging server.
var debugSignal os.Signal = unix.SIGUSR2

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

// findGoodNotifyDir determines a good target directory
// to create the unix socket used to signal successful
// background startup (via sdnotify).
// A directory is "good" if it seems writable and its
// name is short enough.
func findGoodNotifyDir() (string, error) {
	goodEnough := func(s string) bool {
		if len(s) >= 104-1-len("sdnotify/notify.sock")-10 {
			// On BSD, binding to a socket is limited to a path length of 104 characters
			// (including the NUL terminator). In glibc, this limit is 108 characters.
			// macOS also has a tendency to produce very long temporary directory names.
			return false
		}
		st, err := os.Stat(s)
		if err != nil {
			return false
		}
		if !st.IsDir() || st.Mode()&0222 == 0 /* any write bits? */ {
			// Note: we're confident the directory is unsuitable if none of the
			// write bits are set, however there could be a false positive
			// if some write bits are set.
			//
			// For example, if the process runs as a UID that does not match
			// the directory owner UID or GID, and the write mode is 0220 or
			// less, the sd socket creation will still fail.
			// As such, the mode check here is merely a heuristic. We're
			// OK with that: the actual write failure will produce a clear
			// error message.
			return false
		}
		return true
	}

	// Was --socket-dir configured? Try to use that.
	if serverSocketDir != "" && goodEnough(serverSocketDir) {
		return serverSocketDir, nil
	}
	// Do we have a temp directory? Try to use that.
	if tmpDir := os.TempDir(); goodEnough(tmpDir) {
		return tmpDir, nil
	}
	// Can we perhaps use the current directory?
	if cwd, err := os.Getwd(); err == nil && goodEnough(cwd) {
		return cwd, nil
	}

	// Note: we do not attempt to use the configured on-disk store
	// directory(ies), because they may point to a filesystem that does
	// not support unix sockets.

	return "", errors.WithHintf(
		errors.Newf("no suitable directory found for the --background notify socket"),
		"Avoid using --%s altogether (preferred), or use a shorter directory name "+
			"for --socket-dir, TMPDIR or the current directory.", cliflags.Background.Name)
}

func maybeRerunBackground() (bool, error) {
	if startBackground {
		notifyDir, err := findGoodNotifyDir()
		if err != nil {
			return true, err
		}

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

		return true, sdnotify.Exec(cmd, notifyDir)
	}
	return false, nil
}

func disableOtherPermissionBits() {
	mask := unix.Umask(0000)
	mask |= 00007
	_ = unix.Umask(mask)
}
