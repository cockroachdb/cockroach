// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows

package cli

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// DrainSignals is the list of signals that trigger the start of a shutdown
// sequence ("server drain").
//
// The first time they're received, both signals initiate a drain just the same.
// The behavior between the two differs if they're received a second time (or,
// more generally, after the drain had started):
// - a second SIGTERM is ignored.
// - a second SIGINT terminates the process abruptly.
var DrainSignals = []os.Signal{unix.SIGINT, unix.SIGTERM}

// termSignal is the signal that causes an idempotent graceful
// shutdown (i.e. second occurrence does not incur hard shutdown).
var termSignal os.Signal = unix.SIGTERM

// quitSignal is the signal to recognize to dump Go stacks.
var quitSignal os.Signal = unix.SIGQUIT

// debugSignal is the signal to open a pprof debugging server.
var debugSignal os.Signal = unix.SIGUSR2

// exitAbruptlySignal is the signal to make the process exit immediately. It is
// preferable to SIGKILL when running with coverage instrumentation because the
// coverage profile gets dumped on exit.
var exitAbruptlySignal os.Signal = unix.SIGUSR1

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

// closeAllSockets is used in the event of a disk stall, in which case we want
// to terminate the process but may not be able to. A process stalled in disk
// I/O is in uninterruptible sleep within the kernel and cannot be terminated.
// If we can't terminate the process, the next best thing is to quarantine it by
// closing all sockets so that it appears dead to other nodes.
//
// See log.SetMakeProcessUnavailableFunc.
func closeAllSockets() {
	// Close all sockets twice. A LISTEN socket may open a new socket after we
	// list all FDs. If that's the case, the socket will be closed by the second
	// call.
	//
	// TODO(jackson,#96342): This doesn't prevent the retry mechanisms from
	// opening new outgoing connections. Consider marking the rpc.Context as
	// poisoned to prevent new outgoing connections.

	_ = closeAllSocketsOnce()
	_ = closeAllSocketsOnce()

	// It's unclear what to do with errors. We try to close all sockets in an
	// emergency where we can't exit the process but want to quarantine it by
	// removing all communication with the outside world. If we fail to close
	// all sockets, panicking is unlikely to be able to terminate the process.
	// We do nothing so that if the log sink is NOT stalled, we'll write the
	// disk stall log entry.
}

func closeAllSocketsOnce() error {
	fds, err := findOpenSocketFDs()
	// NB: Intentionally ignore `err`. findOpenSocketFDs may return a non-empty
	// slice of FDs with a non-nil error. We want to close the descriptors we
	// were able to identify regardless of any error.
	for _, fd := range fds {
		// Ignore errors so that if we can't close all sockets, we close as many
		// as we can. When finished, return a combined error.
		fdErr := unix.Shutdown(fd, unix.SHUT_RDWR)
		err = errors.CombineErrors(err, fdErr)
	}
	return err
}

func findOpenSocketFDs() ([]int, error) {
	f, err := os.Open("/dev/fd")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dirnames, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var fds []int
	for _, name := range dirnames {
		// From the Linux /proc/[pid]/fd man page:
		//
		//   For file descriptors for pipes and sockets, the entries
		//   will be symbolic links whose content is the file type with
		//   the inode.  A readlink(2) call on this file returns a
		//   string in the format:
		//
		//     type:[inode]
		//
		//   For example, socket:[2248868] will be a socket and its
		//   inode is 2248868.  For sockets, that inode can be used to
		//   find more information in one of the files under
		//   /proc/net/.
		//
		// We `readlink` each directory entry, and check that the destination
		// has the `socket:` prefix.
		dst, readLinkErr := os.Readlink(filepath.Join("/dev/fd", name))
		if readLinkErr != nil {
			// Stumble forward.
			err = errors.CombineErrors(err, readLinkErr)
			continue
		}
		if !strings.HasPrefix(dst, "socket:") {
			continue
		}
		fd, atoiErr := strconv.Atoi(name)
		if atoiErr != nil {
			// Stumble forward.
			err = errors.CombineErrors(err, atoiErr)
			continue
		}
		fds = append(fds, fd)
	}
	return fds, err
}
