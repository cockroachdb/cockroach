// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var emergencyStopFunc struct {
	syncutil.Mutex
	fn func()
}

// EmergencyStop invokes the emergency stop function set through
// SetEmergencyStopFunc, if any. EmergencyStop is a hack to close network
// connections in the event of a disk stall that may prevent the process from
// exiting.
func EmergencyStop() {
	emergencyStopFunc.Lock()
	fn := emergencyStopFunc.fn
	emergencyStopFunc.Unlock()
	if fn != nil {
		fn()
	}
}

// SetEmergencyStopFunc sets a function that will be called when EmergencyStop
// is called.
func SetEmergencyStopFunc(fn func()) {
	emergencyStopFunc.Lock()
	emergencyStopFunc.fn = fn
	emergencyStopFunc.Unlock()
}

// SetExitFunc allows setting a function that will be called to exit
// the process when a Fatal message is generated. The supplied bool,
// if true, suppresses the stack trace, which is useful for test
// callers wishing to keep the logs reasonably clean.
//
// Use ResetExitFunc() to reset.
func SetExitFunc(hideStack bool, f func(exit.Code)) {
	if f == nil {
		panic("nil exit func invalid")
	}
	setExitErrFunc(hideStack, func(x exit.Code, err error) { f(x) })
}

// setExitErrFunc is like SetExitFunc but the function can also
// observe the error that is triggering the exit.
func setExitErrFunc(hideStack bool, f func(exit.Code, error)) {
	logging.mu.Lock()
	defer logging.mu.Unlock()

	logging.mu.exitOverride.f = f
	logging.mu.exitOverride.hideStack = hideStack
}

// ResetExitFunc undoes any prior call to SetExitFunc.
func ResetExitFunc() {
	logging.mu.Lock()
	defer logging.mu.Unlock()

	logging.mu.exitOverride.f = nil
	logging.mu.exitOverride.hideStack = false
}

// exitLocked is called if there is trouble creating or writing log files, or
// writing to stderr. It flushes the logs and exits the program; there's no
// point in hanging around.
//
// l.outputMu is held; l.fileSink.mu is not held; logging.mu is not held.
func (l *loggerT) exitLocked(err error, code exit.Code) {
	l.outputMu.AssertHeld()

	l.reportErrorEverywhereLocked(context.Background(), err)

	logging.mu.Lock()
	f := logging.mu.exitOverride.f
	logging.mu.Unlock()
	if f != nil {
		f(code, err)
	} else {
		exit.WithCode(code)
	}
}

// reportErrorEverywhereLocked writes the error details to both the
// process' original stderr and the log file if configured.
//
// This assumes l.outputMu is held, but l.fileSink.mu is not held.
func (l *loggerT) reportErrorEverywhereLocked(ctx context.Context, err error) {
	// Make a valid log entry for this error.
	entry := makeUnstructuredEntry(
		ctx, severity.ERROR, channel.OPS,
		2,    /* depth */
		true, /* redactable */
		"logging error: %v", err)

	// Either stderr or our log file is broken. Try writing the error to both
	// streams in the hope that one still works or else the user will have no idea
	// why we crashed.
	//
	// Note that we're already in error. If an additional error is encountered
	// here, we can't do anything but raise our hands in the air.

	// TODO(knz): we may want to push this information to more channels,
	// e.g. to the OPS channel, if we are reporting an error while writing
	// to a non-OPS channel.

	for _, s := range l.sinkInfos {
		sink := s.sink
		if logpb.Severity_ERROR >= s.threshold.get(entry.ch) && sink.active() {
			buf := s.formatter.formatEntry(entry)
			_ = sink.output(buf.Bytes(), sinkOutputOptions{ignoreErrors: true})
			putBuffer(buf)
		}
	}
}

// MustCloseAllSockets is used in the event of a disk stall, in which we want to
// terminate the process but may not be able to. A process stalled in disk I/O
// cannot be terminated. If we can't terminate the process, the next best thing
// is to quarantine it by closing all sockets so that it appears dead to other
// nodes.
//
// See SetEmergencyStopFunc.
func MustCloseAllSockets() {
	// Close all sockets twice. A LISTEN socket may open a new socket after we
	// list all FDs. If that's the case, it'll be closed by the second call.
	_ = closeAllSockets()
	_ = closeAllSockets()

	// It's unclear what to do with errors. We try to close all sockets in an
	// emergency where we can't exit the process but want to quarantine it by
	// removing all communication with the outside world. If we fail to close
	// all sockets, panicking is unlikely to be able to terminate the process.
	// We do nothing so that if the log sink is NOT stalled, we'll write the
	// disk stall log entry.
}

func closeAllSockets() error {
	fds, err := findOpenSocketFDs()
	// NB: Intentionally ignore `err`. findOpenSocketFDs may return a non-empty
	// slice of FDs with a non-nil error. We want to close the descriptors we
	// were able to identify regardless of any error.
	for _, fd := range fds {
		// Ignore errors so that if we can't close all sockets, we close as many
		// as we can. When finished, return the first error observed.
		if fdErr := forceCloseSocket(fd); err == nil {
			err = fdErr
		}
	}
	return err
}

func findOpenSocketFDs() ([]int, error) {
	f, err := os.Open("/proc/self/fd")
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
		dst, readLinkErr := os.Readlink(filepath.Join("/proc/self/fd", name))
		if readLinkErr != nil {
			// Stumble forward.
			err = readLinkErr
			continue
		}
		if !strings.HasPrefix(dst, "socket:") {
			continue
		}
		fd, atoiErr := strconv.Atoi(name)
		if atoiErr != nil {
			// Stumble forward.
			err = errors.Wrapf(atoiErr, "fd %q; dst %q", name, dst)
			continue
		}
		fds = append(fds, fd)
	}
	return fds, err
}
