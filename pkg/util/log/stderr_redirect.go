// Copyright 2017 The Cockroach Authors.
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
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// OrigStderr points to the original stderr stream.
var OrigStderr = func() *os.File {
	fd, err := dupFD(os.Stderr.Fd())
	if err != nil {
		panic(err)
	}

	return os.NewFile(fd, os.Stderr.Name())
}()

// LoggingToStderr returns true if log messages of the given severity
// sent to the main logger are also visible on stderr. This is used
// e.g. by the startup code to announce server details both on the
// external stderr and to the log file.
//
// This is also the logic used by Shout calls.
func LoggingToStderr(s Severity) bool {
	return s >= mainLog.stderrThreshold.get()
}

// hijackStderr replaces stderr with the given file descriptor.
//
// A client that wishes to use the original stderr (the process'
// external stderr stream) must use OrigStderr defined above.
func hijackStderr(f *os.File) error {
	return redirectStderr(f)
}

// restoreStderr cancels the effect of hijackStderr().
func restoreStderr() error {
	return redirectStderr(OrigStderr)
}

// osStderrMu ensures that concurrent redirects of stderr don't
// overwrite each other.
var osStderrMu syncutil.Mutex
