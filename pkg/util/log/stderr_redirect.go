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

import "os"

// OrigStderr points to the original stderr stream.
var OrigStderr = func() *os.File {
	fd, err := dupFD(os.Stderr.Fd())
	if err != nil {
		panic(err)
	}

	return os.NewFile(fd, os.Stderr.Name())
}()

// stderrRedirected returns true if and only if logging captures
// stderr output to the log file. This is used e.g. by Shout() to
// determine whether to report to standard error in addition to logs.
func (l *loggingT) stderrRedirected() bool {
	return l.stderrThreshold > Severity_INFO && !l.noStderrRedirect
}

// hijackStderr replaces stderr with the given file descriptor.
//
// A client that wishes to use the original stderr must use
// OrigStderr defined above.
func hijackStderr(f *os.File) error {
	return redirectStderr(f)
}

// restoreStderr cancels the effect of hijackStderr().
func restoreStderr() error {
	return redirectStderr(OrigStderr)
}
