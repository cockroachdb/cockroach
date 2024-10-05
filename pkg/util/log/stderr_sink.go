// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Type of a stderr copy sink.
type stderrSink struct {
	// the --no-color flag. When set it disables escapes code on the
	// stderr copy.
	noColor syncutil.AtomicBool
}

// activeAtSeverity implements the logSink interface.
func (l *stderrSink) active() bool { return true }

// attachHints implements the logSink interface.
func (l *stderrSink) attachHints(stacks []byte) []byte {
	return stacks
}

// output implements the logSink interface.
func (l *stderrSink) output(b []byte, _ sinkOutputOptions) error {
	_, err := OrigStderr.Write(b)
	return err
}

// exitCode implements the logSink interface.
func (l *stderrSink) exitCode() exit.Code {
	return exit.LoggingStderrUnavailable()
}
