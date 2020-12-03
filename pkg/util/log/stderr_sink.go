// Copyright 2020 The Cockroach Authors.
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
func (l *stderrSink) output(_ bool, b []byte) error {
	_, err := OrigStderr.Write(b)
	return err
}

// exitCode implements the logSink interface.
func (l *stderrSink) exitCode() exit.Code {
	return exit.LoggingStderrUnavailable()
}

// emergencyOutput implements the logSink interface.
func (l *stderrSink) emergencyOutput(b []byte) {
	_, _ = OrigStderr.Write(b)
}
