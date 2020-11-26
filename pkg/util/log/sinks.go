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
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// logSink abstracts the destination of logging events, after all
// their details have been collected into a logpb.Entry.
//
// Each logger can have zero or more logSinks attached to it.
type logSink interface {
	// activeAtSeverity returns true if this sink accepts entries at
	// severity sev or higher.
	activeAtSeverity(sev logpb.Severity) bool

	// attachHints attaches some hints about the location of the message
	// to the stack message.
	attachHints([]byte) []byte

	// getFormatter retrieves the entry formatter for this sink.
	getFormatter() logFormatter

	// output emits some formatted bytes to this sink.
	// the sink is invited to perform an extra flush if indicated
	// by the argument. This is set to true for e.g. Fatal
	// entries.
	//
	// The parent logger's outputMu is held during this operation: log
	// sinks must not recursively call into logging when implementing
	// this method.
	output(extraSync bool, b []byte) error

	// exitCode returns the exit code to use if the logger decides
	// to terminate because of an error in output().
	exitCode() exit.Code

	// emergencyOutput attempts to emit some formatted bytes, and
	// ignores any errors.
	//
	// The parent logger's outputMu is held during this operation: log
	// sinks must not recursively call into logging when implementing
	// this method.
	emergencyOutput([]byte)
}

var _ logSink = (*stderrSink)(nil)
var _ logSink = (*fileSink)(nil)
