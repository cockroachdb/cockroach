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

import "github.com/cockroachdb/cockroach/pkg/cli/exit"

//go:generate mockgen -package=log -source=sinks.go -destination=mock_generated.go -mock_names=logSink=MockLogSink logSink

// sinkOutputOptions provides various options for a logSink.output call.
type sinkOutputOptions struct {
	// extraFlush invites an explicit flush of any buffering.
	extraFlush bool
	// ignoreErrors disables internal error handling (i.e. fail fast).
	ignoreErrors bool
	// forceSync forces synchronous operation of this output operation.
	// That is, it will block until the output has been handled.
	forceSync bool
}

// logSink abstracts the destination of logging events, after all
// their details have been collected into a logpb.Entry.
//
// Each logger can have zero or more logSinks attached to it.
type logSink interface {
	// active returns true if this sink is currently active.
	active() bool

	// attachHints attaches some hints about the location of the message
	// to the stack message.
	attachHints([]byte) []byte

	// output emits some formatted bytes to this sink.
	// the sink is invited to perform an extra flush if indicated
	// by the argument. This is set to true for e.g. Fatal
	// entries.
	//
	// The parent logger's outputMu is held during this operation: log
	// sinks must not recursively call into logging when implementing
	// this method.
	output(b []byte, opts sinkOutputOptions) error

	// exitCode returns the exit code to use if the logger decides
	// to terminate because of an error in output().
	exitCode() exit.Code

	// emergencyOutput attempts to emit some formatted bytes, and
	// ignores any errors.
	//
	// The parent logger's outputMu is held during this operation: log
	// sinks must not recursively call into logging when implementing
	// this method.
	// emergencyOutput([]byte)
}

var _ logSink = (*stderrSink)(nil)
var _ logSink = (*fileSink)(nil)
var _ logSink = (*fluentSink)(nil)
var _ logSink = (*httpSink)(nil)
var _ logSink = (*bufferSink)(nil)
