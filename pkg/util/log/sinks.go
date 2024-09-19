// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import "github.com/cockroachdb/cockroach/pkg/cli/exit"

//go:generate mockgen -package=log -destination=mocks_generated_test.go --mock_names=TestingLogSink=MockLogSink . TestingLogSink

// TestingLogSink is exported for mock generation.
// This is painful, but it seems to be the only way, for the moment, to
// generate this mock.
//
// The reason is that there's no way to inject build tags into the current
// bazel rules for gomock.
type TestingLogSink = logSink

// sinkOutputOptions provides various options for a logSink.output call.
type sinkOutputOptions struct {
	// extraFlush invites an explicit flush of any buffering.
	extraFlush bool
	// ignoreErrors disables internal error handling (i.e. fail fast).
	ignoreErrors bool
	// tryForceSync attempts to force a synchronous operation of this
	// output operation. That is, it will block until the output has been
	// handled, so long as the underlying sink can support the operation at
	// that moment.
	//
	// This isn't an ironclad guarantee, but in the vast majority of
	// scenarios, this option will be honored.
	//
	// If a sink can't support a synchronous flush, it should do its
	// best to ensure a flush is imminent which will include the log
	// message that accompanies the tryForceSync option. It should also
	// give some indication that it was unable to do so.
	tryForceSync bool
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
var _ logSink = (*bufferedSink)(nil)
