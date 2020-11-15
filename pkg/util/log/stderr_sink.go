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

// Type of a stderr copy sink.
type stderrSink struct {
	// the --no-color flag. When set it disables escapes code on the
	// stderr copy.
	noColor bool

	// Level at or beyond which entries are output to this sink.
	threshold Severity

	// formatter for entries written via this sink.
	formatter logFormatter
}

// output writes the provided entry and potential stack
// trace(s) to the process' external stderr stream.
func (l *stderrSink) output(b []byte) error {
	_, err := OrigStderr.Write(b)
	return err
}
