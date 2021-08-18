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

import "github.com/cockroachdb/cockroach/pkg/cli/exit"

type mockSink struct{}

var _ logSink = (*mockSink)(nil)

func (*mockSink) active() bool {
	return true
}
func (*mockSink) attachHints(b []byte) []byte {
	return b
}
func (*mockSink) output(extraSync bool, b []byte) error {
	return nil
}
func (*mockSink) exitCode() exit.Code {
	return exit.Success()
}
func (*mockSink) emergencyOutput([]byte) {}

type mockLogFormatter struct{}

var _ logFormatter = (*mockLogFormatter)(nil)

func (*mockLogFormatter) formatterName() string {
	return "mock"
}
func (*mockLogFormatter) doc() string {
	return "mock"
}
func (*mockLogFormatter) formatEntry(entry logEntry) *buffer {
	buf := getBuffer()
	buf.WriteString(entry.payload.message)
	return buf
}
