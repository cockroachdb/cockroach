// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// FindMsgInRecording returns the index of the first Span containing msg in its
// logs, or -1 if no Span is found.
func FindMsgInRecording(recording Recording, msg string) int {
	for i, sp := range recording {
		if LogsContainMsg(sp, msg) {
			return i
		}
	}
	return -1
}

// LogsContainMsg returns true if a Span's logs contain the given message.
func LogsContainMsg(sp tracingpb.RecordedSpan, msg string) bool {
	for _, l := range sp.Logs {
		// NOTE: With out logs, each LogRecord has a single field ("event") and
		// value.
		for _, f := range l.Fields {
			if strings.Contains(f.Value, msg) {
				return true
			}
		}
	}
	return false
}
