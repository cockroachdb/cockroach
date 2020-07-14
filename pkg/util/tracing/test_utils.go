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

import "strings"

// FindMsgInRecording returns the index of the first span containing msg in its
// logs, or -1 if no span is found.
func FindMsgInRecording(recording Recording, msg string) int {
	for i, sp := range recording {
		if logsContainMsg(sp.Logs, msg) {
			return i
		}
	}
	return -1
}

func logsContainMsg(logs []LogRecord, msg string) bool {
	for _, l := range logs {
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

// OnlyFollowerReads looks through all the RPCs and asserts that every single
// one resulted in a follower read. Returns false if no RPCs are found.
func OnlyFollowerReads(rec Recording) bool {
	foundFollowerRead := false
	for _, sp := range rec {
		if sp.Operation == "/cockroach.roachpb.Internal/Batch" &&
			sp.Tags["span.kind"] == "server" {
			if logsContainMsg(sp.Logs, "serving via follower read") {
				foundFollowerRead = true
			} else {
				return false
			}
		}
	}
	return foundFollowerRead
}
