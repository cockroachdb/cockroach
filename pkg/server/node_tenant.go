// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import "github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"

// redactRecording redacts the sensitive parts of log messages in the recording.
// It is used to record KV traces going to tenants other than the sytem tenant
// (the system tenant can receive unredacted recordings).
//
// Unstructured log messages get redacted and tags get stripped. Structured
// messages are left untouched (for better or worse).
// The unstructured log messages get completely dropped unless the
// `trace.redactable.enabled` cluster setting is set. This setting makes the log
// messages properly redactable (i.e. only the sensitive parts are wrapped in
// redaction markers) at some performance cost, whereas without it each log
// message is wholly wrapped in redaction markers.
//
// The recording is modified in place.
func redactRecording(rec tracingpb.Recording) {
	for i := range rec {
		sp := &rec[i]
		sp.TagGroups = nil
		for j := range sp.Logs {
			record := &sp.Logs[j]
			record.Message = record.Message.Redact()
		}
	}
}
