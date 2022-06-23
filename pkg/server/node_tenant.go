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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TraceRedactedMarker is used to replace logs that weren't redacted.
const TraceRedactedMarker = redact.RedactableString("verbose trace message redacted")

// redactRecordingForTenant redacts the sensitive parts of log messages in the
// recording if the tenant to which this recording is intended is not the system
// tenant (the system tenant gets an. See https://github.com/cockroachdb/cockroach/issues/70407.
// The recording is modified in place.
//
// tenID is the tenant that will receive this recording.
func redactRecordingForTenant(tenID roachpb.TenantID, rec tracingpb.Recording) error {
	if tenID == roachpb.SystemTenantID {
		return nil
	}
	for i := range rec {
		sp := &rec[i]
		sp.Tags = nil // TODO(benbardin): Remove for 23.1.
		sp.TagGroups = nil
		for j := range sp.Logs {
			record := &sp.Logs[j]
			if record.Message != "" && !sp.RedactableLogs {
				// If Message is set, the record should have been produced by a 22.1
				// node that also sets RedactableLogs.
				return errors.AssertionFailedf(
					"recording has non-redactable span with the Message field set: %s", sp)
			}
			record.Message = record.Message.Redact()

			// For compatibility with old versions, also redact DeprecatedFields.
			for k := range record.DeprecatedFields {
				field := &record.DeprecatedFields[k]
				if field.Key != tracingpb.LogMessageField {
					// We don't have any of these fields, but let's not take any
					// chances (our dependencies might slip them in).
					field.Value = TraceRedactedMarker
					continue
				}
				if !sp.RedactableLogs {
					// If we're handling a span that originated from an (early patch
					// release) 22.1 node, all the containing information will be
					// stripped. Note that this is not the common path here, as most
					// information in the trace will be from the local node, which
					// always creates redactable logs.
					field.Value = TraceRedactedMarker
					continue
				}
				field.Value = field.Value.Redact()
			}
		}
	}
	return nil
}
