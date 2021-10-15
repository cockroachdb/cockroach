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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
)

const sRedactedMarker = "verbose trace message redacted"

func maybeRedactRecording(tenID roachpb.TenantID, rec tracing.Recording) {
	if tenID == roachpb.SystemTenantID {
		return
	}
	// For tenants, strip the verbose log messages. See:
	// https://github.com/cockroachdb/cockroach/issues/70407
	for i := range rec {
		sp := &rec[i]
		sp.Tags = nil
		for j := range sp.Logs {
			record := &sp.Logs[j]
			for k := range record.Fields {
				field := &record.Fields[k]

				if !sp.RedactableLogs {
					// If we're handling a span that does not support redactability, all
					// the containing information will be stripped.
					field.Value = sRedactedMarker
				} else if field.Key != tracingpb.LogMessageField {
					// We don't have any of these fields, but let's not take any
					// chances (our dependencies might slip them in).
					field.Value = tracingpb.MaybeRedactableString(redact.Sprint(redact.SafeString(sRedactedMarker)))
				} else {
					field.Value = tracingpb.MaybeRedactableString(redact.RedactableString(field.Value).Redact())
				}
			}
		}
	}
}
