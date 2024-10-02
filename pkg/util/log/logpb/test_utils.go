// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logpb

import (
	"github.com/cockroachdb/cockroach/pkg/util/jsonbytes"
	"github.com/cockroachdb/redact"
)

// TestingStructuredLogEvent is an implementation of EventPayload for use in
// tests, in order to avoid importing the eventpb package.
type TestingStructuredLogEvent struct {
	CommonEventDetails
	Channel
	Event string
}

var _ EventPayload = (*TestingStructuredLogEvent)(nil)

// CommonDetails is part of the EventPayload interface.
func (f TestingStructuredLogEvent) CommonDetails() *CommonEventDetails {
	return &f.CommonEventDetails
}

// LoggingChannel is part of the EventPayload interface.
func (f TestingStructuredLogEvent) LoggingChannel() Channel {
	return f.Channel
}

// AppendJSONFields is part of the EventPayload interface.
func (f TestingStructuredLogEvent) AppendJSONFields(
	printComma bool, b redact.RedactableBytes,
) (bool, redact.RedactableBytes) {
	printComma, b = f.CommonEventDetails.AppendJSONFields(printComma, b)
	if f.Event != "" {
		if printComma {
			b = append(b, ',')
		}
		printComma = true
		b = append(b, "\"Event\":\""...)
		b = append(b, redact.StartMarker()...)
		b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(f.Event)))))
		b = append(b, redact.EndMarker()...)
		b = append(b, '"')
	}
	return printComma, b
}
