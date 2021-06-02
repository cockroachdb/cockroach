// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// TraceToJSON returns the string representation of the trace in JSON format.
//
// TraceToJSON assumes that the first span in the recording contains all the
// other spans.
func TraceToJSON(trace Recording) (string, error) {
	root := normalizeSpan(trace[0], trace)
	marshaller := jsonpb.Marshaler{
		Indent: "\t",
	}
	str, err := marshaller.MarshalToString(&root)
	if err != nil {
		return "", err
	}
	return str, nil
}

func normalizeSpan(s tracingpb.RecordedSpan, trace Recording) tracingpb.NormalizedSpan {
	var n tracingpb.NormalizedSpan
	n.Operation = s.Operation
	n.StartTime = s.StartTime
	n.Duration = s.Duration
	n.Tags = s.Tags
	n.Logs = s.Logs
	n.StructuredRecords = s.StructuredRecords

	for _, ss := range trace {
		if ss.ParentSpanID != s.SpanID {
			continue
		}
		n.Children = append(n.Children, normalizeSpan(ss, trace))
	}
	return n
}

// MessageToJSONString converts a protocol message into a JSON string. The
// emitDefaults flag dictates whether fields with zero values are rendered or
// not.
func MessageToJSONString(msg protoutil.Message, emitDefaults bool) (string, error) {
	// Convert to json.
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return "", errors.Newf("error when converting %s to JSON string", proto.MessageName(msg))
	}

	return msgJSON, nil
}

// RedactAndTruncateError redacts the error and truncates the string
// representation of the error to a fixed length.
func RedactAndTruncateError(err error) string {
	maxErrLength := 250
	redactedErr := string(redact.Sprintf("%v", err))
	if len(redactedErr) < maxErrLength {
		maxErrLength = len(redactedErr)
	}
	return redactedErr[:maxErrLength]
}
