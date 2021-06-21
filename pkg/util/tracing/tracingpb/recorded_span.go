// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracingpb

import (
	"fmt"
	"strings"
	"time"

	types "github.com/gogo/protobuf/types"
)

// LogMessageField is the field name used for the opentracing.Span.LogFields()
// for a log message.
const LogMessageField = "event"

func (s *RecordedSpan) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("=== %s (id: %d parent: %d)\n", s.Operation, s.SpanID, s.ParentSpanID))
	for _, ev := range s.Logs {
		sb.WriteString(fmt.Sprintf("%s %s\n", ev.Time.UTC().Format(time.RFC3339Nano), ev.Msg()))
	}
	return sb.String()
}

// Structured visits the data passed to RecordStructured for the Span from which
// the RecordedSpan was created.
func (s *RecordedSpan) Structured(visit func(*types.Any, time.Time)) {
	// Check if the RecordedSpan is from a node running a version less than 21.2.
	// If it is, we set the "recorded at" time to the StartTime of the span.
	// TODO(adityamaru): Remove this code in 22.1 since all RecordedSpans will
	// have StructuredRecords in 21.2+ nodes.
	if s.StructuredRecords == nil {
		for _, item := range s.DeprecatedInternalStructured {
			visit(item, s.StartTime)
		}
		return
	}
	for _, sr := range s.StructuredRecords {
		visit(sr.Payload, sr.Time)
	}
}

// Msg extracts the message of the LogRecord, which is either in an "event" or
// "error" field.
func (l LogRecord) Msg() string {
	for _, f := range l.Fields {
		key := f.Key
		if key == LogMessageField {
			return f.Value
		}
		if key == "error" {
			return fmt.Sprint("error:", f.Value)
		}
	}
	return ""
}
