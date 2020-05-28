// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"strings"
	"time"
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
