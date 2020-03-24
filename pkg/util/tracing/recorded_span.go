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
)

// LogMessageField is the field name used for the opentracing.Span.LogFields()
// for a log message.
const LogMessageField = "event"

func (s *RecordedSpan) String() string {
	sb := strings.Builder{}
	for _, ev := range s.Logs {
		sb.WriteString(ev.String())
		sb.WriteRune('\n')
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
