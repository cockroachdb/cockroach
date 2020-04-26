// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/logtags"
)

// formatTags appends the tags to a strings.Builder. If there are no tags,
// returns false.
func formatTags(ctx context.Context, buf *strings.Builder) bool {
	tags := logtags.FromContext(ctx)
	if tags == nil {
		return false
	}
	buf.WriteByte('[')
	tags.FormatToString(buf)
	buf.WriteString("] ")
	return true
}

// MakeMessage creates a structured log entry.
func MakeMessage(ctx context.Context, format string, args []interface{}) string {
	var buf strings.Builder
	formatTags(ctx, &buf)
	annotateUnsafe(&buf, args)
	if len(args) == 0 {
		buf.WriteString(format)
	} else if len(format) == 0 {
		fmt.Fprint(&buf, args...)
	} else {
		fmt.Fprintf(&buf, format, args...)
	}
	return buf.String()
}

func annotateUnsafe(buf *strings.Builder, args []interface{}) {
	for i := range args {
		if _, ok := args[i].(SafeType); !ok {
			args[i] = &redactable{args[i]}
		}
	}
}

type redactable struct {
	arg interface{}
}

func (r *redactable) Format(s fmt.State, verb rune) {
	_, _ = s.Write(startRedact)
	fmt.Fprintf(s, fmt.Sprintf("%%%c", verb), r.arg)
	_, _ = s.Write(endRedact)
}

var startRedact = []byte{0x0E}
var endRedact = []byte{0x0F}

// addStructured creates a structured log entry to be written to the
// specified facility of the logger.
func addStructured(ctx context.Context, s Severity, depth int, format string, args []interface{}) {
	file, line, _ := caller.Lookup(depth + 1)
	msg := MakeMessage(ctx, format, args)

	if s == Severity_FATAL {
		// We load the ReportingSettings from the a global singleton in this
		// call path. See the singleton's comment for a rationale.
		if sv := settings.TODO(); sv != nil {
			SendCrashReport(ctx, sv, depth+2, format, args, ReportTypePanic)
		}
	}
	// MakeMessage already added the tags when forming msg, we don't want
	// eventInternal to prepend them again.
	eventInternal(ctx, (s >= Severity_ERROR), false /*withTags*/, "%s:%d %s", file, line, msg)
	mainLog.outputLogEntry(s, file, line, msg)
}
