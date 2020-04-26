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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/errors"
)

// MakeMessage creates a structured log entry.
// TODO(knz): this should be deprecated, and MakeEntry
// used instead.
func MakeMessage(ctx context.Context, format string, args []interface{}) string {
	return makeMessageInternal(ctx, false, 0, format, args, false /*redactable*/)
}

func makeMessageInternal(
	ctx context.Context,
	addCounter bool,
	n uint64,
	format string,
	args []interface{},
	redactable bool,
) string {
	var buf strings.Builder
	if redactable {
		redactTags(ctx, &buf)
		annotateUnsafe(args)
	} else {
		formatTags(ctx, &buf)
	}
	if addCounter {
		buf.WriteString(strconv.FormatUint(n, 10))
		buf.WriteByte(' ')
	}
	if len(args) == 0 {
		buf.WriteString(format)
	} else if len(format) == 0 {
		fmt.Fprint(&buf, args...)
	} else {
		fmt.Fprintf(&buf, format, args...)
	}
	return buf.String()
}

// addStructured creates a structured log entry to be written to the
// specified facility of the logger.
func addStructured(ctx context.Context, s Severity, depth int, format string, args []interface{}) {
	if s == Severity_FATAL {
		// We load the ReportingSettings from the a global singleton in this
		// call path. See the singleton's comment for a rationale.
		if sv := settings.TODO(); sv != nil {
			err := errors.NewWithDepthf(depth+1, "fatal error: "+format, args...)
			sendCrashReport(ctx, sv, err, ReportTypePanic)
		}
	}

	file, line, _ := caller.Lookup(depth + 1)
	redactable := logging.redactableLogs
	msg := makeMessageInternal(ctx, false /*addCounter*/, 0, format, args, redactable)
	// makeMessageInternal already added the tags when forming msg, we don't want
	// eventInternal to prepend them again.
	eventInternal(ctx, (s >= Severity_ERROR), false /*withTags*/, "%s:%d %s", file, line, msg)
	mainLog.outputLogEntry(s, file, line, msg, redactable)
}
