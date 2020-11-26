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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
)

// FormatWithContextTags formats the string and prepends the context
// tags.
//
// Redaction markers are *not* inserted. The resulting
// string is generally unsafe for reporting.
func FormatWithContextTags(ctx context.Context, format string, args ...interface{}) string {
	var buf strings.Builder
	formatTags(ctx, true /* brackets */, &buf)
	formatArgs(&buf, format, args...)
	return buf.String()
}

// addStructured creates a structured log entry to be written to the
// specified facility of the logger.
func addStructured(
	ctx context.Context, sev Severity, depth int, format string, args ...interface{},
) {
	if sev == severity.FATAL {
		// We load the ReportingSettings from the a global singleton in this
		// call path. See the singleton's comment for a rationale.
		if sv := settings.TODO(); sv != nil {
			err := errors.NewWithDepthf(depth+1, "log.Fatal: "+format, args...)
			sendCrashReport(ctx, sv, err, ReportTypeLogFatal)
		}
	}

	entry := MakeEntry(
		ctx, sev, &debugLog.logCounter, depth+1, true /* redactable */, format, args...)
	if sp, el, ok := getSpanOrEventLog(ctx); ok {
		eventInternal(sp, el, (sev >= severity.ERROR), entry)
	}
	debugLog.outputLogEntry(entry)
}
