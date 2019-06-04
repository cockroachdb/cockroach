// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sqltelemetry

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// RecordError processes a SQL error. This includes both incrementing
// telemetry counters, and sending a sentry report for internal
// (assertion) errors.
func RecordError(ctx context.Context, err error, sv *settings.Values) {
	// In any case, record the counters.
	telemetry.RecordError(err)

	// Now check for crash reporting.
	if pgErr, ok := pgerror.GetPGCause(err); ok && pgErr.Code == pgerror.CodeInternalError {
		// We want to log the internal error regardless of whether a
		// report is sent to sentry below.
		log.Errorf(ctx, "encountered internal error:\n%+v", pgErr)

		if !log.ShouldSendReport(sv) {
			return
		}

		// From here we want to collect data suitable for sentry
		// reporting, taking care to omit PII.

		// If there are no details, don't even bother to try.
		if len(pgErr.SafeDetail) == 0 {
			log.SendReport(ctx, "<redacted>", log.ReportTypeError, nil)
			return
		}

		// A report can contain at most one "message", at most one
		// "exception", but then it can contain arbitrarily many "extra"
		// fields.
		//
		// So we populate the packet as follow:
		// - the "exception" will contain the first detail with
		//   a populated encoded exception field.
		// - the "message" will contain the concatenation of all
		//   registered messages.
		// - the "extra" will contain all the encoded stack traces.

		var firstError *string
		var exc log.ReportableObject
		extras := make(map[string]interface{})
		var msgBuf bytes.Buffer

		for i, d := range pgErr.SafeDetail {
			msg := "<redacted>"
			if d.SafeMessage != "" {
				msg = d.SafeMessage
				if firstError == nil {
					firstError = &d.SafeMessage
				}
			}
			fmt.Fprintf(&msgBuf, "(%d) %s\n", i, msg)

			// Try a stack trace: this produces sentry "exceptions".
			if d.EncodedStackTrace != "" {
				stKey := fmt.Sprintf("stacktrace_%d", i)
				if st, ok := log.DecodeStackTrace(d.EncodedStackTrace); ok {
					if exc == nil {
						exc = log.NewException(d.SafeMessage, st)
					}
					extras[stKey] = log.PrintStackTrace(st)
				} else {
					// The stack trace could not be decoded, still try to
					// include it so that there is "something" to work with.
					extras[stKey] = "--raw--\n" + d.EncodedStackTrace
				}
			}
		}

		// Determine a head message for the report.
		headMsg := "<unknown error>"
		if firstError != nil {
			headMsg = *firstError
		}

		// Make the message part more informational.
		msgBuf.WriteString("(see stack traces in additional data)")
		details := log.NewReportMessage(msgBuf.String())

		// Finally, send the report.
		if exc != nil {
			log.SendReport(ctx, headMsg, log.ReportTypeError, extras, details, exc)
		} else {
			log.SendReport(ctx, headMsg, log.ReportTypeError, extras, details)
		}
	}
}
