// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqltelemetry

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func otherErrorCounter(typ string, err error) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("othererror.%s.%T", typ, err))
}

// RecordError processes a SQL error. This includes both incrementing
// telemetry counters, and sending a sentry report for internal
// (assertion) errors.
func RecordError(
	ctx context.Context,
	sv *settings.Values,
	err error,
	safeDetails []sqlerror.SafeDetailPayload,
	telemetryKeys []string,
) {
	// Is this a pgerror?
	pgErr, ok := pgerror.GetPGCause(err)
	if !ok {
		// No: do simple telemetry.
		typ := log.ErrorSource(err)
		if typ == "" {
			typ = "unknown"
		}
		telemetry.Inc(otherErrorCounter(typ, err))
		return
	}

	// In any case, count the error code.
	telemetry.Count("errorcodes." + pgErr.Code)

	// Increment any additional counters derived
	// from the provided telemetry keys.
	for _, details := range telemetryKeys {
		var prefix string
		switch pgErr.Code {
		case pgerror.CodeFeatureNotSupportedError:
			prefix = "unimplemented."
		case pgerror.CodeInternalError:
			prefix = "internalerror."
		default:
			prefix = "othererror." + pgErr.Code + "."
		}
		telemetry.Count(prefix + details)
	}

	// Special handling for important errors: internal error,s corruption, etc.
	if strings.HasPrefix(pgErr.Code, "XX0") {
		// We want to log the internal error regardless of whether a
		// report is sent to sentry below.
		log.Errorf(ctx, "encountered internal error:\n%+v", pgErr)
	}

	// Special handling for uncategorized errors.
	if pgErr.Code == pgerror.CodeUncategorizedError && pgErr.Source != nil {
		typ := fmt.Sprintf("%s:%d in %s()", pgErr.Source.File, pgErr.Source.Line, pgErr.Source.Function)
		telemetry.Inc(otherErrorCounter(typ, pgErr))
	} else if pgErr.Code == pgerror.CodeInternalError {
		// Special handling for internal errors.
		if !log.ShouldSendReport(sv) {
			return
		}

		// From here we want to collect data suitable for sentry
		// reporting, taking care to omit PII.

		// If there are no details, don't even bother to try.
		if len(safeDetails) == 0 {
			log.SendReport(ctx, "<redacted>", nil)
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

		for i := range safeDetails {
			d := &safeDetails[i]
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
			log.SendReport(ctx, headMsg, extras, details, exc)
		} else {
			log.SendReport(ctx, headMsg, extras, details)
		}
	}
}

// RecordInternalTrackingError files telemetry for a simple issue
// number and error message.
func RecordInternalTrackingError(ctx context.Context, sv *settings.Values, issue int, msg string) {
	err := sqlerror.NewInternalTrackingError(issue, msg)
	pgErr, details, keys := sqlerror.Flatten(err)
	RecordError(ctx, sv, pgErr, details, keys)
}
