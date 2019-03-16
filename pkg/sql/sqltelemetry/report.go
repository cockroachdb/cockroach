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
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// RecordError processes a SQL error. This includes both incrementing
// telemetry counters, and sending a sentry report for internal
// (assertion) errors.
func RecordError(ctx context.Context, err error, sv *settings.Values) {
	telemetry.RecordError(err)
	if pgErr, ok := pgerror.GetPGCause(err); ok && pgErr.Code == pgerror.CodeInternalError {
		// We want to log the internal error regardless of whether a
		// report is sent to sentry below.
		log.Errorf(ctx, "encountered internal error:\n%+v", pgErr)

		if !log.ShouldSendReport(sv) {
			return
		}

		// From here we want to collect data suitable for sentry
		// reporting, taking care to omit PII.
		if len(pgErr.SafeDetail) == 0 {
			log.SendReport(ctx, "<redacted>")
		}
		reportables := make([]log.ReportableObject, len(pgErr.SafeDetail))
		var firstError *string
		for i, d := range pgErr.SafeDetail {
			msg := "<redacted>"
			if d.SafeMessage != "" {
				msg = d.SafeMessage
				if firstError == nil {
					firstError = &d.SafeMessage
				}
			}

			// Try a stack trace: this produces sentry "exceptions".
			if d.EncodedStackTrace != "" {
				if st, ok := log.DecodeStackTrace(d.EncodedStackTrace); ok {
					reportables[i] = log.NewException(d.SafeMessage, st)
					continue
				}
			}
			// No stack trace or stack trace invalid.
			reportables[i] = log.NewReportMessage(msg)
		}
		headMsg := "<unknown error>"
		if firstError != nil {
			headMsg = *firstError
		}

		log.SendReport(ctx, headMsg, reportables...)
	}
}
