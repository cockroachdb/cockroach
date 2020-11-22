// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// RecordError processes a SQL error. This includes both incrementing
// telemetry counters, and sending a sentry report for internal
// (assertion) errors.
func RecordError(ctx context.Context, err error, sv *settings.Values) {
	// In any case, record the counters.
	telemetry.RecordError(err)

	code := pgerror.GetPGCode(err)
	switch {
	case code == pgcode.Uncategorized:
		// For compatibility with 19.1 telemetry, keep track of the number
		// of occurrences of errors without code in telemetry. Over time,
		// we'll want this count to go down (i.e. more errors becoming
		// qualified with a code).
		//
		// TODO(knz): figure out if this telemetry is still useful.
		telemetry.Inc(UncategorizedErrorCounter)

	case code == pgcode.Internal || errors.HasAssertionFailure(err):
		// This is an assertion failure / crash.
		//
		// Note: not all assertion failures end up with code "internal".
		// For example, an assertion failure "underneath" a schema change
		// failure during a COMMIT for a multi-stmt txn will mask the
		// internal code and replace it with
		// TransactionCommittedWithSchemaChangeFailure.
		//
		// Conversely, not all errors with code "internal" are assertion
		// failures, but we still want to log/register them.

		// We want to log the internal error regardless of whether a
		// report is sent to sentry below.
		log.Errorf(ctx, "encountered internal error:\n%+v", err)

		if logcrash.ShouldSendReport(sv) {
			event, extraDetails := errors.BuildSentryReport(err)
			logcrash.SendReport(ctx, logcrash.ReportTypeError, event, extraDetails)
		}
	}
}
