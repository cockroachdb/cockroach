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
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RecordError processes a SQL error. This includes both incrementing
// telemetry counters, and sending a sentry report for internal
// (assertion) errors.
func RecordError(ctx context.Context, err error, sv *settings.Values) {
	// In any case, record the counters.
	telemetry.RecordError(err)

	// Now check for crash reporting.
	if code := pgerror.GetPGCode(err); code == pgcode.Internal || errors.HasAssertionFailure(err) {
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

		if log.ShouldSendReport(sv) {
			msg, details, extraDetails := errors.BuildSentryReport(err)
			log.SendReport(ctx, msg, log.ReportTypeError, extraDetails, details...)
		}
	}
}
