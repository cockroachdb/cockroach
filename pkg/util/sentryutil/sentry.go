// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sentryutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// SendReport creates a Sentry report about the error, if the settings allow.
// The format string will be reproduced ad litteram in the report; the arguments
// will be sanitized.
func SendReport(ctx context.Context, sv *settings.Values, err error) {
	if !logcrash.ShouldSendReport(sv) {
		return
	}
	event, extraDetails := errors.BuildSentryReport(err)
	logcrash.SendReport(ctx, logcrash.ReportTypeError, event, extraDetails)
}
