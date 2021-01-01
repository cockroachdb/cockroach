// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnostics

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// ReportFrequency is the interval at which diagnostics data should be reported.
var ReportFrequency = settings.RegisterPublicNonNegativeDurationSetting(
	"diagnostics.reporting.interval",
	"interval at which diagnostics data should be reported",
	time.Hour,
)
