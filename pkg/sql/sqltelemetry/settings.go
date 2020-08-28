// Copyright 2020 The Cockroach Authors.
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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
)

// RegisterSettingChange registers a cluster setting change performed
// via SQL to telemetry.
func RegisterSettingChange(
	setting settings.Setting, settingName, encodedValue string, usingDefault bool,
) {
	switch settingName {
	case stats.AutoStatsClusterSettingName:
		switch encodedValue {
		case "true":
			telemetry.Inc(TurnAutoStatsOnUseCounter)
		case "false":
			telemetry.Inc(TurnAutoStatsOffUseCounter)
		}
	case ConnAuditingClusterSettingName:
		switch encodedValue {
		case "true":
			telemetry.Inc(TurnConnAuditingOnUseCounter)
		case "false":
			telemetry.Inc(TurnConnAuditingOffUseCounter)
		}
	case AuthAuditingClusterSettingName:
		switch encodedValue {
		case "true":
			telemetry.Inc(TurnAuthAuditingOnUseCounter)
		case "false":
			telemetry.Inc(TurnAuthAuditingOffUseCounter)
		}
	case ReorderJoinsLimitClusterSettingName:
		val, err := strconv.ParseInt(encodedValue, 10, 64)
		if err != nil {
			break
		}
		ReportJoinReorderLimit(int(val))
	case VectorizeClusterSettingName:
		val, err := strconv.Atoi(encodedValue)
		if err != nil {
			break
		}
		validatedExecMode, isValid := sessiondata.VectorizeExecModeFromString(sessiondata.VectorizeExecMode(val).String())
		if !isValid {
			break
		}
		telemetry.Inc(VecModeCounter(validatedExecMode.String()))
	}
}
