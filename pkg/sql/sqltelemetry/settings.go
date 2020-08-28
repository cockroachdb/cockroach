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
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// RegisterSettingChange registers a cluster setting change performed
// via SQL to telemetry.
func RegisterSettingChange(
	setting settings.Setting, settingName, encodedValue string, usingDefault bool,
) error {
	reportedValue := "changed"
	if usingDefault || safeReportableSettings[settingName] {
		// The default value is always safe.
		// A value is also safe if the setting declares it is.
		reportedValue = encodedValue
	} else {
		// Maybe we can report some details.
		switch setting.Typ() {
		case "b" /* boolean */ :
			// Always safe. Can report the value as-is.
			reportedValue = encodedValue

		case "e" /* enum */ :
			// Let's represent the enum value as a string.
			e, ok := setting.(*settings.EnumSetting)
			if !ok {
				return errors.AssertionFailedf("a non-enum setting pretends to be an enum: %T", setting)
			}
			reportedValue = e.ValueAsString(encodedValue)

		case "i" /* integer */, "z" /* size */ :
			// Quantize it. This reduces the amount of different values
			// accumulated in the counter sub-system, and somewhat
			// anonymizes the data.
			val, err := strconv.ParseInt(encodedValue, 10, 64)
			if err != nil {
				// Unsure what kind of int that is. Bail out: the value is not
				// reportable.
				break
			}
			reportedValue = strconv.FormatInt(telemetry.Bucket10(val), 10)

		case "f":
			val, err := strconv.ParseFloat(encodedValue, 64)
			if err != nil {
				// Unsure what kind of float that is. Bail out: value not reportable.
				break
			}
			// At this point, all float settings are really percentages.
			// Quantize to two digits: that's a good precision for a
			// percentage.
			reportedValue = fmt.Sprintf("%.2f", val)

		case "d" /* duration */ :
			val, err := time.ParseDuration(encodedValue)
			if err != nil {
				// Unsure what kind of float that is. Bail out: value not reportable.
				break
			}
			reportedValue = telemetry.BucketDuration(val).String()

		case "s":
			// Definitely unsafe. Not reporting the string.
		case "m":
			// State machine. Currently only the version setting uses this,
			// where the state labels (and thus encodedValue) would be safe
			// for reporting. However nothing in the API mandates that state
			// machine labels remain safe for reporting; so in case new state
			// machines are added in the future, we don't want to silently
			// start leaking information here.
		default:
			// If this panic is encountered, this means a new setting type
			// was added. Add a case above.
			return errors.AssertionFailedf("unknown setting type: %s", setting.Typ())
		}
	}

	counterName := fmt.Sprintf("%s%s=%s", settingChangePrefix, settingName, reportedValue)
	telemetry.Inc(telemetry.GetCounter(counterName))

	// For compatibility with pre-v20.2 telemetry
	if legacyCounter, ok := legacyCounters[counterName]; ok {
		telemetry.Inc(legacyCounter)
	}

	return nil
}

var safeReportableSettings = map[string]bool{
	// The reorder join limit is really an enum with 63 values.
	// We consider all of them as interesting.
	ReorderJoinsLimitClusterSettingName: true,
}

const settingChangePrefix = "setting-change."

var legacyCounters = map[string]telemetry.Counter{
	// The following pick up the values as of v20.2 for compatibility
	// with pre-v20.2 telemetry.
	settingChangePrefix + VectorizeClusterSettingName + "=on":      telemetry.GetCounterOnce("sql.exec.vectorized-setting.on"),
	settingChangePrefix + VectorizeClusterSettingName + "=off":     telemetry.GetCounterOnce("sql.exec.vectorized-setting.off"),
	settingChangePrefix + VectorizeClusterSettingName + "=201auto": telemetry.GetCounterOnce("sql.exec.vectorized-setting.201auto"),
	// More compatibility with pre-v20.2 telemetry.
	settingChangePrefix + AutoStatsClusterSettingName + "=true":       TurnAutoStatsOnUseCounter,
	settingChangePrefix + AutoStatsClusterSettingName + "=false":      TurnAutoStatsOffUseCounter,
	settingChangePrefix + ConnAuditingClusterSettingName + "=true":    TurnConnAuditingOnUseCounter,
	settingChangePrefix + ConnAuditingClusterSettingName + "=false":   TurnConnAuditingOffUseCounter,
	settingChangePrefix + AuthAuditingClusterSettingName + "=true":    TurnAuthAuditingOnUseCounter,
	settingChangePrefix + AuthAuditingClusterSettingName + "=false":   TurnAuthAuditingOffUseCounter,
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=0":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-0"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=1":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-1"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=2":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-2"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=3":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-3"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=4":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-4"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=5":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-5"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=6":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-6"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=7":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-7"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=8":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-8"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=9":  telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-9"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=10": telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-10"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=11": telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-11"),
	settingChangePrefix + ReorderJoinsLimitClusterSettingName + "=12": telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-12"),
}
