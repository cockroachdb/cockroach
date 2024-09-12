// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// checkForPostUpgradeThrottlePreCond verifies if a major upgrade is permitted. It's
// intended for upgrades to versions that no longer support the old core license,
// preventing upgrades that could result in immediate throttling or reliance on
// the deprecated license.
func checkForPostUpgradeThrottlePreCond(
	_ context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Allow upgrades if the license enforcer is disabled, such as in valid cases
	// like single-node setups, since no throttling restrictions apply.
	if deps.LicenseEnforcer.GetIsDisabled() {
		return nil
	}

	// To avoid unexpected disruptions in production clusters, we must block upgrades
	// to 24.3 or later if throttling would be triggered.
	if !deps.LicenseEnforcer.GetHasLicense() {
		return errors.New("upgrade blocked: no license installed")
	}
	if deps.LicenseEnforcer.GetRequiresTelemetry() && !logcrash.DiagnosticsReportingEnabled.Get(&deps.Settings.SV) {
		return errors.New("upgrade blocked: the installed license requires telemetry, " +
			"but it is currently disabled. Enable telemetry by setting diagnostics.reporting.enabled to true.")
	}
	return nil
}

func checkForPostUpgradeThrottleProcessing(
	context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps,
) error {
	// All the heavy lifting for this migration step is done in the precondition.
	// The actual upgrade step is a no-op.
	return nil
}
