// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	// Skip the upgrade check in the following cases:
	// - The license enforcer is disabled, such as in valid scenarios like single-node setups
	//   where no throttling restrictions apply.
	// - Running a unit test, as most tests don't have a license installed. This behavior can
	//   be overridden using a testing knob for unit tests that need to specifically validate
	//   this check.
	// - The `COCKROACH_UPGRADE_SKIP_LICENSE_CHECK` environment variable is set. This can be used
	//   by acceptance tests that lack a license installation.
	if deps.LicenseEnforcer.GetIsDisabled() ||
		(buildutil.CrdbTestBuild && (deps.TestingKnobs == nil || !deps.TestingKnobs.ForceCheckLicenseViolation)) ||
		envutil.EnvOrDefaultBool("COCKROACH_UPGRADE_SKIP_LICENSE_CHECK", false) {
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
