// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCheckLicenseViolations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Set up the storage cluster at v1.
	v1 := clusterversion.MinSupported.Version()
	v2 := clusterversion.V24_3_MaybePreventUpgradeForCoreLicenseDeprecation.Version()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v1,
		false, // initializeVersion
	)

	require.NoError(t, clusterversion.Initialize(ctx, v1, &settings.SV))

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v1,
			},
			LicenseTestingKnobs: &license.TestingKnobs{
				SkipDisable: true,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				ForceCheckLicenseViolation: true,
			},
			// Make the upgrade faster by accelerating jobs.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	// Attempt an upgrade without a license.
	db := sqlutils.MakeSQLRunner(srv.SQLConn(t))
	db.ExpectErr(t, "upgrade blocked: no license installed",
		"SET CLUSTER SETTING version = $1", v2.String())

	// Install a trial license with telemetry off, which will still block the upgrade.
	db.Exec(t, "SET CLUSTER SETTING diagnostics.reporting.enabled = false")
	licenseEnforcer := srv.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer
	licenseEnforcer.RefreshForLicenseChange(ctx, license.LicTypeTrial, timeutil.Unix(1724329716, 0))
	db.ExpectErr(t, "upgrade blocked: the installed license requires telemetry",
		"SET CLUSTER SETTING version = $1", v2.String())

	// Enabling telemetry will allow the upgrade to proceed.
	db.Exec(t, "SET CLUSTER SETTING diagnostics.reporting.enabled = true")
	db.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())
}
