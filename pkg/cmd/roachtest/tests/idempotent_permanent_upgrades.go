// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func registerIdempotentPermanentUpgrades(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "upgrade/idempotent-permanent-upgrades",
		Owner:            registry.OwnerServer,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runIdempotentPermanentUpgrades(ctx, t, c)
		},
	})
}

func runIdempotentPermanentUpgrades(ctx context.Context, t test.Test, c cluster.Cluster) {
	// This test isn't really a "mixed version test" per se, but it uses the
	// mixedversion framework since it makes it very easy to test upgrades from
	// older versions.
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(),
		// We want to start from v22.1 or earlier.
		mixedversion.MinUpgrades(4),
		mixedversion.MaxUpgrades(5),
		mixedversion.NeverUseFixtures,
		// It seems that we to turn off secure mode to be able to set cluster
		// settings during setup. This also makes it easier to mess around with
		// the root password in the test without causing other issues.
		mixedversion.ClusterSettingOption(install.SecureOption(false)),
		// If the cluster has a license, The upgrademanager uses a stale read
		// to check if the last permanent upgrade was already completed. In
		// testing scenarios, that can lead to the cluster thinking that the
		// last permanent upgrade did not run, even if that's not the case. When
		// that happens, this 23.1 code [1] will run, which will cause rows
		// to be added in system.migrations for the 0.0-x permanent upgrades.
		// However, non-enterprise clusters and clusters that are not doing
		// multiple upgrades within the same hour will not use a stale read, so
		// the insertion of the 0.0-x rows will not happen. To replicate that
		// behavior, we clear the enterprise license setting here.
		//
		// [1] https://github.com/cockroachdb/cockroach/blob/v23.1.0/pkg/upgrade/upgrademanager/manager.go#L261-L281
		mixedversion.ClusterSettingOption(
			install.ClusterSettingsOption{
				"enterprise.license": "",
			},
		),
	)

	mvt.OnStartup("remove default postgres database",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if err := h.System.Exec(r, "DROP DATABASE IF EXISTS postgres CASCADE;"); err != nil {
				return err
			}
			return nil
		},
	)
	const rootPassword = "password"
	mvt.OnStartup("manually set a root password",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// This is safe to do since the test runs in insecure mode, so nothing
			// ever needs this password.
			if err := h.System.Exec(
				r, `UPDATE system.users SET "hashedPassword" = decode($1, 'escape') WHERE username = 'root'`, rootPassword,
			); err != nil {
				return err
			}
			return nil
		},
	)
	mvt.AfterUpgradeFinalized("verify that postgres database was not added back",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			var count int
			row := h.System.QueryRow(r, "SELECT count(*) FROM [SHOW DATABASES] WHERE database_name = 'postgres'")
			if err := row.Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				return errors.New("postgres database was added back")
			}
			return nil
		},
	)
	mvt.AfterUpgradeFinalized("verify that root password is still set",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			var actualPassword string
			row := h.System.QueryRow(
				r, `SELECT encode("hashedPassword", 'escape') FROM system.users WHERE username = 'root'`,
			)
			if err := row.Scan(&actualPassword); err != nil {
				return err
			}
			if actualPassword != rootPassword {
				return errors.Newf("expected root password to be %s, got %s", rootPassword, actualPassword)
			}
			return nil
		},
	)
	mvt.Run()
}
