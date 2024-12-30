// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
)

// validateSystemSchemaTenantVersion is the minimum version after
// which we start ensuring that the schema for a tenant is the same
// whether we upgraded to a version or bootstrapped in it. Prior to
// this version, the check is expected to fail due to #129643.
var validateSystemSchemaTenantVersion = clusterupgrade.MustParseVersion("v24.3.0-alpha.00000000")

func diff(a, b string) error {
	if a == b {
		return nil
	}

	diffStr, diffErr := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:       difflib.SplitLines(a),
		B:       difflib.SplitLines(b),
		Context: 5,
	})

	if diffErr != nil {
		return errors.Wrap(diffErr, "failed to produce diff")
	}

	return fmt.Errorf("diff:\n%s", diffStr)
}

type tenantSystemSchemaComparison struct {
	name         string
	bootstrapped string
	upgraded     string
}

func newTenantSystemSchemaComparison(name string) *tenantSystemSchemaComparison {
	return &tenantSystemSchemaComparison{name: name}
}

func (c tenantSystemSchemaComparison) Diff() error {
	if err := diff(c.upgraded, c.bootstrapped); err != nil {
		tenantDesc := "system"
		if c.name != install.SystemInterfaceName {
			tenantDesc = "non-system"
		}

		return errors.Newf(
			"After upgrading, `USE system; SHOW CREATE ALL TABLES;` "+
				"does not match expected output after version upgrade for %s tenant: %w",
			tenantDesc, err,
		)
	}

	return nil
}

func validateSystemSchemaAfterUpgradeTest(
	ctx context.Context, t test.Test, c cluster.Cluster, opts ...mixedversion.CustomOption,
) {
	// Obtain system table definitions with `SHOW CREATE ALL TABLES` in the SYSTEM db.
	obtainSystemSchema := func(
		ctx context.Context, l *logger.Logger, c cluster.Cluster, node int, virtualCluster string,
	) string {
		// Create a connection to the database cluster.
		db := c.Conn(ctx, l, node, option.VirtualClusterName(virtualCluster))
		defer db.Close()

		sqlRunner := sqlutils.MakeSQLRunner(db)

		// Prepare the SQL query.
		sql := `USE SYSTEM; SHOW CREATE ALL TABLES;`

		// Execute the SQL query.
		rows := sqlRunner.QueryStr(t, sql)
		sort.Slice(rows, func(i, j int) bool {
			return strings.Compare(rows[i][0], rows[j][0]) < 0
		})
		// Extract return.
		var sb strings.Builder
		for _, row := range rows {
			sb.WriteString(row[0])
			sb.WriteString("\n")
		}

		return sb.String()
	}

	systemComparison := newTenantSystemSchemaComparison(install.SystemInterfaceName)
	var tenantComparison *tenantSystemSchemaComparison
	var deploymentMode mixedversion.DeploymentMode

	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(), opts...)
	mvt.AfterUpgradeFinalized(
		"obtain system schema from the upgraded cluster",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			if !h.Context().ToVersion.IsCurrent() {
				// Only validate the system schema if we're upgrading to the version
				// under test.
				return nil
			}

			deploymentMode = h.DeploymentMode()
			systemComparison.upgraded = obtainSystemSchema(ctx, l, c, 1, systemComparison.name)
			if h.IsMultitenant() {
				tenantComparison = newTenantSystemSchemaComparison(h.Tenant.Descriptor.Name)
				tenantComparison.upgraded = obtainSystemSchema(ctx, l, c, 1, tenantComparison.name)
			}

			return nil
		},
	)
	mvt.Run()

	// Start a cluster with the latest binary and get the system schema
	// from the cluster.
	c.Wipe(ctx, c.All())
	settings := install.MakeClusterSettings()

	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings)
	systemComparison.bootstrapped = obtainSystemSchema(ctx, t.L(), c, 1, systemComparison.name)

	validateTenant := tenantComparison != nil && clusterupgrade.CurrentVersion().AtLeast(validateSystemSchemaTenantVersion)

	if validateTenant {
		var startOpts option.StartOpts

		switch deploymentMode {
		case mixedversion.SharedProcessDeployment:
			t.L().Printf("creating shared-process tenant")
			startOpts = option.StartSharedVirtualClusterOpts(tenantComparison.name)

		case mixedversion.SeparateProcessDeployment:
			t.L().Printf("creating separate-process tenant")
			startOpts = option.StartVirtualClusterOpts(tenantComparison.name, c.Node(1))

		default:
			t.Fatal(fmt.Errorf("programming error: unexpected deployment mode %q", deploymentMode))
		}

		c.StartServiceForVirtualCluster(ctx, t.L(), startOpts, settings)
		tenantComparison.bootstrapped = obtainSystemSchema(ctx, t.L(), c, 1, tenantComparison.name)
	}

	if err := systemComparison.Diff(); err != nil {
		t.Fatal(err)
	}
	t.L().Printf("validation succeeded for system tenant")

	if validateTenant {
		if err := tenantComparison.Diff(); err != nil {
			t.Fatal(err)
		}

		if err := diff(systemComparison.upgraded, tenantComparison.upgraded); err != nil {
			t.Fatal(fmt.Errorf("comparing system schema of system and tenant: %w", err))
		}

		t.L().Printf("validation succeeded for non-system tenant")
	}
}

// This test tests that, after bootstrapping a cluster from a previous
// release's binary and upgrading it to the latest version, the `system`
// database "contains the expected tables".
// Specifically, we do the check with `USE system; SHOW CREATE ALL TABLES;`
// and assert that the output matches the expected output content.
func runValidateSystemSchemaAfterVersionUpgrade(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	validateSystemSchemaAfterUpgradeTest(ctx, t, c,
		// We limit the number of upgrades since the test is not expected to work
		// on versions older than 22.2.
		mixedversion.MaxUpgrades(3),
		// Fixtures are generated on a version that's too old for this test.
		mixedversion.NeverUseFixtures,
		// Separate-process deployments can't run in 1-node clusters since
		// the tenant process can die when the storage cluster is
		// restarting. See `runValidateSystemSchemaAfterVersionUpgradeSeparateProcess`
		// for a variant of this test for separate-process deployments.
		mixedversion.EnabledDeploymentModes(
			mixedversion.SystemOnlyDeployment,
			mixedversion.SharedProcessDeployment,
		),
	)
}

// Like `runValidateSystemSchemaAfterVersionUpgrade`, but for
// separate-process deployments.
func runValidateSystemSchemaAfterVersionUpgradeSeparateProcess(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	validateSystemSchemaAfterUpgradeTest(ctx, t, c,
		// We limit the number of upgrades since the test is not expected to work
		// on versions older than 22.2.
		mixedversion.MaxUpgrades(3),
		// Fixtures are generated on a version that's too old for this test.
		mixedversion.NeverUseFixtures,
		mixedversion.EnabledDeploymentModes(mixedversion.SeparateProcessDeployment),
	)
}

func registerValidateSystemSchemaAfterVersionUpgradeSeparateProcess(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "validate-system-schema-after-version-upgrade/separate-process",
		Owner:            registry.OwnerSQLFoundations,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Cluster:          r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runValidateSystemSchemaAfterVersionUpgradeSeparateProcess(ctx, t, c)
		},
	})
}
