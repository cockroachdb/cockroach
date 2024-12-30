// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerImportMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "import/mixed-versions",
		Owner:            registry.OwnerSQLQueries,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			warehouses := 100
			if c.IsLocal() {
				warehouses = 10
			}
			runImportMixedVersions(ctx, t, c, warehouses)
		},
	})
}

func runImportMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster, warehouses int) {
	// NB: We rely on the testing framework to choose a random predecessor to
	// upgrade from.
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(),
		// We test only upgrades from 23.2 in this test because it uses
		// the `workload fixtures import` command, which is only supported
		// reliably multi-tenant mode starting from that version.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
		// Only use the latest version of each release to work around #127029.
		mixedversion.AlwaysUseLatestPredecessors,
	)
	runImport := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		if err := h.Exec(r, "DROP DATABASE IF EXISTS tpcc CASCADE;"); err != nil {
			return err
		}
		node := c.All().SeededRandNode(r)[0]
		cmd := tpccImportCmdWithCockroachBinary(test.DefaultCockroachPath, "", "tpcc", warehouses) + fmt.Sprintf(" {pgurl%s}", c.Node(node))
		l.Printf("executing %q on node %d", cmd, node)
		return c.RunE(ctx, option.WithNodes(c.Node(node)), cmd)
	}
	mvt.InMixedVersion("import", runImport)
	mvt.AfterUpgradeFinalized("import", runImport)
	mvt.Run()
}
