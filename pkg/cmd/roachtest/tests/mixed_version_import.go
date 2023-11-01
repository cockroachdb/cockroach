// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
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
		Suites:           registry.Suites(registry.Nightly),
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
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All())
	runImport := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		if err := h.Exec(r, "DROP DATABASE IF EXISTS tpcc CASCADE;"); err != nil {
			return err
		}
		node := h.RandomNode(r, c.All())
		cmd := tpccImportCmdWithCockroachBinary(test.DefaultCockroachPath, warehouses) + fmt.Sprintf(" {pgurl%s}", c.Node(node))
		l.Printf("executing %q on node %d", cmd, node)
		return c.RunE(ctx, c.Node(node), cmd)
	}
	mvt.InMixedVersion("import", runImport)
	mvt.AfterUpgradeFinalized("import", runImport)
	mvt.Run()
}
