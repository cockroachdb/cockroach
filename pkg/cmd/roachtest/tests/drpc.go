// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/stretchr/testify/require"
)

func registerDRPCTests(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "drpc/setting-during-rollback",
		Owner:            registry.OwnerServer,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNodeCount(1)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runUpdateSettingDuringUpgradeRollback(ctx, t, c)
		},
	})
}

// runUpdateSettingDuringUpgradeRollback tests the scenario where in a mixed-mode
// cluster with auto-finalization disabled, if a 25.4 node enables the DRPC
// cluster setting and then the cluster rolls back to 25.3, the setting should
// not remain enabled.
func runUpdateSettingDuringUpgradeRollback(ctx context.Context, t test.Test, c cluster.Cluster) {
	currentVersion := clusterupgrade.CurrentVersion()
	previousVersionStr, err := release.LatestPredecessor(&currentVersion.Version)
	if err != nil {
		t.Fatal(err)
	}
	previousVersion := clusterupgrade.MustParseVersion(previousVersionStr)

	t.Status("starting cluster with predecessor version: ", previousVersionStr)
	predecessorBinary, err := clusterupgrade.UploadCockroach(
		ctx, t, t.L(), c, c.All(), previousVersion)
	if err != nil {
		t.Fatal(err)
	}

	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings(install.BinaryOption(predecessorBinary))
	c.Start(ctx, t.L(), opts, settings, c.CRDBNodes())

	t.Status("disabling auto-finalization")
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	preserveVersion := previousVersion.Format("%X.%Y")
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("SET CLUSTER SETTING cluster.preserve_downgrade_option = '%s'", preserveVersion),
	); err != nil {
		t.Fatal(err)
	}

	t.Status("upgrading node 1 to current version")
	if err := clusterupgrade.RestartNodesWithNewBinary(
		ctx, t, t.L(), c, c.Node(1), option.DefaultStartOpts(), currentVersion,
	); err != nil {
		t.Fatal(err)
	}

	t.Status("enabling DRPC setting on the upgraded node")
	upgradedDB := c.Conn(ctx, t.L(), 1)
	defer upgradedDB.Close()

	if _, err := upgradedDB.ExecContext(
		ctx, "SET CLUSTER SETTING rpc.drpc.enabled = true"); err != nil {
		t.Fatalf("failed to set DRPC setting: %v", err)
	}

	t.Status("running TPCC workload")
	initCmd := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", c.CRDBNodes())
	runCmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", c.CRDBNodes()).
		Flag("duration", "30s")

	if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), initCmd.String()); err != nil {
		t.Fatalf("failed to initialize TPCC workload: %v", err)
	}
	if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd.String()); err != nil {
		t.Fatalf("failed to run TPCC workload with DRPC: %v", err)
	}

	t.Status("downgrading node 1 back to predecessor version")
	if err := clusterupgrade.RestartNodesWithNewBinary(
		ctx, t, t.L(), c, c.Node(1), option.DefaultStartOpts(), previousVersion,
	); err != nil {
		t.Fatal(err)
	}

	t.Status("running TPCC workload after downgrade")
	if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd.String()); err != nil {
		t.Fatalf("failed to run TPCC workload after downgrade: %v", err)
	}

	// Only run this check if the predecessor version is 25.3
	if previousVersion.Format("%X.%Y") == "25.3" {
		t.Status("checking if DRPC setting persists after downgrade")
		downgradedDB := c.Conn(ctx, t.L(), 1)
		defer downgradedDB.Close()
		var finalValue bool
		err = downgradedDB.QueryRowContext(
			ctx, "SHOW CLUSTER SETTING rpc.experimental_drpc.enabled").Scan(&finalValue)
		require.NoError(t, err, "could not read DRPC setting after downgrade: %v", err)
		require.False(t, finalValue, "DRPC setting remained enabled after downgrade")
	}
}
