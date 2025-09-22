// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
)

func registerDRPCTests(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "drpc/enable-setting",
		Owner:            registry.OwnerServer,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNodeCount(1)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runEnableSettingWithRollingRestart(ctx, t, c)
		},
	})
}

func runEnableSettingWithRollingRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
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

	t.Status("upgrading all nodes to current version")
	if err := clusterupgrade.RestartNodesWithNewBinary(
		ctx, t, t.L(), c, c.CRDBNodes(), option.DefaultStartOpts(), currentVersion,
	); err != nil {
		t.Fatal(err)
	}
	conns := make(map[int]*gosql.DB)
	dbFunc := func(node int) *gosql.DB {
		if _, ok := conns[node]; !ok {
			conns[node] = c.Conn(ctx, t.L(), node)
		}
		return conns[node]
	}
	defer func() {
		for _, db := range conns {
			db.Close()
		}
	}()
	if err := clusterupgrade.WaitForClusterUpgrade(
		ctx, t.L(), c.CRDBNodes(), dbFunc, clusterupgrade.DefaultUpgradeTimeout); err != nil {
	}

	t.Status("enabling DRPC setting")
	upgradedDB := c.Conn(ctx, t.L(), 1)
	defer upgradedDB.Close()

	if _, err := upgradedDB.ExecContext(
		ctx, "SET CLUSTER SETTING rpc.experimental_drpc.enabled = true"); err != nil {
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

	t.Status("restarting all nodes gradually")
	for _, nodeID := range c.CRDBNodes() {
		t.L().Printf("stopping node %d", nodeID)
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(nodeID))

		t.L().Printf("starting node %d", nodeID)
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(nodeID))

		// Wait for node to be ready by performing a simple query
		t.L().Printf("waiting for readiness of node %d", nodeID)
		db := c.Conn(ctx, t.L(), nodeID)
		if _, err := db.ExecContext(ctx, `SELECT 1`); err != nil {
			db.Close()
			t.Fatalf("node %d failed to become ready: %v", nodeID, err)
		}
		db.Close()
		t.Status("running TPCC workload after restarting: %d", nodeID)
		if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd.String()); err != nil {
			t.Fatalf("failed to run TPCC workload after restarting: %d", nodeID)
		}
	}
}
