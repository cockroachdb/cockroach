// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	dataDir = "/mnt/data1/"
	tarFile = dataDir + "cockroach.tar.gz"
)

type multiClusterConfig struct {
	srcCluster option.NodeListOption
	dstCluster option.NodeListOption
}

func setupClusters(c cluster.Cluster) multiClusterConfig {
	nodes := c.Spec().NodeCount
	nodesPerCluster := nodes / 2

	return multiClusterConfig{
		srcCluster: c.Range(1, nodesPerCluster),
		dstCluster: c.Range(nodesPerCluster+1, 2*nodesPerCluster),
	}
}

func startCluster(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption, skipInit bool,
) error {
	clusterSetting := install.MakeClusterSettings()
	srcStartOpts := option.NewStartOpts(
		option.NoBackupSchedule,
		option.WithInitTarget(nodes[0]),
	)
	if skipInit {
		srcStartOpts.RoachprodOpts.SkipInit = true
	}

	t.Status("starting cluster with nodes ", nodes)
	return c.StartE(ctx, t.L(), srcStartOpts, clusterSetting, nodes)
}

func copyAndTransferData(
	ctx context.Context, t test.Test, c cluster.Cluster, cfg multiClusterConfig,
) error {
	t.Status("creating archive and transferring data to destination cluster")
	// Create tar archive on source cluster
	cmd := fmt.Sprintf("tar -czf %s -C %s cockroach", tarFile, dataDir)
	if err := c.RunE(ctx, option.WithNodes(cfg.srcCluster), cmd); err != nil {
		return fmt.Errorf("creating tar archive: %w", err)
	}

	// Get destination IPs
	dstNodeIps, err := c.InternalIP(ctx, t.L(), cfg.dstCluster)
	if err != nil {
		return fmt.Errorf("getting destination IPs: %w", err)
	}

	g := ctxgroup.WithContext(ctx)
	for idx, n := range cfg.srcCluster {
		node := n
		dstIP := dstNodeIps[idx]
		g.Go(func() error {
			cmd := fmt.Sprintf("scp %s ubuntu@%s:%s", tarFile, dstIP, dataDir)
			t.L().Printf("Node %d: %s", node, cmd)
			return c.RunE(ctx, option.WithNodes(option.NodeListOption{node}), cmd)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Extract tar archive
	cmd = fmt.Sprintf("tar -xzf %s -C %s", tarFile, dataDir)
	if err := c.RunE(ctx, option.WithNodes(cfg.dstCluster), cmd); err != nil {
		return fmt.Errorf("extracting tar archive: %w", err)
	}
	return nil
}

func verifyDataCorrectness(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	cfg multiClusterConfig,
	startTime, endTime hlc.Timestamp,
) error {
	t.Status("running data verification")
	srcConn := c.Conn(ctx, t.L(), cfg.srcCluster[0])
	defer srcConn.Close()
	dstConn := c.Conn(ctx, t.L(), cfg.dstCluster[0])
	defer dstConn.Close()

	// checks that the source and destination cluster data match, table by table. Comparison covers the complete dataset that
	// existed in source cluster from startup until its pre-transfer shutdown.
	if err := replicationutils.InvestigateFingerprints(ctx, srcConn, dstConn, startTime, endTime); err != nil {
		return err
	}

	db := c.Conn(ctx, t.L(), cfg.dstCluster[0], option.VirtualClusterName(install.SystemInterfaceName))
	defer db.Close()
	t.Status("checking for replica divergence on destination cluster")
	return timeutil.RunWithTimeout(ctx, "consistency check", 20*time.Minute,
		func(ctx context.Context) error {
			return roachtestutil.CheckReplicaDivergenceOnDB(ctx, t.L(), db)
		},
	)
}

// registerKVStopAndCopy tests cluster data persistence and recovery when storage data volumes
// are relocated. It:
// 1. Starts a cluster and runs a workload
// 2. Performs a complete cluster shutdown
// 3. Copies the storage data volumes to different nodes
// 4. Restarts the cluster with the relocated data
func registerKVStopAndCopy(r registry.Registry) {
	runStopAndCopy := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		cfg := setupClusters(c)

		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := startCluster(ctx, t, c, cfg.srcCluster, false); err != nil {
			t.Fatal(err)
		}

		// Initialize the database with a lot of ranges
		t.Status("running workload on source")
		c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach workload run kv --init --read-percent=0 --splits=1000 --duration=5m {pgurl:1}")

		// Stop source cluster
		t.Status("stopping source cluster")
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), cfg.srcCluster)
		endTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

		if err := copyAndTransferData(ctx, t, c, cfg); err != nil {
			t.Fatal(err)
		}

		// start destination cluster
		if err := startCluster(ctx, t, c, cfg.dstCluster, true); err != nil {
			t.Fatal(err)
		}

		// restart source cluster
		if err := startCluster(ctx, t, c, cfg.srcCluster, true); err != nil {
			t.Fatal(err)
		}

		if err := verifyDataCorrectness(ctx, t, c, cfg, startTime, endTime); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:             "kv/stop-and-copy/nodes=6",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(6, spec.CPU(8)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Run:              runStopAndCopy,
		Timeout:          defaultTimeout,
	})
}
