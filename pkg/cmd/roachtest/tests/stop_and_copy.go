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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	nodesPerCluster = 3
	dataDir         = "/mnt/data1/"
	tarFile         = dataDir + "cockroach.tar.gz"
)

type multiClusterConfig struct {
	srcCluster option.NodeListOption
	dstCluster option.NodeListOption
}

func setupClusters(c cluster.Cluster) multiClusterConfig {
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
	// This command creates a tar archive on the source cluster.
	if err := c.RunE(ctx, option.WithNodes(cfg.srcCluster), "tar", "-czf", tarFile, "-C", dataDir, "cockroach"); err != nil {
		return errors.Wrapf(err, "creating tar archive")
	}

	// Fetch IP address of nodes from destination cluster.
	dstNodeIps, err := c.InternalIP(ctx, t.L(), cfg.dstCluster)
	if err != nil {
		return errors.Wrapf(err, "getting destination IPs")
	}

	// This code copies data in parallel from source cluster nodes to their corresponding destination nodes.
	m := t.NewGroup(task.WithContext(ctx))
	for idx, n := range cfg.srcCluster {
		node := n
		dstIP := dstNodeIps[idx]
		m.Go(func(ctx context.Context, l *logger.Logger) error {
			return c.RunE(ctx, option.WithNodes(option.NodeListOption{node}), "scp", tarFile, fmt.Sprintf("ubuntu@%s:%s", dstIP, dataDir))
		})
	}
	m.Wait()

	// This command extracts the tar archive in the destination data directory.
	if err = c.RunE(ctx, option.WithNodes(cfg.dstCluster), "tar", "-xzf", tarFile, "-C", dataDir); err != nil {
		return errors.Wrapf(err, "extracting tar archive:")
	}
	return nil
}

func fingerprintDatabases(
	ctx context.Context, t test.Test, c cluster.Cluster, node int,
) (map[string]map[string]int64, error) {
	dbConn := c.Conn(ctx, t.L(), node)
	defer dbConn.Close()

	fingerPrints, err := fingerprintutils.FingerprintAllDatabases(ctx, dbConn, false, fingerprintutils.Stripped())
	if err != nil {
		return nil, err
	}
	return fingerPrints, nil
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

		if err := startCluster(ctx, t, c, cfg.srcCluster, false); err != nil {
			t.Fatal(err)
		}

		// Initialize the database.
		t.Status("running workload on source cluster")
		c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach workload run kv --init --read-percent=0 --splits=1000 --duration=5m {pgurl:1}")

		// Calculate the fingerprints for all databases on the source cluster, excluding the system database.
		t.Status("computing databases fingerprint on source cluster")
		srcFingerprints, err := fingerprintDatabases(ctx, t, c, cfg.srcCluster[0])
		if err != nil {
			t.Fatal(err)
		}

		// Shut down the source cluster.
		t.Status("stopping source cluster")
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), cfg.srcCluster)

		if err := copyAndTransferData(ctx, t, c, cfg); err != nil {
			t.Fatal(err)
		}

		// Start the destination cluster.
		if err := startCluster(ctx, t, c, cfg.dstCluster, true); err != nil {
			t.Fatal(err)
		}

		// Calculate the fingerprints for all databases on the destination cluster, excluding the system database.
		t.Status("computing databases fingerprint on destination cluster")
		dstFingerprints, err := fingerprintDatabases(ctx, t, c, cfg.dstCluster[0])
		if err != nil {
			t.Fatal(err)
		}

		// Match the database fingerprints of source and destination cluster.
		if err := fingerprintutils.CompareMultipleDatabaseFingerprints(srcFingerprints, dstFingerprints); err != nil {
			t.Fatal(err)
		}

		t.Status("checking for replica divergence on destination cluster")
		db := c.Conn(ctx, t.L(), cfg.dstCluster[0], option.VirtualClusterName(install.SystemInterfaceName))
		defer db.Close()
		err = timeutil.RunWithTimeout(ctx, "consistency check", 20*time.Minute,
			func(ctx context.Context) error {
				return roachtestutil.CheckReplicaDivergenceOnDB(ctx, t.L(), db)
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:             "stop-and-copy/nodes=3plus3",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(2*nodesPerCluster, spec.CPU(8)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Run:              runStopAndCopy,
		Timeout:          defaultTimeout,
	})
}
