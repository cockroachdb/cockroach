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

type stopAndCopyTest struct {
	cluster.Cluster
	t          test.Test
	srcCluster option.NodeListOption
	dstCluster option.NodeListOption
}

func setupClusters(c cluster.Cluster, t test.Test) stopAndCopyTest {
	return stopAndCopyTest{
		Cluster:    c,
		t:          t,
		srcCluster: c.Range(1, nodesPerCluster),
		dstCluster: c.Range(nodesPerCluster+1, 2*nodesPerCluster),
	}
}

func (s *stopAndCopyTest) startCluster(
	ctx context.Context, nodes option.NodeListOption, skipInit bool,
) error {
	clusterSetting := install.MakeClusterSettings()
	srcStartOpts := option.NewStartOpts(
		option.NoBackupSchedule,
		option.WithInitTarget(nodes[0]),
	)
	if skipInit {
		srcStartOpts.RoachprodOpts.SkipInit = true
	}

	s.t.Status("starting cluster with nodes ", nodes)
	return s.StartE(ctx, s.t.L(), srcStartOpts, clusterSetting, nodes)
}

func (s *stopAndCopyTest) copyAndTransferData(ctx context.Context) error {
	s.t.Status("creating archive and transferring data to destination cluster")
	// This command creates a tar archive on the source cluster.
	if err := s.RunE(ctx, option.WithNodes(s.srcCluster), "tar", "-czf", tarFile, "-C", dataDir, "cockroach"); err != nil {
		return errors.Wrapf(err, "creating tar archive")
	}

	// Fetch IP address of nodes from destination cluster.
	dstNodeIps, err := s.InternalIP(ctx, s.t.L(), s.dstCluster)
	if err != nil {
		return errors.Wrapf(err, "getting destination IPs")
	}

	// This code copies data in parallel from source cluster nodes to their corresponding destination nodes.
	m := s.t.NewGroup(task.WithContext(ctx))
	for idx, n := range s.srcCluster {
		node := n
		dstIP := dstNodeIps[idx]
		m.Go(func(ctx context.Context, l *logger.Logger) error {
			return s.RunE(ctx, option.WithNodes(option.NodeListOption{node}), "scp", tarFile, fmt.Sprintf("ubuntu@%s:%s", dstIP, dataDir))
		})
	}
	m.Wait()

	// This command extracts the tar archive in the destination data directory.
	if err = s.RunE(ctx, option.WithNodes(s.dstCluster), "tar", "-xzf", tarFile, "-C", dataDir); err != nil {
		return errors.Wrapf(err, "extracting tar archive:")
	}
	return nil
}

func (s *stopAndCopyTest) clearGossipBootstrap(ctx context.Context) error {
	s.t.Status("clearing gossip bootstrap info on destination cluster")
	return s.RunE(ctx, option.WithNodes(s.dstCluster), fmt.Sprintf("./cockroach debug clear-gossip-bootstrap %s/cockroach", dataDir))
}

func (s *stopAndCopyTest) checkGossipNodes(ctx context.Context, nodes option.NodeListOption) error {
	db := s.Conn(ctx, s.t.L(), nodes[0])
	defer db.Close()
	rows, err := db.Query("SELECT count(*) FROM crdb_internal.gossip_nodes")
	if err != nil {
		return err
	}
	defer rows.Close()
	var count int
	if rows.Next() {
		if err = rows.Scan(&count); err != nil {
			return err
		}
	}
	if count != nodesPerCluster {
		return errors.Newf("expected %d nodes, got %d", nodesPerCluster, count)
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
		cfg := setupClusters(c, t)

		if err := cfg.startCluster(ctx, cfg.srcCluster, false); err != nil {
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

		if err := cfg.copyAndTransferData(ctx); err != nil {
			t.Fatal(err)
		}

		// Start the destination cluster.
		if err := cfg.startCluster(ctx, cfg.dstCluster, true); err != nil {
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

func registerStopAndCopyGossipBootstrap(r registry.Registry) {
	runStopAndCopy := func(ctx context.Context, t test.Test, c cluster.Cluster, clearGossip bool) {
		cfg := setupClusters(c, t)

		if err := cfg.startCluster(ctx, cfg.srcCluster, false /*skipInit*/); err != nil {
			t.Fatal(err)
		}

		t.Status("importing bank workload on source cluster")
		c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach workload init bank --rows=1000 {pgurl:1}")

		// It should be extremely unlikely, but if we shut down the cluster too quickly after
		// startup it sometimes does not get the chance to persist any gossip bootstrap info.
		t.Status("sleeping for 30 seconds")
		time.Sleep(30 * time.Second)

		t.Status("stopping source cluster")
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), cfg.srcCluster)

		if err := cfg.copyAndTransferData(ctx); err != nil {
			t.Fatal(err)
		}

		if clearGossip {
			if err := cfg.clearGossipBootstrap(ctx); err != nil {
				t.Fatal(err)
			}
		}

		// Start the source cluster.
		if err := cfg.startCluster(ctx, cfg.srcCluster, true /*skipInit*/); err != nil {
			t.Fatal(err)
		}

		// Start the destination cluster.
		if err := cfg.startCluster(ctx, cfg.dstCluster, true /*skipInit*/); err != nil {
			t.Fatal(err)
		}

		if err := cfg.checkGossipNodes(ctx, cfg.srcCluster); err != nil {
			t.Fatal(err)
		}

		if err := cfg.checkGossipNodes(ctx, cfg.dstCluster); err != nil {
			if clearGossip {
				t.Fatal(err)
			} else {
				// If we didn't clear the gossip bootstrap info, it's expected that the destination
				// cluster will attempt to connect to the source cluster and fail to start properly.
				t.L().Printf("expected error checking gossip nodes on source cluster: %s", err)
				return
			}
		}
	}

	// stop-and-copy/gossip-bootstrap/clear tests that the cockroach debug clear-gossip-bootstrap
	// command works as expected. It:
	// 1. Starts the source cluster and imports some cold data into it.
	// 2. Shuts down the source cluster.
	// 3. Copies the storage data volumes to the destination cluster.
	// 4. Wipes the gossip bootstrap metadata on the destination cluster.
	// 5. Starts both clusters and verifies that they can both operate independently.
	//
	// This is intended to simulate a scenario where we take fixtures or snapshots of a cluster
	// for testing purposes. Without wiping the gossip bootstrap metadata, the second cluster
	// will read the persisted gossip bootstrap information and attempt to connect to the first cluster,
	// causing node liveness issues.
	r.Add(registry.TestSpec{
		Name:             "stop-and-copy/gossip-bootstrap/clear",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(2 * nodesPerCluster),
		CompatibleClouds: registry.AllExceptLocal,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          30 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runStopAndCopy(ctx, t, c, true)
		},
	})

	// stop-and-copy/gossip-bootstrap/no-clear is similar to above, except we _don't_
	// wipe the gossip bootstrap metadata on the destination cluster. We expect the
	// destination cluster to attempt to connect to the source cluster and fail
	// to start properly.
	r.Add(registry.TestSpec{
		Name:             "stop-and-copy/gossip-bootstrap/no-clear",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(2 * nodesPerCluster),
		CompatibleClouds: registry.AllExceptLocal,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          30 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runStopAndCopy(ctx, t, c, false)
		},
	})
}
