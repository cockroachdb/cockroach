// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	multiStoreNodes         = 3
	multiStoreStoresPerNode = 2
)

func registerMultiStoreRemove(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "multi-store-remove",
		Owner:             registry.OwnerStorage,
		Cluster:           r.MakeClusterSpec(multiStoreNodes, spec.SSD(multiStoreStoresPerNode)),
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly),
		Timeout:           30 * time.Minute,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run:               runMultiStoreRemove,
	})
}

// runMultiStoreRemove tests that a cluster running multi-store nodes can
// survive having a store removed from one of its nodes. The test does the
// following:
//   - Creates a three node cluster. Each node has more than one store (i.e.
//     multi-store).
//   - Import enough data to place ranges across all stores.
//   - Stop n1 and restart it with a store missing.
//   - Wait for the removed store to be marked as dead.
//   - Wait for the ranges on the dead store to be moved to other stores.
func runMultiStoreRemove(ctx context.Context, t test.Test, c cluster.Cluster) {
	t.Status("starting cluster")
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.StoreCount = multiStoreStoresPerNode
	// TODO(jackson): Allow WAL failover to be enabled once it's able to
	// tolerate the removal of a store. Today, the mapping of failover
	// secondaries is fixed, making WAL failover incompatible with the removal
	// of a store.
	startOpts.RoachprodOpts.WALFailover = ""
	startSettings := install.MakeClusterSettings()
	// Speed up the replicate queue.
	startSettings.Env = append(startSettings.Env, "COCKROACH_SCAN_INTERVAL=30s")
	c.Start(ctx, t.L(), startOpts, startSettings, c.Range(1, 3))

	// Confirm that there are 6 stores live.
	t.Status("store setup")
	conn := c.Conn(ctx, t.L(), 2)
	defer conn.Close()
	var count int
	r := conn.QueryRowContext(ctx, `SELECT count(*) FROM crdb_internal.kv_store_status;`)
	if err := r.Scan(&count); err != nil {
		t.Fatalf("store status: %s", err)
	}
	wantStores := multiStoreNodes * multiStoreStoresPerNode
	if count != wantStores {
		t.Fatalf("expected %d stores; got %d", wantStores, count)
	}

	// Import data.
	t.Status("importing fixture")
	c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach", "workload", "fixtures", "import", "bank", "--db=tinybank",
		"--payload-bytes=100", "--ranges=1000", "--rows=100000", "--seed=1", "{pgurl:1}")

	// Ensure that all stores have ranges.
	t.Status("validating stores")
	r = conn.QueryRowContext(ctx, `SELECT count(*) FROM crdb_internal.kv_store_status WHERE range_count = 0;`)
	if err := r.Scan(&count); err != nil {
		t.Fatalf("range count: %s", err)
	}
	if count > 0 {
		t.Fatalf("wanted no stores without ranges; found %d", count)
	}

	// Lower the dead store detection threshold to make dead store detection
	// faster.
	const stmt = "SET CLUSTER SETTING server.time_until_store_dead = '30s'"
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		t.Fatal(err)
	}

	// Bring down node 1.
	t.Status("removing store from n1")
	node := c.Node(1)
	m := c.NewMonitor(ctx, node)
	m.ExpectDeaths(1)
	stopOpts := option.DefaultStopOpts()
	c.Stop(ctx, t.L(), stopOpts, node)

	// Start node 1 back up without one of its stores.
	t.Status("restarting n1")
	startOpts.RoachprodOpts.StoreCount = multiStoreStoresPerNode - 1
	if err := c.StartE(ctx, t.L(), startOpts, startSettings, node); err != nil {
		t.Fatalf("restarting node: %s", err)
	}

	// Wait for the store to be marked as dead.
	t.Status("awaiting store death")
	if err := retry.ForDuration(2*time.Minute, func() error {
		r = conn.QueryRowContext(ctx, `SELECT count(*) FROM crdb_internal.kv_store_status;`)
		if err := r.Scan(&count); err != nil {
			t.Fatalf("store status: %s", err)
		}
		want := multiStoreNodes*multiStoreStoresPerNode - 1
		if count == want {
			return nil
		}
		return errors.Newf("waiting for %d stores; got %d", want, count)
	}); err != nil {
		t.Fatalf("awaiting store death: %s", err)
	}

	// Wait for up-replication.
	// NOTE: At the time of writing, under-replicated ranges are computed using
	// node liveness, rather than store liveness, so we instead compare the range
	// count to the current per-store replica count to compute whether all there
	// are still under-replicated ranges from the dead store.
	// TODO(travers): Once #123561 is solved, re-work this.
	t.Status("awaiting up-replication")
	tStart := timeutil.Now()
	var oldReplicas int
	for {
		var ranges, replicas int
		if err := conn.QueryRowContext(ctx,
			`SELECT
			    (SELECT count(1) FROM crdB_internal.ranges) AS ranges
			  , (SELECT sum(range_count) FROM crdb_internal.kv_store_status) AS replicas`,
		).Scan(&ranges, &replicas); err != nil {
			t.Fatalf("replication status: %s", err)
		}
		if replicas == 3*ranges {
			t.L().Printf("up-replication complete")
			break
		}
		if timeutil.Since(tStart) > 30*time.Second || oldReplicas != replicas {
			t.L().Printf("still waiting for replication (%d / %d)", replicas, 3*ranges)
		}
		oldReplicas = replicas
		time.Sleep(5 * time.Second)
	}
	t.Status("done")
}
