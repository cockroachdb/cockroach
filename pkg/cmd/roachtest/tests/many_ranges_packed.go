// Copyright 2026 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Bin-packing dimensions for the many-ranges test.
//
// LSCT exercises range-count scaling on a 300-node cluster, which is too
// expensive to run continuously. This test substitutes node count with
// stores-per-node: 10 nodes × 8 stores = 80 stores hosting ~3M replicas
// (1M ranges × RF=3), or roughly 37k replicas per store. That is at the
// upper end of an un-quiesced per-store working budget estimated from
// architectural reasoning; the first job of this test is to either
// validate that estimate empirically or expose where it breaks down.
const (
	manyRangesPackedCRDBNodes     = 10
	manyRangesPackedStoresPerNode = 8
	manyRangesPackedSplits        = 1_000_000
)

func registerManyRangesPacked(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "many-ranges/1m",
		Owner: registry.OwnerKV,
		// 32 vCPU / 64 GB / 8 local SSDs per node. spec.CPU(32) selects the
		// Auto memory ratio (2 GB/vCPU above 16 vCPUs); see
		// pkg/cmd/roachtest/spec/machine_type.go. Multi-disk local SSD is
		// GCE-only, hence CompatibleClouds below.
		Cluster: r.MakeClusterSpec(
			manyRangesPackedCRDBNodes+1,
			spec.CPU(32),
			spec.Disks(manyRangesPackedStoresPerNode),
			spec.WorkloadNode(),
			spec.WorkloadNodeCPU(8),
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		// The pre-split alone can take well over an hour at 1M splits; add
		// up-replication wait + 1h workload + slack.
		Timeout:           8 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run:               runManyRangesPacked,
	})
}

// runManyRangesPacked establishes whether CockroachDB can sustain ~1M
// ranges on a small multi-store cluster, as a continuously-runnable
// alternative to LSCT for range-count scaling regressions.
//
// The test pre-splits a kv workload table into 1M ranges, waits for
// up-replication, then runs a light kv workload for an hour. Pre-split
// ranges start near-empty so disk and memory cost stay modest; the
// workload exists to keep replicas un-quiesced so the interesting failure
// modes (raft tick storms, queue lag, allocator throughput) actually
// surface. On success the test verifies the cluster is responsive, the
// expected number of ranges materialized, and replicas are within an
// imbalance band across the 80 stores.
func runManyRangesPacked(ctx context.Context, t test.Test, c cluster.Cluster) {
	settings := install.MakeClusterSettings()
	// Speed up scanner-driven work (queues, gossip) so up-replication
	// converges within the test timeout at 1M ranges.
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")

	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.StoreCount = manyRangesPackedStoresPerNode

	t.Status("starting multi-store cluster")
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Confirm all stores came up. A miscount here surfaces multi-store
	// configuration errors immediately rather than as a downstream
	// imbalance failure hours later.
	wantStores := manyRangesPackedCRDBNodes * manyRangesPackedStoresPerNode
	var gotStores int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM crdb_internal.kv_store_status").Scan(&gotStores); err != nil {
		t.Fatal(err)
	}
	if gotStores != wantStores {
		t.Fatalf("expected %d stores, got %d", wantStores, gotStores)
	}

	t.Status("waiting for system-range up-replication")
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), db))

	// Disable both queues so the pre-split count is preserved exactly:
	// the merge queue would otherwise re-merge the near-empty ranges, and
	// load-based splitting would inflate the count beyond what we asked
	// for.
	t.Status("disabling range merge and load-based split queues")
	for _, stmt := range []string{
		"SET CLUSTER SETTING kv.range_merge.queue_enabled = false",
		"SET CLUSTER SETTING kv.range_split.by_load_enabled = false",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(errors.Wrapf(err, "applying %q", stmt))
		}
	}

	t.Status(fmt.Sprintf("pre-splitting kv table into %d ranges", manyRangesPackedSplits))
	splitStart := timeutil.Now()
	c.Run(ctx, option.WithNodes(c.WorkloadNode()),
		fmt.Sprintf("./cockroach workload init kv --splits=%d {pgurl:1}",
			manyRangesPackedSplits))
	t.L().Printf("pre-split completed in %s", timeutil.Since(splitStart))

	t.Status("waiting for new-range up-replication")
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), db))

	const workloadDuration = 1 * time.Hour
	t.Status(fmt.Sprintf("running kv workload for %s to keep ranges un-quiesced",
		workloadDuration))
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
		"./cockroach workload run kv --duration=%s --read-percent=50 "+
			"--concurrency=64 --max-rate=5000 --tolerate-errors {pgurl:1-%d}",
		workloadDuration, manyRangesPackedCRDBNodes))

	t.Status("verifying cluster health")
	if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatal(errors.Wrap(err, "post-workload SELECT 1"))
	}

	var rangeCount int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM crdb_internal.ranges_no_leases").Scan(&rangeCount); err != nil {
		t.Fatal(err)
	}
	t.L().Printf("final range count: %d", rangeCount)
	if rangeCount < manyRangesPackedSplits {
		t.Fatalf("expected at least %d ranges, got %d",
			manyRangesPackedSplits, rangeCount)
	}

	// Per-store replica balance. The query is the same one
	// runReplicaImbalance uses in split.go. The 1.5× max/min bound is
	// intentionally loose for the first nightly runs; tighten once we
	// have a baseline.
	const balanceQuery = `
WITH ranges AS (
    SELECT replicas FROM crdb_internal.ranges_no_leases
), store_ids AS (
    SELECT unnest(replicas) AS store_id FROM ranges
), counts AS (
    SELECT store_id, count(1) AS n FROM store_ids GROUP BY store_id
)
SELECT min(n), max(n) FROM counts;
`
	var minPerStore, maxPerStore int
	if err := db.QueryRowContext(ctx, balanceQuery).Scan(&minPerStore, &maxPerStore); err != nil {
		t.Fatal(err)
	}
	ratio := float64(maxPerStore) / float64(minPerStore)
	t.L().Printf("replicas per store: min=%d max=%d max/min=%.2f",
		minPerStore, maxPerStore, ratio)
	const maxImbalance = 1.5
	if ratio > maxImbalance {
		t.Fatalf("replica distribution too imbalanced: max/min=%.2f (limit %.2f)",
			ratio, maxImbalance)
	}
}
