// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const storeMetricsStoresPerNode = 4

func registerStoreMetrics(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "store-metrics",
		Owner: registry.OwnerKV,
		// Single node with multiple stores to test that all stores have the same
		// set of metrics, catching bugs where secondary stores (which bootstrap
		// asynchronously) are missing metrics.
		Cluster:           r.MakeClusterSpec(1, spec.SSD(storeMetricsStoresPerNode)),
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly),
		Timeout:           5 * time.Minute,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run:               runStoreMetrics,
	})
}

// runStoreMetrics verifies that all stores on a multi-store node have the same
// set of metrics. This test catches bugs where secondary store metrics are not
// properly hooked up due to asynchronous bootstrap. See issue #159046 for an
// example of such a bug.
//
// The test:
//  1. Starts a single-node cluster with multiple stores.
//  2. Queries crdb_internal.node_metrics to get the set of metric names for each store.
//  3. Computes the symmetric difference between stores' metric sets.
//  4. Asserts (with retry) that the symmetric difference is empty.
//  5. Stops and restarts the node.
//  6. Repeats the check to verify metrics are correct after restart.
func runStoreMetrics(ctx context.Context, t test.Test, c cluster.Cluster) {
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.StoreCount = storeMetricsStoresPerNode
	startSettings := install.MakeClusterSettings()

	// checkMetricsParity queries the metrics for each store and verifies they
	// have the same set of metric names.
	checkMetricsParity := func(ctx context.Context) error {
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()

		// Query metric names for each store.
		rows, err := conn.QueryContext(ctx, `
			SELECT store_id, name
			FROM crdb_internal.node_metrics
			WHERE store_id IS NOT NULL
			ORDER BY store_id, name
		`)
		if err != nil {
			return errors.Wrap(err, "querying store metrics")
		}
		defer rows.Close()

		// Collect metrics per store.
		storeMetrics := make(map[int][]string)
		for rows.Next() {
			var storeID int
			var name string
			if err := rows.Scan(&storeID, &name); err != nil {
				return errors.Wrap(err, "scanning store metrics")
			}
			storeMetrics[storeID] = append(storeMetrics[storeID], name)
		}
		if err := rows.Err(); err != nil {
			return errors.Wrap(err, "iterating store metrics")
		}

		if len(storeMetrics) < storeMetricsStoresPerNode {
			return errors.Newf("expected at least 2 stores, got %d", len(storeMetrics))
		}

		// Get sorted store IDs for consistent comparison.
		storeIDs := maps.Keys(storeMetrics)
		sort.Ints(storeIDs)

		// Compare each store's metrics to the first store.
		referenceStoreID := storeIDs[0]
		referenceMetrics := storeMetrics[referenceStoreID]

		for _, storeID := range storeIDs[1:] {
			currentMetrics := storeMetrics[storeID]
			onlyInReference, onlyInCurrent := symmetricDiff(referenceMetrics, currentMetrics)

			if len(onlyInReference) > 0 || len(onlyInCurrent) > 0 {
				return errors.Newf(
					"metric mismatch between store %d and store %d:\n"+
						"  only in store %d: %v\n"+
						"  only in store %d: %v",
					referenceStoreID, storeID,
					referenceStoreID, onlyInReference,
					storeID, onlyInCurrent,
				)
			}
		}

		t.L().Printf("all %d stores have identical metric sets (%d metrics each)",
			len(storeMetrics), len(referenceMetrics))
		return nil
	}

	// Start the cluster.
	t.Status("starting cluster with multiple stores")
	c.Start(ctx, t.L(), startOpts, startSettings, c.Node(1))

	// Wait for metrics parity after initial start.
	t.Status("checking metrics parity after initial start")
	if err := retry.ForDuration(30*time.Second, func() error {
		return checkMetricsParity(ctx)
	}); err != nil {
		t.Fatal(err)
	}

	// Stop and restart the node.
	t.Status("stopping node")
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(1))

	t.Status("restarting node")
	c.Start(ctx, t.L(), startOpts, startSettings, c.Node(1))

	// Wait for metrics parity after restart.
	t.Status("checking metrics parity after restart")
	if err := retry.ForDuration(30*time.Second, func() error {
		return checkMetricsParity(ctx)
	}); err != nil {
		t.Fatal(err)
	}

	t.Status("done")
}

// symmetricDiff computes the symmetric difference between two string slices.
// Returns elements only in a and elements only in b.
func symmetricDiff(a, b []string) (onlyInA, onlyInB []string) {
	aSet := make(map[string]struct{}, len(a))
	for _, s := range a {
		aSet[s] = struct{}{}
	}

	bSet := make(map[string]struct{}, len(b))
	for _, s := range b {
		bSet[s] = struct{}{}
	}

	for _, s := range a {
		if _, ok := bSet[s]; !ok {
			onlyInA = append(onlyInA, s)
		}
	}

	for _, s := range b {
		if _, ok := aSet[s]; !ok {
			onlyInB = append(onlyInB, s)
		}
	}

	return onlyInA, onlyInB
}
