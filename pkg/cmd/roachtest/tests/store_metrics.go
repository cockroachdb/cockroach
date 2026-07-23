// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/slices"
)

const storeMetricsStoresPerNode = 4

func registerStoreMetrics(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "store-metrics",
		Owner: registry.OwnerKV,
		// Single node with multiple stores to test that all stores have the same
		// set of metrics.
		Cluster:           r.MakeClusterSpec(1, spec.Disks(storeMetricsStoresPerNode)),
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Weekly),
		Timeout:           5 * time.Minute,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run:               runStoreMetrics,
	})
}

type metricKey struct {
	name    string
	hasNode bool
}

// runStoreMetrics verifies that store metrics are correctly labeled, including
// the node label for asynchronously bootstrapped stores. See issue #159046 for
// an example of such a bug.
//
// This test will catch inconsistency bugs across the entire system, not just
// store-related metrics. Failures of this test should be forwarded to the
// appropriate team based on the metric.
//
// The test:
//  1. Starts a single-node cluster with multiple stores.
//  2. Fetches /_status/vars and parses store-labeled metrics (excluding histogram buckets).
//  3. Builds a map keyed by (metric name, hasNodeLabel) -> count of stores.
//  4. Asserts (with retry) that each count equals the store count.
//  5. Stops and restarts the node.
//  6. Repeats the check and compares the two maps.
func runStoreMetrics(ctx context.Context, t test.Test, c cluster.Cluster) {
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.StoreCount = storeMetricsStoresPerNode
	startSettings := install.MakeClusterSettings()

	getMetricsMap := func(ctx context.Context) (map[metricKey]int, error) {
		var result map[metricKey]int
		var lastInconsistencies []string
		if err := retry.ForDuration(30*time.Second, func() error {
			metricsMap, err := buildMetricsData(ctx, t, c)
			if err != nil {
				lastInconsistencies = nil
				return err
			}
			var inconsistencies []string
			for key, count := range metricsMap {
				if count != storeMetricsStoresPerNode {
					inconsistencies = append(
						inconsistencies,
						fmt.Sprintf("metric %q (has_node=%t) has %d entries; want %d",
							key.name, key.hasNode, count, storeMetricsStoresPerNode),
					)
				}
			}
			lastInconsistencies = inconsistencies
			if len(inconsistencies) > 0 {
				return errors.New("metric inconsistencies found")
			}
			result = metricsMap
			return nil
		}); err != nil {
			if len(lastInconsistencies) > 20 {
				t.L().Printf("found %d metric inconsistencies (showing first 20)", len(lastInconsistencies))
				lastInconsistencies = lastInconsistencies[:20]
			} else {
				t.L().Printf("found %d metric inconsistencies: ", len(lastInconsistencies))
			}
			t.L().Printf("%s", strings.Join(lastInconsistencies, " "))
			return nil, err
		}
		return result, nil
	}

	t.Status("starting cluster with multiple stores")
	c.Start(ctx, t.L(), startOpts, startSettings, c.Node(1))

	t.Status("checking metrics parity between stores after initial start")
	metricsMap1, err := getMetricsMap(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Status("stopping node")
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(1))

	t.Status("restarting node")
	c.Start(ctx, t.L(), startOpts, startSettings, c.Node(1))

	t.Status("checking metrics parity between stores after restart")
	metricsMap2, err := getMetricsMap(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(metricsMap1, metricsMap2) {
		var diffs []string

		// Find metrics in map1 but not in map2.
		for key, count1 := range metricsMap1 {
			if _, ok := metricsMap2[key]; !ok {
				diffs = append(diffs, fmt.Sprintf("metric %q (has_node=%t): in map1 (count=%d) but NOT in map2",
					key.name, key.hasNode, count1))
			}
		}

		// Find metrics in map2 but not in map1.
		for key, count2 := range metricsMap2 {
			if _, ok := metricsMap1[key]; !ok {
				diffs = append(diffs, fmt.Sprintf("metric %q (has_node=%t): in map2 (count=%d) but NOT in map1",
					key.name, key.hasNode, count2))
			}
		}

		slices.Sort(diffs)
		t.L().Printf("found %d metric differences between before and after restart:", len(diffs))
		for _, diff := range diffs {
			t.L().Printf("  %s", diff)
		}
		t.Fatalf("metrics map mismatch after restart")
	}
}

func buildMetricsData(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (map[metricKey]int, error) {
	adminUIAddrs, err := c.ExternalAdminUIAddr(
		ctx, t.L(), c.Node(1), option.VirtualClusterName(install.SystemInterfaceName),
	)
	if err != nil {
		return nil, err
	}
	url := "https://" + adminUIAddrs[0] + "/_status/vars"
	client := roachtestutil.DefaultHTTPClient(
		c, t.L(), roachtestutil.VirtualCluster(install.SystemInterfaceName),
	)
	resp, err := client.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.Newf("invalid non-200 status code %v from %s", resp.StatusCode, url)
	}

	metricsMap := make(map[metricKey]int)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		name, labels, ok := parsePromLine(scanner.Text())
		if !ok {
			continue
		}
		hasStore, hasNode, hasLE, err := parseLabelInfo(labels)
		if err != nil {
			return nil, err
		}
		if hasLE || !hasStore {
			continue
		}
		key := metricKey{name: name, hasNode: hasNode}
		metricsMap[key]++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(metricsMap) == 0 {
		return nil, errors.New("no store metrics found in /_status/vars")
	}

	return metricsMap, nil
}

// parsePromLine extracts the metric name and the label section from a
// Prometheus exposition line. It skips empty lines and comment/metadata lines
// that start with '#'. The expected format is:
//
//	metric_name{key="value",other="value"} <number>
//
// It returns the metric name and the raw label string between { and }.
func parsePromLine(line string) (name, labels string, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	start := strings.Index(line, "{")
	if start == -1 {
		return "", "", false
	}
	end := strings.Index(line[start+1:], "}")
	if end == -1 {
		return "", "", false
	}
	end += start + 1
	name = strings.TrimSpace(line[:start])
	labels = line[start+1 : end]
	if name == "" {
		return "", "", false
	}
	return name, labels, true
}

// parseLabelInfo scans a raw label string and extracts presence and values.
// The labels string is expected to be the comma-separated key=value pairs
// inside {...} (e.g. `store="1",node_id="1",le="0.5"`).
func parseLabelInfo(labels string) (hasStore bool, hasNode bool, hasLE bool, err error) {
	for _, part := range strings.Split(labels, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		key := kv[0]
		switch key {
		case "store":
			hasStore = true
		case "node_id":
			hasNode = true
		case "le":
			hasLE = true
		}
	}
	return hasStore, hasNode, hasLE, nil
}
