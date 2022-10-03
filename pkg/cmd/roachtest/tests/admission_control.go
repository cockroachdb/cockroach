// Copyright 2022 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

func registerAdmission(r registry.Registry) {
	// TODO(irfansharif): Can we write these tests using cgroups instead?
	// Limiting CPU/bandwidth directly?

	// TODO(irfansharif): Some of these tests hooks into prometheus/grafana.
	// It'd be nice to use the grafana annotations API to explicitly annotate
	// the points at which we do cluster-level things, like set zone configs to
	// trigger a round of snapshots.

	// TODO(irfansharif): Integrate with probabilistic tracing machinery,
	// capturing outliers automatically for later analysis.

	// TODO(irfansharif): Look into clusterstats and what that emits to
	// roachperf. Need to munge with histogram data to compute % test run spent
	// over some latency threshold. Will be Useful to track over time.

	registerElasticControlForBackups(r)
	registerMultiStoreOverload(r)
	registerSnapshotOverload(r)
	registerTPCCOverload(r)
	registerIndexOverload(r)
	// TODO(irfansharif): Once registerMultiTenantFairness is unskipped and
	// observed to be non-flaky for 3-ish months, transfer ownership to the AC
	// group + re-home it here.
}

// setupAdmissionControlGrafana sets up prometheus and grafana on the last node of the cluster.
// It imports the dashboard at http://go.crdb.dev/p/snapshot-admission-control-grafana. As part
// of setting up the cluster.
func setupAdmissionControlGrafana(ctx context.Context, t test.Test, c cluster.Cluster) func() {
	workloadNode := c.Spec().NodeCount
	t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0])
	promCfg.WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
	promCfg.WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
	promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
		"/", makeWorkloadScrapeNodes(c.Node(workloadNode).InstallNodes()[0], []workloadInstance{
			{nodes: c.Node(workloadNode)},
		})))
	promCfg.WithGrafanaDashboard("http://go.crdb.dev/p/snapshot-admission-control-grafana")
	_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
	return cleanupFunc
}
