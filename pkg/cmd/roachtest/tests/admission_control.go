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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
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

	registerMultiStoreOverload(r)
	registerSnapshotOverload(r)
	registerTPCCOverload(r)
	registerIndexOverload(r)
	// TODO(irfansharif): Once registerMultiTenantFairness is unskipped and
	// observed to be non-flaky for 3-ish months, transfer ownership to the AC
	// group + re-home it here.
}

// getWriteIO returns the median rate of IO from the provided list. It first
// runs a command to determine the PID of the CRDB process and then monitors how
// much activity it does over the next 30 seconds. Finally, it will take the max
// throughput across all machines in the list of nodes passed in. Typically this
// is used in conjunction with throttleWriteIO to limit the max IO relative to
// the current usage.
func getWriteIO(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption,
) int {
	// Measure the amount of write throughput over a 30 second window.
	cmd := `PID=$(systemctl show --property MainPID --value cockroach | xargs pgrep -P)
START=$(grep ^write_bytes /proc/$PID/io  | awk '{print $2}')
sleep 30 
END=$(grep ^write_bytes /proc/$PID/io  | awk '{print $2}')
echo \( $END - $START \) / 30 | bc`

	writeThroughput, err := c.RunWithDetails(ctx, t.L(), node, cmd)
	if err != nil {
		t.Fatal(err)
	}
	maxThroughput := 0
	for i, wt := range writeThroughput {
		storeThroughput, err := strconv.Atoi(strings.TrimSpace(wt.Stdout))
		if err != nil {
			t.L().Printf("stdout:\n%v\n", wt.Stdout)
			t.Fatal(err)
		}
		t.Status(fmt.Sprintf("store %d throughput = %d", i, storeThroughput))
		if storeThroughput > maxThroughput {
			maxThroughput = storeThroughput
		}
	}
	return maxThroughput
}

// throttleWriteIO will limit the write io performance of a node to the provided
// rate using Linux capabilities.
func throttleWriteIO(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption, rate int,
) {
	t.Status(fmt.Sprintf("throttling IO to (%d Bps) and running to verify no impact", rate))

	cmd := `DEV=$(lsblk | grep /mnt/data1 | cut -f 2 -d " ") 
echo $DEV %d | sudo tee  /sys/fs/cgroup/blkio/system.slice/cockroach.service/blkio.throttle.write_bps_device`
	cmd = fmt.Sprintf(cmd, rate)

	t.Status(fmt.Sprintf("running cmd: `%s`", cmd))
	c.Run(ctx, node, cmd)
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
