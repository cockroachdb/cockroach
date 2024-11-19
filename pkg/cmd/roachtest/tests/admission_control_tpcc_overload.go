// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// tpccOlapQuery is a contrived query that seems to do serious damage to a
// cluster. The query itself is a hash join with a selective filter and a
// limited sort.
const tpccOlapQuery = `SELECT
    i_id, s_w_id, s_quantity, i_price
FROM
    stock JOIN item ON s_i_id = i_id
WHERE
    s_quantity < 100 AND i_price > 90
ORDER BY
    i_price DESC, s_quantity ASC
LIMIT
    100;`

type tpccOLAPSpec struct {
	Nodes       int
	CPUs        int
	Warehouses  int
	Concurrency int
}

func (s tpccOLAPSpec) run(ctx context.Context, t test.Test, c cluster.Cluster) {
	setupTPCC(
		ctx, t, t.L(), c, tpccOptions{
			Warehouses: s.Warehouses, SetupType: usingImport,
		})
	const queryFileName = "queries.sql"
	// querybench expects the entire query to be on a single line.
	queryLine := `"` + strings.Replace(tpccOlapQuery, "\n", " ", -1) + `"`
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), "echo", queryLine, "> "+queryFileName)
	t.Status("waiting")
	m := c.NewMonitor(ctx, c.CRDBNodes())
	rampDuration := 2 * time.Minute
	duration := 3 * time.Minute
	labels := getTpccLabels(s.Warehouses, rampDuration, duration, map[string]string{"concurrency": strconv.Itoa(s.Concurrency)})
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running querybench")
		cmd := fmt.Sprintf(
			"./workload run querybench --db tpcc"+
				" --tolerate-errors=t"+
				" --concurrency=%d"+
				" --query-file %s "+
				" %s"+
				" --ramp=%s --duration=%s {pgurl:1-%d}",
			s.Concurrency, queryFileName, roachtestutil.GetWorkloadHistogramArgs(t, c, labels),
			rampDuration, duration, c.Spec().NodeCount-1)
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	})
	m.Wait()

	// Before checking liveness, set the gRPC logging[^1] to verbose. We stopped the
	// load so this should be OK in terms of overhead, and it can explain to us why
	// we see gRPC connections buckle in this workload. See:
	//
	// https://github.com/cockroachdb/cockroach/issues/96543
	//
	// [^1]: google.golang.org/grpc/grpclog/component.go
	for i := 1; i <= c.Spec().NodeCount-1; i++ {
		_, err := c.Conn(ctx, t.L(), i).ExecContext(ctx, `SELECT crdb_internal.set_vmodule('component=2');`)
		if err != nil {
			t.L().PrintfCtx(ctx, "ignoring vmodule error: %s", err)
		}
	}
	defer func() {
		for i := 1; i <= c.Spec().NodeCount-1; i++ {
			_, err := c.Conn(ctx, t.L(), i).ExecContext(ctx, `SELECT crdb_internal.set_vmodule('');`)
			if err != nil {
				t.L().PrintfCtx(ctx, "ignoring vmodule-reset error: %s", err)
			}
		}
	}()
	verifyNodeLiveness(ctx, c, t, duration)
}

// Check that node liveness did not fail more than maxFailures times across
// all of the nodes.
func verifyNodeLiveness(
	ctx context.Context, c cluster.Cluster, t test.Test, runDuration time.Duration,
) {
	const maxFailures = 10
	adminURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(1))
	if err != nil {
		t.Fatal(err)
	}
	now := timeutil.Now()
	var response tspb.TimeSeriesQueryResponse
	// Retry because timeseries queries can fail if the underlying inter-node
	// connections are in a failed state which can happen due to overload.
	// Now that the load has stopped, this should resolve itself soon.
	// Even with 60 retries we'll at most spend 30s attempting to fetch
	// the metrics.
	if err := retry.WithMaxAttempts(ctx, retry.Options{
		MaxBackoff: 500 * time.Millisecond,
	}, 60, func() (err error) {
		response, err = getMetrics(ctx, c, t, adminURLs[0], "", now.Add(-runDuration), now, []tsQuery{
			{
				name:      "cr.node.liveness.heartbeatfailures",
				queryType: total,
			},
		})
		return err
	}); err != nil {
		t.Fatalf("failed to fetch liveness metrics: %v", err)
	}
	if len(response.Results[0].Datapoints) <= 1 {
		t.Fatalf("not enough datapoints in timeseries query response: %+v", response)
	}
	datapoints := response.Results[0].Datapoints
	finalCount := int(datapoints[len(datapoints)-1].Value)
	initialCount := int(datapoints[0].Value)
	if failures := finalCount - initialCount; failures > maxFailures {
		t.Fatalf("Node liveness failed %d times, expected no more than %d",
			failures, maxFailures)
	} else {
		t.L().Printf("Node liveness failed %d times which is fewer than %d",
			failures, maxFailures)
	}
}

func registerTPCCOverload(r registry.Registry) {
	specs := []tpccOLAPSpec{
		{
			CPUs:        8,
			Concurrency: 96,
			Nodes:       3,
			Warehouses:  50,
		},
	}
	for _, s := range specs {
		name := fmt.Sprintf("admission-control/tpcc-olap/nodes=%d/cpu=%d/w=%d/c=%d",
			s.Nodes, s.CPUs, s.Warehouses, s.Concurrency)
		r.Add(registry.TestSpec{
			Name:                       name,
			Owner:                      registry.OwnerAdmissionControl,
			Benchmark:                  true,
			CompatibleClouds:           registry.AllExceptAWS,
			Suites:                     registry.Suites(registry.Weekly),
			Cluster:                    r.MakeClusterSpec(s.Nodes+1, spec.CPU(s.CPUs), spec.WorkloadNode()),
			Run:                        s.run,
			EncryptionSupport:          registry.EncryptionMetamorphic,
			Leases:                     registry.MetamorphicLeases,
			Timeout:                    20 * time.Minute,
			RequiresDeprecatedWorkload: true, // uses querybench
		})
	}
}

// This test begins a ramping TPCC workload that will overwhelm the CRDB nodes.
// There is no way to "pass" this test since the 6 nodes can't possibly handle
// 10K warehouses. If they could handle this load, then the test should be
// changed to increase that count. The purpose of the test is twofold. First, to
// ensure nodes don't crash under unsustainable overload, but instead do
// something more reasonable. Second, to check that the throughput of the system
// stays close to the max rate even under high overload conditions. Without the
// custom setting to limit the number of open transactions per gateway, CRDB
// nodes will eventually OOM around 3-4 hours through the ramp period.
func registerTPCCSevereOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/tpcc-severe-overload",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(7, spec.CPU(8), spec.WorkloadNode(), spec.WorkloadNodeCPU(8)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          5 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			warehouseCount := 10000
			rampTime := 4 * time.Hour
			if c.IsLocal() {
				warehouseCount = 10
				rampTime = 4 * time.Minute
			}

			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
			t.Status("initializing (~30m)")
			cmd := fmt.Sprintf(
				"./cockroach workload fixtures import tpcc --checks=false --warehouses=%d {pgurl:1}",
				warehouseCount,
			)
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()
			_, err := db.Exec(`SET CLUSTER SETTING server.max_open_transactions_per_gateway = 100`)
			require.NoError(t, err)
			// Drop admin privileges, server.max_open_transactions_per_gateway
			// does not apply to admin users.
			_, err = db.Exec("REVOKE admin FROM roachprod")
			require.NoError(t, err)

			// Without the cluster setting, this test run passes through 4 "phases"
			// 1) No admission control, low latencies (up to ~1500 warehouses).
			// 2) Admission control delays, growing latencies (up to ~3000 warehouses).
			// 3) High latencies (100s+), queues building (up to ~4500 warehouse).
			// 4) Memory and goroutine unbounded growth with eventual node crashes (up to ~6000 warehouse).
			t.Status("running workload (fails in ~1 hours)")
			cmd = fmt.Sprintf(
				"./cockroach workload run tpcc --duration=1s --ramp=%s --tolerate-errors --warehouses=%d {pgurl%s}",
				rampTime,
				warehouseCount,
				c.CRDBNodes(),
			)
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		},
	})
}
