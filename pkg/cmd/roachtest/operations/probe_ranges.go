// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runProbeRanges runs a read or write stmt against crdb_internal.probe_ranges.
func runProbeRanges(
	ctx context.Context, o operation.Operation, c cluster.Cluster, writeProbe bool,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	conn := c.Conn(ctx, o.L(), nid, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	probeType := "read"
	if writeProbe {
		probeType = "write"
	}

	o.Status(fmt.Sprintf("executing crdb_internal.probe-ranges %s statement against node %d", probeType, nid))

	probeRangeStmt := fmt.Sprintf(`select COUNT(error) as errors from crdb_internal.probe_ranges(INTERVAL '1s', '%s')`, probeType)

	range_errors, query_err := conn.QueryContext(ctx, probeRangeStmt)
	if query_err != nil {
		o.Fatal(query_err)
	}

	var errors int
	if scan_err := range_errors.Scan(&errors); scan_err != nil {
		o.Status(fmt.Sprintf("found %d errors while executing crdb_internal.probe-ranges %s statement against node %d", errors, probeType, nid))
	}

	return nil
}

func registerProbeRanges(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "probe-ranges/read",
		Owner:              registry.OwnerKV,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase, registry.OperationRequiresZeroUnavailableRanges},
		Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
			return runProbeRanges(ctx, o, c, false)
		},
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "probe-ranges/write",
		Owner:              registry.OwnerKV,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase, registry.OperationRequiresZeroUnavailableRanges},
		Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
			return runProbeRanges(ctx, o, c, true)
		},
	})
}
