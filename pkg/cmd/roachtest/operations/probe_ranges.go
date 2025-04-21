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
	ctx context.Context, o operation.Operation, c cluster.Cluster, write_probe bool,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	conn := c.Conn(ctx, o.L(), nid, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	probeType := "read"
	if write_probe {
		probeType = "write"
	}
	probeRangeStmt := fmt.Sprintf(`select range_id, error from crdb_internal.probe_ranges(INTERVAL '1s', '%s')`, probeType)

	_, err := conn.ExecContext(ctx, probeRangeStmt)
	if err != nil {
		o.Fatal(err)
	}

	return nil
}

func registerProbeRanges(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "probe-ranges-read",
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
		Name:               "probe-ranges-write",
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
