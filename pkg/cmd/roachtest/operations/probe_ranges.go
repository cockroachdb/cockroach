// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func runProbeRanges(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	conn := c.Conn(ctx, o.L(), nid, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	probeReadRangeStmt := `select range_id, error from crdb_internal.probe_ranges(INTERVAL '1s', 'read') where error != ''`

	probeWriteRangeStmt := `select range_id, error from crdb_internal.probe_ranges(INTERVAL '1s', 'write') where error != ''`

	_, readErr := conn.ExecContext(ctx, probeReadRangeStmt)
	if readErr != nil {
		o.Fatal(readErr)
	}
	_, writeErr := conn.ExecContext(ctx, probeWriteRangeStmt)
	if writeErr != nil {
		o.Fatal(writeErr)
	}

	return nil
}

func registerProbeRanges(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "probe-ranges",
		Owner:              registry.OwnerSRE,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase, registry.OperationRequiresZeroUnavailableRanges},
		Run:                runProbeRanges,
	})
}
