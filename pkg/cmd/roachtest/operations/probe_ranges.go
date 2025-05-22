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

// runProbeRanges runs a read or write stmt against crdb_internal.probe_ranges which
// tests for range availability which, if unhealthy, would result in serious failures
// at higher levels of the DB.
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

	probeRangeStmt := fmt.Sprintf(`select count(error) as errors from crdb_internal.probe_ranges(INTERVAL '1s', '%s') where error != ''`, probeType)

	range_errors, query_err := conn.QueryContext(ctx, probeRangeStmt)
	if query_err != nil {
		o.Fatal(query_err)
	}

	var errors int
	if !range_errors.Next() {
		o.Fatalf("Failed to scan value from crdb_internal.probe-ranges %s statement against node %d", probeType, nid)
	}
	if scanErr := range_errors.Scan(&errors); scanErr != nil {
		o.Fatalf("Failed to scan value from crdb_internal.probe-ranges %s statement against node %d", probeType, nid)
	} else {
		// This would reflect an issue in KV layer, in which case we run another query to collect helpful information.
		if errors > 0 {
			o.Status(fmt.Sprintf("Found %d errors while executing crdb_internal.probe-ranges %s statement against node %d", errors, probeType, nid))
			sampleErrorStmt := fmt.Sprintf(`select range_id, error from crdb_internal.probe_ranges(INTERVAL '1s', '%s') where error != '' LIMIT 5`, probeType)
			sampleErrors, query_err := conn.QueryContext(ctx, sampleErrorStmt)
			if query_err != nil {
				o.Fatal(query_err)
			}

			// Log the specific errors, to assist debugging.
			for sampleErrors.Next() {
				var rangeId int
				var rangeError string
				if scanError := sampleErrors.Scan(&rangeId, &rangeError); scanError != nil {
					o.Status(fmt.Sprintf("Failed to scan value from crdb_internal.probe-ranges %s statement against node %d", probeType, nid))
					o.Fatal(scanError)
				}
				o.Status(fmt.Sprintf("Error on node %d on range %d: %s", nid, rangeId, rangeError))
			}

			o.Fatalf("Found range errors when probing via crdb_internal.probe-ranges %s statement against node %d", probeType, nid)
		} else {
			o.Status(fmt.Sprintf("Successfully executed crdb_internal.probe-ranges %s statement against node %d with no errors.", probeType, nid))
		}
	}

	return nil
}

func registerProbeRanges(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "probe-ranges/read",
		Owner:              registry.OwnerKV, // Contact SRE first, as they own the prober.
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
		Owner:              registry.OwnerKV, // Contact SRE first, as they own the prober.
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase, registry.OperationRequiresZeroUnavailableRanges},
		Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
			return runProbeRanges(ctx, o, c, true)
		},
	})
}
