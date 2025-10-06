// Copyright 2024 The Cockroach Authors.
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

func runManualCompaction(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	conn := c.Conn(ctx, o.L(), nid, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	dbName := pickRandomDB(ctx, o, conn, systemDBs)
	tableName := pickRandomTable(ctx, o, conn, dbName)
	sid := pickRandomStore(ctx, o, conn, nid)

	compactionStmt := fmt.Sprintf(`SELECT crdb_internal.compact_engine_span(
				%d, %d,
				(SELECT raw_start_key FROM [SHOW RANGES FROM TABLE %s.%s WITH KEYS] LIMIT 1),
				(SELECT raw_end_key FROM [SHOW RANGES FROM TABLE %s.%s WITH KEYS] LIMIT 1))`,
		nid, sid, dbName, tableName, dbName, tableName)
	o.Status(fmt.Sprintf("compacting a range for table %s.%s in n%d, s%d",
		dbName, tableName, nid, sid))
	_, err := conn.ExecContext(ctx, compactionStmt)
	if err != nil {
		o.Fatal(err)
	}
	return nil
}

func registerManualCompaction(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "manual-compaction",
		Owner:              registry.OwnerStorage,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.OnlyGCE,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnavailableRanges},
		Run:                runManualCompaction,
	})
}
