// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// This operation opens a SQL session and keeps it open for a long duration,
// periodically running a query from keepaliveQueries to prevent idle-timeout
// disconnection.
const (
	holdConnectionDuration  = 720 * time.Hour
	holdConnectionKeepAlive = 5 * time.Minute
	holdConnectionHeartbeat = 1 * time.Hour
)

var keepAliveQueries = []string{
	"SELECT 1",
	"SELECT 1, 2",
	"SELECT now()",
	"SELECT version()",
	"SELECT current_user",
	"SELECT count(*) FROM crdb_internal.cluster_sessions",
	"SELECT count(*) FROM crdb_internal.node_statement_statistics",
}

func runHoldConnection(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	pool := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = pool.Close() }()

	conn, err := pool.Conn(ctx)
	if err != nil {
		o.Fatal(err)
	}

	defer func() { _ = conn.Close() }()

	o.Status(fmt.Sprintf("Holding connection open for %s", holdConnectionDuration))

	ticker := time.NewTicker(holdConnectionKeepAlive)
	defer ticker.Stop()
	heartbeat := time.NewTicker(holdConnectionHeartbeat)
	defer heartbeat.Stop()
	deadline := time.After(holdConnectionDuration)

	start := timeutil.Now()
	queriesRun := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-deadline:
			o.Status("Hold duration elapsed, releasing connection")
			return nil
		case <-heartbeat.C:
			remaining := holdConnectionDuration - timeutil.Since(start)
			o.Status(fmt.Sprintf("session alive: %d queries run, %s remaining",
				queriesRun, remaining.Round(time.Minute)))
		case <-ticker.C:
			q := keepAliveQueries[rand.IntN(len(keepAliveQueries))]
			if _, err := conn.ExecContext(ctx, q); err != nil {
				o.Fatal(err)
			}
			queriesRun++
		}
	}
}

func registerHoldConnection(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "hold-connection",
		Owner:              registry.OwnerTestEng,
		Timeout:            holdConnectionDuration + 5*time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run:                runHoldConnection,
	})
}
