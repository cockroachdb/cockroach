// Copyright 2026 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

// findTPCCDatabase scans the cluster for a TPC-C database by looking
// for known database names (cct_tpcc, tpcc). Returns the database name
// if found, or an empty string if no TPC-C database exists.
func findTPCCDatabase(ctx context.Context, o operation.Operation, c cluster.Cluster) string {
	conn := c.Conn(
		ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster),
	)
	defer conn.Close()

	dbWhitelist := []string{"cct_tpcc", "tpcc"}
	rows, err := conn.QueryContext(
		ctx, "SELECT database_name FROM [SHOW DATABASES]",
	)
	if err != nil {
		o.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			o.Fatal(err)
		}
		for _, candidate := range dbWhitelist {
			if candidate == dbName {
				return dbName
			}
		}
	}
	return ""
}

// runTPCCCheck runs the TPC-C spec consistency checks against the live
// cluster. Each check verifies a relational invariant between TPC-C
// tables (e.g., W_YTD = sum(D_YTD)). Checks are read-only and safe to
// run concurrently with an active workload.
func runTPCCCheck(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	dbName := findTPCCDatabase(ctx, o, c)
	if dbName == "" {
		o.Status("no tpcc database found, skipping consistency checks")
		return nil
	}
	o.Status(fmt.Sprintf("found tpcc database: %s", dbName))

	// Open a connection targeting the TPC-C database so all queries
	// resolve table names correctly.
	conn := c.Conn(
		ctx, o.L(), 1,
		option.VirtualClusterName(roachtestflags.VirtualCluster),
		option.DBName(dbName),
	)
	defer conn.Close()

	checks := tpcc.AllChecks()
	for _, check := range checks {
		if check.LoadOnly || check.SkipForLongDuration || check.Expensive {
			continue
		}

		o.Status(fmt.Sprintf("running tpcc check %s", check.Name))
		if err := check.Fn(conn, "'-10s'"); err != nil {
			o.Errorf("tpcc check %s failed: %v", check.Name, err)
		} else {
			o.Status(fmt.Sprintf("tpcc check %s passed", check.Name))
		}
	}

	return nil
}

func registerTPCCCheck(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "tpcc-consistency-check",
		Owner:              registry.OwnerTestEng,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies: []registry.OperationDependency{
			registry.OperationRequiresPopulatedDatabase,
		},
		Run: runTPCCCheck,
	})
}
