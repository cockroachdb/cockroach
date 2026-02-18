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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
)

// This operation runs a SHOW TABLES command on a randomly selected database.
// It does not require cleanup as it only reads data without making changes.

func runShowTables(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = conn.Close() }()

	// Pick a random database to run SHOW TABLES on
	dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)

	o.Status(fmt.Sprintf("Running SHOW TABLES command on database %s", dbName))

	// Execute the SHOW TABLES command
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SHOW TABLES FROM %s", dbName))
	if err != nil {
		o.Fatal(err)
	}
	defer rows.Close()

	// Process and log the results
	tableCount := 0
	cols, err := rows.Columns()
	if err != nil {
		o.Fatal(err)
	}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		for i := range vals {
			vals[i] = new(interface{})
		}
		if err := rows.Scan(vals...); err != nil {
			o.Fatal(err)
		}
		tableCount++
	}
	if err := rows.Err(); err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("SHOW TABLES completed successfully, found %d tables in database %s", tableCount, dbName))

	// No cleanup needed since this is a read-only operation
	return nil
}

func registerShowTables(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "show-tables",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            5 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run:                runShowTables,
	})
}
