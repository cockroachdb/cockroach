// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func runAddIndex(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.TenantName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()
	dbName := pickRandomDB(ctx, o, conn)
	tableName := pickRandomTable(ctx, o, conn, dbName)
	o.SetCleanupState("db", dbName)
	o.SetCleanupState("table", tableName)

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT column_name FROM [SHOW COLUMNS FROM %s.%s]", dbName, tableName))
	if err != nil {
		o.Fatal(err)
	}
	rows.Next()
	if !rows.Next() {
		o.Fatalf("not enough columns in table %s.%s", dbName, tableName)
	}
	var colName string
	if err := rows.Scan(&colName); err != nil {
		o.Fatal(err)
	}

	indexName := fmt.Sprintf("add_index_op_%d", rng.Uint32())
	o.Status(fmt.Sprintf("adding index to column %s", colName))
	createIndexStmt := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s)", indexName, dbName, tableName, colName)
	_, err = conn.ExecContext(ctx, createIndexStmt)
	if err != nil {
		o.Fatal(err)
	}
	o.SetCleanupState("index", indexName)

	o.Status(fmt.Sprintf("index %s created", indexName))
}

func cleanupAddIndex(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.TenantName(roachtestflags.VirtualCluster))
	defer conn.Close()

	dbName := o.GetCleanupState("db")
	tableName := o.GetCleanupState("table")
	indexName := o.GetCleanupState("index")

	o.Status(fmt.Sprintf("dropping index %s", indexName))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX %s.%s@%s", dbName, tableName, indexName))
	if err != nil {
		o.Fatal(err)
	}
}

func registerAddIndex(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "add-index",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          24 * time.Hour,
		CompatibleClouds: registry.AllClouds,
		Dependency:       registry.OperationRequiresDatabaseSchema,
		Run:              runAddIndex,
		CleanupWaitTime:  5 * time.Minute,
		Cleanup:          cleanupAddIndex,
	})
}
