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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func runAddColumn(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1)
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()
	dbName := pickRandomDB(ctx, o, conn)
	tableName := pickRandomTable(ctx, o, conn, dbName)
	o.SetCleanupState("db", dbName)
	o.SetCleanupState("table", tableName)

	colName := fmt.Sprintf("add_column_op_%d", rng.Uint32())
	o.Status(fmt.Sprintf("adding column %s to table %s.%s", colName, dbName, tableName))
	addColStmt := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s VARCHAR", dbName, tableName, colName)
	_, err := conn.ExecContext(ctx, addColStmt)
	if err != nil {
		o.Fatal(err)
	}
	o.SetCleanupState("column", colName)

	o.Status(fmt.Sprintf("column %s created", colName))
}

func cleanupAddColumn(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1)
	defer conn.Close()

	dbName := o.GetCleanupState("db")
	tableName := o.GetCleanupState("table")
	columnName := o.GetCleanupState("column")

	o.Status(fmt.Sprintf("dropping column %s", columnName))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s CASCADE", dbName, tableName, columnName))
	if err != nil {
		o.Fatal(err)
	}
}

func registerAddColumn(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "add-column",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          4 * time.Hour,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.AllSuites,
		Dependency:       registry.OperationRequiresDatabaseSchema,
		Run:              runAddColumn,
		CleanupWaitTime:  5 * time.Minute,
		Cleanup:          cleanupAddColumn,
	})
}
