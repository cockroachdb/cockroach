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

type cleanupAddedColumn struct {
	db, table, column string
}

func (cl *cleanupAddedColumn) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping column %s", cl.column))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s CASCADE", cl.db, cl.table, cl.column))
	if err != nil {
		o.Fatal(err)
	}
}

func runAddColumn(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()
	dbName := pickRandomDB(ctx, o, conn, systemDBs)
	tableName := pickRandomTable(ctx, o, conn, dbName)

	colName := fmt.Sprintf("add_column_op_%d", rng.Uint32())
	o.Status(fmt.Sprintf("adding column %s to table %s.%s", colName, dbName, tableName))
	addColStmt := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s VARCHAR DEFAULT 'default'", dbName, tableName, colName)
	_, err := conn.ExecContext(ctx, addColStmt)
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("column %s created", colName))
	return &cleanupAddedColumn{
		db:     dbName,
		table:  tableName,
		column: colName,
	}
}

func registerAddColumn(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "add-column",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          24 * time.Hour,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:              runAddColumn,
	})
}
