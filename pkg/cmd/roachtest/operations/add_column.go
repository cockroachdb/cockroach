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

type cleanupAddedColumn struct {
	db, table, column string
	locked            bool
}

func (cl *cleanupAddedColumn) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	if cl.locked {
		setSchemaLocked(ctx, o, conn, cl.db, cl.table, false /* lock */)
		defer setSchemaLocked(ctx, o, conn, cl.db, cl.table, true /* lock */)
	}
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
	isNotNull := rng.Float64() < 0.8
	colQualification := ""
	if r := rng.Float64(); r < 0.25 {
		colQualification = "AS ('virtual') VIRTUAL"
	} else if r < 0.5 {
		colQualification = "AS ('stored') STORED"
	} else {
		colQualification = "DEFAULT 'default'"
	}
	if isNotNull {
		colQualification += " NOT NULL"
	}

	// If the table's schema is locked, then unlock the table and make sure it will
	// be re-locked during cleanup.
	// TODO(#129694): Remove schema unlocking/re-locking once automation is internalized.
	locked := isSchemaLocked(o, conn, dbName, tableName)
	if locked {
		setSchemaLocked(ctx, o, conn, dbName, tableName, false /* lock */)
		// Re-lock the table if necessary, so that it stays locked during any wait
		// period before cleanup.
		defer setSchemaLocked(ctx, o, conn, dbName, tableName, true /* lock */)
	}

	o.Status(fmt.Sprintf("adding column %s to table %s.%s", colName, dbName, tableName))
	addColStmt := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s VARCHAR %s", dbName, tableName, colName, colQualification)
	_, err := conn.ExecContext(ctx, addColStmt)
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("column %s created", colName))

	return &cleanupAddedColumn{
		db:     dbName,
		table:  tableName,
		column: colName,
		locked: locked,
	}
}

func registerAddColumn(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "add-column",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runAddColumn,
	})
}
