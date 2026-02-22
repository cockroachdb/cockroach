// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// genericSQLCleanup is a generic cleanup struct that drops a SQL object
// (e.g., ROLE, USER, TYPE, TABLE) using a simple DROP statement.
// It assumes the object name was generated and is safe to drop unconditionally.
type genericSQLCleanup struct {
	objectType string
	objectName string
}

func (cl *genericSQLCleanup) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping %s %s", cl.objectType, cl.objectName))
	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("DROP %s %s", cl.objectType, cl.objectName))
	if err != nil {
		o.Fatal(err)
	}
}

func runSQLOperation(
	objectType string, namePrefix string, createSQL func(name string) string,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
		defer conn.Close()

		rng, _ := randutil.NewPseudoRand()
		name := fmt.Sprintf("%s_%d", namePrefix, rng.Uint32())

		createStmt := createSQL(name)

		o.Status(fmt.Sprintf("creating %s %s", objectType, name))
		if _, err := conn.ExecContext(ctx, createStmt); err != nil {
			o.Fatal(err)
		}

		o.Status(fmt.Sprintf("%s %s created", objectType, name))

		return &genericSQLCleanup{
			objectType: objectType,
			objectName: name,
		}
	}
}

// runCreateTableAs picks a random database and table, then creates a new table
// using CREATE TABLE ... AS SELECT from that table.
func runCreateTableAs(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
	tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

	rng, _ := randutil.NewPseudoRand()
	name := fmt.Sprintf("roachtest_ctas_%d", rng.Uint32())

	createStmt := fmt.Sprintf(
		"CREATE TABLE %s.%s AS SELECT * FROM %s.%s LIMIT 100",
		dbName, name, dbName, tableName,
	)

	o.Status(fmt.Sprintf("creating TABLE %s.%s from %s.%s", dbName, name, dbName, tableName))
	if _, err := conn.ExecContext(ctx, createStmt); err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("TABLE %s.%s created", dbName, name))
	return &genericSQLCleanup{
		objectType: "TABLE",
		objectName: fmt.Sprintf("%s.%s", dbName, name),
	}
}

// runCreateMaterializedView picks a random database and table, then creates a
// materialized view selecting from that table.
func runCreateMaterializedView(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
	tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

	rng, _ := randutil.NewPseudoRand()
	name := fmt.Sprintf("roachtest_mv_%d", rng.Uint32())

	createStmt := fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s LIMIT 100",
		dbName, name, dbName, tableName,
	)

	o.Status(fmt.Sprintf("creating MATERIALIZED VIEW %s.%s from %s.%s", dbName, name, dbName, tableName))
	if _, err := conn.ExecContext(ctx, createStmt); err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("MATERIALIZED VIEW %s.%s created", dbName, name))
	return &genericSQLCleanup{
		objectType: "MATERIALIZED VIEW",
		objectName: fmt.Sprintf("%s.%s", dbName, name),
	}
}

// registerCreateSQLOperations registers a group of SQL object creation operations
// (e.g., create-role, create-user, create-type, create-table) under a shared
// framework that handles execution and cleanup consistently.
func registerCreateSQLOperations(r registry.Registry) {
	objs := []struct {
		name       string
		objectType string
		prefix     string
		createSQL  func(name string) string
	}{
		{
			name:       "create-role",
			objectType: "ROLE",
			prefix:     "roachtest_role",
			createSQL: func(name string) string {
				return fmt.Sprintf("CREATE ROLE %s", name)
			},
		},
		{
			name:       "create-user",
			objectType: "USER",
			prefix:     "roachtest_user",
			createSQL: func(name string) string {
				return fmt.Sprintf("CREATE USER %s", name)
			},
		},
		{
			name:       "create-type",
			objectType: "TYPE",
			prefix:     "roachtest_enum",
			createSQL: func(name string) string {
				enumVals := []string{"'one'", "'two'", "'three'"}
				return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)", name, strings.Join(enumVals, ", "))
			},
		},
		{
			name:       "create-table",
			objectType: "TABLE",
			prefix:     "roachtest_table",
			createSQL: func(name string) string {
				return fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, val STRING)", name)
			},
		},
		{
			name:       "create-sequence",
			objectType: "SEQUENCE",
			prefix:     "roachtest_sequence",
			createSQL: func(name string) string {
				return fmt.Sprintf("CREATE SEQUENCE %s", name)
			},
		},
	}

	for _, obj := range objs {
		r.AddOperation(registry.OperationSpec{
			Name:               obj.name,
			Owner:              registry.OwnerSQLFoundations,
			Timeout:            time.Hour,
			CompatibleClouds:   registry.AllClouds,
			CanRunConcurrently: registry.OperationCanRunConcurrently,
			Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
			Run:                runSQLOperation(obj.objectType, obj.prefix, obj.createSQL),
		})
	}

	// Register operations that need to discover tables dynamically.
	r.AddOperation(registry.OperationSpec{
		Name:               "create-table-as",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runCreateTableAs,
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "create-materialized-view",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runCreateMaterializedView,
	})
}
