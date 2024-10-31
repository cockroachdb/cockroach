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

type cleanupAddedDatabase struct {
	db string
}

func (cl *cleanupAddedDatabase) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping database %s", cl.db))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s", cl.db))
	if err != nil {
		o.Fatal(err)
	}
}

func runAddDatabase(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()
	withOpts := ""
	if rng.Float64() < 0.25 {
		withOpts += " ENCODING = 'UTF-8'"
	}
	if rng.Float64() < 0.25 {
		withOpts += " CONNECTION LIMIT = -1"
	}
	if rng.Float64() < 0.5 {
		roleName := pickRandomRole(ctx, o, conn)
		withOpts += fmt.Sprintf(" OWNER = %s", roleName)
	}
	// TODO(jaylim-crl): Consider creating a multi-region database in the future.

	dbName := fmt.Sprintf("add_database_op_%d", rng.Uint32())
	o.Status(fmt.Sprintf("adding database %s", dbName))
	createDatabaseStmt := fmt.Sprintf("CREATE DATABASE %s%s", dbName, withOpts)
	_, err := conn.ExecContext(ctx, createDatabaseStmt)
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("database %s created", dbName))

	return &cleanupAddedDatabase{db: dbName}
}

func registerAddDatabase(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "add-database",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          24 * time.Hour,
		CompatibleClouds: registry.AllClouds,
		// Note: If we decide to add tables to this database, we should consider
		// updating this to OperationCannotRunConcurrently. This would help
		// prevent indexes and columns from being added to the created tables,
		// which we'll be dropping during the cleanup phase unless we want to
		// exclude the created database entirely from those operations.
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run:                runAddDatabase,
	})
}
