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

// This is a schema change operation that does the following:
// Operation Run:
// 1. Create a new user with randomly generated name.
// 2. GRANT ALL on a randomly selected DB and a table to the newly created user.
// Operation Cleanup
// 1. REVOKE ALL on the same table from the user.
// 2. Drop the user.

type revoke struct {
	db, table, dbUser string
}

func (cl *revoke) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = conn.Close() }()

	o.Status(fmt.Sprintf("Revoking ALL for user %s from table %s.%s", cl.dbUser, cl.db, cl.table))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("REVOKE ALL ON TABLE %s.%s FROM %s", cl.db, cl.table, cl.dbUser))
	if err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("Revoked ALL for user %s from table %s.%s", cl.dbUser, cl.db, cl.table))
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP USER %s", cl.dbUser))
	if err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("Dropped user %s", cl.dbUser))
}

func runGrant(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = conn.Close() }()

	rng, _ := randutil.NewTestRand()
	dbName := pickRandomDB(ctx, o, conn, systemDBs)
	tableName := pickRandomTable(ctx, o, conn, dbName)
	// the dbUser cannot have a number in the beginning. So, adding an "a" to ensure that the first letter will not be a number
	dbUser := fmt.Sprintf("roachprod_ops_add_role_%s", randutil.RandString(rng, 10, randutil.PrintableKeyAlphabet))

	o.Status(fmt.Sprintf("Creating user %s", dbUser))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", dbUser, dbUser))
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("Granting ALL for user %s to table %s.%s", dbUser, dbName, tableName))
	_, err = conn.ExecContext(ctx, fmt.Sprintf("GRANT ALL ON TABLE %s.%s TO %s", dbName, tableName, dbUser))
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("Granted ALL for user %s to table %s.%s", dbUser, dbName, tableName))

	return &revoke{
		db:     dbName,
		table:  tableName,
		dbUser: dbUser,
	}
}

func registerGrantRevoke(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "grant-revoke",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runGrant,
	})
}
