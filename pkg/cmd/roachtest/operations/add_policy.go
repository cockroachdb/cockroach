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

type cleanupRLSPolicy struct {
	db, table       string
	policies        []string
	forceRLS        bool
	rlsWasEnabled   bool
	forceRLSWasUsed bool
	locked          bool
}

func (cl *cleanupRLSPolicy) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	if cl.locked {
		helpers.SetSchemaLocked(ctx, o, conn, cl.db, cl.table, false /* lock */)
		defer helpers.SetSchemaLocked(ctx, o, conn, cl.db, cl.table, true /* lock */)
	}

	// Drop all policies that were created
	for _, policy := range cl.policies {
		o.Status(fmt.Sprintf("dropping policy %s on table %s.%s", policy, cl.db, cl.table))
		_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP POLICY %s ON %s.%s", policy, cl.db, cl.table))
		if err != nil {
			o.Fatal(err)
		}
	}

	// Disable RLS or restore it to its previous state
	if cl.rlsWasEnabled {
		// Restore to previous state
		o.Status(fmt.Sprintf("restoring row level security to previous state for %s.%s", cl.db, cl.table))

		forceClause := ""
		if cl.forceRLSWasUsed {
			forceClause = ", FORCE ROW LEVEL SECURITY"
		}

		_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.%s ENABLE ROW LEVEL SECURITY%s", cl.db, cl.table, forceClause))
		if err != nil {
			o.Fatal(err)
		}
	} else {
		// Disable RLS if it wasn't enabled before
		o.Status(fmt.Sprintf("disabling row level security for %s.%s", cl.db, cl.table))
		_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.%s DISABLE ROW LEVEL SECURITY", cl.db, cl.table))
		if err != nil {
			o.Fatal(err)
		}
	}
}

func runAddPolicy(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()

	// Pick a random table
	dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
	tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

	// Check if the table already has RLS enabled and if it has FORCE RLS enabled
	// by examining the CREATE TABLE statement
	var tblName, createStmt string
	err := conn.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", dbName, tableName)).Scan(&tblName, &createStmt)
	if err != nil {
		o.Fatal(err)
	}

	rlsEnabled := strings.Contains(createStmt, "ENABLE ROW LEVEL SECURITY")
	forceRLSWasUsed := strings.Contains(createStmt, "FORCE ROW LEVEL SECURITY")

	// If the table's schema is locked, then unlock the table and make sure it will
	// be re-locked during cleanup.
	locked := helpers.IsSchemaLocked(o, conn, dbName, tableName)
	if locked {
		helpers.SetSchemaLocked(ctx, o, conn, dbName, tableName, false /* lock */)
		defer helpers.SetSchemaLocked(ctx, o, conn, dbName, tableName, true /* lock */)
	}

	// Enable RLS on the table
	shouldForceRLS := rng.Intn(2) == 0 // 50% chance of using FORCE
	forceClause := ""
	if shouldForceRLS {
		forceClause = ", FORCE ROW LEVEL SECURITY"
	}

	o.Status(fmt.Sprintf("enabling row level security on table %s.%s%s", dbName, tableName, forceClause))
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.%s ENABLE ROW LEVEL SECURITY%s", dbName, tableName, forceClause))
	if err != nil {
		o.Fatal(err)
	}

	// Create between 5-20 policies
	numPolicies := rng.Intn(16) + 5
	policies := make([]string, 0, numPolicies)

	operations := []string{"ALL", "SELECT", "INSERT", "UPDATE", "DELETE"}
	users := []string{"public", "current_user", "session_user"}

	for i := 0; i < numPolicies; i++ {
		// Pick a random operation
		operation := operations[rng.Intn(len(operations))]

		// Pick a random user
		user := users[rng.Intn(len(users))]

		// Create unique policy name
		policyName := fmt.Sprintf("rls_policy_%s_%d", operation, rng.Uint32())
		policies = append(policies, policyName)

		o.Status(fmt.Sprintf("creating policy %s on table %s.%s for %s to %s",
			policyName, dbName, tableName, user, operation))

		withCheck := ""
		using := ""

		// WITH CHECK does is not supported for INSERT and DELETE
		if operation != "SELECT" && operation != "DELETE" {
			withCheck = "WITH CHECK (true)"
		}

		// USING is not supported for INSERT
		if operation != "INSERT" {
			using = "USING (true)"
		}

		_, err = conn.ExecContext(ctx, fmt.Sprintf(`
			CREATE POLICY %s ON %s.%s 
			FOR %s 
			TO %s 
			%s 
			%s
		`, policyName, dbName, tableName, operation, user, using, withCheck))
		if err != nil {
			o.Fatal(err)
		}
	}

	o.Status(fmt.Sprintf("created %d RLS policies on table %s.%s", numPolicies, dbName, tableName))

	// Wait for a random amount of time (1-60 seconds) before cleanup
	waitTime := time.Duration(rng.Intn(60)+1) * time.Second
	o.Status(fmt.Sprintf("waiting for %s before cleanup", waitTime))
	select {
	case <-time.After(waitTime):
	case <-ctx.Done():
		return nil
	}

	return &cleanupRLSPolicy{
		db:              dbName,
		table:           tableName,
		policies:        policies,
		forceRLS:        shouldForceRLS,
		rlsWasEnabled:   rlsEnabled,
		forceRLSWasUsed: forceRLSWasUsed,
		locked:          locked,
	}
}

func registerAddPolicy(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "add-policy",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runAddPolicy,
	})
}
