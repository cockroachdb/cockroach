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
	originalRLSStmt string
	waitDuration    time.Duration
}

func (cl *cleanupRLSPolicy) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status(fmt.Sprintf("Scheduling cleanup to happen after %s", cl.waitDuration))

	// Start a goroutine to handle the wait and cleanup. Since this runs in the
	// background, we also create a new context so that the background goroutine
	// isn't aborted by the parent context.
	go func() {
		newCtx := context.Background()

		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			newCtx, cancel = context.WithDeadline(newCtx, deadline.Add(cl.waitDuration))
			defer cancel()
		}
		ctx = newCtx

		// Wait for the specified duration before performing cleanup.
		time.Sleep(cl.waitDuration)

		conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
		defer conn.Close()

		// Switch to the database where the table is located
		o.Status(fmt.Sprintf("switching to database %s for cleanup", cl.db))
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", cl.db)); err != nil {
			o.Fatal(err)
		}

		// Drop all policies that were created
		for _, policy := range cl.policies {
			o.Status(fmt.Sprintf("dropping policy %s on table %s.%s", policy, cl.db, cl.table))
			_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP POLICY %s ON %s.%s", policy, cl.db, cl.table))
			if err != nil {
				o.Fatal(err)
			}
		}

		// Restore original RLS state or disable it if it wasn't enabled before
		if cl.originalRLSStmt != "" {
			// Restore the original RLS state
			o.Status(fmt.Sprintf("restoring original row level security state for %s.%s", cl.db, cl.table))
			if _, err := conn.ExecContext(ctx, cl.originalRLSStmt); err != nil {
				o.Fatal(err)
			}
		} else {
			// If the table didn't have RLS before, disable it
			o.Status(fmt.Sprintf("disabling row level security for %s.%s", cl.db, cl.table))
			_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.%s DISABLE ROW LEVEL SECURITY, NO FORCE ROW LEVEL SECURITY", cl.db, cl.table))
			if err != nil {
				o.Fatal(err)
			}
		}
	}()
}

func runAddRLSPolicy(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = conn.Close() }()

	rng, _ := randutil.NewPseudoRand()

	// Pick a random table
	dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
	tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

	// Check if the table already has RLS enabled and store the original statement if needed
	var tblName, createStmt string
	err := conn.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", dbName, tableName)).Scan(&tblName, &createStmt)
	if err != nil {
		o.Fatal(err)
	}

	// Look for any RLS statements in the CREATE TABLE
	originalRLSStmt := ""

	// Check if RLS is enabled and capture the original statement
	if strings.Contains(createStmt, "ROW LEVEL SECURITY") {
		// Extract the ALTER TABLE statement for RLS
		lines := strings.Split(createStmt, "\n")
		for _, line := range lines {
			if strings.Contains(line, "ROW LEVEL SECURITY") {
				addMissingSemicolon := ""
				if !strings.Contains(line, ";") {
					addMissingSemicolon = ";"
				}
				// Store the line as the original RLS statement and semicolon if missing
				originalRLSStmt = strings.TrimSpace(line + addMissingSemicolon)
				break
			}
		}
	}

	// Enable RLS on the table with random FORCE option
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

	// Create between 0-5 policies
	numPolicies := rng.Intn(6)
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
			// Randomly choose between true or false
			checkExpr := "true"
			if rng.Intn(2) == 0 {
				checkExpr = "false"
			}
			withCheck = fmt.Sprintf("WITH CHECK (%s)", checkExpr)
		}

		// USING is not supported for INSERT
		if operation != "INSERT" {
			// Randomly choose between true or false
			usingExpr := "true"
			if rng.Intn(2) == 0 {
				usingExpr = "false"
			}
			using = fmt.Sprintf("USING (%s)", usingExpr)
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

	// Return the cleanup struct with wait duration.
	waitTime := time.Hour
	return &cleanupRLSPolicy{
		db:              dbName,
		table:           tableName,
		policies:        policies,
		originalRLSStmt: originalRLSStmt,
		waitDuration:    waitTime,
	}
}

func registerAddRLSPolicy(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "add-rls-policy",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runAddRLSPolicy,
	})
}
