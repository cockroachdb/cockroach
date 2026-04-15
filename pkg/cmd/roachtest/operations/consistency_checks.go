// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runConsistencyChecks connects to a random cluster node and verifies
// schema consistency by querying crdb_internal.invalid_objects.
func runConsistencyChecks(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()
	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	conn := c.Conn(ctx, o.L(), nid, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status("starting SQL consistency checks")
	checkInvalidObjects(ctx, o, conn)
	o.Status("SQL consistency checks complete")

	return nil
}

// checkInvalidObjects queries crdb_internal.invalid_objects for broken
// schema references such as dangling foreign keys, missing types, or
// orphaned descriptors.
func checkInvalidObjects(ctx context.Context, o operation.Operation, conn *gosql.DB) {
	o.Status("checking for invalid objects")
	invalids, err := queryInvalidObjects(ctx, conn)
	if err != nil {
		o.Fatal(err)
	}
	if len(invalids) == 0 {
		o.Status("no invalid objects found")
		return
	}

	// Re-check after a delay to filter transient inconsistencies caused
	// by concurrent DDL operations.
	o.Status(fmt.Sprintf(
		"found %d invalid object(s), rechecking after delay to filter transient states",
		len(invalids),
	))
	recheckTimer := time.NewTimer(2 * time.Minute)
	defer recheckTimer.Stop()
	select {
	case <-recheckTimer.C:
	case <-ctx.Done():
		o.Fatalf("context canceled while waiting for recheck: %v", ctx.Err())
	}

	invalids, err = queryInvalidObjects(ctx, conn)
	if err != nil {
		o.Fatal(err)
	}
	if len(invalids) == 0 {
		o.Status("no invalid objects found on recheck")
		return
	}
	o.Fatalf("found %d persistent invalid object(s):\n%s",
		len(invalids), strings.Join(invalids, "\n"))
}

// queryInvalidObjects runs a single query against
// crdb_internal.invalid_objects and returns the results as formatted
// strings.
func queryInvalidObjects(ctx context.Context, conn *gosql.DB) ([]string, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	rows, err := conn.QueryContext(checkCtx,
		`SELECT database_name, schema_name, obj_name, error FROM "".crdb_internal.invalid_objects`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var invalids []string
	for rows.Next() {
		var dbName, schemaName, objName, objError string
		if err := rows.Scan(&dbName, &schemaName, &objName, &objError); err != nil {
			return nil, err
		}
		invalids = append(invalids, fmt.Sprintf("%s.%s.%s: %s", dbName, schemaName, objName, objError))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return invalids, nil
}

func registerConsistencyCheck(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:                    "consistency-check",
		Owner:                   registry.OwnerKV,
		Timeout:                 1 * time.Hour,
		CompatibleClouds:        registry.AllClouds,
		CanRunConcurrently:      registry.OperationCannotRunConcurrentlyWithItself,
		Dependencies:            []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		WaitBeforeNextExecution: 12 * time.Hour,
		Run:                     runConsistencyChecks,
	})
}
