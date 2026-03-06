// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
)

func runConsistencyChecks(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
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
	invalids, err := queryInvalidObjects(ctx, o, conn)
	if err != nil {
		return
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
	select {
	case <-time.After(2 * time.Minute):
	case <-ctx.Done():
		o.Errorf("context canceled while waiting for recheck: %v", ctx.Err())
		return
	}

	invalids, err = queryInvalidObjects(ctx, o, conn)
	if err != nil {
		return
	}
	if len(invalids) == 0 {
		o.Status("no invalid objects found on recheck")
		return
	}
	for _, inv := range invalids {
		o.Status(fmt.Sprintf("invalid object: %s", inv))
	}
	o.Errorf("found %d persistent invalid object(s)", len(invalids))
}

// queryInvalidObjects runs a single query against
// crdb_internal.invalid_objects and returns the results as formatted
// strings. On error it reports via o.Errorf and returns nil.
func queryInvalidObjects(
	ctx context.Context, o operation.Operation, conn *gosql.DB,
) ([]string, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	rows, err := conn.QueryContext(checkCtx,
		`SELECT database_name, schema_name, obj_name, error FROM "".crdb_internal.invalid_objects`)
	if err != nil {
		o.Errorf("invalid_objects query failed: %v", err)
		return nil, err
	}
	defer rows.Close()

	var invalids []string
	for rows.Next() {
		var dbName, schemaName, objName, objError string
		if err := rows.Scan(&dbName, &schemaName, &objName, &objError); err != nil {
			o.Errorf("invalid_objects scan failed: %v", err)
			return nil, err
		}
		invalids = append(invalids, fmt.Sprintf("%s.%s.%s: %s", dbName, schemaName, objName, objError))
	}
	if err := rows.Err(); err != nil {
		o.Errorf("invalid_objects iteration failed: %v", err)
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
