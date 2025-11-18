// Copyright 2025 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

func inspectRunner() func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runInspect(ctx, o, c)
	}
}

func runInspect(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1)
	defer conn.Close()

	_, err := conn.ExecContext(ctx, "SET enable_inspect_command = true;")
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pgcode.MakeCode(string(pqErr.Code)) == pgcode.FeatureNotSupported {
			o.Status("skipping INSPECT operation on unsupported cluster version")
			return nil
		}
		o.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			o.Fatal(err)
		}
		databases = append(databases, dbName)
	}
	if err := rows.Err(); err != nil {
		o.Fatal(err)
	}

	for _, dbName := range databases {
		o.Status(fmt.Sprintf("inspecting database %s", dbName))
		_, err := conn.ExecContext(ctx, fmt.Sprintf("INSPECT DATABASE %s", lexbase.EscapeSQLIdent(dbName)))
		if err != nil {
			o.Fatal(err)
		}
	}

	return nil
}

func registerInspect(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "inspect/database",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Run:                inspectRunner(),
	})
}
