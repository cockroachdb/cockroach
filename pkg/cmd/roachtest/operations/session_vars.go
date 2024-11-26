// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type sessionVariableOp struct {
	Name      string
	DBPattern string
	// See cluster_settings.go for a collection of utilities that can be used for
	// the Generator function.
	Generator func() string
	Owner     registry.Owner
}

func setSessionVariables(
	ctx context.Context, o operation.Operation, c cluster.Cluster, op sessionVariableOp,
) registry.OperationCleanup {
	value := op.Generator()
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	// Select a random database that matches the pattern.
	row := conn.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT database_name
		FROM [SHOW DATABASES]
		WHERE database_name ~ '%s'
		ORDER BY random()
		LIMIT 1`,
		op.DBPattern,
	))
	var dbName string
	if err := row.Scan(&dbName); err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			o.Status(fmt.Sprintf("did not find a db matching pattern %q", op.DBPattern))
			return nil
		}
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("setting session variable %s on database %s to %s", op.Name, dbName, value))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER DATABASE %s SET %s = '%s'", dbName, op.Name, value))
	if err != nil {
		o.Fatal(err)
	}
	return nil
}

func registerSessionVariables(r registry.Registry) {
	ops := []sessionVariableOp{
		// Sets the default transaction isolation level for a tpcc-like database.
		// 1-hour cycle.
		{
			Name:      "default_transaction_isolation",
			DBPattern: "tpcc",
			Generator: timeBasedValues(
				timeutil.Now,
				[]string{"read committed", "repeatable read", "serializable"},
				1*time.Hour,
			),
			Owner: registry.OwnerSQLFoundations,
		},
		// Sets plan_cache_mode to "auto" or "force_custom_plan" for any
		// database. "force_generic_plan" is not used because it can produce
		// very inefficient query plans in some cases, e.g., full-table scans.
		// 1-hour cycle.
		{
			Name:      "plan_cache_mode",
			DBPattern: "",
			Generator: timeBasedValues(
				timeutil.Now,
				[]string{"auto", "force_custom_plan"},
				1*time.Hour,
			),
			Owner: registry.OwnerSQLQueries,
		},
		// Toggles prefer_lookup_joins_for_fks for any database.
		// 1-hour cycle.
		{
			Name:      "prefer_lookup_joins_for_fks",
			DBPattern: "",
			Generator: timeBasedValues(
				timeutil.Now,
				[]string{"true", "false"},
				1*time.Hour,
			),
			Owner: registry.OwnerSQLQueries,
		},
	}
	for _, op := range ops {
		r.AddOperation(registry.OperationSpec{
			Name:               "session-variables/scheduled/" + op.Name,
			Owner:              op.Owner,
			Timeout:            5 * time.Minute,
			CompatibleClouds:   registry.AllClouds,
			CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
			Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
			Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
				return setSessionVariables(ctx, o, c, op)
			},
		})
	}
}
