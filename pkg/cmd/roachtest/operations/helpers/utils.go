// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package helpers

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// SystemDBs lists dbs created by default on a new cockroachdb cluster. These
// may not be mutable and should be excluded by most operations.
var SystemDBs = []string{"system", "information_schema", "crdb_internal", "defaultdb", "postgres"}

// rowEvaluator evaluates and extracts the values from each row passed as rowValues
type rowEvaluator func(rowValues []string) error

// PickRandomDB returns roachtestflags.DBName if not empty.
// Otherwise, picks a random DB that isn't one of `excludeDBs` on the
// target cluster connected to by `conn`.
func PickRandomDB(
	ctx context.Context, o operation.Operation, conn *gosql.DB, excludeDBs []string,
) string {
	if roachtestflags.DBName != "" {
		return roachtestflags.DBName
	}

	rng, _ := randutil.NewPseudoRand()

	// Pick a random table.
	dbs, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
	}
	var dbNames []string
	for dbs.Next() {
		var dbName string
		if err := dbs.Scan(&dbName); err != nil {
			o.Fatal(err)
		}
		isExcluded := false
		for i := range excludeDBs {
			if excludeDBs[i] == dbName {
				isExcluded = true
				break
			}
		}
		if isExcluded {
			continue
		}
		dbNames = append(dbNames, dbName)
	}
	if len(dbNames) == 0 {
		o.Fatalf("unexpected zero active dbs found in cluster")
	}
	return dbNames[rng.Intn(len(dbNames))]
}

// PickRandomTable returns roachtestflags.TableName if not empty.
// Otherwise, picks a random table from given database.
func PickRandomTable(
	ctx context.Context, o operation.Operation, conn *gosql.DB, dbName string,
) string {
	if roachtestflags.TableName != "" {
		return roachtestflags.TableName
	}

	rng, _ := randutil.NewPseudoRand()

	// Pick a random table.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		o.Fatal(err)
	}

	tables, err := conn.QueryContext(ctx, "SELECT table_name FROM [SHOW TABLES]")
	if err != nil {
		o.Fatal(err)
	}
	var tableNames []string
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			o.Fatal(err)
		}
		tableNames = append(tableNames, tableName)
	}
	if len(tableNames) == 0 {
		o.Fatalf("unexpected zero active tables found in db %s", dbName)
	}
	return tableNames[rng.Intn(len(tableNames))]
}

func PickRandomRole(ctx context.Context, o operation.Operation, conn *gosql.DB) string {
	rng, _ := randutil.NewPseudoRand()

	roles, err := conn.QueryContext(ctx, "SELECT username FROM [SHOW ROLES]")
	if err != nil {
		o.Fatal(err)
	}
	var roleNames []string
	for roles.Next() {
		var name string
		if err := roles.Scan(&name); err != nil {
			o.Fatal(err)
		}
		roleNames = append(roleNames, name)
	}
	if len(roleNames) == 0 {
		o.Fatalf("unexpected zero active roles found in cluster")
	}
	return roleNames[rng.Intn(len(roleNames))]
}

func DrainNode(
	ctx context.Context, o operation.Operation, c cluster.Cluster, node option.NodeListOption,
) {
	o.Status(fmt.Sprintf("draining node %s", node.NodeIDsString()))

	addr, err := c.InternalAddr(ctx, o.L(), node)
	if err != nil {
		o.Fatal(err)
	}

	cmd := roachtestutil.NewCommand("./%s node drain", o.ClusterCockroach()).
		WithEqualsSyntax().
		Flag("host", addr[0]).
		Flag("logtostderr", "INFO").
		MaybeFlag(c.IsSecure(), "certs-dir", "certs").
		MaybeOption(!c.IsSecure(), "insecure").
		Option("self")

	// On the drt-cluster, the drain process has been observed to fail intermittently,
	// causing the node to reject SQL client connections while remaining healthy for other subsystems.
	// To make the node accept SQL connections again, a manual restart is required.
	// To avoid manual intervention, the drain operation will be retried a few times before failing the operation.
	// Once the GitHub issue (https://github.com/cockroachdb/cockroach/issues/130853) is fixed, fallback to c.Run without retries.
	opts := retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second,
		MaxRetries:     3,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		err = c.RunE(ctx, option.WithNodes(node), cmd.String())
		if err == nil {
			return
		}
	}
	o.Fatalf("drain failed: %v", err)
}

func DecommissionNode(
	ctx context.Context, o operation.Operation, c cluster.Cluster, node option.NodeListOption,
) {
	o.Status(fmt.Sprintf("decommissioning node %s", node.NodeIDsString()))

	addr, err := c.InternalAddr(ctx, o.L(), node)
	if err != nil {
		o.Fatal(err)
	}

	cmd := roachtestutil.NewCommand("./%s node decommission", o.ClusterCockroach()).
		WithEqualsSyntax().
		Flag("host", addr[0]).
		Flag("logtostderr", "INFO").
		MaybeFlag(c.IsSecure(), "certs-dir", "certs").
		MaybeOption(!c.IsSecure(), "insecure").
		Option("self")

	c.Run(ctx, option.WithNodes(node), cmd.String())
}

// PickRandomStore picks a random store in the node.
func PickRandomStore(ctx context.Context, o operation.Operation, conn *gosql.DB, nodeId int) int {
	rng, _ := randutil.NewPseudoRand()
	storeIds, err := conn.QueryContext(ctx,
		fmt.Sprintf("SELECT store_id FROM crdb_internal.kv_store_status where node_id=%d", nodeId))
	if err != nil {
		o.Fatal(err)
	}
	var stores []int
	for storeIds.Next() {
		var storeId int
		if err := storeIds.Scan(&storeId); err != nil {
			o.Fatal(err)
		}
		stores = append(stores, storeId)
	}
	if len(stores) == 0 {
		o.Fatalf("unexpected zero active stores found in node %d", nodeId)
	}
	return stores[rng.Intn(len(stores))]
}

// ExecuteQuery executes the query and invokes the rowEvaluator for processing each row
func ExecuteQuery(ctx context.Context, cb rowEvaluator, conn *gosql.DB, query string) error {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	colHeaders, err := rows.Columns()
	if err != nil {
		return err
	}
	// Will be used to read data while iterating rows.
	colPointers := make([]interface{}, len(colHeaders))
	colContainer := make([]string, len(colHeaders))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	for rows.Next() {
		err = rows.Scan(colPointers...)
		if err != nil {
			return err
		}
		rowValues := make([]string, len(colContainer))
		copy(rowValues, colContainer)
		err = cb(rowValues)
		if err != nil {
			break
		}
	}
	return err
}

// EnvOrDefaultInt returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt(name string, value int) (int, error) {
	if str, present := os.LookupEnv(name); present {
		v, err := strconv.ParseInt(str, 0, 0)
		if err != nil {
			return 0, err
		}
		return int(v), nil
	}
	return value, nil
}
