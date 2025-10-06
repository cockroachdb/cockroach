// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// systemDBs lists dbs created by default on a new cockroachdb cluster. These
// may not be mutable and should be excluded by most operations.
var systemDBs = []string{"system", "information_schema", "crdb_internal", "defaultdb", "postgres"}

// pickRandomDB picks a random DB that isn't one of `excludeDBs` on the
// target cluster connected to by `conn`.
func pickRandomDB(
	ctx context.Context, o operation.Operation, conn *gosql.DB, excludeDBs []string,
) string {
	rng, _ := randutil.NewPseudoRand()

	// Pick a random table.
	dbs, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
		return ""
	}
	var dbNames []string
	for dbs.Next() {
		var dbName string
		if err := dbs.Scan(&dbName); err != nil {
			o.Fatal(err)
			return ""
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
		return ""
	}
	return dbNames[rng.Intn(len(dbNames))]
}

func pickRandomTable(
	ctx context.Context, o operation.Operation, conn *gosql.DB, dbName string,
) string {
	rng, _ := randutil.NewPseudoRand()

	// Pick a random table.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		o.Fatal(err)
		return ""
	}

	tables, err := conn.QueryContext(ctx, "SELECT table_name FROM [SHOW TABLES]")
	if err != nil {
		o.Fatal(err)
		return ""
	}
	var tableNames []string
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			o.Fatal(err)
			return ""
		}
		tableNames = append(tableNames, tableName)
	}
	if len(tableNames) == 0 {
		o.Fatalf("unexpected zero active tables found in db %s", dbName)
		return ""
	}
	return tableNames[rng.Intn(len(tableNames))]
}

func drainNode(
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

	c.Run(ctx, option.WithNodes(node), cmd.String())
}

func decommissionNode(
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

// Pick a random store in the node.
func pickRandomStore(ctx context.Context, o operation.Operation, conn *gosql.DB, nodeId int) int {
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
