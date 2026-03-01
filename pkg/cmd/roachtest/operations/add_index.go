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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
)

type cleanupAddedIndex struct {
	db, table, index string
}

func (cl *cleanupAddedIndex) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping index %s", cl.index))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX %s.%s@%s", cl.db, cl.table, cl.index))
	if err != nil {
		o.Fatal(err)
	}
}

func runAddIndex(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()
	dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
	tableName := helpers.PickRandomTable(ctx, o, conn, dbName)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		o.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`
SELECT
	attname, atttypid
FROM
	pg_catalog.pg_attribute
WHERE
	attrelid = '%s.%s'::REGCLASS::OID;
`, dbName, tableName))
	if err != nil {
		o.Fatal(err)
	}
	rows.Next()
	if !rows.Next() {
		o.Status("found a table with only one column, skipping")
		return nil
	}
	var colName string
	var colType oid.Oid
	if err := rows.Scan(&colName, &colType); err != nil {
		o.Fatal(err)
	}

	predicateClause := ""
	// If a types OID is known basic SQL type, we can optionally choose to make
	// this a partial index.
	if typ, exists := types.OidToType[colType]; exists {
		randomValue := randgen.RandDatum(rng, typ, false)
		predicates := []string{"<", ">", "<=", ">=", "=", "<>"}
		predicate := predicates[rng.Intn(len(predicates))]
		str := tree.AsStringWithFlags(randomValue, tree.FmtParsable)
		// Use an RNG to determine if we want to add the final predicate,
		// currently there is a 50% chance of making partial indexes.
		if rng.Intn(2) != 0 {
			predicateClause = fmt.Sprintf("WHERE (%s %s %s)", colName, predicate, str)
		}
	}

	indexName := fmt.Sprintf("add_index_op_%d", rng.Uint32())

	// Determine whether to use an inverted index based on the column's type.
	// Inverted indexes are supported on JSON, ARRAY, GEOGRAPHY, GEOMETRY, and some STRING types.
	// If eligible, we apply a 50% chance of choosing to create an inverted index.
	indexUsingClause := ""
	if typ, exists := types.OidToType[colType]; exists {
		if typ.Family() == types.ArrayFamily ||
			typ.Family() == types.JsonFamily ||
			typ.Family() == types.GeographyFamily ||
			typ.Family() == types.GeometryFamily ||
			typ.Family() == types.StringFamily {
			if rng.Intn(2) == 0 {
				indexUsingClause = "INVERTED"
			}
		}
	}
	// Fallback to hash-sharded index randomly if not inverted. Hash-sharded
	// indexes cannot be combined with partial indexes (WHERE clause).
	hashClause := ""
	if indexUsingClause == "" && predicateClause == "" && rng.Intn(2) == 0 {
		hashClause = "USING HASH"
	}

	o.Status(fmt.Sprintf("adding index %s on %s.%s (column %s) %s", indexName, dbName, tableName, colName, predicateClause))

	var createIndexStmt string
	if indexUsingClause == "INVERTED" {
		createIndexStmt = fmt.Sprintf("CREATE INVERTED INDEX %s ON %s.%s (%s) %s",
			indexName,
			dbName,
			tableName,
			colName,
			predicateClause,
		)
	} else {
		createIndexStmt = fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) %s %s",
			indexName,
			dbName,
			tableName,
			colName,
			hashClause,
			predicateClause,
		)
	}

	_, err = conn.ExecContext(ctx, createIndexStmt)
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("index %s created", indexName))

	return &cleanupAddedIndex{
		db:    dbName,
		table: tableName,
		index: indexName,
	}
}

func registerAddIndex(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "add-index",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runAddIndex,
	})
}
