// Copyright 2024 The Cockroach Authors.
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
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s.%s@%s", cl.db, cl.table, cl.index))
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
	predicateBody := ""
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
			predicateBody = fmt.Sprintf("(%s %s %s)", colName, predicate, str)
			predicateClause = "WHERE " + predicateBody
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
			typ.Family() == types.GeometryFamily {
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

	// Pick a column outside the partial-index predicate to mutate during the
	// backfill. The pattern targets the class of bugs fixed by #166123: if
	// the optimizer prunes the fetch of a column needed to evaluate the
	// partial-index predicate (e.g. because the UPDATE only writes a
	// different column family), the in-flight backfill can produce a
	// corrupt index. The mutation is a no-op self-UPDATE (`c = c`) so it
	// is type-agnostic and does not change observable table contents but
	// still goes through the optimizer's mutation-projection paths.
	updateCol := pickUpdateColumn(ctx, o, conn, dbName, tableName, colName)

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

	// Run concurrent self-UPDATEs against an unrelated column for the
	// duration of the (synchronous) CREATE INDEX. CREATE INDEX blocks
	// until the backfill completes, so the goroutine has a wide window
	// to interleave mutations with the backfill on a real cluster.
	stopUpdates := make(chan struct{})
	updatesDone := make(chan struct{})
	if updateCol != "" {
		go func() {
			defer close(updatesDone)
			runConcurrentUpdates(ctx, o, c, dbName, tableName, updateCol, stopUpdates)
		}()
	} else {
		close(updatesDone)
	}

	_, err = conn.ExecContext(ctx, createIndexStmt)
	close(stopUpdates)
	<-updatesDone
	if err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("index %s created", indexName))

	// Validate partial-index consistency post-backfill. We skip inverted
	// indexes (multiple entries per row make a count-based check
	// inapplicable) and full-table indexes (covered by other harnesses).
	if predicateBody != "" && indexUsingClause != "INVERTED" {
		validatePartialIndex(ctx, o, conn, dbName, tableName, indexName, predicateBody)
	}

	return &cleanupAddedIndex{
		db:    dbName,
		table: tableName,
		index: indexName,
	}
}

// pickUpdateColumn returns a column on dbName.tableName that is safe to
// self-UPDATE (`c = c`) concurrently with a CREATE INDEX. It excludes
// primary-key columns, generated columns, and the supplied excludeCol
// (typically the partial-index predicate column). Returns "" if no
// such column exists or discovery fails — callers must treat that as
// "skip concurrent updates" rather than fatal.
func pickUpdateColumn(
	ctx context.Context, o operation.Operation, conn *gosql.DB, dbName, tableName, excludeCol string,
) string {
	rng, _ := randutil.NewPseudoRand()
	q := fmt.Sprintf(`
SELECT c.column_name
FROM information_schema.columns AS c
WHERE c.table_catalog = '%s'
  AND c.table_name = '%s'
  AND c.is_generated = 'NEVER'
  AND c.column_name <> '%s'
  AND c.column_name NOT IN (
    SELECT k.column_name
    FROM information_schema.table_constraints AS t
    JOIN information_schema.key_column_usage AS k
      ON t.constraint_name = k.constraint_name
     AND t.table_catalog  = k.table_catalog
     AND t.table_schema   = k.table_schema
     AND t.table_name     = k.table_name
    WHERE t.constraint_type = 'PRIMARY KEY'
      AND t.table_catalog = '%s'
      AND t.table_name    = '%s'
  )
`, dbName, tableName, excludeCol, dbName, tableName)
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		o.Status(fmt.Sprintf("could not enumerate updatable columns (skipping concurrent updates): %v", err))
		return ""
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return ""
		}
		cols = append(cols, c)
	}
	if len(cols) == 0 {
		return ""
	}
	return cols[rng.Intn(len(cols))]
}

// runConcurrentUpdates issues no-op self-UPDATEs against updateCol in
// small batches until stop is closed. The self-UPDATE shape leaves
// table contents unchanged but still drives the mutation through the
// optimizer's projection-pruning paths, which is the path implicated
// in #166123.
func runConcurrentUpdates(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	dbName, tableName, updateCol string,
	stop <-chan struct{},
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	updateStmt := fmt.Sprintf("UPDATE %s.%s SET %s = %s LIMIT 1000",
		dbName, tableName, updateCol, updateCol)

	var iters, errs int
	for {
		select {
		case <-stop:
			o.Status(fmt.Sprintf("concurrent updates: %d batches done, %d errors", iters, errs))
			return
		case <-ctx.Done():
			return
		default:
		}
		if _, err := conn.ExecContext(ctx, updateStmt); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Schema-change-adjacent retry errors are expected; log a few
			// and keep going so the backfill window stays under load.
			errs++
			if errs <= 3 {
				o.Status(fmt.Sprintf("concurrent update error (continuing): %v", err))
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		iters++
		// Throttle so the cluster isn't saturated by this side-channel.
		time.Sleep(50 * time.Millisecond)
	}
}

// validatePartialIndex compares the row count of the new partial index
// against the row count of the primary index filtered by the same
// predicate, taken at a single AS OF SYSTEM TIME timestamp. Concurrent
// foreground writes will move both counts together; only an actual
// index/table divergence (the symptom of #166123-class bugs) shows up
// as a mismatch.
func validatePartialIndex(
	ctx context.Context,
	o operation.Operation,
	conn *gosql.DB,
	dbName, tableName, indexName, predicateBody string,
) {
	predicateBody = strings.TrimSpace(predicateBody)
	q := fmt.Sprintf(`
SELECT
  (SELECT count(*) FROM %s.%s@%s    AS OF SYSTEM TIME '-5s')                AS idx_count,
  (SELECT count(*) FROM %s.%s@primary AS OF SYSTEM TIME '-5s' WHERE %s)     AS tbl_count
`, dbName, tableName, indexName, dbName, tableName, predicateBody)

	var idxCount, tblCount int
	if err := conn.QueryRowContext(ctx, q).Scan(&idxCount, &tblCount); err != nil {
		// Validation is best-effort; if we can't run the query (e.g.
		// the table got dropped by another operation) don't fail the
		// op on that account.
		o.Status(fmt.Sprintf("partial-index validation skipped (%v)", err))
		return
	}
	if idxCount != tblCount {
		o.Fatalf("partial index %s.%s@%s diverges from primary: idx=%d, tbl WHERE %s = %d",
			dbName, tableName, indexName, idxCount, predicateBody, tblCount)
	}
	o.Status(fmt.Sprintf("validated partial index %s: %d rows match predicate", indexName, idxCount))
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
