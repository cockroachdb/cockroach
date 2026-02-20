// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/ldrrandgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestRowDiffFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	d := RowDiff{
		ColNames:         []string{"id", "name", "value"},
		RowA:             []any{1, "alice", 10},
		MVCCTimestampA:   hlc.Timestamp{WallTime: 1e9, Logical: 1},
		OriginTimestampA: hlc.Timestamp{WallTime: 5e8},
		RowB:             nil,
	}
	require.Equal(t,
		"RowA={id: '1', name: 'alice', value: '10'} (mvcc=1.000000000,1, origin=0.500000000,0) "+
			"RowB=<nil> (mvcc=0,0, origin=0,0)",
		d.String(),
	)
}

func TestDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create two tables with the same schema.
	sqlDB.Exec(t, `CREATE TABLE diff_a (
		id INT PRIMARY KEY,
		name STRING,
		value INT
	)`)
	sqlDB.Exec(t, `CREATE TABLE diff_b (
		id INT PRIMARY KEY,
		name STRING,
		value INT
	)`)

	t.Run("identical", func(t *testing.T) {
		sqlDB.Exec(t, "DELETE FROM diff_a")
		sqlDB.Exec(t, "DELETE FROM diff_b")
		sqlDB.Exec(t, "INSERT INTO diff_a VALUES (1, 'alice', 10), (2, 'bob', 20)")
		sqlDB.Exec(t, "INSERT INTO diff_b VALUES (1, 'alice', 10), (2, 'bob', 20)")

		diffs, err := Diff(db, "diff_a", db, "diff_b", 100)
		require.NoError(t, err)
		require.Empty(t, diffs)
	})

	t.Run("only-in-a", func(t *testing.T) {
		sqlDB.Exec(t, "DELETE FROM diff_a")
		sqlDB.Exec(t, "DELETE FROM diff_b")
		sqlDB.Exec(t, "INSERT INTO diff_a VALUES (1, 'alice', 10), (2, 'bob', 20)")
		sqlDB.Exec(t, "INSERT INTO diff_b VALUES (1, 'alice', 10)")

		diffs, err := Diff(db, "diff_a", db, "diff_b", 100)
		require.NoError(t, err)
		require.Len(t, diffs, 1)
		require.NotNil(t, diffs[0].RowA)
		require.Nil(t, diffs[0].RowB)
		// MVCC timestamp should be populated for the side that has the row.
		require.True(t, diffs[0].MVCCTimestampA.IsSet())
		// Origin timestamp is empty for locally-written data.
		require.True(t, diffs[0].OriginTimestampA.IsEmpty())
		// Side B has no row, so timestamps should be empty.
		require.True(t, diffs[0].MVCCTimestampB.IsEmpty())
		require.True(t, diffs[0].OriginTimestampB.IsEmpty())
	})

	t.Run("only-in-b", func(t *testing.T) {
		sqlDB.Exec(t, "DELETE FROM diff_a")
		sqlDB.Exec(t, "DELETE FROM diff_b")
		sqlDB.Exec(t, "INSERT INTO diff_a VALUES (1, 'alice', 10)")
		sqlDB.Exec(t, "INSERT INTO diff_b VALUES (1, 'alice', 10), (2, 'bob', 20)")

		diffs, err := Diff(db, "diff_a", db, "diff_b", 100)
		require.NoError(t, err)
		require.Len(t, diffs, 1)
		require.Nil(t, diffs[0].RowA)
		require.NotNil(t, diffs[0].RowB)
	})

	t.Run("value-mismatch", func(t *testing.T) {
		sqlDB.Exec(t, "DELETE FROM diff_a")
		sqlDB.Exec(t, "DELETE FROM diff_b")
		sqlDB.Exec(t, "INSERT INTO diff_a VALUES (1, 'alice', 10)")
		sqlDB.Exec(t, "INSERT INTO diff_b VALUES (1, 'alice', 99)")

		diffs, err := Diff(db, "diff_a", db, "diff_b", 100)
		require.NoError(t, err)
		require.Len(t, diffs, 1)
		require.NotNil(t, diffs[0].RowA)
		require.NotNil(t, diffs[0].RowB)
		// Both sides should have MVCC timestamps.
		require.True(t, diffs[0].MVCCTimestampA.IsSet())
		require.True(t, diffs[0].MVCCTimestampB.IsSet())
	})

	t.Run("result-limit", func(t *testing.T) {
		sqlDB.Exec(t, "DELETE FROM diff_a")
		sqlDB.Exec(t, "DELETE FROM diff_b")
		sqlDB.Exec(t, "INSERT INTO diff_a VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
		// diff_b is empty, so all 3 rows are only-in-a.

		diffs, err := Diff(db, "diff_a", db, "diff_b", 2)
		require.NoError(t, err)
		require.Len(t, diffs, 2)
	})

	t.Run("empty-tables", func(t *testing.T) {
		sqlDB.Exec(t, "DELETE FROM diff_a")
		sqlDB.Exec(t, "DELETE FROM diff_b")

		diffs, err := Diff(db, "diff_a", db, "diff_b", 100)
		require.NoError(t, err)
		require.Empty(t, diffs)
	})
}

// TestDiffRandomSchema generates two tables with the same random schema using
// the LDR table generator, inserts different random data into each, and
// validates that Diff correctly reports no differences when comparing a table
// to itself and detects differences when the tables have divergent content.
func TestDiffRandomSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	rng, seed := randutil.NewTestRand()
	t.Logf("random seed: %v", seed)

	const numTables = 2
	created := 0
	for attempt := 0; created < numTables; attempt++ {
		createTable := ldrrandgen.GenerateLDRTable(ctx, rng, "diff_rand", false /* supportKVWriter */)

		tableNameA := fmt.Sprintf("diff_rand_a_%d", attempt)
		tableNameB := fmt.Sprintf("diff_rand_b_%d", attempt)

		// Extract the table body and create both tables with the same schema.
		// The random schema generator can produce features (e.g. partitions)
		// that require a CCL binary. Skip these schemas and retry.
		fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
		createTable.FormatBody(fmtCtx)
		body := fmtCtx.CloseAndGetString()
		t.Logf("attempt %d schema: CREATE TABLE t %s", attempt, body)

		createA := fmt.Sprintf("CREATE TABLE %s %s", tree.NameString(tableNameA), body)
		if _, err := db.Exec(createA); err != nil {
			t.Logf("skipping schema (attempt %d): %v", attempt, err)
			continue
		}
		createB := fmt.Sprintf("CREATE TABLE %s %s", tree.NameString(tableNameB), body)
		if _, err := db.Exec(createB); err != nil {
			t.Logf("skipping schema (attempt %d): %v", attempt, err)
			continue
		}

		tableA, err := LoadTable(db, tableNameA)
		require.NoError(t, err)
		require.NotEmpty(t, tableA.PrimaryKey, "table %s has no primary key", tableNameA)

		// Insert random rows into both tables.
		insertRows(t, ctx, db, rng, &tableA, tableNameA, 10)
		insertRows(t, ctx, db, rng, &tableA, tableNameB, 10)

		// Verify both tables have rows. Some random schemas have constraints
		// that cause all inserts to fail silently. Skip these.
		var countA, countB int
		require.NoError(t, db.QueryRow(
			fmt.Sprintf("SELECT count(*) FROM %s", tree.NameString(tableNameA)),
		).Scan(&countA))
		require.NoError(t, db.QueryRow(
			fmt.Sprintf("SELECT count(*) FROM %s", tree.NameString(tableNameB)),
		).Scan(&countB))
		if countA == 0 || countB == 0 {
			t.Logf("skipping schema (attempt %d): tables have %d and %d rows", attempt, countA, countB)
			continue
		}

		t.Run(fmt.Sprintf("self-diff-%d", created), func(t *testing.T) {
			diffs, err := Diff(db, tableNameA, db, tableNameA, 100)
			require.NoError(t, err)
			require.Empty(t, diffs, "diffing table against itself should produce no differences")
		})

		t.Run(fmt.Sprintf("fingerprint-diff-%d", created), func(t *testing.T) {
			fpA := fingerprint(t, sqlDB, tableNameA)
			fpB := fingerprint(t, sqlDB, tableNameB)

			diffs, err := Diff(db, tableNameA, db, tableNameB, 100)
			require.NoError(t, err)
			if fpA != fpB {
				require.NotEmpty(t, diffs,
					"fingerprints differ (a=%s, b=%s) but Diff returned no differences", fpA, fpB)
			}
		})
		created++
	}
}

// insertRows inserts n random rows into the given table using the TableWriter.
// Rows that fail to insert (e.g. due to unique constraint violations on
// secondary indexes) are silently skipped.
func insertRows(
	t *testing.T,
	ctx context.Context,
	db *gosql.DB,
	rng *rand.Rand,
	table *Table,
	tableName string,
	n int,
) {
	t.Helper()
	// The TableWriter needs the table name to match, so override it.
	writerTable := *table
	writerTable.Name = tableName
	w := NewTableWriter(db, writerTable)
	for i := 0; i < n; i++ {
		row, err := table.RandomRow(rng, 0 /* nullPct */)
		require.NoError(t, err)
		// Ignore execution errors from unique constraint violations on
		// secondary indexes or other schema-level constraints.
		_ = w.UpsertRow(ctx, row)
	}
}

// fingerprint returns the primary index fingerprint for a table.
func fingerprint(t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string) string {
	t.Helper()
	var fp string
	sqlDB.QueryRow(t,
		fmt.Sprintf(
			`SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s] LIMIT 1`,
			tree.NameString(tableName),
		),
	).Scan(&fp)
	return fp
}
