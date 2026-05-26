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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
	skip.UnderRace(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create tables used by most subtests. Fully qualified names exercise the
	// schema.table path in diff queries (ensures we don't accidentally quote
	// the whole name as a single identifier).
	sqlDB.Exec(t, `CREATE TABLE diff_a (id INT PRIMARY KEY, name STRING, value INT)`)
	sqlDB.Exec(t, `CREATE TABLE diff_b (id INT PRIMARY KEY, name STRING, value INT)`)
	diffA := QualifiedName{Table: "diff_a", Schema: "public", Database: "defaultdb"}
	diffB := QualifiedName{Table: "diff_b", Schema: "public", Database: "defaultdb"}

	// BIT column tables for the qualified-name regression test.
	sqlDB.Exec(t, `CREATE TABLE bit_a (id INT PRIMARY KEY, bits BIT(15))`)
	sqlDB.Exec(t, `CREATE TABLE bit_b (id INT PRIMARY KEY, bits BIT(15))`)
	bitA := QualifiedName{Table: "bit_a", Schema: "public", Database: "defaultdb"}
	bitB := QualifiedName{Table: "bit_b", Schema: "public", Database: "defaultdb"}

	type diffCheck struct {
		name     string
		tableA   QualifiedName
		tableB   QualifiedName
		setup    []string
		limit    int
		wantLen  int
		validate func(t *testing.T, diffs []RowDiff)
	}

	tests := []diffCheck{
		{
			name:   "identical",
			tableA: diffA, tableB: diffB,
			setup: []string{
				"DELETE FROM diff_a", "DELETE FROM diff_b",
				"INSERT INTO diff_a VALUES (1, 'alice', 10), (2, 'bob', 20)",
				"INSERT INTO diff_b VALUES (1, 'alice', 10), (2, 'bob', 20)",
			},
			limit:   100,
			wantLen: 0,
		},
		{
			name:   "only-in-a",
			tableA: diffA, tableB: diffB,
			setup: []string{
				"DELETE FROM diff_a", "DELETE FROM diff_b",
				"INSERT INTO diff_a VALUES (1, 'alice', 10), (2, 'bob', 20)",
				"INSERT INTO diff_b VALUES (1, 'alice', 10)",
			},
			limit:   100,
			wantLen: 1,
			validate: func(t *testing.T, diffs []RowDiff) {
				require.NotNil(t, diffs[0].RowA)
				require.Nil(t, diffs[0].RowB)
				require.True(t, diffs[0].MVCCTimestampA.IsSet())
				require.True(t, diffs[0].OriginTimestampA.IsEmpty())
				require.True(t, diffs[0].MVCCTimestampB.IsEmpty())
				require.True(t, diffs[0].OriginTimestampB.IsEmpty())
			},
		},
		{
			name:   "only-in-b",
			tableA: diffA, tableB: diffB,
			setup: []string{
				"DELETE FROM diff_a", "DELETE FROM diff_b",
				"INSERT INTO diff_a VALUES (1, 'alice', 10)",
				"INSERT INTO diff_b VALUES (1, 'alice', 10), (2, 'bob', 20)",
			},
			limit:   100,
			wantLen: 1,
			validate: func(t *testing.T, diffs []RowDiff) {
				require.Nil(t, diffs[0].RowA)
				require.NotNil(t, diffs[0].RowB)
			},
		},
		{
			name:   "value-mismatch",
			tableA: diffA, tableB: diffB,
			setup: []string{
				"DELETE FROM diff_a", "DELETE FROM diff_b",
				"INSERT INTO diff_a VALUES (1, 'alice', 10)",
				"INSERT INTO diff_b VALUES (1, 'alice', 99)",
			},
			limit:   100,
			wantLen: 1,
			validate: func(t *testing.T, diffs []RowDiff) {
				require.NotNil(t, diffs[0].RowA)
				require.NotNil(t, diffs[0].RowB)
				require.True(t, diffs[0].MVCCTimestampA.IsSet())
				require.True(t, diffs[0].MVCCTimestampB.IsSet())
			},
		},
		{
			name:   "result-limit",
			tableA: diffA, tableB: diffB,
			setup: []string{
				"DELETE FROM diff_a", "DELETE FROM diff_b",
				"INSERT INTO diff_a VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)",
			},
			limit:   2,
			wantLen: 2,
		},
		{
			name:   "empty-tables",
			tableA: diffA, tableB: diffB,
			setup: []string{
				"DELETE FROM diff_a", "DELETE FROM diff_b",
			},
			limit:   100,
			wantLen: 0,
		},
		{
			// Regression test: LoadTable previously failed to resolve BIT column
			// widths when using a fully qualified table name because typeForOid
			// queried information_schema.columns with the raw (qualified) name.
			name:   "qualified-bit-column",
			tableA: bitA, tableB: bitB,
			setup: []string{
				"DELETE FROM bit_a", "DELETE FROM bit_b",
				"INSERT INTO bit_a VALUES (1, B'100000000000000')",
				"INSERT INTO bit_b VALUES (1, B'100000000000000')",
			},
			limit:   100,
			wantLen: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, stmt := range tc.setup {
				sqlDB.Exec(t, stmt)
			}
			diffs, err := Diff(db, tc.tableA, db, tc.tableB, tc.limit)
			require.NoError(t, err)
			require.Len(t, diffs, tc.wantLen)
			if tc.validate != nil {
				tc.validate(t, diffs)
			}
		})
	}
}

// TestDiffRandomSchema generates two tables with the same random schema using
// the LDR table generator, inserts different random data into each, and
// validates that Diff correctly reports no differences when comparing a table
// to itself and detects differences when the tables have divergent content.
func TestDiffRandomSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	rng, seed := randutil.NewTestRand()
	t.Logf("random seed: %v", seed)

	const numTables = 2
	created := 0
	for attempt := 0; created < numTables; attempt++ {
		writerType := sqlclustersettings.LDRWriterType(sqlclustersettings.LDRImmediateModeWriter.Default())
		createTable := ldrrandgen.GenerateLDRTable(ctx, rng, "diff_rand", writerType)

		shortNameA := fmt.Sprintf("diff_rand_a_%d", attempt)
		shortNameB := fmt.Sprintf("diff_rand_b_%d", attempt)
		// Use fully qualified names to exercise the schema.table path in diff
		// queries.
		tableNameA := QualifiedName{Table: shortNameA, Schema: "public", Database: "defaultdb"}
		tableNameB := QualifiedName{Table: shortNameB, Schema: "public", Database: "defaultdb"}

		// Extract the table body and create both tables with the same schema.
		// The random schema generator can produce features (e.g. partitions)
		// that require a CCL binary. Skip these schemas and retry.
		fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
		createTable.FormatBody(fmtCtx)
		body := fmtCtx.CloseAndGetString()
		t.Logf("attempt %d schema: CREATE TABLE t %s", attempt, body)

		createA := fmt.Sprintf("CREATE TABLE %s %s", shortNameA, body)
		if _, err := db.Exec(createA); err != nil {
			t.Logf("skipping schema (attempt %d): %v", attempt, err)
			continue
		}
		createB := fmt.Sprintf("CREATE TABLE %s %s", shortNameB, body)
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
			fmt.Sprintf("SELECT count(*) FROM %s", tableNameA),
		).Scan(&countA))
		require.NoError(t, db.QueryRow(
			fmt.Sprintf("SELECT count(*) FROM %s", tableNameB),
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
			fpA := fingerprint(t, sqlDB, tableNameA.String())
			fpB := fingerprint(t, sqlDB, tableNameB.String())

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

// TestDiffCrossDatabase is a regression test for #170730. LoadTable fails with
// "no columns detected" when called with a cross-database qualified table name
// from a connection to a different database, because pg_catalog virtual tables
// are scoped to the connection's current database.
func TestDiffCrossDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, "CREATE DATABASE xdb")
	xdbConn := s.SQLConn(t, serverutils.DBName("xdb"))
	xdbSQL := sqlutils.MakeSQLRunner(xdbConn)

	xdbSQL.Exec(t, `CREATE TABLE xdb_a (id INT PRIMARY KEY, name STRING, value INT)`)
	xdbSQL.Exec(t, `CREATE TABLE xdb_b (id INT PRIMARY KEY, name STRING, value INT)`)
	xdbSQL.Exec(t, `INSERT INTO xdb_a VALUES (1, 'alice', 10), (2, 'bob', 20)`)
	xdbSQL.Exec(t, `INSERT INTO xdb_b VALUES (1, 'alice', 10), (2, 'bob', 20)`)

	// Call LoadTable from the defaultdb connection using a cross-database
	// name. Before the fix, this failed because pg_catalog is scoped to the
	// connection's current database.
	xdbA := QualifiedName{Table: "xdb_a", Schema: "public", Database: "xdb"}
	xdbB := QualifiedName{Table: "xdb_b", Schema: "public", Database: "xdb"}
	tableA, err := LoadTable(db, xdbA)
	require.NoError(t, err)
	require.NotEmpty(t, tableA.Cols)

	// Diff the two tables from the defaultdb connection.
	diffs, err := Diff(db, xdbA, db, xdbB, 100)
	require.NoError(t, err)
	require.Empty(t, diffs, "identical tables should produce no diffs")
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
	tableName QualifiedName,
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
			tableName,
		),
	).Scan(&fp)
	return fp
}
