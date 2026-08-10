// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDiscoverSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	testIdx := 0

	datadriven.RunTest(t, "testdata/discover_schema", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "discover":
			testIdx++
			dbName := fmt.Sprintf("db_%d", testIdx)
			sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", tree.NameString(dbName)))

			testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
			testSQL := sqlutils.MakeSQLRunner(testDB)

			for _, stmt := range strings.Split(td.Input, ";") {
				stmt = strings.TrimSpace(stmt)
				if stmt == "" {
					continue
				}
				testSQL.Exec(t, stmt)
			}

			s, err := DiscoverSchema(testDB, dbName)
			require.NoError(t, err)
			return s.String()

		default:
			t.Fatalf("unknown command: %s", td.Cmd)
			return ""
		}
	})
}

func TestDiscoverRandomizedSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)

	rng, _ := randutil.NewTestRand()
	numTables := 5
	stmts := randgen.RandCreateTables(
		ctx, rng, "t", numTables, nil,
		randgen.ForeignKeyMutator,
	)

	dbName := "rand_schema"
	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", tree.NameString(dbName)))

	testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
	testSQL := sqlutils.MakeSQLRunner(testDB)

	var sb strings.Builder
	for _, stmt := range stmts {
		sb.WriteString(tree.SerializeForDisplay(stmt))
		sb.WriteString(";\n")
	}
	testSQL.Exec(t, sb.String())

	s, err := DiscoverSchema(testDB, dbName)
	require.NoError(t, err)

	// Validate table count matches what we generated.
	require.Equal(t, numTables, len(s.Tables),
		"discovered %d tables, expected %d", len(s.Tables), numTables,
	)

	colSet := func(tbl *Table) map[string]bool {
		m := make(map[string]bool, len(tbl.Columns))
		for _, c := range tbl.Columns {
			m[c.Name] = true
		}
		return m
	}

	colNullable := func(tbl *Table) map[string]bool {
		m := make(map[string]bool, len(tbl.Columns))
		for _, c := range tbl.Columns {
			m[c.Name] = c.Nullable
		}
		return m
	}

	for name, tbl := range s.Tables {
		// Table.Name matches its key in Schema.Tables.
		require.Equal(t, name, tbl.Name)

		// Cross-validate: table exists in the database.
		_, err := testDB.Exec(fmt.Sprintf("SELECT * FROM %s LIMIT 0", tree.NameString(tbl.Name)))
		require.NoError(t, err, "table %s should exist in database", tbl.Name)

		// Cross-validate: every column exists on the table and its type was
		// discovered. Type may be nil for user-defined types, but RandCreateTables
		// only emits built-in types so we expect every column to have a type.
		for _, col := range tbl.Columns {
			_, err := testDB.Exec(fmt.Sprintf(
				"SELECT %s FROM %s LIMIT 0",
				tree.NameString(col.Name), tree.NameString(tbl.Name),
			))
			require.NoError(t, err, "column %s.%s should exist in database", tbl.Name, col.Name)
			require.NotNil(t, col.Type, "column %s.%s should have a discovered type", tbl.Name, col.Name)
		}

		// Exactly one PK per table.
		pkCount := 0
		ucNames := make(map[string]bool)
		for _, uc := range tbl.UniqueConstraints {
			if uc.IsPrimary {
				pkCount++
			}
			require.False(t, ucNames[uc.Name],
				"duplicate UC name %s on table %s", uc.Name, tbl.Name,
			)
			ucNames[uc.Name] = true
		}
		require.Equal(t, 1, pkCount,
			"table %s should have exactly 1 PK, got %d", tbl.Name, pkCount,
		)

		cols := colSet(tbl)
		nullable := colNullable(tbl)

		for _, fk := range tbl.OutboundFKs {
			// FKEdge.ReferencingTable matches the table it's stored on.
			require.Equal(t, tbl.Name, fk.ReferencingTable,
				"FK %s stored on table %s but ReferencingTable is %s",
				fk.Name, tbl.Name, fk.ReferencingTable,
			)

			// Referenced table exists.
			refTable, ok := s.Tables[fk.ReferencedTable]
			require.True(t, ok,
				"FK %s references table %s which is not in the schema",
				fk.Name, fk.ReferencedTable,
			)

			// Referenced constraint exists on parent and column counts match.
			var foundUC bool
			for _, uc := range refTable.UniqueConstraints {
				if uc.Name == fk.ReferencedConstraint {
					foundUC = true
					require.Equal(t, len(fk.ReferencingColumns), len(uc.Columns),
						"FK %s column count mismatch", fk.Name,
					)
					break
				}
			}
			require.True(t, foundUC,
				"FK %s references constraint %s which is not on table %s",
				fk.Name, fk.ReferencedConstraint, fk.ReferencedTable,
			)

			// Referencing columns exist in child table's column list.
			for _, col := range fk.ReferencingColumns {
				require.True(t, cols[col],
					"FK %s references column %s which is not on table %s",
					fk.Name, col, tbl.Name,
				)
			}

			// Referenced columns exist in parent table's column list.
			refCols := colSet(refTable)
			for _, col := range fk.ReferencedColumns {
				require.True(t, refCols[col],
					"FK %s references column %s which is not on table %s",
					fk.Name, col, fk.ReferencedTable,
				)
			}

			// Cross-validate: FK columns exist in the database.
			_, err := testDB.Exec(fmt.Sprintf(
				"SELECT %s FROM %s LIMIT 0",
				colList(fk.ReferencingColumns), tree.NameString(tbl.Name),
			))
			require.NoError(t, err,
				"FK %s referencing columns should exist on %s", fk.Name, tbl.Name,
			)
			_, err = testDB.Exec(fmt.Sprintf(
				"SELECT %s FROM %s LIMIT 0",
				colList(fk.ReferencedColumns), tree.NameString(fk.ReferencedTable),
			))
			require.NoError(t, err,
				"FK %s referenced columns should exist on %s", fk.Name, fk.ReferencedTable,
			)

			// Nullable consistency.
			if fk.Nullable {
				for _, col := range fk.ReferencingColumns {
					require.True(t, nullable[col],
						"FK %s is nullable but column %s.%s is NOT NULL",
						fk.Name, tbl.Name, col,
					)
				}
			} else {
				hasNotNull := false
				for _, col := range fk.ReferencingColumns {
					if !nullable[col] {
						hasNotNull = true
						break
					}
				}
				require.True(t, hasNotNull,
					"FK %s is not nullable but all referencing columns are nullable",
					fk.Name,
				)
			}

			// InboundFKs symmetry: parent table should have a matching inbound FK.
			found := false
			for _, inFK := range refTable.InboundFKs {
				if inFK.Name == fk.Name {
					found = true
					break
				}
			}
			require.True(t, found,
				"FK %s on %s has no matching InboundFK on %s",
				fk.Name, tbl.Name, fk.ReferencedTable,
			)
		}
	}
}

func colList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = tree.NameString(c)
	}
	return strings.Join(quoted, ", ")
}
