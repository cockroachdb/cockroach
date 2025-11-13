// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupresolver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestResolveTargetsOptimized tests the optimized ResolveTargets implementation
// that uses Collection APIs instead of loading all descriptors.
func TestResolveTargetsOptimized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Set up test schema with databases, schemas, and tables.
	db.Exec(t, `CREATE DATABASE test_db`)
	db.Exec(t, `CREATE SCHEMA test_db.custom_schema`)
	db.Exec(t, `CREATE TABLE test_db.public.t1 (id INT PRIMARY KEY)`)
	db.Exec(t, `CREATE TABLE test_db.public.t2 (id INT PRIMARY KEY, val TEXT)`)
	db.Exec(t, `CREATE TABLE test_db.custom_schema.t3 (id INT PRIMARY KEY)`)
	db.Exec(t, `CREATE TYPE test_db.public.my_enum AS ENUM ('a', 'b', 'c')`)
	db.Exec(t, `CREATE TABLE test_db.public.t4 (id INT PRIMARY KEY, status test_db.public.my_enum)`)

	// Create another database with tables.
	db.Exec(t, `CREATE DATABASE other_db`)
	db.Exec(t, `CREATE TABLE other_db.public.other_table (id INT PRIMARY KEY)`)

	testCases := []struct {
		name           string
		targets        string
		expectedTables []string
		expectedDBs    []string
	}{
		{
			name:           "single table",
			targets:        "test_db.public.t1",
			expectedTables: []string{"t1"},
			expectedDBs:    []string{"test_db"},
		},
		{
			name:           "multiple tables",
			targets:        "test_db.public.t1, test_db.public.t2",
			expectedTables: []string{"t1", "t2"},
			expectedDBs:    []string{"test_db"},
		},
		{
			name:           "database wildcard",
			targets:        "test_db.*",
			expectedTables: []string{"t1", "t2", "t3", "t4"},
			expectedDBs:    []string{"test_db"},
		},
		{
			name:           "schema wildcard",
			targets:        "test_db.custom_schema.*",
			expectedTables: []string{"t3"},
			expectedDBs:    []string{"test_db"},
		},
		{
			name:           "database target",
			targets:        "DATABASE test_db",
			expectedTables: []string{"t1", "t2", "t3", "t4"},
			expectedDBs:    []string{"test_db"},
		},
		{
			name:           "table with type dependency",
			targets:        "test_db.public.t4",
			expectedTables: []string{"t4"},
			expectedDBs:    []string{"test_db"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the backup targets.
			stmt, err := parser.Parse("BACKUP " + tc.targets + " INTO 'nodelocal://1/test'")
			require.NoError(t, err)
			backupStmt := stmt[0].AST.(*tree.Backup)

			// Get the plan hook state.
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			p, cleanup := sql.NewInternalPlanner(
				"test",
				nil, /* txn */
				username.RootUserName(),
				&sql.MemoryMetrics{},
				&execCfg,
				sql.NewInternalSessionData(ctx, execCfg.Settings, "test"),
			)
			defer cleanup()

			planHook := p.(sql.PlanHookState)

			// Use the optimized ResolveTargets function.
			// Use MaxTimestamp to read all data.
			descriptors, err := ResolveTargets(ctx, planHook, hlc.MaxTimestamp, backupStmt.Targets)
			require.NoError(t, err)

			// Verify we got the expected tables and databases.
			var foundTables []string
			var foundDBs []string
			seenDBs := make(map[string]bool)

			for _, desc := range descriptors {
				switch d := desc.(type) {
				case catalog.TableDescriptor:
					foundTables = append(foundTables, d.GetName())
				case catalog.DatabaseDescriptor:
					if !seenDBs[d.GetName()] {
						foundDBs = append(foundDBs, d.GetName())
						seenDBs[d.GetName()] = true
					}
				}
			}

			// Check that we found the expected tables.
			require.ElementsMatch(t, tc.expectedTables, foundTables,
				"Expected tables %v but got %v", tc.expectedTables, foundTables)

			// Check that we found the expected databases.
			require.ElementsMatch(t, tc.expectedDBs, foundDBs,
				"Expected databases %v but got %v", tc.expectedDBs, foundDBs)

			// Verify we didn't load unnecessary descriptors (e.g., other_db when not needed).
			if tc.name != "database wildcard" && tc.name != "database target" {
				for _, desc := range descriptors {
					if db, ok := desc.(catalog.DatabaseDescriptor); ok {
						require.NotEqual(t, "other_db", db.GetName(),
							"Should not have loaded other_db for target %s", tc.targets)
					}
				}
			}
		})
	}
}

// BenchmarkResolveTargets compares the performance of the old vs new implementation.
func BenchmarkResolveTargets(b *testing.B) {
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Create many databases and tables to simulate a large cluster.
	numDatabases := 10
	tablesPerDB := 100

	for i := 0; i < numDatabases; i++ {
		dbName := fmt.Sprintf("db_%d", i)
		db.Exec(b, fmt.Sprintf("CREATE DATABASE %s", dbName))
		for j := 0; j < tablesPerDB; j++ {
			tableName := fmt.Sprintf("t_%d", j)
			db.Exec(b, fmt.Sprintf("CREATE TABLE %s.%s (id INT PRIMARY KEY)", dbName, tableName))
		}
	}

	// Parse a simple backup target that should only need a few descriptors.
	stmt, err := parser.Parse("BACKUP db_0.t_0, db_0.t_1, db_0.t_2 INTO 'nodelocal://1/test'")
	require.NoError(b, err)
	backupStmt := stmt[0].AST.(*tree.Backup)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	b.Run("OldImplementation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				p, cleanup := sql.NewInternalPlanner(
					"bench",
					nil,
					username.RootUserName(),
					&sql.MemoryMetrics{},
					&execCfg,
					sql.NewInternalSessionData(ctx, execCfg.Settings, "bench"),
				)
				defer cleanup()

				planHook := p.(sql.PlanHookState)

				// Use the old implementation that loads all descriptors.
				_, _, _, _, err := ResolveTargetsToDescriptors(
					ctx, planHook, hlc.MaxTimestamp, backupStmt.Targets,
				)
				require.NoError(b, err)
			}()
		}
	})

	b.Run("NewImplementation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				p, cleanup := sql.NewInternalPlanner(
					"bench",
					nil,
					username.RootUserName(),
					&sql.MemoryMetrics{},
					&execCfg,
					sql.NewInternalSessionData(ctx, execCfg.Settings, "bench"),
				)
				defer cleanup()

				planHook := p.(sql.PlanHookState)

				// Use the new optimized implementation.
				_, err := ResolveTargets(ctx, planHook, hlc.MaxTimestamp, backupStmt.Targets)
				require.NoError(b, err)
			}()
		}
	})
}
