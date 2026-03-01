// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestResolveTargets tests the ResolveTargets implementation that uses
// Collection APIs for efficient descriptor resolution.
func TestResolveTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	db.Exec(t, `CREATE TYPE test_db.custom_schema.unused_enum AS ENUM ('x', 'y')`)
	db.Exec(t, `CREATE TYPE test_db.public.my_enum AS ENUM ('a', 'b', 'c')`)
	db.Exec(t, `CREATE TABLE test_db.public.t4 (id INT PRIMARY KEY, status test_db.public.my_enum)`)
	db.Exec(t, `USE test_db`)
	db.Exec(t, `CREATE FUNCTION my_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$`)
	db.Exec(t, `CREATE TABLE t5 (id INT PRIMARY KEY, val INT DEFAULT my_func())`)

	// Create another database with tables.
	db.Exec(t, `CREATE DATABASE other_db`)
	db.Exec(t, `CREATE TABLE other_db.public.other_table (id INT PRIMARY KEY)`)

	testCases := []struct {
		name                     string
		targets                  string
		expectedTables           []string
		expectedDBs              []string
		expectedTypes            []string
		expectedFuncs            []string
		expectedExpandedDBNames  []string // Database names that should be in expandedDBs
		expectedRequestedDBNames []string // Database names that should be in requestedDBs
		expectedTablePatternCnt  int      // Number of entries in descsByTablePattern
	}{
		// Fully qualified table patterns: db.sch.tb
		{
			name:                    "single table fully qualified",
			targets:                 "TABLE test_db.public.t1",
			expectedTables:          []string{"t1"},
			expectedDBs:             []string{"test_db"},
			expectedTablePatternCnt: 1,
		},
		{
			name:                    "table in custom schema",
			targets:                 "TABLE test_db.custom_schema.t3",
			expectedTables:          []string{"t3"},
			expectedDBs:             []string{"test_db"},
			expectedTablePatternCnt: 1,
		},
		// Partially qualified table pattern: db.tb (assumes public schema)
		{
			name:                    "table with db and implicit public schema",
			targets:                 "TABLE test_db.t1",
			expectedTables:          []string{"t1"},
			expectedDBs:             []string{"test_db"},
			expectedTablePatternCnt: 1,
		},
		// Multiple tables
		{
			name:                    "multiple tables",
			targets:                 "TABLE test_db.public.t1, test_db.public.t2",
			expectedTables:          []string{"t1", "t2"},
			expectedDBs:             []string{"test_db"},
			expectedTablePatternCnt: 2,
		},
		{
			name:                    "tables from different schemas",
			targets:                 "TABLE test_db.public.t1, test_db.custom_schema.t3",
			expectedTables:          []string{"t1", "t3"},
			expectedDBs:             []string{"test_db"},
			expectedTablePatternCnt: 2,
		},
		{
			name:                    "tables from different databases",
			targets:                 "TABLE test_db.public.t1, other_db.public.other_table",
			expectedTables:          []string{"t1", "other_table"},
			expectedDBs:             []string{"test_db", "other_db"},
			expectedTablePatternCnt: 2,
		},
		// Database wildcard: db.* - adds to expandedDBs but not requestedDBs
		{
			name:                    "database wildcard",
			targets:                 "TABLE test_db.*",
			expectedTables:          []string{"t1", "t2", "t3", "t4", "t5"},
			expectedDBs:             []string{"test_db"},
			expectedTypes:           []string{"my_enum", "_my_enum", "unused_enum", "_unused_enum"},
			expectedExpandedDBNames: []string{"test_db"},
			expectedTablePatternCnt: 0, // Wildcards don't add to descsByTablePattern
		},
		// Schema wildcard: db.sch.* - adds to expandedDBs.
		{
			name:                    "schema wildcard public",
			targets:                 "TABLE test_db.public.*",
			expectedTables:          []string{"t1", "t2", "t4", "t5"},
			expectedDBs:             []string{"test_db"},
			expectedTypes:           []string{"my_enum", "_my_enum"},
			expectedExpandedDBNames: []string{"test_db"},
			expectedTablePatternCnt: 0, // Wildcards don't add to descsByTablePattern
		},
		{
			name:                    "schema wildcard custom",
			targets:                 "TABLE test_db.custom_schema.*",
			expectedTables:          []string{"t3"},
			expectedDBs:             []string{"test_db"},
			expectedTypes:           []string{"unused_enum", "_unused_enum"},
			expectedExpandedDBNames: []string{"test_db"},
			expectedTablePatternCnt: 0, // Wildcards don't add to descsByTablePattern
		},
		// DATABASE target - adds to both expandedDBs and requestedDBs
		{
			name:                     "database target",
			targets:                  "DATABASE test_db",
			expectedTables:           []string{"t1", "t2", "t3", "t4", "t5"},
			expectedDBs:              []string{"test_db"},
			expectedTypes:            []string{"my_enum", "_my_enum", "unused_enum", "_unused_enum"},
			expectedExpandedDBNames:  []string{"test_db"},
			expectedRequestedDBNames: []string{"test_db"},
			expectedTablePatternCnt:  0,
		},
		{
			name:                     "multiple databases",
			targets:                  "DATABASE test_db, other_db",
			expectedTables:           []string{"t1", "t2", "t3", "t4", "t5", "other_table"},
			expectedDBs:              []string{"test_db", "other_db"},
			expectedTypes:            []string{"my_enum", "_my_enum", "unused_enum", "_unused_enum"},
			expectedExpandedDBNames:  []string{"test_db", "other_db"},
			expectedRequestedDBNames: []string{"test_db", "other_db"},
			expectedTablePatternCnt:  0,
		},
		// Table with type dependency
		{
			name:                    "table with type dependency",
			targets:                 "TABLE test_db.public.t4",
			expectedTables:          []string{"t4"},
			expectedDBs:             []string{"test_db"},
			expectedTypes:           []string{"my_enum", "_my_enum"},
			expectedTablePatternCnt: 1,
		},
		// Table with function dependency
		{
			name:                    "table with function dependency",
			targets:                 "TABLE test_db.public.t5",
			expectedTables:          []string{"t5"},
			expectedDBs:             []string{"test_db"},
			expectedFuncs:           []string{"my_func"},
			expectedTablePatternCnt: 1,
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
			// Use the server's current clock timestamp to read data.
			descriptors, expandedDBs, requestedDBs, descsByTablePattern, err := ResolveTargets(ctx, planHook, s.Clock().Now(), backupStmt.Targets)
			require.NoError(t, err)

			// Verify we got the expected tables, databases, types, and functions.
			var foundTables []string
			var foundDBs []string
			var foundTypes []string
			var foundFuncs []string
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
				case catalog.TypeDescriptor:
					foundTypes = append(foundTypes, d.GetName())
				case catalog.FunctionDescriptor:
					foundFuncs = append(foundFuncs, d.GetName())
				}
			}

			// Check that we found the expected tables.
			require.ElementsMatch(t, tc.expectedTables, foundTables,
				"Expected tables %v but got %v", tc.expectedTables, foundTables)

			// Check that we found the expected databases.
			require.ElementsMatch(t, tc.expectedDBs, foundDBs,
				"Expected databases %v but got %v", tc.expectedDBs, foundDBs)

			// Check that we found the expected types (if specified).
			if tc.expectedTypes != nil {
				require.ElementsMatch(t, tc.expectedTypes, foundTypes,
					"Expected types %v but got %v", tc.expectedTypes, foundTypes)
			}

			// Check that we found the expected functions (if specified).
			if tc.expectedFuncs != nil {
				require.ElementsMatch(t, tc.expectedFuncs, foundFuncs,
					"Expected functions %v but got %v", tc.expectedFuncs, foundFuncs)
			}

			// Verify expandedDBs contains expected database IDs.
			if tc.expectedExpandedDBNames != nil {
				var expandedDBNames []string
				for _, dbID := range expandedDBs {
					// Find the database descriptor with this ID.
					for _, desc := range descriptors {
						if db, ok := desc.(catalog.DatabaseDescriptor); ok && db.GetID() == dbID {
							expandedDBNames = append(expandedDBNames, db.GetName())
							break
						}
					}
				}
				require.ElementsMatch(t, tc.expectedExpandedDBNames, expandedDBNames,
					"Expected expandedDBs %v but got %v", tc.expectedExpandedDBNames, expandedDBNames)
			} else {
				require.Empty(t, expandedDBs, "Expected expandedDBs to be empty")
			}

			// Verify requestedDBs contains expected database descriptors.
			if tc.expectedRequestedDBNames != nil {
				var requestedDBNames []string
				for _, db := range requestedDBs {
					requestedDBNames = append(requestedDBNames, db.GetName())
				}
				require.ElementsMatch(t, tc.expectedRequestedDBNames, requestedDBNames,
					"Expected requestedDBs %v but got %v", tc.expectedRequestedDBNames, requestedDBNames)
			} else {
				require.Empty(t, requestedDBs, "Expected requestedDBs to be empty")
			}

			// Verify descsByTablePattern has expected number of entries.
			require.Equal(t, tc.expectedTablePatternCnt, len(descsByTablePattern),
				"Expected %d entries in descsByTablePattern but got %d",
				tc.expectedTablePatternCnt, len(descsByTablePattern))

			// Verify we didn't load unnecessary descriptors (e.g., other_db when not needed).
			// Skip this check for tests that explicitly include other_db.
			expectOtherDB := tc.name == "tables from different databases" || tc.name == "multiple databases"
			if !expectOtherDB {
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

// TestResolveWildcard tests the resolveWildcard helper that determines the
// database, optional target schema, and schema scope for wildcard patterns.
func TestResolveWildcard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Set up databases and schemas for the test scenarios.
	db.Exec(t, `CREATE DATABASE my_db`)
	db.Exec(t, `CREATE SCHEMA my_db.my_schema`)
	// "ambig" exists as both a database and a schema within my_db, so we can
	// verify that the database match takes priority for 2-part patterns.
	db.Exec(t, `CREATE DATABASE ambig`)
	db.Exec(t, `CREATE SCHEMA my_db.ambig`)

	currentDB := "my_db"
	searchPath := sessiondata.MakeSearchPath([]string{"public"})

	testCases := []struct {
		name                string
		pattern             string
		expectedDB          string
		expectedSchema      string
		expectedSchemaScope bool
		expectedErr         string
	}{{
		name:                "db wildcard",
		pattern:             "my_db.*",
		expectedDB:          "my_db",
		expectedSchema:      "",
		expectedSchemaScope: false,
	}, {
		name:                "schema wildcard",
		pattern:             "my_db.my_schema.*",
		expectedDB:          "my_db",
		expectedSchema:      "my_schema",
		expectedSchemaScope: true,
	}, {
		name:                "public schema wildcard",
		pattern:             "my_db.public.*",
		expectedDB:          "my_db",
		expectedSchema:      "public",
		expectedSchemaScope: true,
	}, {
		// 2-part where first part is a schema (not a database).
		//
		// TODO(msbutler): this will lead to the caller backing up the whole db if
		// the currentDB is my_db, which is unexpected.
		name:                "schema wildcard, no db",
		pattern:             "my_schema.*",
		expectedDB:          "my_db",
		expectedSchema:      "my_schema",
		expectedSchemaScope: false,
	}, {
		// Bare wildcard.
		//
		// TODO(msbutler): this also seems unexpected. Why constrain to the public
		// schema?
		name:                "bare *",
		pattern:             "*",
		expectedDB:          "my_db",
		expectedSchema:      "public",
		expectedSchemaScope: false,
	}, {
		// Ambiguous: name matches both a database and a schema. The database
		// match takes priority for a 2-part pattern.
		name:                "ambiguous name prefers db",
		pattern:             "ambig.*",
		expectedDB:          "ambig",
		expectedSchema:      "",
		expectedSchemaScope: false,
	}, {
		// Error: name matches nothing.
		name:        "unknown name",
		pattern:     "no_such_thing.*",
		expectedErr: "no_such_thing",
	}}

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parser.Parse(
				fmt.Sprintf("BACKUP TABLE %s INTO 'nodelocal://1/test'", tc.pattern),
			)
			require.NoError(t, err)
			backupStmt := stmts[0].AST.(*tree.Backup)
			pattern, err := backupStmt.Targets.Tables.TablePatterns[0].NormalizeTablePattern()
			require.NoError(t, err)
			sel, ok := pattern.(*tree.AllTablesSelector)
			require.True(t, ok, "expected AllTablesSelector, got %T", pattern)

			err = sql.DescsTxn(ctx, &execCfg, func(
				ctx context.Context, txn isql.Txn, col *descs.Collection,
			) error {
				r := &simpleResolver{col: col, txn: txn.KV()}
				res, err := resolveWildcard(ctx, sel, r, currentDB, searchPath)
				if tc.expectedErr != "" {
					require.Error(t, err)
					require.ErrorContains(t, err, tc.expectedErr)
					return nil
				}
				require.NoError(t, err)
				require.Equal(t, tc.expectedDB, res.db.GetName())
				if tc.expectedSchema == "" {
					require.Nil(t, res.targetSchema,
						"expected nil targetSchema but got %v", res.targetSchema)
				} else {
					require.Equal(t, tc.expectedSchema, res.targetSchema.GetName())
				}
				require.Equal(t, tc.expectedSchemaScope, res.hasSchemaScope)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// BenchmarkResolveTargets benchmarks the ResolveTargets implementation.
func BenchmarkResolveTargets(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

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
	stmt, err := parser.Parse("BACKUP TABLE db_0.t_0, db_0.t_1, db_0.t_2 INTO 'nodelocal://1/test'")
	require.NoError(b, err)
	backupStmt := stmt[0].AST.(*tree.Backup)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	b.ResetTimer()
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

			_, _, _, _, err := ResolveTargets(ctx, planHook, s.Clock().Now(), backupStmt.Targets)
			require.NoError(b, err)
		}()
	}
}
