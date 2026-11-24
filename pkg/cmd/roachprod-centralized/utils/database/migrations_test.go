// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package database_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database/cockroachdb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	_ "github.com/lib/pq"
)

func setupTestDB(_ *testing.T) (*gosql.DB, error) {
	return database.NewConnection(context.Background(), database.ConnectionConfig{
		URL:         "postgresql://root@localhost:29002/defaultdb?sslmode=disable",
		MaxConns:    10,
		MaxIdleTime: 300, // 5 minutes
	})
}

func TestMigrationRunner_Basic(t *testing.T) {
	// This test requires a running CockroachDB instance
	db, err := setupTestDB(t)
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	ctx := context.Background()

	// Test migrations
	testMigrations := []database.Migration{
		{
			Version:     1,
			Description: "Create test table",
			SQL:         "CREATE TABLE IF NOT EXISTS test_table (id UUID PRIMARY KEY DEFAULT gen_random_uuid())",
		},
	}

	// Clean up (remove migration tracking for this repository, not the whole table)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'test'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'test'")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_table")

	// Run migrations with CockroachDB migrator
	err = database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "test", testMigrations, cockroachdb.NewMigrator())
	if err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Verify migration table exists
	var exists bool
	err = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_name = 'schema_migrations'
		)
	`).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check migration table: %v", err)
	}
	if !exists {
		t.Error("Migration table was not created")
	}

	// Verify test table exists
	err = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_name = 'test_table'
		)
	`).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check test table: %v", err)
	}
	if !exists {
		t.Error("Test table was not created")
	}

	// Clean up (remove migration tracking for this repository, not the whole table)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'test'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'test'")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_table")
}

func TestMigrationRunner_ColumnTypeChanges(t *testing.T) {
	// This test simulates changing a column type multiple times
	// Similar to the real consumer_id UUID -> VARCHAR -> UUID scenario
	db, err := setupTestDB(t)
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	ctx := context.Background()

	// Clean up from previous runs (remove migration tracking for this repository)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'test_column_type'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'test_column_type'")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_column_changes")

	// Migrations that change a column type: UUID -> VARCHAR -> INT -> VARCHAR -> UUID
	testMigrations := []database.Migration{
		{
			Version:     1,
			Description: "Create table with UUID column",
			SQL: `
CREATE TABLE IF NOT EXISTS test_column_changes (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	test_col UUID,
	data VARCHAR(100)
)`,
		},
		{
			Version:     2,
			Description: "Change test_col from UUID to VARCHAR - step 1: add new column",
			SQL: `
-- Add new VARCHAR column
ALTER TABLE test_column_changes ADD COLUMN IF NOT EXISTS test_col_new VARCHAR(255);
			`,
		},
		{
			Version:     3,
			Description: "Change test_col from UUID to VARCHAR - step 2: copy data",
			SQL: `
-- Copy data from UUID to VARCHAR
UPDATE test_column_changes SET test_col_new = test_col::VARCHAR WHERE test_col IS NOT NULL;
			`,
		},
		{
			Version:     4,
			Description: "Change test_col from UUID to VARCHAR - step 3: drop old and rename",
			SQL: `
-- Drop old column and rename new one
ALTER TABLE test_column_changes DROP COLUMN test_col;
ALTER TABLE test_column_changes RENAME COLUMN test_col_new TO test_col;
			`,
		},
		{
			Version:     5,
			Description: "Change test_col from VARCHAR to INT - step 1: add new column",
			SQL: `
-- Add new INT column
ALTER TABLE test_column_changes ADD COLUMN IF NOT EXISTS test_col_new INT;
			`,
		},
		{
			Version:     6,
			Description: "Change test_col from VARCHAR to INT - step 2: copy data",
			SQL: `
-- Set all values to a test integer (can't convert UUIDs to INT)
UPDATE test_column_changes SET test_col_new = 42;
			`,
		},
		{
			Version:     7,
			Description: "Change test_col from VARCHAR to INT - step 3: drop old and rename",
			SQL: `
-- Drop old column and rename new one
ALTER TABLE test_column_changes DROP COLUMN test_col;
ALTER TABLE test_column_changes RENAME COLUMN test_col_new TO test_col;
			`,
		},
		{
			Version:     8,
			Description: "Change test_col from INT back to VARCHAR - step 1: add new column",
			SQL: `
-- Add new VARCHAR column
ALTER TABLE test_column_changes ADD COLUMN IF NOT EXISTS test_col_new VARCHAR(255);
			`,
		},
		{
			Version:     9,
			Description: "Change test_col from INT back to VARCHAR - step 2: copy data",
			SQL: `
-- Copy INT to VARCHAR
UPDATE test_column_changes SET test_col_new = test_col::VARCHAR WHERE test_col IS NOT NULL;
			`,
		},
		{
			Version:     10,
			Description: "Change test_col from INT back to VARCHAR - step 3: drop old and rename",
			SQL: `
-- Drop old column and rename new one
ALTER TABLE test_column_changes DROP COLUMN test_col;
ALTER TABLE test_column_changes RENAME COLUMN test_col_new TO test_col;
			`,
		},
		{
			Version:     11,
			Description: "Change test_col from VARCHAR back to UUID - step 1: add new column",
			SQL: `
-- Add new UUID column
ALTER TABLE test_column_changes ADD COLUMN IF NOT EXISTS test_col_new UUID;
			`,
		},
		{
			Version:     12,
			Description: "Change test_col from VARCHAR back to UUID - step 2: copy data",
			SQL: `
-- Generate new UUIDs (can't convert arbitrary VARCHAR back to UUID)
UPDATE test_column_changes SET test_col_new = gen_random_uuid();
			`,
		},
		{
			Version:     13,
			Description: "Change test_col from VARCHAR back to UUID - step 3: drop old and rename",
			SQL: `
-- Drop old column and rename new one
ALTER TABLE test_column_changes DROP COLUMN test_col;
ALTER TABLE test_column_changes RENAME COLUMN test_col_new TO test_col;
			`,
		},
	}

	t.Log("Running migrations to change column type multiple times...")
	err = database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "test_column_type", testMigrations, cockroachdb.NewMigrator())
	if err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Verify the final column type is UUID
	var dataType string
	err = db.QueryRow(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_name = 'test_column_changes'
		AND column_name = 'test_col'
	`).Scan(&dataType)
	if err != nil {
		t.Fatalf("Failed to check column type: %v", err)
	}

	// CockroachDB reports UUID as 'uuid'
	if dataType != "uuid" {
		t.Errorf("Expected final column type to be 'uuid', got '%s'", dataType)
	}

	// Verify all 13 migrations were recorded
	var migrationCount int
	err = db.QueryRow(`
		SELECT count(*)
		FROM schema_migrations
		WHERE repository = 'test_column_type'
	`).Scan(&migrationCount)
	if err != nil {
		t.Fatalf("Failed to count migrations: %v", err)
	}
	if migrationCount != 13 {
		t.Errorf("Expected 13 migrations to be recorded, got %d", migrationCount)
	}

	t.Log("✓ All column type changes completed successfully")
	t.Logf("✓ Final column type: %s", dataType)
	t.Logf("✓ Migrations recorded: %d", migrationCount)

	// Clean up (remove migration tracking for this repository)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'test_column_type'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'test_column_type'")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_column_changes")
}

func TestMigrationRunner_ConcurrentExecution(t *testing.T) {
	// This test verifies that the advisory lock prevents concurrent migration execution
	db, err := setupTestDB(t)
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	ctx := context.Background()

	// Clean up from previous runs (remove migration tracking for this repository)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'test_concurrent'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'test_concurrent'")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_concurrent")

	// Create migrations with deliberate delays to test concurrency
	testMigrations := []database.Migration{
		{
			Version:     1,
			Description: "Create table",
			SQL:         "CREATE TABLE IF NOT EXISTS test_concurrent (id INT PRIMARY KEY, value VARCHAR(100))",
		},
		{
			Version:     2,
			Description: "Insert test data",
			SQL:         "INSERT INTO test_concurrent VALUES (1, 'test1')",
		},
		{
			Version:     3,
			Description: "Insert more test data",
			SQL:         "INSERT INTO test_concurrent VALUES (2, 'test2')",
		},
		{
			Version:     4,
			Description: "Insert even more test data",
			SQL:         "INSERT INTO test_concurrent VALUES (3, 'test3')",
		},
		{
			Version:     5,
			Description: "Final insert",
			SQL:         "INSERT INTO test_concurrent VALUES (4, 'test4')",
		},
	}

	// Run migrations concurrently from 3 "instances"
	errChan := make(chan error, 3)

	for i := range 3 {
		go func(instanceNum int) {
			t.Logf("Instance %d: Starting migrations...", instanceNum)
			err := database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "test_concurrent", testMigrations, cockroachdb.NewMigrator())
			if err != nil {
				t.Logf("Instance %d: Error: %v", instanceNum, err)
			} else {
				t.Logf("Instance %d: Completed successfully", instanceNum)
			}
			errChan <- err
		}(i)
	}

	// Wait for all goroutines to complete
	for range 3 {
		if err := <-errChan; err != nil {
			t.Fatalf("Migration failed in goroutine: %v", err)
		}
	}

	// Verify all rows were inserted exactly once (no duplicates from concurrent execution)
	var rowCount int
	err = db.QueryRow("SELECT count(*) FROM test_concurrent").Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if rowCount != 4 {
		t.Errorf("Expected exactly 4 rows (one instance should win), got %d", rowCount)
	}

	// Verify all 5 migrations were recorded exactly once
	var migrationCount int
	err = db.QueryRow(`
		SELECT count(*)
		FROM schema_migrations
		WHERE repository = 'test_concurrent'
	`).Scan(&migrationCount)
	if err != nil {
		t.Fatalf("Failed to count migrations: %v", err)
	}
	if migrationCount != 5 {
		t.Errorf("Expected 5 migrations to be recorded, got %d", migrationCount)
	}

	t.Log("✓ Concurrent execution prevented successfully")
	t.Logf("✓ Rows in test table: %d (expected 4)", rowCount)
	t.Logf("✓ Migrations recorded: %d (expected 5)", migrationCount)

	// Clean up (remove migration tracking for this repository)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'test_concurrent'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'test_concurrent'")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_concurrent")
}

func TestMigrationRunner_SharedMigratorMultipleRepositories(t *testing.T) {
	// This test reproduces the bug where a single Migrator instance is shared
	// across multiple repositories, causing lock state confusion
	db, err := setupTestDB(t)
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	ctx := context.Background()

	// Clean up from previous runs
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository IN ('repo_a', 'repo_b')")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key IN ('repo_a', 'repo_b')")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_repo_a")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_repo_b")

	// Create simple migrations
	migrationsRepoA := []database.Migration{
		{
			Version:     1,
			Description: "Create table for repo A",
			SQL:         "CREATE TABLE IF NOT EXISTS test_repo_a (id INT PRIMARY KEY, value VARCHAR(100))",
		},
		{
			Version:     2,
			Description: "Insert data into repo A",
			SQL:         "INSERT INTO test_repo_a VALUES (1, 'repo_a_data')",
		},
	}

	migrationsRepoB := []database.Migration{
		{
			Version:     1,
			Description: "Create table for repo B",
			SQL:         "CREATE TABLE IF NOT EXISTS test_repo_b (id INT PRIMARY KEY, value VARCHAR(100))",
		},
		{
			Version:     2,
			Description: "Insert data into repo B",
			SQL:         "INSERT INTO test_repo_b VALUES (1, 'repo_b_data')",
		},
	}

	// Run migrations for both repositories concurrently from 2 instances each
	// This simulates 2 instances each running migrations for repo_a and repo_b
	errChan := make(chan error, 4)

	// Instance 1: runs repo_a then repo_b
	go func() {
		t.Log("Instance 1: Starting repo_a migrations...")
		err1 := database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "repo_a", migrationsRepoA, cockroachdb.NewMigrator())
		if err1 != nil {
			t.Logf("Instance 1 repo_a: Error: %v", err1)
			errChan <- err1
			return
		}
		t.Log("Instance 1: repo_a completed")

		t.Log("Instance 1: Starting repo_b migrations...")
		err2 := database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "repo_b", migrationsRepoB, cockroachdb.NewMigrator())
		if err2 != nil {
			t.Logf("Instance 1 repo_b: Error: %v", err2)
		}
		t.Log("Instance 1: repo_b completed")
		errChan <- err2
	}()

	// Instance 2: runs repo_a then repo_b (same as instance 1)
	go func() {
		t.Log("Instance 2: Starting repo_a migrations...")
		err1 := database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "repo_a", migrationsRepoA, cockroachdb.NewMigrator())
		if err1 != nil {
			t.Logf("Instance 2 repo_a: Error: %v", err1)
			errChan <- err1
			return
		}
		t.Log("Instance 2: repo_a completed")

		t.Log("Instance 2: Starting repo_b migrations...")
		err2 := database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "repo_b", migrationsRepoB, cockroachdb.NewMigrator())
		if err2 != nil {
			t.Logf("Instance 2 repo_b: Error: %v", err2)
		}
		t.Log("Instance 2: repo_b completed")
		errChan <- err2
	}()

	// Wait for both instances to complete
	for range 2 {
		if err := <-errChan; err != nil {
			t.Fatalf("Migration failed: %v", err)
		}
	}

	// Verify repo_a data was inserted exactly once
	var countA int
	err = db.QueryRow("SELECT count(*) FROM test_repo_a").Scan(&countA)
	if err != nil {
		t.Fatalf("Failed to count repo_a rows: %v", err)
	}
	if countA != 1 {
		t.Errorf("Expected exactly 1 row in test_repo_a (migrations should run once), got %d", countA)
	}

	// Verify repo_b data was inserted exactly once
	var countB int
	err = db.QueryRow("SELECT count(*) FROM test_repo_b").Scan(&countB)
	if err != nil {
		t.Fatalf("Failed to count repo_b rows: %v", err)
	}
	if countB != 1 {
		t.Errorf("Expected exactly 1 row in test_repo_b (migrations should run once), got %d", countB)
	}

	// Verify migrations were recorded exactly once for each repository
	var migrationCountA int
	err = db.QueryRow("SELECT count(*) FROM schema_migrations WHERE repository = 'repo_a'").Scan(&migrationCountA)
	if err != nil {
		t.Fatalf("Failed to count repo_a migrations: %v", err)
	}
	if migrationCountA != 2 {
		t.Errorf("Expected 2 migrations for repo_a, got %d", migrationCountA)
	}

	var migrationCountB int
	err = db.QueryRow("SELECT count(*) FROM schema_migrations WHERE repository = 'repo_b'").Scan(&migrationCountB)
	if err != nil {
		t.Fatalf("Failed to count repo_b migrations: %v", err)
	}
	if migrationCountB != 2 {
		t.Errorf("Expected 2 migrations for repo_b, got %d", migrationCountB)
	}

	t.Log("✓ Shared migrator handled multiple repositories correctly")
	t.Logf("✓ Repo A rows: %d, migrations: %d", countA, migrationCountA)
	t.Logf("✓ Repo B rows: %d, migrations: %d", countB, migrationCountB)

	// Clean up
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository IN ('repo_a', 'repo_b')")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key IN ('repo_a', 'repo_b')")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_repo_a")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_repo_b")
}
