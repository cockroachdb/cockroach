// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"
	crdbmigrator "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database/cockroachdb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	_ "github.com/lib/pq"
)

// TestTasksMigrations validates that the actual tasks repository migrations work correctly.
// This is an integration test using production migration code.
func TestTasksMigrations(t *testing.T) {
	db, err := database.NewConnection(context.Background(), database.ConnectionConfig{
		URL: "postgresql://root@localhost:26257/defaultdb?sslmode=disable",
	})
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	ctx := context.Background()

	// Clean up (remove migration tracking for this repository, not the whole table)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'tasks_test'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'tasks_test'")
	_, _ = db.Exec("DROP TABLE IF EXISTS tasks")

	// Run tasks migrations
	err = database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "tasks_test", GetTasksMigrations(), crdbmigrator.NewMigrator())
	if err != nil {
		t.Fatalf("Failed to run tasks migrations: %v", err)
	}

	// Verify tasks table exists with expected schema
	var exists bool
	err = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'tasks'
		)
	`).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check tasks table: %v", err)
	}
	if !exists {
		t.Fatal("Tasks table was not created")
	}

	// Verify consumer_id is VARCHAR (after migration 4)
	var dataType string
	err = db.QueryRow(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_name = 'tasks' AND column_name = 'consumer_id'
	`).Scan(&dataType)
	if err != nil {
		t.Fatalf("Failed to check consumer_id column type: %v", err)
	}
	if dataType != "character varying" {
		t.Errorf("Expected consumer_id to be VARCHAR, got %s", dataType)
	}

	// Verify concurrency_key column exists (added in migration 5)
	err = db.QueryRow(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_name = 'tasks' AND column_name = 'concurrency_key'
	`).Scan(&dataType)
	if err != nil {
		t.Fatalf("Failed to check concurrency_key column: %v", err)
	}
	if dataType != "character varying" {
		t.Errorf("Expected concurrency_key to be VARCHAR, got %s", dataType)
	}

	// Test idempotency - running again should not fail
	err = database.RunMigrationsForRepository(ctx, logger.DefaultLogger, db, "tasks_test", GetTasksMigrations(), crdbmigrator.NewMigrator())
	if err != nil {
		t.Fatalf("Failed to run migrations second time (idempotency test): %v", err)
	}

	// Verify correct number of migrations recorded
	var migrationCount int
	err = db.QueryRow(`
		SELECT count(*) FROM schema_migrations WHERE repository = 'tasks_test'
	`).Scan(&migrationCount)
	if err != nil {
		t.Fatalf("Failed to count migrations: %v", err)
	}
	expectedCount := len(GetTasksMigrations())
	if migrationCount != expectedCount {
		t.Errorf("Expected %d migrations recorded, got %d", expectedCount, migrationCount)
	}

	// Clean up (remove migration tracking for this repository)
	_, _ = db.Exec("DELETE FROM schema_migrations WHERE repository = 'tasks_test'")
	_, _ = db.Exec("DELETE FROM migration_locks WHERE lock_key = 'tasks_test'")
	_, _ = db.Exec("DROP TABLE IF EXISTS tasks")
}
