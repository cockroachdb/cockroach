// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Migrator implements database-specific migration operations for CockroachDB.
// Uses INSERT-based locking to ensure only one instance can run migrations
// for a given repository at a time.
//
// The locking mechanism works as follows:
// 1. Before acquiring, clean up stale locks (>2 minutes old) from crashed instances
// 2. Attempt to INSERT a lock row with ON CONFLICT DO NOTHING
// 3. Check RowsAffected: 1 = lock acquired, 0 = another instance holds it
// 4. Lock is held for the duration of the migration transaction
// 5. Lock is released by DELETing the row after COMMIT
//
// Instances that can't acquire the lock wait and check if migrations are complete.
// This prevents timeouts when migrations take longer than expected.
//
// Migrator instances track the repository they're currently locking.
type Migrator struct {
	currentRepository string
}

// NewMigrator creates a new CockroachDB migrator.
func NewMigrator() *Migrator {
	return &Migrator{}
}

// InitializeSchema creates the schema_migrations and migration_locks tables.
func (m *Migrator) InitializeSchema(
	ctx context.Context, conn *gosql.Conn, repository string,
) error {
	// Create schema_migrations table
	_, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			repository STRING NOT NULL,
			version INT NOT NULL,
			description STRING NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (repository, version)
		)
	`)
	if err != nil {
		return errors.Wrap(err, "failed to create schema_migrations table")
	}

	// Create migration_locks table with a generation counter for optimistic locking
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS migration_locks (
			lock_key STRING PRIMARY KEY,
			generation INT NOT NULL DEFAULT 0,
			acquired_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return errors.Wrap(err, "failed to create migration_locks table")
	}

	// Ensure lock row exists for this repository
	_, err = conn.ExecContext(ctx,
		`INSERT INTO migration_locks (lock_key, generation) VALUES ($1, 0)
		 ON CONFLICT (lock_key) DO NOTHING`,
		repository)
	if err != nil {
		return errors.Wrap(err, "failed to create lock row")
	}

	return nil
}

// AcquireLock attempts to acquire a distributed lock.
// Uses INSERT with ON CONFLICT to atomically detect if another instance holds the lock.
// Also handles stale lock cleanup - locks older than 2 minutes are considered from crashed instances.
func (m *Migrator) AcquireLock(
	ctx context.Context, conn *gosql.Conn, repository string,
) (bool, error) {
	// First, clean up any stale locks (older than 2 minutes)
	// This handles cases where an instance crashed while holding the lock
	staleLockThreshold := 2 * time.Minute
	_, err := conn.ExecContext(ctx,
		`DELETE FROM migration_locks
		 WHERE lock_key = $1 AND acquired_at < now() - $2::INTERVAL`,
		repository+"_active",
		fmt.Sprintf("%d seconds", int(staleLockThreshold.Seconds())))
	if err != nil {
		return false, errors.Wrap(err, "failed to clean up stale locks")
	}

	// Generate a unique instance ID for this lock holder
	instanceID := fmt.Sprintf("%d", timeutil.Now().UnixNano())

	// Try to INSERT a lock record
	// If another instance already has the lock, ON CONFLICT DO NOTHING will affect 0 rows
	result, err := conn.ExecContext(ctx,
		`INSERT INTO migration_locks (lock_key, generation, acquired_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (lock_key) DO NOTHING`,
		repository+"_active", instanceID)

	if err != nil {
		return false, errors.Wrap(err, "failed to insert lock")
	}

	// Check if we successfully inserted (acquired the lock)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "failed to check rows affected")
	}

	if rowsAffected == 0 {
		// Another instance already holds the lock (ON CONFLICT DO NOTHING didn't insert)
		return false, nil
	}

	// Successfully acquired the lock
	// Now start a transaction for the migrations
	_, err = conn.ExecContext(ctx, "BEGIN")
	if err != nil {
		// Clean up the lock
		_, _ = conn.ExecContext(ctx, `DELETE FROM migration_locks WHERE lock_key = $1`, repository+"_active")
		return false, errors.Wrap(err, "failed to begin transaction")
	}

	// Store the repository for ReleaseLock
	m.currentRepository = repository

	return true, nil
}

// ReleaseLock releases the distributed lock by committing the transaction and deleting the lock row.
// This method should only be called after successfully acquiring the lock.
func (m *Migrator) ReleaseLock(ctx context.Context, conn *gosql.Conn) error {
	// Commit the migration transaction first
	_, err := conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	// Delete the lock row to release the lock
	_, err = conn.ExecContext(ctx, `DELETE FROM migration_locks WHERE lock_key = $1`, m.currentRepository+"_active")
	if err != nil {
		return errors.Wrap(err, "failed to delete lock row")
	}

	// Clear the current repository
	m.currentRepository = ""
	return nil
}

// GetCompletedVersions returns a map of completed migration versions.
func (m *Migrator) GetCompletedVersions(
	ctx context.Context, conn *gosql.Conn, repository string,
) (map[int]bool, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT version FROM schema_migrations WHERE repository = $1",
		repository)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query completed migrations")
	}
	defer rows.Close()

	completed := make(map[int]bool)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, errors.Wrap(err, "failed to scan migration version")
		}
		completed[version] = true
	}

	return completed, errors.Wrap(rows.Err(), "error iterating migration versions")
}

// ExecuteMigration runs a single migration and records its completion atomically.
// Uses savepoints to ensure atomicity within the outer lock transaction.
func (m *Migrator) ExecuteMigration(
	ctx context.Context,
	l *logger.Logger,
	conn *gosql.Conn,
	repository string,
	migration database.Migration,
) error {
	// Use savepoint for atomic migration execution within the lock transaction
	savepointName := fmt.Sprintf("migration_%d", migration.Version)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", savepointName))
	if err != nil {
		return errors.Wrapf(err, "failed to create savepoint for migration %d", migration.Version)
	}

	// Track if we need to rollback to savepoint
	released := false
	defer func() {
		if !released {
			// Use background context to ensure rollback always works,
			// even if migration times out or context is cancelled.
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer rollbackCancel()

			if _, err := conn.ExecContext(rollbackCtx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName)); err != nil {
				// Log error but don't fail - we're already in an error path
				l.Error(
					"failed to rollback savepoint for migration",
					slog.String("savepoint", savepointName),
					slog.Int("version", migration.Version),
					slog.Any("error", err),
				)
			}
		}
	}()

	// Execute migration SQL
	_, err = conn.ExecContext(ctx, migration.SQL)
	if err != nil {
		return errors.Wrapf(err, "migration SQL execution failed for version %d", migration.Version)
	}

	// Record migration completion
	_, err = conn.ExecContext(ctx,
		`INSERT INTO schema_migrations (repository, version, description, applied_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (repository, version) DO NOTHING`,
		repository, migration.Version, migration.Description, timeutil.Now())
	if err != nil {
		return errors.Wrapf(err, "failed to record migration %d completion", migration.Version)
	}

	// Release savepoint (commits the migration work)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
	if err != nil {
		return errors.Wrapf(err, "failed to release savepoint for migration %d", migration.Version)
	}

	released = true
	return nil
}
