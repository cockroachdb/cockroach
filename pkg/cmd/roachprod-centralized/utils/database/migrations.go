// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package database

import (
	"context"
	gosql "database/sql"
	"log/slog"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// Migration represents a single database migration.
type Migration struct {
	Version     int    // Unique version number (e.g., 1, 2, 3...)
	Description string // Human-readable description
	SQL         string // SQL to execute
}

// MigrationRunner handles database migrations with distributed locking.
// Uses a Migrator implementation for database-specific operations.
type MigrationRunner struct {
	db         *gosql.DB
	repository string
	migrations []Migration
	migrator   Migrator
}

// NewMigrationRunner creates a new migration runner.
func NewMigrationRunner(
	db *gosql.DB, repository string, migrations []Migration, migrator Migrator,
) *MigrationRunner {
	return &MigrationRunner{
		db:         db,
		repository: repository,
		migrations: migrations,
		migrator:   migrator,
	}
}

// RunMigrations executes all pending migrations with distributed locking.
// If another instance is running migrations, waits for it to complete.
func (mr *MigrationRunner) RunMigrations(ctx context.Context, l *logger.Logger) error {
	retryDelay := 5 * time.Second

	for {
		acquired, err := mr.tryRunWithLock(ctx, l)
		if err != nil {
			return err
		}

		if acquired {
			return nil
		}

		// Another instance is running migrations
		// Check if there are any pending migrations left
		conn, err := mr.db.Conn(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get database connection")
		}

		hasPending, err := mr.hasPendingMigrations(ctx, conn)
		_ = conn.Close()
		if err != nil {
			return errors.Wrap(err, "failed to check pending migrations")
		}

		if !hasPending {
			// No pending migrations, the other instance finished
			l.Info("migrations completed by another instance", slog.String("repository", mr.repository))
			return nil
		}

		// Still have pending migrations, wait and retry
		l.Debug(
			"waiting for another instance to complete migrations",
			slog.String("repository", mr.repository),
		)
		time.Sleep(retryDelay)
	}
}

// tryRunWithLock attempts to acquire the lock and run migrations.
func (mr *MigrationRunner) tryRunWithLock(ctx context.Context, l *logger.Logger) (bool, error) {
	conn, err := mr.db.Conn(ctx)
	if err != nil {
		return false, errors.Wrap(err, "failed to get database connection")
	}
	defer func() {
		_ = conn.Close()
	}()

	// Initialize schema (creates tables if they don't exist)
	if err := mr.migrator.InitializeSchema(ctx, conn, mr.repository); err != nil {
		return false, errors.Wrap(err, "failed to initialize schema")
	}

	l.Debug("attempting to acquire migration lock", slog.String("repository", mr.repository))

	// Try to acquire the lock
	acquired, err := mr.migrator.AcquireLock(ctx, conn, mr.repository)
	if err != nil {
		return false, errors.Wrap(err, "failed to acquire migration lock")
	}

	if !acquired {
		l.Debug("failed to acquire lock, another instance holds it", slog.String("repository", mr.repository))
		return false, nil
	}

	l.Debug("acquired migration lock, starting migrations", slog.String("repository", mr.repository))

	// Run migrations while holding the lock
	migrationErr := mr.runMigrations(ctx, l, conn)

	l.Debug("releasing migration lock", slog.String("repository", mr.repository))

	// Always release the lock
	if err := mr.migrator.ReleaseLock(ctx, conn); err != nil {
		l.Warn("failed to release migration lock",
			slog.String("repository", mr.repository),
			slog.Any("error", err))
	}

	l.Debug("released migration lock", slog.String("repository", mr.repository))

	return true, migrationErr
}

// hasPendingMigrations checks if there are any migrations that haven't been applied yet.
func (mr *MigrationRunner) hasPendingMigrations(
	ctx context.Context, conn *gosql.Conn,
) (bool, error) {
	// Initialize schema first to ensure tables exist
	if err := mr.migrator.InitializeSchema(ctx, conn, mr.repository); err != nil {
		return false, errors.Wrap(err, "failed to initialize schema")
	}

	completed, err := mr.migrator.GetCompletedVersions(ctx, conn, mr.repository)
	if err != nil {
		return false, errors.Wrap(err, "failed to get completed migrations")
	}

	for _, migration := range mr.migrations {
		if !completed[migration.Version] {
			return true, nil
		}
	}

	return false, nil
}

// runMigrations executes all pending migrations.
func (mr *MigrationRunner) runMigrations(
	ctx context.Context, l *logger.Logger, conn *gosql.Conn,
) error {
	// Get completed migrations
	completed, err := mr.migrator.GetCompletedVersions(ctx, conn, mr.repository)
	if err != nil {
		return errors.Wrap(err, "failed to get completed migrations")
	}

	// Sort migrations by version
	sort.Slice(mr.migrations, func(i, j int) bool {
		return mr.migrations[i].Version < mr.migrations[j].Version
	})

	// Count pending migrations
	pendingCount := 0
	for _, migration := range mr.migrations {
		if !completed[migration.Version] {
			pendingCount++
		}
	}

	if pendingCount == 0 {
		l.Info("no pending migrations to run",
			slog.String("repository", mr.repository))
		return nil
	}

	l.Info("running pending migrations",
		slog.String("repository", mr.repository),
		slog.Int("pending_count", pendingCount))

	// Execute pending migrations
	for _, migration := range mr.migrations {
		if completed[migration.Version] {
			continue
		}

		l.Info("running migration",
			slog.String("repository", mr.repository),
			slog.Int("version", migration.Version),
			slog.String("description", migration.Description))

		if err := mr.migrator.ExecuteMigration(ctx, l, conn, mr.repository, migration); err != nil {
			return errors.Wrapf(err, "failed to run migration %d (%s)",
				migration.Version, migration.Description)
		}

		l.Info("migration completed successfully",
			slog.String("repository", mr.repository),
			slog.Int("version", migration.Version))
	}

	l.Info("all migrations completed successfully",
		slog.String("repository", mr.repository),
		slog.Int("completed_count", pendingCount))

	return nil
}
