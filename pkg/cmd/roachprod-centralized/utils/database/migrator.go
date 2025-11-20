// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package database

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// Migrator defines the interface for database-specific migration operations.
// Different database backends (CockroachDB, PostgreSQL, SQLite) can provide
// their own implementations with backend-specific locking and migration strategies.
type Migrator interface {
	// InitializeSchema creates the necessary tables for migration tracking and locking.
	// Should be idempotent (safe to call multiple times).
	InitializeSchema(ctx context.Context, conn *gosql.Conn, repository string) error

	// AcquireLock attempts to acquire a distributed lock for running migrations.
	// Returns true if lock was acquired, false if another instance holds it.
	// The lock should remain held until ReleaseLock is called.
	AcquireLock(ctx context.Context, conn *gosql.Conn, repository string) (bool, error)

	// ReleaseLock releases the distributed lock.
	ReleaseLock(ctx context.Context, conn *gosql.Conn) error

	// GetCompletedVersions returns a set of migration versions that have been completed.
	GetCompletedVersions(ctx context.Context, conn *gosql.Conn, repository string) (map[int]bool, error)

	// ExecuteMigration runs a single migration and records its completion atomically.
	// Should be called while holding the lock.
	ExecuteMigration(ctx context.Context, l *logger.Logger, conn *gosql.Conn, repository string, migration Migration) error
}
