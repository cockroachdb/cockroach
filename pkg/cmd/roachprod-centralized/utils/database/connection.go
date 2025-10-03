// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package database

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
)

// ConnectionConfig holds database connection configuration.
type ConnectionConfig struct {
	URL         string
	MaxConns    int
	MaxIdleTime int // seconds
}

// NewConnection creates and configures a new database connection.
func NewConnection(ctx context.Context, config ConnectionConfig) (*gosql.DB, error) {
	if config.URL == "" {
		return nil, fmt.Errorf("database URL is required")
	}

	db, err := gosql.Open("postgres", config.URL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database connection")
	}

	// Configure connection pool
	if config.MaxConns > 0 {
		db.SetMaxOpenConns(config.MaxConns)
		db.SetMaxIdleConns(config.MaxConns / 2)
	} else {
		// Default connection pool settings
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(10)
	}

	if config.MaxIdleTime > 0 {
		db.SetConnMaxIdleTime(time.Duration(config.MaxIdleTime) * time.Second)
	} else {
		// Default to 5 minutes
		db.SetConnMaxIdleTime(5 * time.Minute)
	}

	// Set connection lifetime to prevent stale connections
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "failed to connect to database")
	}

	return db, nil
}

// RunMigrationsForRepository runs migrations for a specific repository.
// The migrator parameter must be provided (e.g., cockroachdb.NewMigrator()).
func RunMigrationsForRepository(
	ctx context.Context,
	l *logger.Logger,
	db *gosql.DB,
	repositoryName string,
	migrations []Migration,
	migrator Migrator,
) error {
	if migrator == nil {
		return errors.New("migrator cannot be nil")
	}

	runner := NewMigrationRunner(db, repositoryName, migrations, migrator)
	if err := runner.RunMigrations(ctx, l); err != nil {
		return errors.Wrapf(err, "failed to run %s repository migrations", repositoryName)
	}
	return nil
}
