// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package migrations

import (
	"context"
	"embed"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose/v3"
)

// sqlMigrations embeds all the .sql file containing migrations to be run by
// Goose.
//go:embed sqlmigrations/*.sql
var sqlMigrations embed.FS

// defaultSinkDBName is the name of the database to be used by default
const defaultSinkDBName = "obsservice"

// RunDBMigrations brings the SQL schema in the sink cluster up to date.
//
// sinkPGURL is the connection string for the sink cluster. If it includes a
// database, that database will be used. If it doesn't, a default one will be
// used.
func RunDBMigrations(ctx context.Context, sinkPGURL string) error {
	connCfg, err := pgx.ParseConfig(sinkPGURL)
	if err != nil {
		return err
	}
	if connCfg.Database == "" {
		fmt.Printf("No database explicitly provided in --sink-pgurl. Using %q.\n", defaultSinkDBName)
		connCfg.Database = defaultSinkDBName
	}

	if log.V(2) {
		goose.SetVerbose(true)
	}
	goose.SetBaseFS(sqlMigrations)

	db := stdlib.OpenDB(*connCfg)
	// We need to create the database by hand; Goose expects the database to exist.
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+connCfg.Database); err != nil {
		return err
	}
	// goose will <db>.obs_admin.migrations to store the migration bookkeeping.
	if _, err := db.ExecContext(ctx, "CREATE schema IF NOT EXISTS obs_admin"); err != nil {
		return err
	}
	goose.SetTableName("obs_admin.migrations")
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	// Run the missing migrations, if any.
	if err := goose.Up(db, "sqlmigrations"); err != nil {
		return err
	}
	return nil
}
