// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
)

func runSQLInitNonCRDB(ctx context.Context, gen workload.Generator, urls []string) error {
	lc := strings.ToLower(*dataLoader)
	if lc == "auto" {
		lc = "insert"
	}
	if lc != "insert" {
		return errors.Errorf("data loader %q is not supported for dialect %q", *dataLoader, *dialect)
	}

	initDB, err := gosql.Open(`pgx`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	defer initDB.Close()
	initDB.SetMaxOpenConns(*initConns)
	initDB.SetMaxIdleConns(*initConns)

	if err := dropSQLTables(ctx, initDB, gen); err != nil {
		return err
	}

	l := workloadsql.InsertsDataLoader{
		BatchSize:      0,
		Concurrency:    *initConns,
		DisableCRDBDDL: true,
	}
	if _, err := l.InitialDataLoad(ctx, initDB, gen); err != nil {
		return err
	}

	if h, ok := gen.(workload.Hookser); ok {
		if h.Hooks().PostLoad != nil {
			if err := h.Hooks().PostLoad(ctx, initDB); err != nil {
				return errors.Wrapf(err, "could not postload")
			}
		}
	}
	return nil
}

func dropSQLTables(ctx context.Context, db *gosql.DB, gen workload.Generator) error {
	if !*drop {
		return nil
	}
	for _, table := range gen.Tables() {
		if table.ObjectPrefix != nil && table.ObjectPrefix.ExplicitCatalog {
			return errors.Errorf("drop not supported for multi-database table %q", table.Name)
		}
		tableName := table.GetResolvedName()
		stmt := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName.String())
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return errors.Wrapf(err, "dropping table %q", table.Name)
		}
	}
	return nil
}
