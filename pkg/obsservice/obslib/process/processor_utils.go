// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package process

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func OpenDBSync(ctx context.Context, sinkPGURL string) (*gosql.DB, error) {
	defaultSinkDBName := "obsservice"
	connCfg, err := pgxpool.ParseConfig(sinkPGURL)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid --sink-pgurl (%s)", sinkPGURL)
	}
	if connCfg.ConnConfig.Database != defaultSinkDBName {
		if connCfg.ConnConfig.Database != "" {
			log.Warningf(ctx,
				"--sink-pgurl string contains a database name (%s) other than 'obsservice' - overriding",
				connCfg.ConnConfig.Database)
		}
		// We don't want to accidentally write things to the wrong DB in the event that
		// one is accidentally provided in the --sink-pgurl (as is common with defaultdb).
		// Always override to defaultSinkDBName.
		connCfg.ConnConfig.Database = defaultSinkDBName
	}

	return stdlib.OpenDB(*connCfg.ConnConfig), nil
}
