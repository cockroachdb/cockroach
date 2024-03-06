// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package operations

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func pickRandomDB(ctx context.Context, o operation.Operation, conn *gosql.DB) string {
	rng, _ := randutil.NewPseudoRand()

	// Pick a random table.
	dbs, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
		return ""
	}
	var dbNames []string
	for dbs.Next() {
		var dbName string
		if err := dbs.Scan(&dbName); err != nil {
			o.Fatal(err)
			return ""
		}
		if dbName == "system" || dbName == "information_schema" || dbName == "crdb_internal" || dbName == "defaultdb" || dbName == "postgres" {
			continue
		}
		dbNames = append(dbNames, dbName)
	}
	if len(dbNames) == 0 {
		o.Fatalf("unexpected zero active dbs found in cluster")
		return ""
	}
	return dbNames[rng.Intn(len(dbNames))]
}

func pickRandomTable(ctx context.Context, o operation.Operation, conn *gosql.DB, dbName string) string {
	rng, _ := randutil.NewPseudoRand()

	// Pick a random table.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		o.Fatal(err)
		return ""
	}

	tables, err := conn.QueryContext(ctx, "SELECT table_name FROM [SHOW TABLES]")
	if err != nil {
		o.Fatal(err)
		return ""
	}
	var tableNames []string
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			o.Fatal(err)
			return ""
		}
		tableNames = append(tableNames, tableName)
	}
	if len(tableNames) == 0 {
		o.Fatalf("unexpected zero active tables found in db %s", dbName)
		return ""
	}
	return tableNames[rng.Intn(len(tableNames))]
}
