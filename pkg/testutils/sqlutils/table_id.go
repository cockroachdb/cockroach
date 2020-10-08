// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"context"
	"testing"
)

// QueryDatabaseID returns the database ID of the specified database using the
// system.namespace table.
func QueryDatabaseID(t testing.TB, sqlDB DBHandle, dbName string) uint32 {
	dbIDQuery := `
		SELECT id FROM system.namespace
		WHERE name = $1 AND "parentSchemaID" = 0 AND "parentID" = 0
	`
	var dbID uint32
	result := sqlDB.QueryRowContext(context.Background(), dbIDQuery, dbName)
	if err := result.Scan(&dbID); err != nil {
		t.Fatal(err)
	}
	return dbID
}

// QueryTableID returns the table ID of the specified database.table
// using the system.namespace table.
func QueryTableID(
	t testing.TB, sqlDB DBHandle, dbName, schemaName string, tableName string,
) uint32 {
	tableIDQuery := `
 SELECT tables.id FROM system.namespace tables
   JOIN system.namespace dbs ON dbs.id = tables."parentID"
	 JOIN system.namespace schemas ON schemas.id = tables."parentSchemaID"
   WHERE dbs.name = $1 AND schemas.name = $2 AND tables.name = $3
 `
	var tableID uint32
	result := sqlDB.QueryRowContext(
		context.Background(),
		tableIDQuery, dbName,
		schemaName,
		tableName,
	)
	if err := result.Scan(&tableID); err != nil {
		t.Fatal(err)
	}
	return tableID
}
