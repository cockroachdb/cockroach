// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sqlutils

import (
	"context"
	"testing"
)

// QueryDatabaseID returns the database ID of the specified database using the
// system.namespace table.
func QueryDatabaseID(t testing.TB, sqlDB DBHandle, dbName string) uint32 {
	dbIDQuery := `SELECT id FROM system.namespace WHERE name = $1 AND "parentID" = 0`
	var dbID uint32
	result := sqlDB.QueryRowContext(context.Background(), dbIDQuery, dbName)
	if err := result.Scan(&dbID); err != nil {
		t.Fatal(err)
	}
	return dbID
}

// QueryTableID returns the table ID of the specified database.table
// using the system.namespace table.
func QueryTableID(t testing.TB, sqlDB DBHandle, dbName, tableName string) uint32 {
	tableIDQuery := `
 SELECT tables.id FROM system.namespace tables
   JOIN system.namespace dbs ON dbs.id = tables."parentID"
   WHERE dbs.name = $1 AND tables.name = $2
 `
	var tableID uint32
	result := sqlDB.QueryRowContext(context.Background(), tableIDQuery, dbName, tableName)
	if err := result.Scan(&tableID); err != nil {
		t.Fatal(err)
	}
	return tableID
}
