// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlutils

import (
	gosql "database/sql"
)

// QueryDatabaseID returns the database ID of the specified database using the
// system.namespace table.
func QueryDatabaseID(sqlDB *gosql.DB, dbName string) (uint32, error) {
	dbIDQuery := `SELECT id FROM system.public.namespace WHERE name = $1 AND "parentID" = 0`
	var dbID uint32
	result := sqlDB.QueryRow(dbIDQuery, dbName)
	if err := result.Scan(&dbID); err != nil {
		return 0, err
	}
	return dbID, nil
}

// QueryTableID returns the table ID of the specified database.table
// using the system.namespace table.
func QueryTableID(sqlDB *gosql.DB, dbName, tableName string) (uint32, error) {
	tableIDQuery := `
 SELECT tables.id FROM system.public.namespace tables
   JOIN system.public.namespace dbs ON dbs.id = tables."parentID"
   WHERE dbs.name = $1 AND tables.name = $2
 `
	var tableID uint32
	result := sqlDB.QueryRow(tableIDQuery, dbName, tableName)
	if err := result.Scan(&tableID); err != nil {
		return 0, err
	}
	return tableID, nil
}
