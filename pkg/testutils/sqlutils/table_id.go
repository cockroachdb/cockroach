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
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package sqlutils

import (
	gosql "database/sql"
	"testing"
)

// QueryTableID returns the table ID of the specified database.table
// using the system.namespace table.
func QueryTableID(t *testing.T, sqlDB *gosql.DB, dbName, tableName string) uint32 {
	tableIDQuery := `
 SELECT tables.id FROM system.namespace tables
   JOIN system.namespace dbs ON dbs.id = tables.parentid
   WHERE dbs.name = $1 AND tables.name = $2
 `
	var tableID uint32
	result := sqlDB.QueryRow(tableIDQuery, dbName, tableName)
	if err := result.Scan(&tableID); err != nil {
		t.Fatal(err)
	}
	return tableID
}
